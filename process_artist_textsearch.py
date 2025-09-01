#!/usr/bin/env python3
import asyncio
import re
import time
import unicodedata
import urllib.parse
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import aiohttp

from storage import iso_now
from colors import Colors


def process_artist_name_for_text_search(
    artist_name: str, 
    convert_to_lowercase: bool = False, 
    remove_symbols_and_diacritics: bool = False
) -> str:
    """
    Process artist name for text search based on configuration options.
    
    Args:
        artist_name: Original artist name from database/API
        convert_to_lowercase: Whether to convert to lowercase
        remove_symbols_and_diacritics: Whether to remove symbols and diacritics
        
    Returns:
        Processed artist name for text search
        
    Examples:
        >>> process_artist_name_for_text_search("Café Tacvba", False, True)
        'Cafe Tacvba'
        >>> process_artist_name_for_text_search("Metallica", True, False)
        'metallica'
        >>> process_artist_name_for_text_search("Sigur Rós", True, True)
        'sigur ros'
    """
    if not artist_name or not artist_name.strip():
        return artist_name
        
    processed_name = artist_name.strip()

    if remove_symbols_and_diacritics:
        # Remove diacritics (accents, umlauts, etc.)
        processed_name = unicodedata.normalize('NFKD', processed_name)
        processed_name = ''.join(c for c in processed_name if not unicodedata.combining(c))

        # Replace common word separators with spaces
        # Include various types of dashes and separators
        processed_name = re.sub(r'[-_.\/\u2013\u2014\u2010\u2011]+', ' ', processed_name)

        # Remove remaining symbols but keep alphanumeric and spaces
        # Use explicit character class to avoid \w underscore inclusion issue
        processed_name = re.sub(r'[^a-zA-Z0-9\s]', '', processed_name)

        # Clean up multiple spaces and trim
        processed_name = re.sub(r'\s+', ' ', processed_name).strip()

    if convert_to_lowercase:
        processed_name = processed_name.lower()

    return processed_name


class SafeRateLimiter:
    """Production-safe rate limiter with circuit breaker and backoff"""
    
    def __init__(
        self,
        requests_per_second: float = 3.0,
        max_concurrent: int = 5,
        circuit_breaker_threshold: int = 25,
        backoff_factor: float = 0.5,
        max_backoff_seconds: float = 30.0
    ):
        self.base_rate = requests_per_second
        self.current_rate = requests_per_second
        self.max_concurrent = max_concurrent
        
        # Rate limiting
        self.request_times = deque()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Circuit breaker
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.consecutive_failures = 0
        self.last_failure_time = 0
        self.backoff_factor = backoff_factor
        self.max_backoff_seconds = max_backoff_seconds
        
        # Statistics
        self.total_requests = 0
        self.total_successes = 0
        self.total_rate_limits = 0
        self.total_errors = 0
        self.circuit_breaker_trips = 0
    
    async def acquire(self) -> bool:
        """Acquire permission to make a request. Returns False if circuit breaker is open."""
        if self._is_circuit_breaker_open():
            return False
        
        await self.semaphore.acquire()
        
        try:
            await self._rate_limit()
            self.total_requests += 1
            return True
        except Exception:
            self.semaphore.release()
            raise
    
    def release(self, status_code: int, response_time_seconds: float):
        """Release the semaphore and record the result"""
        self.semaphore.release()
        
        if status_code == 200:
            self.total_successes += 1
            self.consecutive_failures = 0
            # Gradually restore rate after success
            if self.current_rate < self.base_rate:
                self.current_rate = min(self.current_rate * 1.05, self.base_rate)
                
        elif status_code == 429:  # Rate limited - this is bad, reduce rate
            self.total_rate_limits += 1
            self.consecutive_failures += 1
            self.last_failure_time = time.time()
            self.current_rate *= 0.5
            print(f"⚠️  Rate limited! Reducing rate to {self.current_rate:.2f} req/sec")
            
        elif status_code in (0, "TIMEOUT") or str(status_code).startswith("EXC:"):  # Connection issues
            self.total_errors += 1
            self.consecutive_failures += 1
            self.last_failure_time = time.time()
            self.current_rate *= 0.8
            print(f"⚠️  Connection error {status_code}! Reducing rate to {self.current_rate:.2f} req/sec")
            
        # For text search: 503, 404, and other HTTP errors are mostly EXPECTED
        # Don't reduce rate aggressively for these - they're part of normal search cache warming
        else:
            self.consecutive_failures = 0  # Reset failures for expected responses
    
    async def _rate_limit(self):
        """Implement token bucket rate limiting"""
        now = time.time()
        
        # Remove old request timestamps
        while self.request_times and now - self.request_times[0] > 1.0:
            self.request_times.popleft()
        
        # Check if we're at the rate limit
        if len(self.request_times) >= self.current_rate:
            oldest_request = self.request_times[0]
            wait_time = 1.0 - (now - oldest_request)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                now = time.time()
                while self.request_times and now - self.request_times[0] > 1.0:
                    self.request_times.popleft()
        
        self.request_times.append(now)
    
    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker should prevent requests"""
        if self.consecutive_failures < self.circuit_breaker_threshold:
            return False
        
        time_since_failure = time.time() - self.last_failure_time
        backoff_time = min(
            self.backoff_factor ** (self.consecutive_failures - self.circuit_breaker_threshold),
            self.max_backoff_seconds
        )
        
        if time_since_failure < backoff_time:
            self.circuit_breaker_trips += 1
            return True
        
        # Try to reset circuit breaker
        self.consecutive_failures = max(0, self.consecutive_failures - 1)
        return False
    
    def get_stats(self) -> dict:
        """Get current statistics"""
        success_rate = (self.total_successes / self.total_requests) if self.total_requests > 0 else 0
        
        return {
            "total_requests": self.total_requests,
            "success_rate": f"{success_rate:.1%}",
            "rate_limits_hit": self.total_rate_limits,
            "server_errors": self.total_errors,
            "current_rate": f"{self.current_rate:.2f} req/sec",
            "circuit_breaker_failures": self.consecutive_failures,
            "circuit_breaker_trips": self.circuit_breaker_trips,
            "circuit_breaker_open": self._is_circuit_breaker_open()
        }


async def check_text_search_with_cache_warming(
    session: aiohttp.ClientSession,
    artist_name: str,
    target_base_url: str,
    max_attempts: int = 25,
    delay_between_attempts: float = 0.5,
    timeout: int = 10,
    convert_to_lowercase: bool = False,
    remove_symbols_and_diacritics: bool = False
) -> Tuple[str, str, int, float]:
    """
    Perform text search with cache warming - keep trying until success or max attempts
    Returns: (status, last_code, attempts_used, total_response_time)
    """
    search_url = f"{target_base_url.rstrip('/')}/search"
    total_response_time = 0
    
    # Process artist name based on configuration options
    processed_name = process_artist_name_for_text_search(
        artist_name, convert_to_lowercase, remove_symbols_and_diacritics
    )
    
    # URL encode the processed artist name for the query
    query = urllib.parse.quote_plus(processed_name.strip())
    search_params = {"type": "all", "query": query}
    
    for attempt in range(max_attempts):
        start_time = time.time()
        try:
            async with session.get(search_url, params=search_params) as resp:
                response_time = time.time() - start_time
                total_response_time += response_time
                status_code = resp.status
                
                if status_code == 200:
                    # SUCCESS! Text search cache warmed
                    return "success", str(status_code), attempt + 1, total_response_time
                
                # For text search warming, retry non-200 responses (similar to MBID warming)
                # 503, 404, etc. may indicate cache is still building
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            total_response_time += response_time
            status_code = "TIMEOUT"
        except Exception as e:
            response_time = time.time() - start_time
            total_response_time += response_time
            status_code = f"EXC:{type(e).__name__}"
        
        # Wait between attempts (unless it's the last attempt)
        if attempt < max_attempts - 1:
            await asyncio.sleep(delay_between_attempts)
    
    # Exhausted all attempts without success
    return "timeout", str(status_code), max_attempts, total_response_time


async def check_text_searches_concurrent_with_timing(
    to_check: List[str],
    ledger: Dict[str, Dict],
    cfg: dict,
    storage,
    overall_start_time: float,
    offset: int
) -> Tuple[int, int]:
    """Check text searches for artist names concurrently with proper timing across batches"""
    
    rate_limiter = SafeRateLimiter(
        requests_per_second=cfg["rate_limit_per_second"],
        max_concurrent=cfg["max_concurrent_requests"],
        circuit_breaker_threshold=cfg["circuit_breaker_threshold"],
        backoff_factor=cfg["backoff_factor"],
        max_backoff_seconds=cfg["max_backoff_seconds"]
    )
    
    new_successes = 0
    new_attempts = 0
    timeout_obj = aiohttp.ClientTimeout(total=cfg["timeout_seconds"])
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    # Track progress stats for this batch
    batch_successes = 0
    batch_attempts = 0
    
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        for i, mbid in enumerate(to_check):
            # Check circuit breaker
            if not await rate_limiter.acquire():
                circuit_text = Colors.error(f"🚫 Circuit breaker open, skipping remaining {len(to_check) - i} text searches", colored_output)
                print(circuit_text)
                break
            
            name = ledger[mbid].get("artist_name", "Unknown")
            
            # Use offset for proper numbering across batches
            global_position = offset + i + 1
            total_to_process = offset + len(to_check)
            
            # Process the name to show what we're actually searching for
            processed_name = process_artist_name_for_text_search(
                name, 
                cfg.get("artist_textsearch_lowercase", False), 
                cfg.get("artist_textsearch_remove_symbols", False)
            )
            
            # Better output format (original -> processed) only if they differ
            if processed_name != name:
                print(f"[{global_position}/{total_to_process}] Text search: '{name}' -> '{processed_name}' ...", end="", flush=True)
            else:
                print(f"[{global_position}/{total_to_process}] Text search for '{name}' ...", end="", flush=True)
            
            try:
                status, last_code, attempts_used, response_time = await check_text_search_with_cache_warming(
                    session,
                    name,
                    cfg["target_base_url"],
                    cfg["max_attempts_per_artist_textsearch"],
                    cfg["delay_between_attempts"],
                    cfg["timeout_seconds"],
                    cfg.get("artist_textsearch_lowercase", False),
                    cfg.get("artist_textsearch_remove_symbols", False)
                )
                
                rate_limiter.release(int(last_code) if last_code.isdigit() else last_code, response_time)
                
                # Update ledger with text search results
                ledger[mbid].update({
                    "text_search_attempted": True,
                    "text_search_success": (status == "success"),
                    "text_search_last_checked": iso_now()
                })
                
                # Count results and display with colors
                new_attempts += 1
                batch_attempts += 1
                
                if status == "success":
                    new_successes += 1
                    batch_successes += 1
                    success_text = Colors.success("SUCCESS", colored_output)
                    print(f" {success_text} (code={last_code}, attempts={attempts_used})")
                else:
                    timeout_text = Colors.error("TIMEOUT", colored_output)
                    print(f" {timeout_text} (code={last_code}, attempts={attempts_used})")
                
            except Exception as e:
                response_time = 1.0  # Estimate for failed requests
                rate_limiter.release("EXC", response_time)
                
                ledger[mbid].update({
                    "text_search_attempted": True,
                    "text_search_success": False,
                    "text_search_last_checked": iso_now()
                })
                
                new_attempts += 1
                batch_attempts += 1
                timeout_text = Colors.error("TIMEOUT", colored_output)
                print(f" {timeout_text} (code=EXC:{type(e).__name__}, attempts={cfg['max_attempts_per_artist_textsearch']})")
            
            # Batch writing
            if global_position % cfg.get("batch_write_frequency", 5) == 0:
                storage.write_artists_ledger(ledger)
            
            # Progress reporting with batch stats
            if global_position % cfg.get("log_progress_every_n", 25) == 0:
                elapsed_time = time.time() - overall_start_time
                searches_per_sec = global_position / max(elapsed_time, 0.1)
                remaining_searches = total_to_process - global_position
                eta_seconds = remaining_searches / max(searches_per_sec, 0.01)
                
                # Calculate ETC (Estimated Time to Completion)
                etc_timestamp = datetime.now() + timedelta(seconds=eta_seconds)
                etc_str = etc_timestamp.strftime("%H:%M")
                
                stats = rate_limiter.get_stats()
                
                # Color the batch success rate
                if batch_attempts > 0:
                    success_rate_text = f"{batch_successes}/{batch_attempts}"
                    if batch_successes == batch_attempts:
                        success_rate_text = Colors.success(success_rate_text, colored_output)
                    elif batch_successes == 0:
                        success_rate_text = Colors.error(success_rate_text, colored_output)
                    else:
                        success_rate_text = Colors.warning(success_rate_text, colored_output)
                else:
                    success_rate_text = "0/0"
                
                print(f"Progress: {global_position}/{total_to_process} ({(global_position/total_to_process*100):.1f}%) - "
                      f"Rate: {searches_per_sec:.1f} searches/sec - ETC: {etc_str} - "
                      f"API: {stats.get('current_rate', 'N/A')} - Batch: {success_rate_text} success")
    
    return new_successes, new_attempts


def process_text_searches_in_batches(
    to_check: List[str], 
    ledger: Dict[str, Dict],
    cfg: dict,
    storage
) -> Tuple[int, int]:
    """Process text searches in batches. Returns (total_new_successes, total_new_attempts)"""
    batch_size = cfg.get("batch_size", 25)
    total_batches = (len(to_check) + batch_size - 1) // batch_size
    total_new_successes = 0
    total_new_attempts = 0
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    # Track timing across all batches
    overall_start_time = time.time()
    total_processed = 0
    
    for batch_idx in range(0, len(to_check), batch_size):
        batch_num = batch_idx // batch_size + 1
        batch = to_check[batch_idx:batch_idx + batch_size]
        
        batch_header = Colors.info(f"=== Text Search Batch {batch_num}/{total_batches} ({len(batch)} artists) ===", colored_output)
        print(batch_header)
        
        batch_successes, batch_attempts = asyncio.run(
            check_text_searches_concurrent_with_timing(batch, ledger, cfg, storage, overall_start_time, total_processed)
        )
        
        total_new_successes += batch_successes
        total_new_attempts += batch_attempts
        total_processed += len(batch)
        
        # Write after each batch
        storage.write_artists_ledger(ledger)
        complete_text = Colors.success(f"Text search batch {batch_num} complete. Ledger updated.", colored_output)
        print(complete_text)
        
        # Optional: brief pause between batches
        if batch_num < total_batches and cfg.get("batch_pause_seconds", 0) > 0:
            time.sleep(cfg["batch_pause_seconds"])
    
    return total_new_successes, total_new_attempts


def process_text_search(to_check: List[str], ledger: Dict[str, Dict], cfg: dict, storage) -> dict:
    """Main entry point for text search cache warming processing"""
    
    if len(to_check) == 0:
        return {"new_successes": 0, "new_failures": 0}
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    processing_header = Colors.bold(f"Processing {len(to_check)} artists for text search cache warming...", colored_output)
    print(processing_header)
    print(f"Settings: {cfg['max_attempts_per_artist_textsearch']} attempts, {cfg['delay_between_attempts']}s delay, "
          f"{cfg['max_concurrent_requests']} concurrent, {cfg['rate_limit_per_second']} req/sec")
    
    # Show text processing configuration if enabled
    if cfg.get("artist_textsearch_lowercase", False) or cfg.get("artist_textsearch_remove_symbols", False):
        processing_options = []
        if cfg.get("artist_textsearch_lowercase", False):
            processing_options.append("lowercase conversion")
        if cfg.get("artist_textsearch_remove_symbols", False):
            processing_options.append("symbol/diacritic removal")
        processing_text = Colors.cyan(f"Text processing: {', '.join(processing_options)}", colored_output)
        print(processing_text)
    else:
        disabled_text = Colors.dim("Text processing: disabled (using original artist names)", colored_output)
        print(disabled_text)
    
    try:
        if cfg.get("batch_size", 25) < len(to_check):
            # Use batch processing for large sets
            successes, attempts = process_text_searches_in_batches(to_check, ledger, cfg, storage)
        else:
            # Process all at once for smaller sets
            successes, attempts = asyncio.run(
                check_text_searches_concurrent_with_timing(to_check, ledger, cfg, storage, time.time(), 0)
            )
            
        # Final write
        storage.write_artists_ledger(ledger)
        
        failures = attempts - successes
        
        return {
            "new_successes": successes,
            "new_failures": failures
        }
        
    except KeyboardInterrupt:
        warning_text = Colors.warning("⚠️  Interrupted by user. Saving progress...", colored_output)
        print(f"\n{warning_text}")
        storage.write_artists_ledger(ledger)
        return {"new_successes": 0, "new_failures": 0}
    except Exception as e:
        error_text = Colors.error(f"ERROR in text search processing: {e}", colored_output)
        print(error_text)
        storage.write_artists_ledger(ledger)
        raise
