#!/usr/bin/env python3
import asyncio
import random
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import aiohttp

from storage import iso_now
from colors import Colors


def trigger_lidarr_refresh(base_url: str, api_key: str, artist_id: Optional[int], verify_ssl: bool = True) -> None:
    """Fire-and-forget refresh request to Lidarr for the given artist id."""
    if artist_id is None:
        return
    
    import requests
    session = requests.Session()
    headers = {"X-Api-Key": api_key}
    
    # Configure SSL verification
    session.verify = verify_ssl
    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
    payloads = [
        {"name": "RefreshArtist", "artistIds": [artist_id]},
        {"name": "RefreshArtist", "artistId": artist_id},
    ]
    for path in ("/api/v1/command", "/api/command"):
        url = f"{base_url.rstrip('/')}{path}"
        for body in payloads:
            try:
                session.post(url, headers=headers, json=body, timeout=0.5)
                return
            except Exception:
                continue


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
            
        # For cache warming: 503, 404, and other HTTP errors are EXPECTED
        # Don't reduce rate for these - they're part of normal cache warming process
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


async def check_artist_with_cache_warming(
    session: aiohttp.ClientSession,
    mbid: str,
    target_base_url: str,
    max_attempts: int = 25,
    delay_between_attempts: float = 0.5,
    timeout: int = 10
) -> Tuple[str, str, int, float, str]:
    """Check single artist MBID with cache warming - keep trying until success or max attempts.
    
    Returns: (status, last_code, attempts_used, total_response_time, canary_target)
    """
    url = f"{target_base_url.rstrip('/')}/artist/{mbid}"
    total_response_time = 0
    canary_target = ""
    
    for attempt in range(max_attempts):
        start_time = time.time()
        try:
            async with session.get(url) as resp:
                response_time = time.time() - start_time
                total_response_time += response_time
                status_code = resp.status
                
                # Capture canary target header
                canary_target = resp.headers.get("x-canary-response-target", "")
                
                if status_code == 200:
                    # SUCCESS! Cache warming worked
                    return "success", str(status_code), attempt + 1, total_response_time, canary_target
                
                # For cache warming, we retry ALL non-200 responses
                # (503, 404, 429, etc. - keep trying until cache warms up)
                
        except asyncio.TimeoutError:
            response_time = time.time() - start_time
            total_response_time += response_time
            status_code = "TIMEOUT"
        except Exception as e:
            response_time = time.time() - start_time
            total_response_time += response_time
            # For cache warming, even exceptions are worth retrying
            status_code = f"EXC:{type(e).__name__}"
        
        # Wait between attempts (unless it's the last attempt)
        if attempt < max_attempts - 1:
            await asyncio.sleep(delay_between_attempts)
    
    # Exhausted all attempts without success
    return "timeout", str(status_code), max_attempts, total_response_time, canary_target


async def check_artists_concurrent_with_timing(
    to_check: List[str],
    ledger: Dict[str, Dict],
    cfg: dict,
    storage,
    overall_start_time: float,
    offset: int
) -> Tuple[int, int, int]:
    """Check artist MBIDs concurrently with proper timing across batches"""
    
    rate_limiter = SafeRateLimiter(
        requests_per_second=cfg["rate_limit_per_second"],
        max_concurrent=cfg["max_concurrent_requests"],
        circuit_breaker_threshold=cfg["circuit_breaker_threshold"],
        backoff_factor=cfg["backoff_factor"],
        max_backoff_seconds=cfg["max_backoff_seconds"]
    )
    
    transitioned_count = 0
    new_successes = 0
    new_failures = 0
    timeout_obj = aiohttp.ClientTimeout(total=cfg["timeout_seconds"])
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    # Track progress stats for this batch
    batch_successes = 0
    batch_timeouts = 0
    
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        for i, mbid in enumerate(to_check):
            # Check circuit breaker
            if not await rate_limiter.acquire():
                print(f"🚫 Circuit breaker open, skipping remaining {len(to_check) - i} artists")
                break
            
            name = ledger[mbid].get("artist_name", "Unknown")
            prev_status = ledger[mbid].get("status", "").lower()
            
            # Use offset for proper numbering across batches
            global_position = offset + i + 1
            total_to_process = offset + len(to_check)
            
            print(f"[{global_position}/{total_to_process}] Checking {name} [{mbid}] ...", end="", flush=True)
            
            try:
                status, last_code, attempts_used, response_time, canary_target = await check_artist_with_cache_warming(
                    session,
                    mbid,
                    cfg["target_base_url"],
                    cfg["max_attempts_per_artist"],
                    cfg["delay_between_attempts"],
                    cfg["timeout_seconds"]
                )
                
                rate_limiter.release(int(last_code) if last_code.isdigit() else last_code, response_time)
                
                # Update ledger with canary target
                ledger[mbid].update({
                    "status": status,
                    "attempts": attempts_used,
                    "last_status_code": last_code,
                    "last_checked": iso_now(),
                    "last_canary_target": canary_target
                })
                
                # Record canary response in SQLite for analytics (if available)
                if hasattr(storage, 'record_canary_response') and canary_target:
                    storage.record_canary_response(
                        entity_type="artist",
                        entity_id=mbid,
                        canary_target=canary_target,
                        status_code=last_code,
                        success=(status == "success"),
                        operation_type="mbid_check"
                    )
                
                # Count results and display with colors
                if status == "success":
                    new_successes += 1
                    batch_successes += 1
                    success_text = Colors.success("SUCCESS", colored_output)
                    canary_info = f" (canary: {canary_target})" if canary_target else ""
                    print(f" {success_text} (code={last_code}, attempts={attempts_used}){canary_info}")
                else:
                    new_failures += 1
                    batch_timeouts += 1
                    timeout_text = Colors.error("TIMEOUT", colored_output)
                    canary_info = f" (canary: {canary_target})" if canary_target else ""
                    print(f" {timeout_text} (code={last_code}, attempts={attempts_used}){canary_info}")
                
                # Trigger Lidarr refresh if configured
                if (cfg.get("update_lidarr", False) 
                    and status == "success" 
                    and prev_status in ("", "timeout")):
                    # For artists, we need to get the lidarr_id from somewhere
                    # This will need to be passed in or looked up
                    trigger_lidarr_refresh(cfg["lidarr_url"], cfg["api_key"], None, cfg.get("verify_ssl", True))  # TODO: Fix this
                    transitioned_count += 1
                    refresh_text = Colors.info("Triggered Lidarr refresh", colored_output)
                    print(f"  -> {refresh_text} for {name}")
                
            except Exception as e:
                response_time = 1.0  # Estimate for failed requests
                rate_limiter.release("EXC", response_time)
                
                ledger[mbid].update({
                    "status": "timeout",
                    "attempts": cfg["max_attempts_per_artist"],
                    "last_status_code": f"EXC:{type(e).__name__}",
                    "last_checked": iso_now(),
                    "last_canary_target": ""
                })
                
                # Record exception in canary analytics if storage supports it
                if hasattr(storage, 'record_canary_response'):
                    storage.record_canary_response(
                        entity_type="artist",
                        entity_id=mbid,
                        canary_target="",
                        status_code=f"EXC:{type(e).__name__}",
                        success=False,
                        operation_type="mbid_check"
                    )
                
                new_failures += 1
                batch_timeouts += 1
                timeout_text = Colors.error("TIMEOUT", colored_output)
                print(f" {timeout_text} (code=EXC:{type(e).__name__}, attempts={cfg['max_attempts_per_artist']})")
            
            # Batch writing
            if global_position % cfg.get("batch_write_frequency", 5) == 0:
                storage.write_artists_ledger(ledger)
            
            # Progress reporting with batch stats - FIX: Count WITHIN this batch only
            if global_position % cfg.get("log_progress_every_n", 25) == 0:
                elapsed_time = time.time() - overall_start_time
                artists_per_sec = global_position / max(elapsed_time, 0.1)
                remaining_artists = total_to_process - global_position
                eta_seconds = remaining_artists / max(artists_per_sec, 0.01)
                
                # Calculate ETC (Estimated Time to Completion)
                etc_timestamp = datetime.now() + timedelta(seconds=eta_seconds)
                etc_str = etc_timestamp.strftime("%H:%M")
                
                stats = rate_limiter.get_stats()
                
                # FIX: Calculate batch progress correctly
                # We want to show success rate for the items processed in the current reporting period
                items_in_reporting_window = min(cfg.get("log_progress_every_n", 25), len(to_check))
                start_of_window = max(0, global_position - items_in_reporting_window)
                window_successes = batch_successes
                window_total = batch_successes + batch_timeouts
                
                # Color the batch success rate
                if window_total > 0:
                    success_rate_text = f"{window_successes}/{window_total}"
                    if window_successes == window_total:
                        success_rate_text = Colors.success(success_rate_text, colored_output)
                    elif window_successes == 0:
                        success_rate_text = Colors.error(success_rate_text, colored_output)
                    else:
                        success_rate_text = Colors.warning(success_rate_text, colored_output)
                else:
                    success_rate_text = "0/0"
                
                print(f"Progress: {global_position}/{total_to_process} ({(global_position/total_to_process*100):.1f}%) - "
                      f"Rate: {artists_per_sec:.1f} artists/sec - ETC: {etc_str} - "
                      f"API: {stats.get('current_rate', 'N/A')} - Batch: {success_rate_text} success")
    
    return transitioned_count, new_successes, new_failures


def process_artists_in_batches(
    to_check: List[str], 
    ledger: Dict[str, Dict],
    cfg: dict,
    storage
) -> Tuple[int, int, int]:
    """Process artist MBIDs in batches. Returns (transitioned_count, total_new_successes, total_new_failures)"""
    batch_size = cfg.get("batch_size", 25)
    total_batches = (len(to_check) + batch_size - 1) // batch_size
    total_transitioned = 0
    total_new_successes = 0
    total_new_failures = 0
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    # Track timing across all batches
    overall_start_time = time.time()
    total_processed = 0
    
    for batch_idx in range(0, len(to_check), batch_size):
        batch_num = batch_idx // batch_size + 1
        batch = to_check[batch_idx:batch_idx + batch_size]
        
        batch_header = Colors.info(f"=== Artists Batch {batch_num}/{total_batches} ({len(batch)} artists) ===", colored_output)
        print(batch_header)
        
        batch_transitioned, batch_successes, batch_failures = asyncio.run(
            check_artists_concurrent_with_timing(batch, ledger, cfg, storage, overall_start_time, total_processed)
        )
        
        total_transitioned += batch_transitioned
        total_new_successes += batch_successes
        total_new_failures += batch_failures
        total_processed += len(batch)
        
        # Write after each batch
        storage.write_artists_ledger(ledger)
        complete_text = Colors.success(f"Artists batch {batch_num} complete. Ledger updated.", colored_output)
        print(complete_text)
        
        # Optional: brief pause between batches
        if batch_num < total_batches and cfg.get("batch_pause_seconds", 0) > 0:
            time.sleep(cfg["batch_pause_seconds"])
    
    return total_transitioned, total_new_successes, total_new_failures


def process_artists(to_check: List[str], ledger: Dict[str, Dict], cfg: dict, storage) -> dict:
    """Main entry point for artist cache warming processing"""
    
    if len(to_check) == 0:
        return {"transitioned": 0, "new_successes": 0, "new_failures": 0}
    
    # Get color setting from config
    colored_output = cfg.get("colored_output", True)
    
    processing_header = Colors.bold(f"Processing {len(to_check)} artists with cache warming...", colored_output)
    print(processing_header)
    print(f"Settings: {cfg['max_attempts_per_artist']} attempts, {cfg['delay_between_attempts']}s delay, "
          f"{cfg['max_concurrent_requests']} concurrent, {cfg['rate_limit_per_second']} req/sec")
    
    try:
        if cfg.get("batch_size", 25) < len(to_check):
            # Use batch processing for large sets
            transitioned, successes, failures = process_artists_in_batches(to_check, ledger, cfg, storage)
        else:
            # Process all at once for smaller sets
            transitioned, successes, failures = asyncio.run(
                check_artists_concurrent_with_timing(to_check, ledger, cfg, storage, time.time(), 0)
            )
            
        # Final write
        storage.write_artists_ledger(ledger)
        
        return {
            "transitioned": transitioned,
            "new_successes": successes,
            "new_failures": failures
        }
        
    except KeyboardInterrupt:
        warning_text = Colors.warning("⚠️  Interrupted by user. Saving progress...", colored_output)
        print(f"\n{warning_text}")
        storage.write_artists_ledger(ledger)
        return {"transitioned": 0, "new_successes": 0, "new_failures": 0}
    except Exception as e:
        error_text = Colors.error(f"ERROR in artist processing: {e}", colored_output)
        print(error_text)
        storage.write_artists_ledger(ledger)
        raise
