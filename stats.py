#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Dict

from config import load_config, validate_config
from main import get_lidarr_artists, get_lidarr_release_groups
from storage import create_storage_backend


def is_stale(last_checked: str, recheck_hours: int) -> bool:
    """Check if a cache entry is stale based on last_checked timestamp and recheck hours"""
    if recheck_hours <= 0:
        return False  # Recheck disabled
    
    if not last_checked:
        return True  # Never checked = stale
    
    try:
        # Parse ISO timestamp (handles both with/without timezone)
        if last_checked.endswith('Z'):
            last_checked = last_checked[:-1] + '+00:00'
        elif '+' not in last_checked and 'T' in last_checked:
            last_checked += '+00:00'
        
        last_time = datetime.fromisoformat(last_checked)
        now = datetime.now(timezone.utc)
        hours_since = (now - last_time).total_seconds() / 3600
        return hours_since >= recheck_hours
    except Exception:
        return True  # Invalid timestamp = stale


def get_hours_until_stale(last_checked: str, recheck_hours: int) -> float:
    """Get hours until entry becomes stale. Returns 0 if already stale or never checked."""
    if recheck_hours <= 0 or not last_checked:
        return 0
    
    try:
        if last_checked.endswith('Z'):
            last_checked = last_checked[:-1] + '+00:00'
        elif '+' not in last_checked and 'T' in last_checked:
            last_checked += '+00:00'
        
        last_time = datetime.fromisoformat(last_checked)
        now = datetime.now(timezone.utc)
        hours_since = (now - last_time).total_seconds() / 3600
        hours_remaining = recheck_hours - hours_since
        return max(0, hours_remaining)
    except Exception:
        return 0


def analyze_artists_stats(artists_ledger: Dict[str, Dict], cache_recheck_hours: int) -> Dict[str, any]:
    """Analyze artist statistics from ledger including staleness information"""
    if not artists_ledger:
        return {
            "total": 0,
            "success": 0,
            "timeout": 0,
            "pending": 0,
            "success_rate": 0.0,
            "text_search_attempted": 0,
            "text_search_success": 0,
            "text_search_success_rate": 0.0,
            "text_search_pending": 0,
            "artists_with_names": 0,
            "stale_mbid_cache": 0,
            "stale_text_search": 0,
            "next_recheck_hours": 0,
            "recheck_enabled": cache_recheck_hours > 0
        }
    
    total = len(artists_ledger)
    success = sum(1 for r in artists_ledger.values() if r.get("status", "").lower() == "success")
    timeout = sum(1 for r in artists_ledger.values() if r.get("status", "").lower() == "timeout")
    pending = total - success - timeout
    success_rate = (success / total * 100) if total > 0 else 0.0
    
    # Text search statistics
    text_search_attempted = sum(1 for r in artists_ledger.values() if r.get("text_search_attempted", False))
    text_search_success = sum(1 for r in artists_ledger.values() if r.get("text_search_success", False))
    text_search_success_rate = (text_search_success / text_search_attempted * 100) if text_search_attempted > 0 else 0.0
    
    # Artists with names that could be text searched but haven't been attempted
    artists_with_names = sum(1 for r in artists_ledger.values() if r.get("artist_name", "").strip())
    text_search_pending = artists_with_names - text_search_attempted
    
    # Staleness statistics (only if recheck is enabled)
    stale_mbid_cache = 0
    stale_text_search = 0
    next_recheck_hours = float('inf')
    
    if cache_recheck_hours > 0:
        for r in artists_ledger.values():
            # Count stale MBID cache entries (successful but stale)
            if (r.get("status", "").lower() == "success" and 
                is_stale(r.get("last_checked", ""), cache_recheck_hours)):
                stale_mbid_cache += 1
            
            # Count stale text search entries (successful but stale)
            if (r.get("text_search_success", False) and 
                is_stale(r.get("text_search_last_checked", ""), cache_recheck_hours)):
                stale_text_search += 1
            
            # Find next recheck time
            for timestamp_field in ["last_checked", "text_search_last_checked"]:
                timestamp = r.get(timestamp_field, "")
                if timestamp:
                    hours_until_stale = get_hours_until_stale(timestamp, cache_recheck_hours)
                    if 0 < hours_until_stale < next_recheck_hours:
                        next_recheck_hours = hours_until_stale
    
    if next_recheck_hours == float('inf'):
        next_recheck_hours = 0
    
    return {
        "total": total,
        "success": success,
        "timeout": timeout,
        "pending": pending,
        "success_rate": success_rate,
        "text_search_attempted": text_search_attempted,
        "text_search_success": text_search_success,
        "text_search_success_rate": text_search_success_rate,
        "text_search_pending": text_search_pending,
        "artists_with_names": artists_with_names,
        "stale_mbid_cache": stale_mbid_cache,
        "stale_text_search": stale_text_search,
        "next_recheck_hours": next_recheck_hours,
        "recheck_enabled": cache_recheck_hours > 0
    }


def analyze_release_groups_stats(rg_ledger: Dict[str, Dict], cache_recheck_hours: int) -> Dict[str, any]:
    """Analyze release group statistics from ledger including staleness information"""
    if not rg_ledger:
        return {
            "total": 0,
            "success": 0,
            "timeout": 0,
            "pending": 0,
            "success_rate": 0.0,
            "eligible_for_processing": 0,
            "stale_entries": 0,
            "next_recheck_hours": 0,
            "recheck_enabled": cache_recheck_hours > 0
        }
    
    total = len(rg_ledger)
    success = sum(1 for r in rg_ledger.values() if r.get("status", "").lower() == "success")
    timeout = sum(1 for r in rg_ledger.values() if r.get("status", "").lower() == "timeout")
    pending = total - success - timeout
    success_rate = (success / total * 100) if total > 0 else 0.0
    
    # Count RGs eligible for processing (artist successfully cached)
    eligible = sum(1 for r in rg_ledger.values() 
                  if r.get("artist_cache_status", "").lower() == "success")
    
    # Staleness statistics (only if recheck is enabled)
    stale_entries = 0
    next_recheck_hours = float('inf')
    
    if cache_recheck_hours > 0:
        for r in rg_ledger.values():
            # Count stale entries (successful but stale)
            if (r.get("status", "").lower() == "success" and 
                is_stale(r.get("last_checked", ""), cache_recheck_hours)):
                stale_entries += 1
            
            # Find next recheck time
            timestamp = r.get("last_checked", "")
            if timestamp:
                hours_until_stale = get_hours_until_stale(timestamp, cache_recheck_hours)
                if 0 < hours_until_stale < next_recheck_hours:
                    next_recheck_hours = hours_until_stale
    
    if next_recheck_hours == float('inf'):
        next_recheck_hours = 0
    
    return {
        "total": total,
        "success": success,
        "timeout": timeout,
        "pending": pending,
        "success_rate": success_rate,
        "eligible_for_processing": eligible,
        "stale_entries": stale_entries,
        "next_recheck_hours": next_recheck_hours,
        "recheck_enabled": cache_recheck_hours > 0
    }


def format_config_summary(cfg: dict) -> str:
    """Format key configuration settings"""
    storage_type = cfg.get("storage_type", "csv")
    
    config_lines = [
        "📋 Key Configuration Settings:",
        f"   Connection & Security:",
        f"     • lidarr_timeout: {cfg.get('lidarr_timeout', 60)}s",
        f"     • verify_ssl: {cfg.get('verify_ssl', True)}",
        f"   API Rate Limiting:",
        f"     • max_concurrent_requests: {cfg.get('max_concurrent_requests', 5)}",
        f"     • rate_limit_per_second: {cfg.get('rate_limit_per_second', 3)}",
        f"     • delay_between_attempts: {cfg.get('delay_between_attempts', 0.5)}s",
        f"   Cache Warming Attempts:",
        f"     • max_attempts_per_artist: {cfg.get('max_attempts_per_artist', 25)}",
        f"     • max_attempts_per_artist_textsearch: {cfg.get('max_attempts_per_artist_textsearch', 25)}",
        f"     • max_attempts_per_rg: {cfg.get('max_attempts_per_rg', 15)}",
        f"   Processing Options:",
        f"     • process_release_groups: {cfg.get('process_release_groups', False)}",
        f"     • process_artist_textsearch: {cfg.get('process_artist_textsearch', True)}",
        f"     • batch_size: {cfg.get('batch_size', 25)}",
        f"     • cache_recheck_hours: {cfg.get('cache_recheck_hours', 72)}",
        f"   Text Search Processing:",
        f"     • artist_textsearch_lowercase: {cfg.get('artist_textsearch_lowercase', False)}",
        f"     • artist_textsearch_transliterate_unicode: {cfg.get('artist_textsearch_transliterate_unicode', False)}",
        f"     • artist_textsearch_remove_symbols: {cfg.get('artist_textsearch_remove_symbols', False)} (deprecated)",
        f"   Storage Backend:",
        f"     • storage_type: {storage_type}",
    ]
    
    if storage_type == "sqlite":
        config_lines.append(f"     • db_path: {cfg.get('db_path', 'mbid_cache.db')}")
    else:
        config_lines.extend([
            f"     • artists_csv_path: {cfg.get('artists_csv_path', 'mbid-artists.csv')}",
            f"     • release_groups_csv_path: {cfg.get('release_groups_csv_path', 'mbid-releasegroups.csv')}"
        ])
    
    return "\n".join(config_lines)


def print_canary_analysis(storage) -> None:
    """Print detailed canary response target analysis"""
    print()
    print("=" * 60)
    print("🎯 CANARY RESPONSE TARGET ANALYSIS")
    print("=" * 60)
    
    try:
        canary_stats = storage.get_canary_statistics()
        
        if not canary_stats:
            print("📊 No canary response data available")
            print("   This is normal for:")
            print("   • CSV storage (limited canary tracking)")
            print("   • Fresh installations")
            print("   • APIs that don't set x-canary-response-target header")
            return
        
        print(f"📊 Found {len(canary_stats)} canary targets with response data")
        print()
        
        # Calculate overall statistics
        total_requests = 0
        total_successes = 0
        
        for target_name, target_data in canary_stats.items():
            for op_type, op_stats in target_data.get("operations", {}).items():
                total_requests += op_stats["total_requests"]
                total_successes += op_stats["successful_requests"]
        
        overall_success_rate = (total_successes / total_requests * 100) if total_requests > 0 else 0.0
        
        print(f"🌐 OVERALL CANARY STATISTICS:")
        print(f"   Total requests across all targets: {total_requests:,}")
        print(f"   Total successful requests: {total_successes:,}")
        print(f"   Overall success rate: {overall_success_rate:.1f}%")
        
        # Show failure breakdown if there are any failures
        total_failures = total_requests - total_successes
        if total_failures > 0:
            print(f"   Total failed requests: {total_failures:,}")
            
            # Get failure breakdown by status code (only works with SQLite storage)
            if hasattr(storage, 'db_path') and os.path.exists(storage.db_path):
                try:
                    with sqlite3.connect(storage.db_path) as conn:
                        conn.row_factory = sqlite3.Row
                        cursor = conn.execute("""
                            SELECT status_code, COUNT(*) as count
                            FROM canary_responses 
                            WHERE success = 0 AND canary_target != ''
                            GROUP BY status_code
                            ORDER BY count DESC
                        """)
                        
                        failure_codes = cursor.fetchall()
                        if failure_codes:
                            print(f"   Failure breakdown:")
                            for row in failure_codes:
                                status_code = row["status_code"]
                                count = row["count"]
                                
                                # Add description for common status codes
                                if status_code == "429":
                                    description = "(rate limited)"
                                elif status_code == "503":
                                    description = "(service unavailable)"
                                elif status_code == "404":
                                    description = "(not found)"
                                elif status_code == "500":
                                    description = "(server error)"
                                elif status_code.startswith("EXC:"):
                                    description = "(connection exception)"
                                elif status_code == "TIMEOUT":
                                    description = "(request timeout)"
                                else:
                                    description = ""
                                
                                print(f"     • {count} × HTTP {status_code} {description}")
                except Exception as e:
                    print(f"   (Could not retrieve failure breakdown: {e})")
        
        print()
        
        # Show successful request distribution between targets
        if total_successes > 0:
            print(f"📊 SUCCESSFUL REQUEST DISTRIBUTION:")
            success_distribution = []
            for target_name, target_data in canary_stats.items():
                target_successes = 0
                for op_stats in target_data.get("operations", {}).values():
                    target_successes += op_stats["successful_requests"]
                
                if target_successes > 0:
                    success_percentage = (target_successes / total_successes * 100)
                    success_distribution.append({
                        "name": target_name,
                        "successes": target_successes,
                        "percentage": success_percentage
                    })
            
            # Sort by success count (highest first)
            success_distribution.sort(key=lambda x: x["successes"], reverse=True)
            
            for dist in success_distribution:
                print(f"   {dist['name']}: {dist['successes']:,} successful requests ({dist['percentage']:.1f}%)")
            print()
        
        # Sort targets by success rate (lowest first to highlight problems)
        sorted_targets = []
        for target_name, target_data in canary_stats.items():
            target_total_requests = 0
            target_total_successes = 0
            
            for op_stats in target_data.get("operations", {}).values():
                target_total_requests += op_stats["total_requests"]
                target_total_successes += op_stats["successful_requests"]
            
            target_success_rate = (target_total_successes / target_total_requests * 100) if target_total_requests > 0 else 0.0
            
            sorted_targets.append({
                "name": target_name,
                "success_rate": target_success_rate,
                "total_requests": target_total_requests,
                "total_successes": target_total_successes,
                "data": target_data
            })
        
        # Sort by success rate (lowest first)
        sorted_targets.sort(key=lambda x: x["success_rate"])
        
        print("🎯 CANARY TARGET BREAKDOWN (sorted by success rate):")
        print()
        
        for target in sorted_targets:
            target_name = target["name"]
            target_success_rate = target["success_rate"]
            target_requests = target["total_requests"]
            target_data = target["data"]
            
            # Color code based on success rate
            if target_success_rate >= 95:
                status_icon = "✅"
            elif target_success_rate >= 80:
                status_icon = "⚠️ "
            else:
                status_icon = "❌"
            
            print(f"{status_icon} {target_name}")
            print(f"   Overall: {target_success_rate:.1f}% success ({target["total_successes"]:,}/{target_requests:,} requests)")
            
            # Show first/last seen if available
            if "first_seen" in target_data and "last_seen" in target_data:
                try:
                    first_seen = datetime.fromisoformat(target_data["first_seen"].replace('Z', '+00:00'))
                    last_seen = datetime.fromisoformat(target_data["last_seen"].replace('Z', '+00:00'))
                    print(f"   Active: {first_seen.strftime('%Y-%m-%d %H:%M')} to {last_seen.strftime('%Y-%m-%d %H:%M')} UTC")
                except Exception:
                    pass
            
            # Break down by operation type
            operations = target_data.get("operations", {})
            if len(operations) > 1:
                print(f"   Operations breakdown:")
                for op_type, op_stats in operations.items():
                    op_success_rate = op_stats["success_rate"]
                    op_requests = op_stats["total_requests"]
                    op_successes = op_stats["successful_requests"]
                    
                    # Color code operation success rate
                    if op_success_rate >= 95:
                        op_icon = "  ✅"
                    elif op_success_rate >= 80:
                        op_icon = "  ⚠️ "
                    else:
                        op_icon = "  ❌"
                    
                    print(f"{op_icon} {op_type}: {op_success_rate:.1f}% ({op_successes:,}/{op_requests:,})")
            
            print()
        
        # Recommendations
        print("🚀 CANARY ANALYSIS RECOMMENDATIONS:")
        
        problem_targets = [t for t in sorted_targets if t["success_rate"] < 80]
        warning_targets = [t for t in sorted_targets if 80 <= t["success_rate"] < 95]
        
        if problem_targets:
            print(f"   ❌ {len(problem_targets)} target(s) with concerning success rates (<80%):")
            for target in problem_targets[:3]:  # Show top 3 worst
                print(f"      • {target['name']}: {target['success_rate']:.1f}% success")
            if len(problem_targets) > 3:
                print(f"      • ... and {len(problem_targets) - 3} more")
        
        if warning_targets:
            print(f"   ⚠️  {len(warning_targets)} target(s) with moderate success rates (80-95%):")
            for target in warning_targets[:2]:  # Show top 2
                print(f"      • {target['name']}: {target['success_rate']:.1f}% success")
            if len(warning_targets) > 2:
                print(f"      • ... and {len(warning_targets) - 2} more")
        
        good_targets = [t for t in sorted_targets if t["success_rate"] >= 95]
        if good_targets:
            print(f"   ✅ {len(good_targets)} target(s) performing well (≥95% success rate)")
        
        if problem_targets or warning_targets:
            print()
            print("   💡 Share this analysis with the API maintainers to help identify:")
            print("      • Problematic canary deployments")
            print("      • Performance differences between API versions")
            print("      • Deployment rollback candidates")
        
    except Exception as e:
        print(f"❌ Error analyzing canary data: {e}")
        print("   This might indicate:")
        print("   • Corrupted canary tracking data")
        print("   • Storage backend issues")
        print("   • Missing database tables (try running cache warmer once)")


def print_cf_cache_analysis(storage) -> None:
    """Print detailed CloudFlare cache status analysis"""
    print()
    print("=" * 60)
    print("☁️ CLOUDFLARE CACHE STATUS ANALYSIS")
    print("=" * 60)
    
    try:
        cf_stats = storage.get_cf_cache_statistics()
        
        if not cf_stats:
            print("📊 No CloudFlare cache status data available")
            print("   This is normal for:")
            print("   • CSV storage (limited CF cache tracking)")
            print("   • Fresh installations")
            print("   • APIs that don't set cf-cache-status header")
            return
        
        # Handle different data structures between CSV and SQLite
        if "basic_counts" in cf_stats:
            # CSV storage - simplified data
            basic_counts = cf_stats["basic_counts"]
            total_responses = sum(basic_counts.values())
            
            if total_responses == 0:
                print("📊 No CloudFlare cache status responses recorded")
                return
                
            print(f"📊 Found {total_responses:,} responses with CF cache status (CSV summary)")
            print()
            
            # Show basic distribution
            print("☁️ CLOUDFLARE CACHE STATUS DISTRIBUTION:")
            for status, count in sorted(basic_counts.items(), key=lambda x: x[1], reverse=True):
                if count > 0:
                    percentage = (count / total_responses) * 100
                    
                    # Add icons and descriptions
                    if status == "HIT":
                        icon = "✅"
                        description = "(served from CloudFlare cache)"
                    elif status == "STALE":
                        icon = "⚠️"
                        description = "(stale content, passed to backend)"
                    elif status == "MISS":
                        icon = "❌"
                        description = "(not in cache, passed to backend)"
                    elif status == "EXPIRED":
                        icon = "🔄"
                        description = "(cache expired, backend building new entry)"
                    elif status == "DYNAMIC":
                        icon = "🔄"
                        description = "(dynamic content, bypassed cache)"
                    else:
                        icon = "❓"
                        description = f"(other status: {status})"
                    
                    print(f"   {icon} {status}: {count:,} responses ({percentage:.1f}%) {description}")
            
            return
        
        # SQLite storage - detailed data
        total_requests = 0
        
        for status_data in cf_stats.values():
            total_requests += status_data["total_requests"]
        
        if total_requests == 0:
            print("📊 No CloudFlare cache status data available")
            return
        
        print(f"📊 Found {len(cf_stats)} cache status types with {total_requests:,} total requests")
        print()
        
        # Calculate key metrics based on actual behavior, not just cache status
        hit_responses = 0
        cached_success_responses = 0  # STALE + 200 (cached successful responses)
        cached_error_responses = 0    # STALE + 503 (cached error responses)
        other_responses = 0
        
        if hasattr(storage, 'db_path') and os.path.exists(storage.db_path):
            try:
                with sqlite3.connect(storage.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute("""
                        SELECT 
                            cf_cache_status,
                            status_code,
                            success,
                            COUNT(*) as count
                        FROM cf_cache_responses 
                        WHERE cf_cache_status != ''
                        GROUP BY cf_cache_status, status_code, success
                        ORDER BY cf_cache_status, status_code
                    """)
                    
                    for row in cursor:
                        cf_status = row["cf_cache_status"]
                        status_code = row["status_code"]
                        success = bool(row["success"])
                        count = row["count"]
                        
                        if cf_status == "HIT" and success:
                            hit_responses += count
                        elif cf_status == "STALE" and success and status_code == "200":
                            cached_success_responses += count
                        elif cf_status == "STALE" and not success and status_code == "503":
                            cached_error_responses += count
                        else:
                            other_responses += count
                        
            except Exception:
                pass
        
        # User-friendly summary based on actual cache behavior
        print("📋 CLOUDFLARE CACHE ANALYSIS:")
        print()
        
        if hit_responses > 0:
            hit_percentage = (hit_responses / total_requests) * 100
            print(f"   ✅ Served from active cache: {hit_responses:,} requests ({hit_percentage:.1f}%)")
            print(f"      True cache hits - optimal performance")
            print(f"      (cf-cache-status: HIT + HTTP Code: 200)")
            print()
        
        if cached_success_responses > 0:
            cached_success_percentage = (cached_success_responses / total_requests) * 100
            print(f"   📋 Served cached successful responses: {cached_success_responses:,} requests ({cached_success_percentage:.1f}%)")
            print(f"      CF serving cached 200 responses with full payload (marked as STALE)")
            print(f"      (cf-cache-status: STALE + HTTP Code: 200)")
            print()
        
        if cached_error_responses > 0:
            cached_error_percentage = (cached_error_responses / total_requests) * 100
            print(f"   ❌ Served cached error responses: {cached_error_responses:,} requests ({cached_error_percentage:.1f}%)")
            print(f"      CF serving cached 503 responses - no useful payload")
            print(f"      (cf-cache-status: STALE + HTTP Code: 503)")
            print()
        
        if other_responses > 0:
            other_percentage = (other_responses / total_requests) * 100
            print(f"   ❓ Other cache behaviors: {other_responses:,} requests ({other_percentage:.1f}%)")
            print(f"      MISS, EXPIRED, or other CF cache statuses")
            print()
        
        # Calculate useful response rate
        useful_responses = hit_responses + cached_success_responses
        if total_requests > 0:
            useful_rate = (useful_responses / total_requests) * 100
            print(f"📊 USEFUL RESPONSES: {useful_rate:.1f}% of requests returned actual data")
            
            if cached_error_responses > 0:
                error_rate = (cached_error_responses / total_requests) * 100
                print(f"⚠️  {error_rate:.1f}% of requests returned cached errors (useless)")
        
        print()
        
        # Get cross-tabulation data from SQLite if available
        cross_tab_data = {}
        
        if hasattr(storage, 'db_path') and os.path.exists(storage.db_path):
            try:
                with sqlite3.connect(storage.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute("""
                        SELECT 
                            cf_cache_status,
                            status_code,
                            success,
                            COUNT(*) as count
                        FROM cf_cache_responses 
                        WHERE cf_cache_status != ''
                        GROUP BY cf_cache_status, status_code, success
                        ORDER BY cf_cache_status, status_code
                    """)
                    
                    for row in cursor:
                        cf_status = row["cf_cache_status"]
                        status_code = row["status_code"]
                        success = bool(row["success"])
                        count = row["count"]
                        
                        if cf_status not in cross_tab_data:
                            cross_tab_data[cf_status] = {}
                        
                        outcome = "SUCCESS" if success else "TIMEOUT"
                        key = f"{status_code} {outcome}"
                        cross_tab_data[cf_status][key] = count
                        
            except Exception as e:
                print(f"   (Could not retrieve detailed cross-tabulation: {e})")
        
        # Cross-tabulation breakdown
        if cross_tab_data:
            print("☁️ CACHE STATUS vs BACKEND RESPONSE:")
            print()
            
            # Define status order and icons
            status_order = ["HIT", "STALE", "MISS", "EXPIRED", "DYNAMIC"]
            status_info = {
                "HIT": {"icon": "✅", "desc": "served from CF cache"},
                "STALE": {"icon": "⚠️", "desc": "CF serving cached content (marked as stale)"},
                "MISS": {"icon": "❌", "desc": "no CF cache, forwarded to backend"},
                "EXPIRED": {"icon": "🔄", "desc": "cache expired, backend contacted for fresh content"},
                "DYNAMIC": {"icon": "🔄", "desc": "dynamic content, bypassed cache"}
            }
            
            for cf_status in status_order:
                if cf_status not in cross_tab_data:
                    continue
                    
                info = status_info.get(cf_status, {"icon": "❓", "desc": f"other status: {cf_status}"})
                total_for_status = sum(cross_tab_data[cf_status].values())
                percentage = (total_for_status / total_requests * 100)
                
                print(f"{info['icon']} {cf_status} ({info['desc']}):")
                print(f"   Total: {total_for_status:,} requests ({percentage:.1f}%)")
                
                # Sort by count (descending)
                sorted_outcomes = sorted(cross_tab_data[cf_status].items(), key=lambda x: x[1], reverse=True)
                
                for outcome, count in sorted_outcomes:
                    outcome_percentage = (count / total_for_status * 100) if total_for_status > 0 else 0
                    
                    # Parse outcome
                    if "SUCCESS" in outcome:
                        outcome_icon = "✅"
                    else:
                        outcome_icon = "❌"
                    
                    print(f"   {outcome_icon} {outcome}: {count:,} requests ({outcome_percentage:.1f}%)")
                
                print()
            
            # Show any remaining statuses not in our predefined list
            for cf_status in cross_tab_data:
                if cf_status not in status_order:
                    total_for_status = sum(cross_tab_data[cf_status].values())
                    percentage = (total_for_status / total_requests * 100)
                    
                    print(f"❓ {cf_status} (other cache status):")
                    print(f"   Total: {total_for_status:,} requests ({percentage:.1f}%)")
                    
                    sorted_outcomes = sorted(cross_tab_data[cf_status].items(), key=lambda x: x[1], reverse=True)
                    for outcome, count in sorted_outcomes:
                        outcome_percentage = (count / total_for_status * 100) if total_for_status > 0 else 0
                        outcome_icon = "✅" if "SUCCESS" in outcome else "❌"
                        print(f"   {outcome_icon} {outcome}: {count:,} requests ({outcome_percentage:.1f}%)")
                    print()
        
        else:
            # Fallback to simple breakdown if cross-tabulation not available
            print("☁️ CACHE STATUS BREAKDOWN:")
            print("   (Cross-tabulation requires SQLite storage with detailed tracking)")
            print()
            
            # Sort by request count (highest first)
            sorted_statuses = sorted(cf_stats.items(), key=lambda x: x[1]["total_requests"], reverse=True)
            
            for status, status_data in sorted_statuses:
                count = status_data["total_requests"]
                percentage = (count / total_requests) * 100
                
                # Add icons and descriptions
                if status == "HIT":
                    icon = "✅"
                    description = "Served directly from CloudFlare cache"
                elif status == "STALE":
                    icon = "⚠️"
                    description = "CF serving cached content (marked as stale)"
                elif status == "MISS":
                    icon = "❌"
                    description = "No cache entry found, forwarded to backend"
                elif status == "EXPIRED":
                    icon = "🔄"
                    description = "Cache expired, backend contacted for fresh content"
                elif status == "DYNAMIC":
                    icon = "🔄"
                    description = "Dynamic content, bypasses cache entirely"
                else:
                    icon = "❓"
                    description = f"Other cache status: {status}"
                
                print(f"   {icon} {status}: {count:,} requests ({percentage:.1f}%)")
                print(f"      {description}")
                print()
        
    except Exception as e:
        print(f"❌ Error analyzing CloudFlare cache data: {e}")
        print("   This might indicate:")
        print("   • Corrupted CF cache tracking data")
        print("   • Storage backend issues")
        print("   • Missing database tables (try running cache warmer once)")


def print_stats_report(cfg: dict, show_canary_stats: bool = False):
    """Generate and print comprehensive stats report"""
    
    print("=" * 60)
    print("🎵 LIDARR CACHE WARMER - STATISTICS REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Configuration summary
    print(format_config_summary(cfg))
    print()
    
    # Create storage backend and load data
    try:
        # Check if storage files exist before creating backend
        storage_type = cfg.get("storage_type", "csv").lower()
        
        if storage_type == "sqlite":
            db_path = cfg.get("db_path", "mbid_cache.db")
            if not os.path.exists(db_path):
                print(f"❌ ERROR: SQLite database not found at {db_path}")
                print(f"   Run the cache warmer first to create the database")
                return
        else:
            artists_csv = cfg.get("artists_csv_path", "mbid-artists.csv") 
            if not os.path.exists(artists_csv):
                print(f"❌ ERROR: Artists CSV not found at {artists_csv}")
                print(f"   Run the cache warmer first to create the CSV files")
                return
        
        storage = create_storage_backend(cfg)
        artists_ledger = storage.read_artists_ledger()
        rg_ledger = storage.read_release_groups_ledger()
    except Exception as e:
        print(f"❌ ERROR: Could not read storage: {e}")
        return
    
    # Fetch current Lidarr data for comparison
    try:
        print("📡 Fetching current data from Lidarr...")
        lidarr_artists = get_lidarr_artists(
            cfg["lidarr_url"], 
            cfg["api_key"], 
            cfg.get("verify_ssl", True),
            cfg.get("lidarr_timeout", 60)
        )
        lidarr_artist_count = len(lidarr_artists)
        
        if cfg.get("process_release_groups", False):
            lidarr_rgs = get_lidarr_release_groups(
                cfg["lidarr_url"], 
                cfg["api_key"], 
                cfg.get("verify_ssl", True),
                cfg.get("lidarr_timeout", 60)
            )
            lidarr_rg_count = len(lidarr_rgs)
        else:
            lidarr_rg_count = 0
            
    except Exception as e:
        print(f"⚠️ WARNING: Could not fetch Lidarr data: {e}")
        print("    Using ledger data only...")
        lidarr_artist_count = len(artists_ledger)
        lidarr_rg_count = len(rg_ledger)
    
    print()
    
    # Artist statistics
    artist_stats = analyze_artists_stats(artists_ledger, cfg.get("cache_recheck_hours", 72))
    print("🎤 ARTIST MBID STATISTICS:")
    print(f"   Total artists in Lidarr: {lidarr_artist_count:,}")
    print(f"   Artists in ledger: {artist_stats['total']:,}")
    print(f"   ✅ Successfully cached: {artist_stats['success']:,} ({artist_stats['success_rate']:.1f}%)")
    print(f"   ❌ Failed/Timeout: {artist_stats['timeout']:,}")
    print(f"   ⏳ Not yet processed: {artist_stats['pending']:,}")
    
    if lidarr_artist_count != artist_stats['total']:
        diff = lidarr_artist_count - artist_stats['total']
        print(f"   📊 Ledger sync: {abs(diff)} artists {'ahead' if diff < 0 else 'behind'} Lidarr")
    
    # Show staleness information if recheck is enabled
    if artist_stats['recheck_enabled'] and artist_stats['stale_mbid_cache'] > 0:
        print(f"   ⏰ Stale MBID cache: {artist_stats['stale_mbid_cache']:,} (older than {cfg.get('cache_recheck_hours', 72)} hours)")
        if artist_stats['next_recheck_hours'] > 0:
            print(f"   🔄 Next recheck in: {artist_stats['next_recheck_hours']:.1f} hours")
    elif artist_stats['recheck_enabled']:
        print(f"   ⏰ All MBID cache entries are fresh (< {cfg.get('cache_recheck_hours', 72)} hours)")
    else:
        print(f"   ⏰ Cache freshness checking disabled (cache_recheck_hours = 0)")
    
    print()
    
    # Text search statistics
    if cfg.get("process_artist_textsearch", True):
        print("🔍 ARTIST TEXT SEARCH STATISTICS:")
        print(f"   Artists with names: {artist_stats['artists_with_names']:,}")
        print(f"   ✅ Text searches attempted: {artist_stats['text_search_attempted']:,}")
        if artist_stats['text_search_attempted'] > 0:
            print(f"   ✅ Text searches successful: {artist_stats['text_search_success']:,} ({artist_stats['text_search_success_rate']:.1f}%)")
            print(f"   ⏳ Text searches pending: {artist_stats['text_search_pending']:,}")
            
            # Calculate text search coverage
            text_coverage = (artist_stats['text_search_attempted'] / artist_stats['artists_with_names'] * 100) if artist_stats['artists_with_names'] > 0 else 0
            print(f"   📊 Text search coverage: {text_coverage:.1f}% of named artists")
            
            # Show staleness information for text search if recheck is enabled
            if artist_stats['recheck_enabled'] and artist_stats['stale_text_search'] > 0:
                print(f"   ⏰ Stale text searches: {artist_stats['stale_text_search']:,} (older than {cfg.get('cache_recheck_hours', 72)} hours)")
            elif artist_stats['recheck_enabled']:
                print(f"   ⏰ All text search cache entries are fresh (< {cfg.get('cache_recheck_hours', 72)} hours)")
        else:
            print(f"   ⏳ Text searches pending: {artist_stats['text_search_pending']:,} (none attempted yet)")
        
        # Show text processing configuration
        text_processing_options = []
        if cfg.get('artist_textsearch_lowercase', False):
            text_processing_options.append("lowercase")
        if cfg.get('artist_textsearch_transliterate_unicode', False):
            text_processing_options.append("Unicode transliteration")
        if cfg.get('artist_textsearch_remove_symbols', False):
            text_processing_options.append("symbol removal (deprecated)")
        
        if text_processing_options:
            print(f"   ⚙️ Text processing: {', '.join(text_processing_options)}")
        else:
            print(f"   ⚙️ Text processing: disabled (original names)")
        
        print()
    else:
        print("🔍 TEXT SEARCH WARMING: Disabled")
        print("   Enable with: process_artist_textsearch = true")
        print()
    
    # Release group statistics (if enabled)
    if cfg.get("process_release_groups", False):
        rg_stats = analyze_release_groups_stats(rg_ledger, cfg.get("cache_recheck_hours", 72))
        print("💿 RELEASE GROUP STATISTICS:")
        print(f"   Total release groups in Lidarr: {lidarr_rg_count:,}")
        print(f"   Release groups in ledger: {rg_stats['total']:,}")
        print(f"   ✅ Successfully cached: {rg_stats['success']:,} ({rg_stats['success_rate']:.1f}%)")
        print(f"   ❌ Failed/Timeout: {rg_stats['timeout']:,}")
        print(f"   ⏳ Not yet processed: {rg_stats['pending']:,}")
        print(f"   🎯 Eligible for processing: {rg_stats['eligible_for_processing']:,}")
        print(f"      (Release groups with successfully cached artists)")
        
        if lidarr_rg_count != rg_stats['total']:
            diff = lidarr_rg_count - rg_stats['total']
            print(f"   📊 Ledger sync: {abs(diff)} release groups {'ahead' if diff < 0 else 'behind'} Lidarr")
        
        # Show staleness information if recheck is enabled
        if rg_stats['recheck_enabled'] and rg_stats['stale_entries'] > 0:
            print(f"   ⏰ Stale cache entries: {rg_stats['stale_entries']:,} (older than {cfg.get('cache_recheck_hours', 72)} hours)")
            if rg_stats['next_recheck_hours'] > 0:
                print(f"   🔄 Next recheck in: {rg_stats['next_recheck_hours']:.1f} hours")
        elif rg_stats['recheck_enabled']:
            print(f"   ⏰ All cache entries are fresh (< {cfg.get('cache_recheck_hours', 72)} hours)")
        
        print()
        
        # Processing efficiency insights
        if rg_stats['total'] > 0:
            eligible_percent = (rg_stats['eligible_for_processing'] / rg_stats['total']) * 100
            print("📈 PROCESSING INSIGHTS:")
            print(f"   Artist cache coverage enables {eligible_percent:.1f}% of RGs for processing")
            if artist_stats['success_rate'] < 80:
                remaining_artists = artist_stats['timeout'] + artist_stats['pending']
                print(f"   💡 Tip: {remaining_artists:,} more artists could unlock additional RGs")
        print()
    
    else:
        print("💿 RELEASE GROUP PROCESSING: Disabled")
        print("   Enable with: process_release_groups = true")
        print()
    
    # Storage efficiency
    storage_type = cfg.get("storage_type", "csv")
    total_entities = artist_stats['total'] + rg_stats.get('total', 0) if cfg.get("process_release_groups") else artist_stats['total']
    
    print("💾 STORAGE INFORMATION:")
    print(f"   Backend: {storage_type.upper()}")
    print(f"   Total entities tracked: {total_entities:,}")
    
    if storage_type == "csv" and total_entities > 1000:
        print("   💡 Tip: Consider switching to SQLite for better performance with large libraries")
        print("        storage_type = sqlite")
    elif storage_type == "sqlite":
        print("   ⚡ Optimized for large libraries with indexed queries")
    
    print()
    
    # Connection health check
    if not cfg.get("verify_ssl", True):
        print("⚠️ SSL VERIFICATION: Disabled")
        print("   WARNING: Only use this in trusted private networks")
        print()
    
    # Unicode processing check
    if cfg.get('artist_textsearch_transliterate_unicode', True):
        try:
            from unidecode import unidecode
            print("🌐 UNICODE SUPPORT: Enabled (unidecode available)")
            print("   International artists will be transliterated for better search results")
        except ImportError:
            print("⚠️ UNICODE SUPPORT: Enabled but unidecode missing!")
            print("   Install with: pip install unidecode")
            print("   Falling back to basic normalization (may not work well)")
        print()
    
    # Check for deprecated option usage
    if cfg.get('artist_textsearch_remove_symbols', False):
        print("⚠️ DEPRECATED OPTION DETECTED:")
        print("   artist_textsearch_remove_symbols is deprecated and may damage non-Latin text")
        print("   Please update config.ini to use artist_textsearch_transliterate_unicode=true instead")
        print()
    
    # Show canary analysis if requested
    if show_canary_stats:
        print_canary_analysis(storage)
    
    # Next steps recommendations
    print("🚀 RECOMMENDATIONS:")
    
    if artist_stats['pending'] > 0:
        print(f"   • Run cache warmer to process {artist_stats['pending']:,} pending artists")
    
    if cfg.get("process_artist_textsearch") and artist_stats['text_search_pending'] > 0:
        print(f"   • Process {artist_stats['text_search_pending']:,} pending text searches")
    
    if cfg.get("process_release_groups") and rg_stats.get('pending', 0) > 0:
        eligible_pending = min(rg_stats['pending'], rg_stats['eligible_for_processing'])
        if eligible_pending > 0:
            print(f"   • Process {eligible_pending:,} eligible release groups")
    
    if artist_stats['success_rate'] > 90 and not cfg.get("process_release_groups"):
        print("   • Consider enabling release group processing: process_release_groups = true")
    
    if not cfg.get("process_artist_textsearch") and artist_stats['success_rate'] > 80:
        print("   • Consider enabling text search warming: process_artist_textsearch = true")
    
    if total_entities > 1000 and storage_type == "csv":
        print("   • Switch to SQLite for better performance: storage_type = sqlite")
    
    # Show stale entries recommendations
    if artist_stats['recheck_enabled']:
        if artist_stats['stale_mbid_cache'] > 0:
            print(f"   • Process {artist_stats['stale_mbid_cache']:,} stale MBID cache entries")
        if artist_stats['stale_text_search'] > 0:
            print(f"   • Process {artist_stats['stale_text_search']:,} stale text search entries")
        if cfg.get("process_release_groups") and rg_stats.get('stale_entries', 0) > 0:
            print(f"   • Process {rg_stats['stale_entries']:,} stale release group entries")
    
    # Show phase processing order
    phases_enabled = []
    if artist_stats['pending'] > 0 or (artist_stats['recheck_enabled'] and artist_stats['stale_mbid_cache'] > 0):
        phases_enabled.append("Phase 1: Artist MBID warming")
    if cfg.get("process_artist_textsearch") and (artist_stats['text_search_pending'] > 0 or (artist_stats['recheck_enabled'] and artist_stats['stale_text_search'] > 0)):
        phases_enabled.append("Phase 2: Text search warming")  
    if cfg.get("process_release_groups") and (rg_stats.get('pending', 0) > 0 or (rg_stats['recheck_enabled'] and rg_stats.get('stale_entries', 0) > 0)):
        phases_enabled.append("Phase 3: Release group warming")
    
    if phases_enabled:
        print(f"   • Next run will execute: {', '.join(phases_enabled)}")
    
    if not show_canary_stats and storage_type == "sqlite":
        print("   • Run with --canary-stats for detailed canary target breakdown")
        print("   • Run with --cf-cache-stats for CloudFlare cache performance analysis")
    
    print()
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate statistics report for Lidarr cache warmer"
    )
    parser.add_argument("--config", required=True, help="Path to INI config (e.g., /data/config.ini)")
    parser.add_argument("--canary-stats", action="store_true", 
                        help="Include detailed canary response target analysis (requires SQLite storage)")
    parser.add_argument("--cf-cache-stats", action="store_true",
                        help="Include detailed CloudFlare cache status analysis (requires SQLite storage)")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"ERROR loading config: {e}", file=sys.stderr)
        sys.exit(2)

    # Validate configuration
    config_issues = validate_config(cfg)
    if config_issues:
        print("Configuration issues found:", file=sys.stderr)
        for issue in config_issues:
            print(f"  - {issue}", file=sys.stderr)
        sys.exit(2)

    # Warn about analysis limitations
    storage_type = cfg.get("storage_type", "csv").lower()
    
    if args.canary_stats and storage_type != "sqlite":
        print("⚠️ WARNING: --canary-stats with CSV storage provides very limited data!", file=sys.stderr)
        print("", file=sys.stderr)
        print("   CSV storage only shows current canary targets from ledger entries.", file=sys.stderr)
        print("   You will NOT get:", file=sys.stderr)
        print("   • Success rate breakdowns by canary target", file=sys.stderr)
        print("   • Historical canary response tracking", file=sys.stderr)
        print("   • Time-based canary analysis", file=sys.stderr)
        print("   • Detailed operation breakdowns", file=sys.stderr)
        print("", file=sys.stderr)
        print("   For full canary analysis, switch to SQLite storage:", file=sys.stderr)
        print("   1. Update config.ini: storage_type = sqlite", file=sys.stderr)
        print("   2. Run cache warmer to start collecting detailed data", file=sys.stderr)
        print("   3. Re-run stats with --canary-stats for full breakdown", file=sys.stderr)
        print("", file=sys.stderr)
        print("   Continuing with limited CSV analysis...", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

    if args.cf_cache_stats and storage_type != "sqlite":
        print("⚠️ WARNING: --cf-cache-stats with CSV storage provides very limited data!", file=sys.stderr)
        print("", file=sys.stderr)
        print("   CSV storage only shows current CF cache status from ledger entries.", file=sys.stderr)
        print("   You will NOT get:", file=sys.stderr)
        print("   • Cache hit rate analysis over time", file=sys.stderr)
        print("   • Historical cache performance tracking", file=sys.stderr)
        print("   • Backend load impact analysis", file=sys.stderr)
        print("   • Detailed operation breakdowns", file=sys.stderr)
        print("", file=sys.stderr)
        print("   For full CF cache analysis, switch to SQLite storage:", file=sys.stderr)
        print("   1. Update config.ini: storage_type = sqlite", file=sys.stderr)
        print("   2. Run cache warmer to start collecting detailed data", file=sys.stderr)
        print("   3. Re-run stats with --cf-cache-stats for full breakdown", file=sys.stderr)
        print("", file=sys.stderr)
        print("   Continuing with limited CSV analysis...", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

    print_stats_report(cfg, args.canary_stats)
    
    # Show CF cache analysis if requested
    if args.cf_cache_stats:
        # Create storage backend to access CF cache data
        try:
            storage = create_storage_backend(cfg)
            print_cf_cache_analysis(storage)
        except Exception as e:
            print(f"❌ Error accessing storage for CF cache analysis: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
