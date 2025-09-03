#!/usr/bin/env python3
import argparse
import os
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


def print_stats_report(cfg: dict):
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
        print(f"⚠️  WARNING: Could not fetch Lidarr data: {e}")
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
            print(f"   ⚙️  Text processing: {', '.join(text_processing_options)}")
        else:
            print(f"   ⚙️  Text processing: disabled (original names)")
        
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
        print("⚠️  SSL VERIFICATION: Disabled")
        print("   WARNING: Only use this in trusted private networks")
        print()
    
    # Unicode processing check
    if cfg.get('artist_textsearch_transliterate_unicode', False):
        try:
            from unidecode import unidecode
            print("🌍 UNICODE SUPPORT: Enabled (unidecode available)")
            print("   International artists will be transliterated for better search results")
        except ImportError:
            print("⚠️  UNICODE SUPPORT: Enabled but unidecode missing!")
            print("   Install with: pip install unidecode")
            print("   Falling back to basic normalization (may not work well)")
        print()
    
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
    
    print()
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Generate statistics report for Lidarr cache warmer"
    )
    parser.add_argument("--config", required=True, help="Path to INI config (e.g., /data/config.ini)")
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

    print_stats_report(cfg)


if __name__ == "__main__":
    main()
