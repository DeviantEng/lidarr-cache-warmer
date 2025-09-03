#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from config import load_config, validate_config
from storage import create_storage_backend, iso_now
from process_manual_entries import process_manual_entries
from colors import Colors


def get_lidarr_artists(base_url: str, api_key: str, verify_ssl: bool = True, timeout: int = 60) -> List[Dict]:
    """Fetch artists from Lidarr and return a list of dicts with {id, name, mbid}."""
    session = requests.Session()
    headers = {"X-Api-Key": api_key}
    
    # Configure SSL verification
    session.verify = verify_ssl
    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Ensure base_url ends with / for proper URL joining
    if not base_url.endswith('/'):
        base_url += '/'

    candidates = [
        "api/v1/artist",
        "api/artist", 
        "api/v3/artist",
    ]

    last_exc = None
    for path in candidates:
        # Use urljoin to properly handle subpaths
        url = urljoin(base_url, path)
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            artists = []
            for a in data:
                mbid = a.get("foreignArtistId") or a.get("mbId") or a.get("mbid")
                name = a.get("artistName") or a.get("name") or "Unknown"
                lidarr_id = a.get("id")
                if mbid:
                    artists.append({"id": lidarr_id, "name": name, "mbid": mbid})
            return artists
        except Exception as e:
            last_exc = e
            continue

    raise RuntimeError(
        f"Could not fetch artists from Lidarr using known endpoints. Last error: {last_exc}"
    )


def get_lidarr_release_groups(base_url: str, api_key: str, verify_ssl: bool = True, timeout: int = 60) -> List[Dict]:
    """Fetch release groups from Lidarr and return a list of dicts with album info."""
    session = requests.Session()
    headers = {"X-Api-Key": api_key}
    
    # Configure SSL verification
    session.verify = verify_ssl
    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Ensure base_url ends with / for proper URL joining
    if not base_url.endswith('/'):
        base_url += '/'

    candidates = [
        "api/v1/album",
        "api/album", 
        "api/v3/album",
    ]

    last_exc = None
    for path in candidates:
        # Use urljoin to properly handle subpaths
        url = urljoin(base_url, path)
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            release_groups = []
            for album in data:
                rg_mbid = album.get("foreignAlbumId") or album.get("mbId") or album.get("mbid")
                album_title = album.get("title") or "Unknown Album"
                artist_mbid = album.get("artist", {}).get("foreignArtistId") if album.get("artist") else None
                artist_name = album.get("artist", {}).get("artistName") if album.get("artist") else "Unknown Artist"
                
                if rg_mbid and artist_mbid:
                    release_groups.append({
                        "rg_mbid": rg_mbid,
                        "rg_title": album_title,
                        "artist_mbid": artist_mbid,
                        "artist_name": artist_name
                    })
            return release_groups
        except Exception as e:
            last_exc = e
            continue

    raise RuntimeError(
        f"Could not fetch release groups from Lidarr using known endpoints. Last error: {last_exc}"
    )


def check_and_handle_various_artists(artists: List[Dict], cfg: dict) -> tuple[List[Dict], bool]:
    """
    Check for Various Artists (89ad4ac3-39f7-470e-963a-56509c546377) and filter it out.
    Returns (filtered_artists_list_without_various_artists, various_artists_detected).
    """
    VARIOUS_ARTISTS_MBID = "89ad4ac3-39f7-470e-963a-56509c546377"
    
    # Find Various Artists in the list
    various_artists_entry = None
    filtered_artists = []
    various_artists_detected = False
    
    for artist in artists:
        if artist["mbid"] == VARIOUS_ARTISTS_MBID:
            various_artists_entry = artist
            various_artists_detected = True
            print(f"🚨 DETECTED: Various Artists in library!")
            print(f"   Artist: {artist['name']} (ID: {artist['id']}, MBID: {artist['mbid']})")
            print(f"   This artist typically has 100,000+ albums and causes severe performance issues.")
            print(f"⚠️  SKIPPING Various Artists - will not be processed for cache warming")
            print(f"   Various Artists and its albums will be excluded from all processing")
        else:
            filtered_artists.append(artist)
    
    if various_artists_detected:
        albums_count = "unknown number of"
        try:
            # Try to get a rough count of albums for user information
            from collections import Counter
            # This is just for user feedback, we don't actually fetch the data
            print(f"📊 Various Artists typically contains 100,000+ albums")
        except:
            pass
        
        print(f"✅ Various Artists filtered out - will not impact cache warming performance")
        print(f"   Cache warming will process {len(filtered_artists)} other artists")
    
    return filtered_artists, various_artists_detected


def filter_release_groups_by_artist(release_groups: List[Dict], allowed_artist_mbids: set) -> List[Dict]:
    """Filter out release groups from excluded artists (like Various Artists)"""
    VARIOUS_ARTISTS_MBID = "89ad4ac3-39f7-470e-963a-56509c546377"
    
    filtered_rgs = []
    excluded_count = 0
    
    for rg in release_groups:
        artist_mbid = rg.get("artist_mbid", "")
        if artist_mbid == VARIOUS_ARTISTS_MBID:
            excluded_count += 1
        elif artist_mbid in allowed_artist_mbids:
            filtered_rgs.append(rg)
    
    if excluded_count > 0:
        print(f"🚫 Excluded {excluded_count:,} release groups from Various Artists")
    
    return filtered_rgs


def trigger_lidarr_refresh(base_url: str, api_key: str, artist_id: Optional[int], verify_ssl: bool = True, timeout: int = 5) -> None:
    """Fire-and-forget refresh request to Lidarr for the given artist id."""
    if artist_id is None:
        return
    session = requests.Session()
    headers = {"X-Api-Key": api_key}
    
    # Configure SSL verification
    session.verify = verify_ssl
    if not verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Ensure base_url ends with / for proper URL joining
    if not base_url.endswith('/'):
        base_url += '/'
        
    payloads = [
        {"name": "RefreshArtist", "artistIds": [artist_id]},
        {"name": "RefreshArtist", "artistId": artist_id},
    ]
    for path in ("api/v1/command", "api/command"):
        # Use urljoin to properly handle subpaths
        url = urljoin(base_url, path)
        for body in payloads:
            try:
                session.post(url, headers=headers, json=body, timeout=timeout)
                return
            except Exception:
                continue


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


def check_api_health(target_base_url: str, timeout: int = 10) -> dict:
    """Pre-flight check of the target API"""
    health_info = {
        "available": False,
        "response_time_ms": None,
        "status_code": None,
        "error": None
    }
    
    try:
        start_time = time.time()
        response = requests.get(target_base_url, timeout=timeout)
        health_info["response_time_ms"] = (time.time() - start_time) * 1000
        health_info["status_code"] = response.status_code
        health_info["available"] = response.status_code < 500
        
    except Exception as e:
        health_info["error"] = str(e)
    
    return health_info


def main():
    parser = argparse.ArgumentParser(
        description="Lidarr cache warmer - warm API caches for artists and release groups."
    )
    parser.add_argument("--config", required=True, help="Path to INI config (e.g., ./data/config.ini)")
    parser.add_argument("--force-artists", action="store_true",
                        help="Force re-check all artists (sets max_attempts_per_artist=1)")
    parser.add_argument("--force-rg", action="store_true",
                        help="Force re-check all release groups (sets max_attempts_per_rg=1)")
    parser.add_argument("--force-text-search", action="store_true",
                        help="Force re-check all text searches for artists")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making API calls")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"ERROR loading config: {e}", file=sys.stderr)
        sys.exit(2)

    # Check if this is a first run (no existing storage)
    storage = create_storage_backend(cfg)
    is_first_run = not storage.exists()
    
    if is_first_run:
        print("🔍 First run detected - no existing storage found")
        print("   Enabling force modes for initial cache discovery (1 attempt per entity)")
        cfg["force_artists"] = True
        cfg["max_attempts_per_artist"] = 1
        
        # Also enable text search discovery on first run if configured
        if cfg.get("process_artist_textsearch", True):
            cfg["force_text_search"] = True
            cfg["max_attempts_per_artist_textsearch"] = 1
            print("   Text search discovery enabled (1 attempt per artist)")
        
        # Also enable release group discovery on first run if configured
        if cfg.get("process_release_groups", False):
            cfg["force_rg"] = True  
            cfg["max_attempts_per_rg"] = 1
            print("   Release group discovery enabled (1 attempt per release group)")

    # Apply CLI overrides for force modes
    if args.force_artists:
        cfg["force_artists"] = True
        cfg["max_attempts_per_artist"] = 1
        print("Force artists mode enabled: max_attempts_per_artist set to 1 for quick refresh.")
    
    if args.force_rg:
        cfg["force_rg"] = True
        cfg["max_attempts_per_rg"] = 1
        print("Force release groups mode enabled: max_attempts_per_rg set to 1 for quick refresh.")

    if args.force_text_search:
        cfg["force_text_search"] = True
        print("Force text search mode enabled: will re-check all text searches.")

    # Apply config file force modes (should also set attempts to 1)
    elif cfg.get("force_artists", False):
        cfg["max_attempts_per_artist"] = 1
        print("Force artists mode enabled from config: max_attempts_per_artist set to 1 for quick refresh.")
        
    if cfg.get("force_rg", False) and not args.force_rg:
        cfg["max_attempts_per_rg"] = 1
        print("Force release groups mode enabled from config: max_attempts_per_rg set to 1 for quick refresh.")

    # Validate configuration
    config_issues = validate_config(cfg)
    if config_issues:
        print("Configuration issues found:", file=sys.stderr)
        for issue in config_issues:
            print(f"  - {issue}", file=sys.stderr)
        sys.exit(2)

    # Show SSL verification status
    if not cfg.get("verify_ssl", True):
        print("⚠️  SSL certificate verification is DISABLED")
        print("   This should only be used in trusted private networks")
    
    # Show timeout configuration
    timeout_value = cfg.get("lidarr_timeout", 60)
    print(f"Lidarr API timeout: {timeout_value} seconds")
    
    # Pre-flight API health check
    print("Performing API health check...")
    api_health = check_api_health(cfg["target_base_url"])
    if api_health["available"]:
        print(f"✅ Lidarr Metadata API is healthy (response time: {api_health['response_time_ms']:.1f}ms)")
    else:
        print(f"⚠️  Lidarr Metadata API health check failed: {api_health.get('error', 'Unknown error')}")
        if not args.dry_run:
            print("Continuing anyway, but expect potential issues...")

    # Fetch data from Lidarr
    try:
        print("Fetching artists from Lidarr...")
        artists = get_lidarr_artists(cfg["lidarr_url"], cfg["api_key"], cfg.get("verify_ssl", True), cfg["lidarr_timeout"])
        
        # Check for and filter out Various Artists
        artists, various_artists_detected = check_and_handle_various_artists(artists, cfg)
        
        print(f"✅ Found {len(artists)} artists in Lidarr (after filtering)")
        
        if cfg["process_release_groups"]:
            print("Fetching release groups from Lidarr...")
            release_groups = get_lidarr_release_groups(cfg["lidarr_url"], cfg["api_key"], cfg.get("verify_ssl", True), cfg["lidarr_timeout"])
            
            # Filter out release groups from Various Artists
            allowed_artist_mbids = {artist["mbid"] for artist in artists}
            release_groups = filter_release_groups_by_artist(release_groups, allowed_artist_mbids)
            
            print(f"✅ Found {len(release_groups)} release groups in Lidarr (after filtering)")
        else:
            release_groups = []
            
    except Exception as e:
        print(f"ERROR fetching data from Lidarr: {e}", file=sys.stderr)
        sys.exit(2)

    # Load existing ledgers using storage backend
    artists_ledger = storage.read_artists_ledger()
    rg_ledger = storage.read_release_groups_ledger()

    # Phase 0.2: Process Manual Entries (if enabled)
    manual_stats = process_manual_entries(cfg, artists_ledger, rg_ledger)

    # Update artists ledger with current Lidarr data
    artists_new_count = 0
    for artist in artists:
        mbid = artist["mbid"]
        name = artist["name"]
        if mbid not in artists_ledger:
            artists_ledger[mbid] = {
                "mbid": mbid,
                "artist_name": name,
                "status": "",
                "attempts": 0,
                "last_status_code": "",
                "last_checked": "",
                # New text search fields
                "text_search_attempted": False,
                "text_search_success": False,
                "text_search_last_checked": "",
            }
            artists_new_count += 1
        else:
            # Update name if changed
            if name and artists_ledger[mbid].get("artist_name") != name:
                artists_ledger[mbid]["artist_name"] = name
            
            # Add text search fields if missing (for existing records)
            if "text_search_attempted" not in artists_ledger[mbid]:
                artists_ledger[mbid]["text_search_attempted"] = False
                artists_ledger[mbid]["text_search_success"] = False
                artists_ledger[mbid]["text_search_last_checked"] = ""

    # Update release groups ledger with current Lidarr data
    rg_new_count = 0
    if cfg["process_release_groups"]:
        for rg in release_groups:
            rg_mbid = rg["rg_mbid"]
            if rg_mbid not in rg_ledger:
                rg_ledger[rg_mbid] = {
                    "rg_mbid": rg_mbid,
                    "rg_title": rg["rg_title"],
                    "artist_mbid": rg["artist_mbid"],
                    "artist_name": rg["artist_name"],
                    "artist_cache_status": artists_ledger.get(rg["artist_mbid"], {}).get("status", ""),
                    "status": "",
                    "attempts": 0,
                    "last_status_code": "",
                    "last_checked": "",
                }
                rg_new_count += 1
            else:
                # Update fields if changed
                rg_ledger[rg_mbid]["rg_title"] = rg["rg_title"]
                rg_ledger[rg_mbid]["artist_name"] = rg["artist_name"]
                rg_ledger[rg_mbid]["artist_cache_status"] = artists_ledger.get(rg["artist_mbid"], {}).get("status", "")

    # Write updated ledgers using storage backend
    storage.write_artists_ledger(artists_ledger)
    if cfg["process_release_groups"]:
        storage.write_release_groups_ledger(rg_ledger)

    print(f"Updated artists ledger: {len(artists)} total ({artists_new_count} new)")
    if cfg["process_release_groups"]:
        print(f"Updated release groups ledger: {len(rg_ledger)} total ({rg_new_count} new)")
    
    # Show manual entries summary if processed
    if manual_stats["enabled"]:
        if manual_stats["errors"] > 0:
            print(f"⚠️  Manual entries had {manual_stats['errors']} validation errors")
        elif manual_stats["artists_new"] > 0 or manual_stats["release_groups_new"] > 0:
            print(f"🔧 Manual entries: +{manual_stats['artists_new']} artists, +{manual_stats['release_groups_new']} release groups")

    if args.dry_run:
        print("\n🧪 DRY RUN MODE - No API calls will be made")
        
        # Show what artists would be processed
        artists_to_check = [mbid for mbid, row in artists_ledger.items() 
                           if (cfg["force_artists"] or 
                               row.get("status", "").lower() not in ("success",) or
                               is_stale(row.get("last_checked", ""), cfg["cache_recheck_hours"]))]
        
        # Calculate breakdown for dry run
        force_count = sum(1 for mbid in artists_to_check if cfg["force_artists"])
        pending_count = sum(1 for mbid in artists_to_check 
                           if mbid in artists_ledger and artists_ledger[mbid].get("status", "").lower() not in ("success",))
        stale_count = sum(1 for mbid in artists_to_check 
                         if mbid in artists_ledger and 
                            artists_ledger[mbid].get("status", "").lower() == "success" and
                            is_stale(artists_ledger[mbid].get("last_checked", ""), cfg["cache_recheck_hours"]))
        
        print(f"Would process {len(artists_to_check)} artists for MBID warming")
        if force_count > 0:
            print(f"   - {force_count} forced re-checks")
        if pending_count > 0:
            print(f"   - {pending_count} pending/failed artists")
        if stale_count > 0:
            print(f"   - {stale_count} stale entries (older than {cfg['cache_recheck_hours']} hours)")
        
        # Show text search candidates
        if cfg["process_artist_textsearch"]:
            text_search_to_check = [mbid for mbid, row in artists_ledger.items()
                                   if row.get("artist_name", "").strip() and
                                      (cfg["force_text_search"] or not row.get("text_search_success", False))]
            print(f"Would process {len(text_search_to_check)} artists for text search warming")
        
        # Show manual entries in dry run
        if manual_stats["enabled"] and manual_stats["file_found"]:
            print(f"Manual entries loaded: {manual_stats['artists_new'] + manual_stats['artists_updated']} artists, {manual_stats['release_groups_new'] + manual_stats['release_groups_updated']} release groups")
        
        if cfg["process_release_groups"]:
            rgs_to_check = [rg_mbid for rg_mbid, row in rg_ledger.items()
                           if row.get("artist_cache_status", "").lower() == "success" and 
                              (cfg["force_rg"] or row.get("status", "").lower() not in ("success",))]
            print(f"Would process {len(rgs_to_check)} release groups")
        return

    # Phase 1: Process Artists (MBID Cache Warming)
    print(f"\n{Colors.bold('=== Phase 1: Artist MBID Cache Warming ===', cfg.get('colored_output', True))}")
    
    # Import and run artist processing
    try:
        from process_artists import process_artists
        artists_to_check = [mbid for mbid, row in artists_ledger.items() 
                           if (cfg["force_artists"] or 
                               row.get("status", "").lower() not in ("success",) or
                               is_stale(row.get("last_checked", ""), cfg["cache_recheck_hours"]))]
        
        if len(artists_to_check) > 0:
            # Show breakdown of why artists are being checked
            force_count = sum(1 for mbid in artists_to_check if cfg["force_artists"])
            pending_count = sum(1 for mbid in artists_to_check 
                               if mbid in artists_ledger and artists_ledger[mbid].get("status", "").lower() not in ("success",))
            stale_count = sum(1 for mbid in artists_to_check 
                             if mbid in artists_ledger and 
                                artists_ledger[mbid].get("status", "").lower() == "success" and
                                is_stale(artists_ledger[mbid].get("last_checked", ""), cfg["cache_recheck_hours"]))
            
            print(f"Will process {len(artists_to_check)} artists for MBID cache warming")
            if force_count > 0:
                print(f"   - {force_count} forced re-checks")
            if pending_count > 0:
                print(f"   - {pending_count} pending/failed artists")
            if stale_count > 0:
                print(f"   - {stale_count} stale entries (older than {cfg['cache_recheck_hours']} hours)")
            
            artist_results = process_artists(artists_to_check, artists_ledger, cfg, storage)
            print(f"Artist MBID warming complete: {artist_results}")
        else:
            print("No artists to process for MBID warming - all already successful and fresh")
            artist_results = {"transitioned": 0, "new_successes": 0, "new_failures": 0}
            
    except ImportError:
        print("ERROR: process_artists.py not found", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"ERROR in artist MBID processing: {e}", file=sys.stderr)
        sys.exit(2)

    # Phase 2: Process Text Search Cache Warming (if enabled)
    if cfg["process_artist_textsearch"]:
        print(f"\n=== Phase 2: Artist Text Search Cache Warming ===")
        
        # Re-read artists ledger to get updated statuses from Phase 1
        artists_ledger = storage.read_artists_ledger()
        
        try:
            from process_artist_textsearch import process_text_search
            
            # Only do text search for artists that have names and meet criteria
            text_search_to_check = [mbid for mbid, row in artists_ledger.items()
                                   if row.get("artist_name", "").strip() and
                                      (cfg["force_text_search"] or 
                                       not row.get("text_search_success", False) or
                                       is_stale(row.get("text_search_last_checked", ""), cfg["cache_recheck_hours"]))]
            
            if len(text_search_to_check) > 0:
                # Show breakdown of text search reasons
                force_count = sum(1 for mbid in text_search_to_check if cfg["force_text_search"])
                pending_count = sum(1 for mbid in text_search_to_check 
                                   if mbid in artists_ledger and not artists_ledger[mbid].get("text_search_success", False))
                stale_count = sum(1 for mbid in text_search_to_check 
                                 if mbid in artists_ledger and 
                                    artists_ledger[mbid].get("text_search_success", False) and
                                    is_stale(artists_ledger[mbid].get("text_search_last_checked", ""), cfg["cache_recheck_hours"]))
                
                print(f"Will process {len(text_search_to_check)} artists for text search cache warming")
                if force_count > 0:
                    print(f"   - {force_count} forced re-checks")
                if pending_count > 0:
                    print(f"   - {pending_count} pending/failed text searches")
                if stale_count > 0:
                    print(f"   - {stale_count} stale text searches (older than {cfg['cache_recheck_hours']} hours)")
                
                text_search_results = process_text_search(text_search_to_check, artists_ledger, cfg, storage)
                print(f"Text search warming complete: {text_search_results}")
            else:
                print("No artists to process for text search warming")
                text_search_results = {"new_successes": 0, "new_failures": 0}
                
        except ImportError:
            print("ERROR: process_artist_textsearch.py not found", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"ERROR in text search processing: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        print("Artist text search processing disabled in config")
        text_search_results = {"new_successes": 0, "new_failures": 0}

    # Phase 3: Process Release Groups (if enabled)
    if cfg["process_release_groups"]:
        print(f"\n=== Phase 3: Release Group Cache Warming ===")
        
        # Re-read artists ledger to get updated statuses, then update RG artist statuses
        artists_ledger = storage.read_artists_ledger()
        
        # Use efficient SQLite update if available, otherwise update in memory
        if hasattr(storage, 'update_release_groups_artist_status'):
            storage.update_release_groups_artist_status(artists_ledger)
        else:
            # Fallback for CSV storage
            for rg_mbid, rg_data in rg_ledger.items():
                artist_mbid = rg_data.get("artist_mbid", "")
                if artist_mbid in artists_ledger:
                    rg_data["artist_cache_status"] = artists_ledger[artist_mbid].get("status", "")
            storage.write_release_groups_ledger(rg_ledger)
        
        try:
            from process_releasegroups import process_release_groups
            
            # Filter RGs: only process those with successful artist cache AND pending RG status
            rgs_to_check = [rg_mbid for rg_mbid, row in rg_ledger.items()
                           if row.get("artist_cache_status", "").lower() == "success" and 
                              (cfg["force_rg"] or 
                               row.get("status", "").lower() not in ("success",) or
                               is_stale(row.get("last_checked", ""), cfg["cache_recheck_hours"]))]
            
            if len(rgs_to_check) > 0:
                # Show breakdown of release group reasons
                force_count = sum(1 for rg_mbid in rgs_to_check if cfg["force_rg"])
                pending_count = sum(1 for rg_mbid in rgs_to_check 
                                   if rg_mbid in rg_ledger and rg_ledger[rg_mbid].get("status", "").lower() not in ("success",))
                stale_count = sum(1 for rg_mbid in rgs_to_check 
                                 if rg_mbid in rg_ledger and 
                                    rg_ledger[rg_mbid].get("status", "").lower() == "success" and
                                    is_stale(rg_ledger[rg_mbid].get("last_checked", ""), cfg["cache_recheck_hours"]))
                
                print(f"Will process {len(rgs_to_check)} release groups (from successfully cached artists)")
                if force_count > 0:
                    print(f"   - {force_count} forced re-checks")
                if pending_count > 0:
                    print(f"   - {pending_count} pending/failed release groups")
                if stale_count > 0:
                    print(f"   - {stale_count} stale entries (older than {cfg['cache_recheck_hours']} hours)")
                
                rg_results = process_release_groups(rgs_to_check, rg_ledger, cfg, storage)
                print(f"Release groups phase complete: {rg_results}")
            else:
                print("No release groups to process")
                rg_results = {"transitioned": 0, "new_successes": 0, "new_failures": 0}
                
        except ImportError:
            print("ERROR: process_releasegroups.py not found", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"ERROR in release group processing: {e}", file=sys.stderr)
            sys.exit(2)
    else:
        print("Release group processing disabled in config")
        rg_results = {"transitioned": 0, "new_successes": 0, "new_failures": 0}

    # Final summary
    print(f"\n=== Final Summary ===")
    print(f"Artist MBID Warming: {artist_results}")
    if cfg["process_artist_textsearch"]:
        print(f"Text Search Warming: {text_search_results}")
    if cfg["process_release_groups"]:
        print(f"Release Groups: {rg_results}")
    
    # Write simple results log
    try:
        # Use config directory + data subdirectory for results log
        config_dir = os.path.dirname(os.path.abspath(args.config))
        results_dir = os.path.join(config_dir, "data") if config_dir else "./data"
        os.makedirs(results_dir, exist_ok=True)
        
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_path = os.path.join(results_dir, f"results_{ts}.log")
        
        # Calculate final stats
        total_artist_successes = sum(1 for r in artists_ledger.values() if r.get("status") == "success")
        total_artist_timeouts = sum(1 for r in artists_ledger.values() if r.get("status") == "timeout")
        total_text_search_attempted = sum(1 for r in artists_ledger.values() if r.get("text_search_attempted", False))
        total_text_search_successes = sum(1 for r in artists_ledger.values() if r.get("text_search_success", False))
        
        if cfg["process_release_groups"]:
            total_rg_successes = sum(1 for r in rg_ledger.values() if r.get("status") == "success")
            total_rg_timeouts = sum(1 for r in rg_ledger.values() if r.get("status") == "timeout")
        else:
            total_rg_successes = 0
            total_rg_timeouts = 0
        
        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(f"finished_at_utc={iso_now()}\n")
            lf.write(f"artists_success={total_artist_successes}\n")
            lf.write(f"artists_timeout={total_artist_timeouts}\n")
            lf.write(f"artists_total={len(artists_ledger)}\n")
            lf.write(f"text_search_attempted={total_text_search_attempted}\n")
            lf.write(f"text_search_success={total_text_search_successes}\n")
            lf.write(f"rg_success={total_rg_successes}\n")
            lf.write(f"rg_timeout={total_rg_timeouts}\n")
            lf.write(f"rg_total={len(rg_ledger)}\n")
            lf.write(f"force_artists={'true' if cfg['force_artists'] else 'false'}\n")
            lf.write(f"force_rg={'true' if cfg['force_rg'] else 'false'}\n")
            lf.write(f"force_text_search={'true' if cfg['force_text_search'] else 'false'}\n")
            lf.write(f"process_release_groups={'true' if cfg['process_release_groups'] else 'false'}\n")
            lf.write(f"process_artist_textsearch={'true' if cfg['process_artist_textsearch'] else 'false'}\n")
            lf.write(f"process_manual_entries={'true' if cfg.get('process_manual_entries', False) else 'false'}\n")
            lf.write(f"manual_artists_added={manual_stats.get('artists_new', 0)}\n")
            lf.write(f"manual_rgs_added={manual_stats.get('release_groups_new', 0)}\n")
            lf.write(f"lidarr_refreshes_triggered={artist_results.get('transitioned', 0)}\n")
            lf.write(f"verify_ssl={'true' if cfg.get('verify_ssl', True) else 'false'}\n")
            lf.write(f"lidarr_timeout={cfg.get('lidarr_timeout', 60)}\n")
            
        print(f"Results log written to: {log_path}")
    except Exception as e:
        print(f"WARNING: Failed to write results log: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
