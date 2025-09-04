#!/usr/bin/env python3
import os
import re
import yaml
from typing import Dict, List, Tuple, Optional


def validate_mbid_format(mbid: str) -> bool:
    """Validate that MBID is a proper UUID format"""
    if not mbid or not isinstance(mbid, str):
        return False
    
    # UUID format: 8-4-4-4-12 hexadecimal characters
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, mbid.lower()))


def load_manual_entries(file_path: str) -> Tuple[Dict, List[str]]:
    """
    Load and validate manual entries from YAML file.
    Returns: (parsed_data, errors)
    """
    errors = []
    
    if not os.path.exists(file_path):
        return {}, [f"Manual entries file not found: {file_path}"]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return {}, [f"Invalid YAML format: {e}"]
    except Exception as e:
        return {}, [f"Error reading file: {e}"]
    
    if not isinstance(data, dict):
        return {}, ["YAML file must contain a dictionary with artist MBIDs as keys"]
    
    # Validate structure
    for artist_mbid, artist_data in data.items():
        if not validate_mbid_format(artist_mbid):
            errors.append(f"Invalid artist MBID format: {artist_mbid}")
            continue
        
        if not isinstance(artist_data, dict):
            errors.append(f"Artist {artist_mbid}: must be a dictionary with 'name' field")
            continue
        
        if 'name' not in artist_data or not artist_data['name'].strip():
            errors.append(f"Artist {artist_mbid}: missing or empty 'name' field")
            continue
        
        # Validate release groups if present
        if 'release-groups' in artist_data:
            rg_list = artist_data['release-groups']
            if not isinstance(rg_list, list):
                errors.append(f"Artist {artist_mbid}: 'release-groups' must be a list")
                continue
            
            for rg_mbid in rg_list:
                if not validate_mbid_format(rg_mbid):
                    errors.append(f"Artist {artist_mbid}: invalid release group MBID format: {rg_mbid}")
    
    return data, errors


def inject_manual_artists(
    manual_data: Dict, 
    artists_ledger: Dict[str, Dict]
) -> Tuple[int, int]:
    """
    Inject manual artists into the artists ledger.
    Returns: (new_artists_count, updated_artists_count)
    """
    new_count = 0
    updated_count = 0
    
    for artist_mbid, artist_info in manual_data.items():
        # Skip if validation failed (should have been caught earlier)
        if not validate_mbid_format(artist_mbid) or 'name' not in artist_info:
            continue
        
        artist_name = artist_info['name'].strip()
        
        if artist_mbid not in artists_ledger:
            # Add new manual artist
            artists_ledger[artist_mbid] = {
                "mbid": artist_mbid,
                "artist_name": artist_name,
                "status": "",
                "attempts": 0,
                "last_status_code": "",
                "last_checked": "",
                "text_search_attempted": False,
                "text_search_success": False,
                "text_search_last_checked": "",
                "manual_entry": True,  # Flag for tracking
                "last_canary_target": "",  # Add canary tracking field
            }
            new_count += 1
        else:
            # Update existing artist (in case name changed)
            if artists_ledger[artist_mbid].get("artist_name") != artist_name:
                artists_ledger[artist_mbid]["artist_name"] = artist_name
                updated_count += 1
            
            # Mark as manual entry and ensure canary field exists
            artists_ledger[artist_mbid]["manual_entry"] = True
            if "last_canary_target" not in artists_ledger[artist_mbid]:
                artists_ledger[artist_mbid]["last_canary_target"] = ""
    
    return new_count, updated_count


def inject_manual_release_groups(
    manual_data: Dict,
    artists_ledger: Dict[str, Dict], 
    rg_ledger: Dict[str, Dict]
) -> Tuple[int, int]:
    """
    Inject manual release groups into the release groups ledger.
    Returns: (new_rg_count, updated_rg_count) 
    """
    new_count = 0
    updated_count = 0
    
    for artist_mbid, artist_info in manual_data.items():
        # Skip if no release groups specified
        if 'release-groups' not in artist_info:
            continue
        
        # Skip if artist validation failed
        if not validate_mbid_format(artist_mbid) or 'name' not in artist_info:
            continue
            
        artist_name = artist_info['name'].strip()
        rg_list = artist_info.get('release-groups', [])
        
        if not isinstance(rg_list, list):
            continue
        
        for rg_mbid in rg_list:
            # Skip invalid RG MBIDs
            if not validate_mbid_format(rg_mbid):
                continue
            
            if rg_mbid not in rg_ledger:
                # Add new manual release group
                rg_ledger[rg_mbid] = {
                    "rg_mbid": rg_mbid,
                    "rg_title": "Manual Entry",  # We don't have the actual title
                    "artist_mbid": artist_mbid,
                    "artist_name": artist_name,
                    "artist_cache_status": artists_ledger.get(artist_mbid, {}).get("status", ""),
                    "status": "",
                    "attempts": 0,
                    "last_status_code": "",
                    "last_checked": "",
                    "manual_entry": True,  # Flag for tracking
                    "last_canary_target": "",  # Add canary tracking field
                }
                new_count += 1
            else:
                # Update existing release group
                if (rg_ledger[rg_mbid].get("artist_name") != artist_name or
                    rg_ledger[rg_mbid].get("artist_mbid") != artist_mbid):
                    rg_ledger[rg_mbid]["artist_name"] = artist_name
                    rg_ledger[rg_mbid]["artist_mbid"] = artist_mbid
                    updated_count += 1
                
                # Mark as manual entry, update artist cache status, and ensure canary field exists
                rg_ledger[rg_mbid]["manual_entry"] = True
                rg_ledger[rg_mbid]["artist_cache_status"] = artists_ledger.get(artist_mbid, {}).get("status", "")
                if "last_canary_target" not in rg_ledger[rg_mbid]:
                    rg_ledger[rg_mbid]["last_canary_target"] = ""
    
    return new_count, updated_count


def process_manual_entries(
    cfg: dict,
    artists_ledger: Dict[str, Dict],
    rg_ledger: Dict[str, Dict]
) -> Dict[str, int]:
    """
    Main function to process manual entries file and inject into ledgers.
    Returns: stats dictionary with counts
    """
    
    if not cfg.get("process_manual_entries", False):
        return {
            "enabled": False,
            "file_found": False,
            "artists_new": 0,
            "artists_updated": 0,
            "release_groups_new": 0,
            "release_groups_updated": 0,
            "errors": 0
        }
    
    file_path = cfg.get("manual_entries_file", "/data/manual_entries.yml")
    
    print(f"🔧 Processing manual entries from: {file_path}")
    
    # Load and validate YAML
    manual_data, errors = load_manual_entries(file_path)
    
    if errors:
        print(f"❌ Manual entries validation errors:")
        for error in errors:
            print(f"   - {error}")
        return {
            "enabled": True,
            "file_found": os.path.exists(file_path),
            "artists_new": 0,
            "artists_updated": 0,
            "release_groups_new": 0,
            "release_groups_updated": 0,
            "errors": len(errors)
        }
    
    if not manual_data:
        print(f"ℹ️  Manual entries file is empty: {file_path}")
        return {
            "enabled": True,
            "file_found": True,
            "artists_new": 0,
            "artists_updated": 0,
            "release_groups_new": 0,
            "release_groups_updated": 0,
            "errors": 0
        }
    
    # Inject artists
    artists_new, artists_updated = inject_manual_artists(manual_data, artists_ledger)
    
    # Inject release groups
    rg_new, rg_updated = inject_manual_release_groups(manual_data, artists_ledger, rg_ledger)
    
    # Log results
    total_artists = len(manual_data)
    total_rgs = sum(len(info.get('release-groups', [])) for info in manual_data.values())
    
    print(f"✅ Manual entries processed successfully:")
    print(f"   - Artists: {artists_new} new, {artists_updated} updated (from {total_artists} in file)")
    print(f"   - Release groups: {rg_new} new, {rg_updated} updated (from {total_rgs} in file)")
    
    return {
        "enabled": True,
        "file_found": True,
        "artists_new": artists_new,
        "artists_updated": artists_updated,
        "release_groups_new": rg_new,
        "release_groups_updated": rg_updated,
        "errors": 0
    }


def get_manual_entries_stats(
    artists_ledger: Dict[str, Dict],
    rg_ledger: Dict[str, Dict]
) -> Dict[str, int]:
    """
    Get statistics about manual entries in the ledgers.
    Returns: stats dictionary
    """
    manual_artists = sum(1 for artist in artists_ledger.values() 
                        if artist.get("manual_entry", False))
    
    manual_rgs = sum(1 for rg in rg_ledger.values() 
                    if rg.get("manual_entry", False))
    
    # Manual artists with successful MBID cache
    manual_artists_success = sum(1 for artist in artists_ledger.values() 
                               if artist.get("manual_entry", False) and 
                                  artist.get("status", "").lower() == "success")
    
    # Manual artists with successful text search
    manual_text_search_success = sum(1 for artist in artists_ledger.values() 
                                   if artist.get("manual_entry", False) and 
                                      artist.get("text_search_success", False))
    
    return {
        "manual_artists_total": manual_artists,
        "manual_artists_mbid_success": manual_artists_success,
        "manual_artists_text_search_success": manual_text_search_success,
        "manual_release_groups_total": manual_rgs
    }
