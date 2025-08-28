#!/usr/bin/env python3
import configparser
import os
import sys
from typing import List

DEFAULT_CONFIG = '''# config.ini
# Generated automatically on first run. Edit and set your Lidarr API key.

[lidarr]
# Default Lidarr URL
base_url = http://192.168.1.103:8686
api_key  = REPLACE_WITH_YOUR_LIDARR_API_KEY

[probe]
# API to probe for each MBID
target_base_url = https://api.lidarr.audio/api/v0.4
timeout_seconds = 10

# Shared API settings
delay_between_attempts = 0.25
max_concurrent_requests = 10
rate_limit_per_second = 5

# Per-entity cache warming settings
max_attempts_per_artist = 25
max_attempts_per_artist_textsearch = 25
max_attempts_per_rg = 15

# Text search processing options
artist_textsearch_lowercase = false
artist_textsearch_remove_symbols = false

# Circuit breaker settings
circuit_breaker_threshold = 50
backoff_factor = 0.5
max_backoff_seconds = 15

[ledger]
# Storage backend: csv (default) or sqlite
storage_type = csv

# CSV file paths (used when storage_type = csv)
artists_csv_path = /data/mbid-artists.csv
release_groups_csv_path = /data/mbid-releasegroups.csv

# SQLite database path (used when storage_type = sqlite)
db_path = /data/mbid_cache.db

[run]
# Processing control - enable/disable each phase
process_release_groups = false
process_artist_textsearch = true
process_manual_entries = false
force_artists = false
force_rg = false
force_text_search = false
batch_size = 25
batch_write_frequency = 5

[manual]
# Manual entry injection from YAML file
manual_entries_file = /data/manual_entries.yml

[actions]
# If true, when a probe transitions from (no status or timeout) -> success,
# trigger a non-blocking refresh of that artist in Lidarr.
update_lidarr = false

[schedule]
# Used by entrypoint.py (scheduler) if you run that directly
interval_seconds = 3600
run_at_start = true
max_runs = 25

[monitoring]
log_progress_every_n = 25
log_level = INFO
'''


def parse_bool(s: str, default: bool = False) -> bool:
    """Parse string to boolean with fallback default"""
    if s is None:
        return default
    return s.strip().lower() in ("1", "true", "yes", "on")


def validate_config(cfg: dict) -> List[str]:
    """Return list of configuration issues"""
    issues = []
    
    # Check required fields
    if not cfg.get("api_key") or "REPLACE_WITH_YOUR" in cfg["api_key"]:
        issues.append("Missing or placeholder Lidarr API key")
    
    # Validate URLs
    for url_key in ["lidarr_url", "target_base_url"]:
        if not cfg.get(url_key, "").startswith(("http://", "https://")):
            issues.append(f"Invalid URL format for {url_key}")
    
    # Check numeric ranges
    if cfg.get("timeout_seconds", 0) < 1:
        issues.append("timeout_seconds must be >= 1")
    
    if cfg.get("rate_limit_per_second", 0) <= 0:
        issues.append("rate_limit_per_second must be > 0")
    
    if cfg.get("max_concurrent_requests", 0) < 1:
        issues.append("max_concurrent_requests must be >= 1")
        
    return issues


def load_config(path: str) -> dict:
    """Load INI config and return a normalized dict of settings with defaults."""
    if not os.path.exists(path) and os.path.exists(path + ".ini"):
        path = path + ".ini"

    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(DEFAULT_CONFIG)
        print(f"Created default config at {path}. Please edit api_key before running again.", file=sys.stderr)
        sys.exit(1)

    cp = configparser.ConfigParser()
    if not cp.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"Config file not found or unreadable: {path}")

    # Load configuration with new structure
    cfg = {
        # Core settings
        "lidarr_url": cp.get("lidarr", "base_url", fallback="http://192.168.1.103:8686"),
        "api_key": cp.get("lidarr", "api_key", fallback=""),
        "target_base_url": cp.get("probe", "target_base_url", fallback="https://api.lidarr.audio/api/v0.4"),
        "timeout_seconds": cp.getint("probe", "timeout_seconds", fallback=10),
        
        # Storage settings
        "storage_type": cp.get("ledger", "storage_type", fallback="csv"),
        "artists_csv_path": cp.get("ledger", "artists_csv_path", fallback="/data/mbid-artists.csv"),
        "release_groups_csv_path": cp.get("ledger", "release_groups_csv_path", fallback="/data/mbid-releasegroups.csv"),
        "db_path": cp.get("ledger", "db_path", fallback="/data/mbid_cache.db"),
        
        # Processing control
        "process_release_groups": parse_bool(cp.get("run", "process_release_groups", fallback="false")),
        "process_artist_textsearch": parse_bool(cp.get("run", "process_artist_textsearch", fallback="true")),
        "process_manual_entries": parse_bool(cp.get("run", "process_manual_entries", fallback="false")),
        "force_artists": parse_bool(cp.get("run", "force_artists", fallback="false")),
        "force_rg": parse_bool(cp.get("run", "force_rg", fallback="false")),
        "force_text_search": parse_bool(cp.get("run", "force_text_search", fallback="false")),
        "update_lidarr": parse_bool(cp.get("actions", "update_lidarr", fallback="false")),
        
        # Manual entries
        "manual_entries_file": cp.get("manual", "manual_entries_file", fallback="/data/manual_entries.yml"),
        
        # Shared API settings
        "delay_between_attempts": cp.getfloat("probe", "delay_between_attempts", fallback=0.25),
        "max_concurrent_requests": cp.getint("probe", "max_concurrent_requests", fallback=10),
        "rate_limit_per_second": cp.getfloat("probe", "rate_limit_per_second", fallback=5),
        
        # Per-entity cache warming settings
        "max_attempts_per_artist": cp.getint("probe", "max_attempts_per_artist", fallback=25),
        "max_attempts_per_artist_textsearch": cp.getint("probe", "max_attempts_per_artist_textsearch", fallback=25),
        "max_attempts_per_rg": cp.getint("probe", "max_attempts_per_rg", fallback=15),
        # Text search processing options
        "artist_textsearch_lowercase": parse_bool(cp.get("probe", "artist_textsearch_lowercase", fallback="false")),
        "artist_textsearch_remove_symbols": parse_bool(cp.get("probe", "artist_textsearch_remove_symbols", fallback="false")),
        # Circuit breaker settings
        "circuit_breaker_threshold": cp.getint("probe", "circuit_breaker_threshold", fallback=50),
        "backoff_factor": cp.getfloat("probe", "backoff_factor", fallback=0.5),
        "max_backoff_seconds": cp.getfloat("probe", "max_backoff_seconds", fallback=15),
        
        # Processing options
        "batch_size": cp.getint("run", "batch_size", fallback=25),
        "batch_write_frequency": cp.getint("run", "batch_write_frequency", fallback=5),
        
        # Monitoring options
        "log_progress_every_n": cp.getint("monitoring", "log_progress_every_n", fallback=25),
        "log_level": cp.get("monitoring", "log_level", fallback="INFO"),
    }

    if not cfg["api_key"] or "REPLACE_WITH_YOUR_LIDARR_API_KEY" in cfg["api_key"]:
        raise ValueError("Missing [lidarr].api_key in config (or still using the placeholder).")

    return cfg
