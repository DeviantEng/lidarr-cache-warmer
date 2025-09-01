#!/usr/bin/env python3
import configparser
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

from config import DEFAULT_CONFIG
from colors import Colors


STOP = False

def _sig_handler(signum, frame):
    global STOP
    STOP = True
    shutdown_msg = Colors.warning(f"Received signal {signum}. Shutting down after current run...", True)
    print(f"[{datetime.now().isoformat()}] {shutdown_msg}", flush=True)

def parse_bool(s: str, default: bool = False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in ("1", "true", "yes", "on")

def main():
    # Resolve CONFIG_PATH and normalize to absolute early
    raw_cfg = os.environ.get("CONFIG_PATH", "/app/data/config.ini")
    config_path = raw_cfg if os.path.isabs(raw_cfg) else os.path.abspath(raw_cfg)

    # If config is missing, create and exit so the user can fill it in
    cfg_dir = os.path.dirname(config_path) or "."
    if not os.path.exists(config_path):
        os.makedirs(cfg_dir, exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(DEFAULT_CONFIG)
        config_created_msg = Colors.info(f"Created default config at {config_path}. Please edit api_key and restart.", True)
        print(f"[{datetime.now().isoformat()}] {config_created_msg}", flush=True)
        sys.exit(1)

    # Load schedule settings
    cp = configparser.ConfigParser()
    if not cp.read(config_path, encoding="utf-8"):
        error_msg = Colors.error(f"Could not read config: {config_path}", True)
        print(f"ERROR: {error_msg}", file=sys.stderr)
        sys.exit(2)

    interval_seconds = cp.getint("schedule", "interval_seconds", fallback=3600)
    run_at_start     = parse_bool(cp.get("schedule", "run_at_start", fallback="true"))
    jitter_seconds   = cp.getint("schedule", "jitter_seconds", fallback=0)  # optional, default 0
    max_runs         = cp.getint("schedule", "max_runs", fallback=50)        # updated default

    if interval_seconds < 1:
        error_msg = Colors.error("[schedule].interval_seconds must be >= 1", True)
        print(f"ERROR: {error_msg}", file=sys.stderr)
        sys.exit(2)

    # Signal handling for graceful exit
    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT, _sig_handler)

    # Show startup configuration
    startup_header = Colors.bold("=== LIDARR CACHE WARMER SCHEDULER STARTED ===", True)
    print(f"[{datetime.now().isoformat()}] {startup_header}", flush=True)
    print(f"   Schedule: Every {interval_seconds}s for up to {max_runs} runs", flush=True)
    print(f"   Run at start: {Colors.green('Yes', True) if run_at_start else Colors.red('No', True)}", flush=True)
    if jitter_seconds > 0:
        print(f"   Jitter: 0-{jitter_seconds}s random delay", flush=True)

    run_count = 0
    first_loop = True

    while not STOP:
        if first_loop and not run_at_start:
            first_loop = False
            delay = interval_seconds
            wait_msg = Colors.cyan(f"Waiting {delay}s before first run...", True)
            print(f"[{datetime.now().isoformat()}] {wait_msg}", flush=True)
            time.sleep(delay)
            if STOP:
                break

        first_loop = False

        # Optional jitter
        delay_before = 0
        if jitter_seconds > 0:
            try:
                delay_before = int.from_bytes(os.urandom(2), "big") % (jitter_seconds + 1)
            except Exception:
                delay_before = 0

        if delay_before > 0:
            jitter_msg = Colors.cyan(f"Sleeping jitter {delay_before}s before run...", True)
            print(f"[{datetime.now().isoformat()}] {jitter_msg}", flush=True)
            time.sleep(delay_before)
            if STOP:
                break

        # Run the main script once
        run_header = Colors.bold(f"Starting lidarr cache warmer (run {run_count + 1}/{max_runs})...", True)
        print(f"[{datetime.now().isoformat()}] {run_header}", flush=True)
        extra = []
        
        # Optional: Environment variable overrides for force modes
        if os.environ.get("FORCE_ARTISTS", "false").lower() in ("1", "true", "yes", "on"):
            extra.append("--force-artists")
            print(f"   {Colors.warning('Force artists mode enabled via environment', True)}", flush=True)
        if os.environ.get("FORCE_RG", "false").lower() in ("1", "true", "yes", "on"):
            extra.append("--force-rg")
            print(f"   {Colors.warning('Force release groups mode enabled via environment', True)}", flush=True)
        if os.environ.get("FORCE_TEXT_SEARCH", "false").lower() in ("1", "true", "yes", "on"):
            extra.append("--force-text-search")
            print(f"   {Colors.warning('Force text search mode enabled via environment', True)}", flush=True)

        # Execute the cache warmer
        proc = subprocess.run(
            ["python", "/app/main.py", "--config", config_path] + extra,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        
        # Report completion status
        run_count += 1
        if proc.returncode == 0:
            complete_msg = Colors.success(f"Run {run_count} complete successfully", True)
        else:
            complete_msg = Colors.error(f"Run {run_count} failed with exit code {proc.returncode}", True)
        print(f"[{datetime.now().isoformat()}] {complete_msg}", flush=True)

        # Check exit conditions
        if max_runs > 0 and run_count >= max_runs:
            max_runs_msg = Colors.info(f"Reached max_runs={max_runs}. Exiting.", True)
            print(f"[{datetime.now().isoformat()}] {max_runs_msg}", flush=True)
            break
        if STOP:
            break

        # Sleep until next run
        next_run_msg = Colors.cyan(f"Sleeping {interval_seconds}s until next run...", True)
        print(f"[{datetime.now().isoformat()}] {next_run_msg}", flush=True)
        for _ in range(interval_seconds):
            if STOP:
                break
            time.sleep(1)

    # Final shutdown message
    exit_msg = Colors.bold("=== LIDARR CACHE WARMER SCHEDULER STOPPED ===", True)
    print(f"[{datetime.now().isoformat()}] {exit_msg}", flush=True)
    if run_count > 0:
        summary_msg = Colors.info(f"Completed {run_count} total runs", True)
        print(f"[{datetime.now().isoformat()}] {summary_msg}", flush=True)


if __name__ == "__main__":
    main()
