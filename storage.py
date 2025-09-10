#!/usr/bin/env python3
import csv
import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List


def iso_now() -> str:
    """Generate ISO timestamp for current UTC time"""
    return datetime.now(timezone.utc).isoformat()


class StorageBackend(ABC):
    """Abstract base class for storage backends"""
    
    @abstractmethod
    def read_artists_ledger(self) -> Dict[str, Dict]:
        """Read artists ledger into a dict keyed by MBID"""
        pass
    
    @abstractmethod
    def write_artists_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write artists ledger from dict"""
        pass
    
    @abstractmethod
    def read_release_groups_ledger(self) -> Dict[str, Dict]:
        """Read release groups ledger into a dict keyed by RG MBID"""
        pass
    
    @abstractmethod
    def write_release_groups_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write release groups ledger from dict"""
        pass
    
    @abstractmethod
    def exists(self) -> bool:
        """Check if storage exists (for first-run detection)"""
        pass
    
    @abstractmethod
    def get_canary_statistics(self) -> Dict[str, Dict]:
        """Get canary response target statistics"""
        pass
    
    @abstractmethod
    def get_cf_cache_statistics(self) -> Dict[str, Dict]:
        """Get CloudFlare cache status statistics"""
        pass


class CSVStorage(StorageBackend):
    """CSV file storage backend (original implementation)"""
    
    def __init__(self, artists_csv_path: str, release_groups_csv_path: str):
        self.artists_csv_path = artists_csv_path
        self.release_groups_csv_path = release_groups_csv_path
    
    def read_artists_ledger(self) -> Dict[str, Dict]:
        """Read existing artists CSV into a dict keyed by MBID."""
        ledger: Dict[str, Dict] = {}
        if not os.path.exists(self.artists_csv_path):
            return ledger
        
        with open(self.artists_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mbid = (row.get("mbid") or "").strip()
                if not mbid:
                    continue
                ledger[mbid] = {
                    "mbid": mbid,
                    "artist_name": row.get("artist_name", ""),
                    "status": (row.get("status") or "").lower().strip(),
                    "attempts": int((row.get("attempts") or "0") or 0),
                    "last_status_code": row.get("last_status_code", ""),
                    "last_checked": row.get("last_checked", ""),
                    # Text search fields (with backwards compatibility)
                    "text_search_attempted": row.get("text_search_attempted", "").lower() in ("true", "1"),
                    "text_search_success": row.get("text_search_success", "").lower() in ("true", "1"),
                    "text_search_last_checked": row.get("text_search_last_checked", ""),
                    # Manual entry field (with backwards compatibility)
                    "manual_entry": row.get("manual_entry", "").lower() in ("true", "1"),
                    # Canary tracking (with backwards compatibility)
                    "last_canary_target": row.get("last_canary_target", ""),
                    # CF Cache Status tracking (with backwards compatibility)
                    "last_cf_cache_status": row.get("last_cf_cache_status", ""),
                }
        return ledger

    def write_artists_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write the artists ledger dict back to CSV atomically."""
        os.makedirs(os.path.dirname(self.artists_csv_path) or ".", exist_ok=True)
        fieldnames = ["mbid", "artist_name", "status", "attempts", "last_status_code", "last_checked",
                      "text_search_attempted", "text_search_success", "text_search_last_checked", 
                      "manual_entry", "last_canary_target", "last_cf_cache_status"]
        tmp_path = self.artists_csv_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for _, row in sorted(ledger.items(), key=lambda kv: (kv[1].get("artist_name", ""), kv[0])):
                writer.writerow(row)
        os.replace(tmp_path, self.artists_csv_path)

    def read_release_groups_ledger(self) -> Dict[str, Dict]:
        """Read existing release groups CSV into a dict keyed by RG MBID."""
        ledger: Dict[str, Dict] = {}
        if not os.path.exists(self.release_groups_csv_path):
            return ledger
        
        with open(self.release_groups_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rg_mbid = (row.get("rg_mbid") or "").strip()
                if not rg_mbid:
                    continue
                ledger[rg_mbid] = {
                    "rg_mbid": rg_mbid,
                    "rg_title": row.get("rg_title", ""),
                    "artist_mbid": row.get("artist_mbid", ""),
                    "artist_name": row.get("artist_name", ""),
                    "artist_cache_status": row.get("artist_cache_status", ""),
                    "status": (row.get("status") or "").lower().strip(),
                    "attempts": int((row.get("attempts") or "0") or 0),
                    "last_status_code": row.get("last_status_code", ""),
                    "last_checked": row.get("last_checked", ""),
                    # Manual entry field (with backwards compatibility)
                    "manual_entry": row.get("manual_entry", "").lower() in ("true", "1"),
                    # Canary tracking (with backwards compatibility)
                    "last_canary_target": row.get("last_canary_target", ""),
                    # CF Cache Status tracking (with backwards compatibility)
                    "last_cf_cache_status": row.get("last_cf_cache_status", ""),
                }
        return ledger

    def write_release_groups_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write the release groups ledger dict back to CSV atomically."""
        os.makedirs(os.path.dirname(self.release_groups_csv_path) or ".", exist_ok=True)
        fieldnames = ["rg_mbid", "rg_title", "artist_mbid", "artist_name", "artist_cache_status", 
                      "status", "attempts", "last_status_code", "last_checked", "manual_entry",
                      "last_canary_target", "last_cf_cache_status"]
        tmp_path = self.release_groups_csv_path + ".tmp"
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for _, row in sorted(ledger.items(), key=lambda kv: (kv[1].get("artist_name", ""), kv[1].get("rg_title", ""), kv[0])):
                writer.writerow(row)
        os.replace(tmp_path, self.release_groups_csv_path)

    def exists(self) -> bool:
        """Check if CSV files exist"""
        return os.path.exists(self.artists_csv_path)
    
    def get_canary_statistics(self) -> Dict[str, Dict]:
        """Get canary response target statistics (CSV doesn't track detailed canary stats)"""
        # For CSV, we can only show basic info from the current ledger state
        artists_ledger = self.read_artists_ledger()
        rg_ledger = self.read_release_groups_ledger()
        
        canary_stats = {}
        
        # Count current canary targets from ledgers
        for artist in artists_ledger.values():
            target = artist.get("last_canary_target", "")
            if target:
                if target not in canary_stats:
                    canary_stats[target] = {"artist_success": 0, "artist_total": 0, "rg_success": 0, "rg_total": 0, "text_search_success": 0, "text_search_total": 0}
                canary_stats[target]["artist_total"] += 1
                if artist.get("status", "").lower() == "success":
                    canary_stats[target]["artist_success"] += 1
                if artist.get("text_search_attempted", False):
                    canary_stats[target]["text_search_total"] += 1
                    if artist.get("text_search_success", False):
                        canary_stats[target]["text_search_success"] += 1
        
        for rg in rg_ledger.values():
            target = rg.get("last_canary_target", "")
            if target:
                if target not in canary_stats:
                    canary_stats[target] = {"artist_success": 0, "artist_total": 0, "rg_success": 0, "rg_total": 0, "text_search_success": 0, "text_search_total": 0}
                canary_stats[target]["rg_total"] += 1
                if rg.get("status", "").lower() == "success":
                    canary_stats[target]["rg_success"] += 1
        
        return canary_stats
    
    def get_cf_cache_statistics(self) -> Dict[str, Dict]:
        """Get CloudFlare cache status statistics (CSV doesn't track detailed CF cache stats)"""
        # For CSV, we can only show basic info from the current ledger state
        artists_ledger = self.read_artists_ledger()
        rg_ledger = self.read_release_groups_ledger()
        
        cf_stats = {"STALE": 0, "HIT": 0, "MISS": 0, "DYNAMIC": 0, "other": 0}
        
        # Count current CF cache statuses from ledgers
        for artist in artists_ledger.values():
            cf_status = artist.get("last_cf_cache_status", "").upper()
            if cf_status in cf_stats:
                cf_stats[cf_status] += 1
            elif cf_status:
                cf_stats["other"] += 1
        
        for rg in rg_ledger.values():
            cf_status = rg.get("last_cf_cache_status", "").upper()
            if cf_status in cf_stats:
                cf_stats[cf_status] += 1
            elif cf_status:
                cf_stats["other"] += 1
        
        return {"basic_counts": cf_stats}


class SQLiteStorage(StorageBackend):
    """SQLite database storage backend"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database with tables and handle migrations"""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            # Create tables with basic structure first
            conn.execute("""
                CREATE TABLE IF NOT EXISTS artists (
                    mbid TEXT PRIMARY KEY,
                    artist_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_status_code TEXT NOT NULL DEFAULT '',
                    last_checked TEXT NOT NULL DEFAULT ''
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS release_groups (
                    rg_mbid TEXT PRIMARY KEY,
                    rg_title TEXT NOT NULL,
                    artist_mbid TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    artist_cache_status TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_status_code TEXT NOT NULL DEFAULT '',
                    last_checked TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY (artist_mbid) REFERENCES artists (mbid)
                )
            """)
            
            # Create canary tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS canary_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    canary_target TEXT NOT NULL,
                    status_code TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    operation_type TEXT NOT NULL DEFAULT 'mbid_check'
                )
            """)
            
            # Create CF cache status tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cf_cache_responses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    cf_cache_status TEXT NOT NULL,
                    status_code TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    operation_type TEXT NOT NULL DEFAULT 'mbid_check'
                )
            """)
            
            # Add text search columns if they don't exist (migration)
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN text_search_attempted INTEGER NOT NULL DEFAULT 0")
                print("Added text_search_attempted column to artists table")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN text_search_success INTEGER NOT NULL DEFAULT 0")
                print("Added text_search_success column to artists table")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN text_search_last_checked TEXT NOT NULL DEFAULT ''")
                print("Added text_search_last_checked column to artists table")
            except sqlite3.OperationalError:
                pass
            
            # Add manual_entry columns if they don't exist (migration)
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN manual_entry INTEGER NOT NULL DEFAULT 0")
                print("Added manual_entry column to artists table")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE release_groups ADD COLUMN manual_entry INTEGER NOT NULL DEFAULT 0")
                print("Added manual_entry column to release_groups table")
            except sqlite3.OperationalError:
                pass
            
            # Add canary tracking columns if they don't exist (migration)
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN last_canary_target TEXT NOT NULL DEFAULT ''")
                print("Added last_canary_target column to artists table")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE release_groups ADD COLUMN last_canary_target TEXT NOT NULL DEFAULT ''")
                print("Added last_canary_target column to release_groups table")
            except sqlite3.OperationalError:
                pass
            
            # Add CF cache status tracking columns if they don't exist (migration)
            try:
                conn.execute("ALTER TABLE artists ADD COLUMN last_cf_cache_status TEXT NOT NULL DEFAULT ''")
                print("Added last_cf_cache_status column to artists table")
            except sqlite3.OperationalError:
                pass
            
            try:
                conn.execute("ALTER TABLE release_groups ADD COLUMN last_cf_cache_status TEXT NOT NULL DEFAULT ''")
                print("Added last_cf_cache_status column to release_groups table")
            except sqlite3.OperationalError:
                pass
            
            # Create indexes for performance (only after columns exist)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_status ON artists (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_text_search ON artists (text_search_attempted, text_search_success)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_manual ON artists (manual_entry)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_canary ON artists (last_canary_target)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artists_cf_cache ON artists (last_cf_cache_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_status ON release_groups (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_artist_status ON release_groups (artist_cache_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_artist_mbid ON release_groups (artist_mbid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_manual ON release_groups (manual_entry)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_canary ON release_groups (last_canary_target)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rg_cf_cache ON release_groups (last_cf_cache_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_canary_target ON canary_responses (canary_target)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_canary_entity ON canary_responses (entity_type, entity_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_canary_timestamp ON canary_responses (timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cf_cache_status ON cf_cache_responses (cf_cache_status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cf_cache_entity ON cf_cache_responses (entity_type, entity_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cf_cache_timestamp ON cf_cache_responses (timestamp)")
            
            conn.commit()

    def read_artists_ledger(self) -> Dict[str, Dict]:
        """Read artists from SQLite into a dict keyed by MBID."""
        ledger: Dict[str, Dict] = {}
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT mbid, artist_name, status, attempts, last_status_code, last_checked,
                       text_search_attempted, text_search_success, text_search_last_checked,
                       manual_entry, last_canary_target, last_cf_cache_status
                FROM artists
                ORDER BY artist_name, mbid
            """)
            
            for row in cursor:
                ledger[row["mbid"]] = {
                    "mbid": row["mbid"],
                    "artist_name": row["artist_name"],
                    "status": row["status"].lower().strip(),
                    "attempts": row["attempts"],
                    "last_status_code": row["last_status_code"],
                    "last_checked": row["last_checked"],
                    "text_search_attempted": bool(row["text_search_attempted"]),
                    "text_search_success": bool(row["text_search_success"]),
                    "text_search_last_checked": row["text_search_last_checked"],
                    "manual_entry": bool(row["manual_entry"]),
                    "last_canary_target": row["last_canary_target"],
                    "last_cf_cache_status": row["last_cf_cache_status"],
                }
        
        return ledger

    def write_artists_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write artists ledger to SQLite with upsert logic."""
        with sqlite3.connect(self.db_path) as conn:
            for mbid, data in ledger.items():
                conn.execute("""
                    INSERT OR REPLACE INTO artists 
                    (mbid, artist_name, status, attempts, last_status_code, last_checked,
                     text_search_attempted, text_search_success, text_search_last_checked, 
                     manual_entry, last_canary_target, last_cf_cache_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data["mbid"],
                    data["artist_name"],
                    data["status"],
                    data["attempts"],
                    data["last_status_code"],
                    data["last_checked"],
                    int(data.get("text_search_attempted", False)),
                    int(data.get("text_search_success", False)),
                    data.get("text_search_last_checked", ""),
                    int(data.get("manual_entry", False)),
                    data.get("last_canary_target", ""),
                    data.get("last_cf_cache_status", "")
                ))
            conn.commit()

    def read_release_groups_ledger(self) -> Dict[str, Dict]:
        """Read release groups from SQLite into a dict keyed by RG MBID."""
        ledger: Dict[str, Dict] = {}
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT rg_mbid, rg_title, artist_mbid, artist_name, artist_cache_status,
                       status, attempts, last_status_code, last_checked, manual_entry,
                       last_canary_target, last_cf_cache_status
                FROM release_groups
                ORDER BY artist_name, rg_title, rg_mbid
            """)
            
            for row in cursor:
                ledger[row["rg_mbid"]] = {
                    "rg_mbid": row["rg_mbid"],
                    "rg_title": row["rg_title"],
                    "artist_mbid": row["artist_mbid"],
                    "artist_name": row["artist_name"],
                    "artist_cache_status": row["artist_cache_status"],
                    "status": row["status"].lower().strip(),
                    "attempts": row["attempts"],
                    "last_status_code": row["last_status_code"],
                    "last_checked": row["last_checked"],
                    "manual_entry": bool(row["manual_entry"]),
                    "last_canary_target": row["last_canary_target"],
                    "last_cf_cache_status": row["last_cf_cache_status"],
                }
        
        return ledger

    def write_release_groups_ledger(self, ledger: Dict[str, Dict]) -> None:
        """Write release groups ledger to SQLite with upsert logic."""
        with sqlite3.connect(self.db_path) as conn:
            for rg_mbid, data in ledger.items():
                conn.execute("""
                    INSERT OR REPLACE INTO release_groups 
                    (rg_mbid, rg_title, artist_mbid, artist_name, artist_cache_status,
                     status, attempts, last_status_code, last_checked, manual_entry,
                     last_canary_target, last_cf_cache_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data["rg_mbid"],
                    data["rg_title"],
                    data["artist_mbid"],
                    data["artist_name"],
                    data["artist_cache_status"],
                    data["status"],
                    data["attempts"],
                    data["last_status_code"],
                    data["last_checked"],
                    int(data.get("manual_entry", False)),
                    data.get("last_canary_target", ""),
                    data.get("last_cf_cache_status", "")
                ))
            conn.commit()

    def exists(self) -> bool:
        """Check if SQLite database exists and has data"""
        if not os.path.exists(self.db_path):
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM artists")
                return cursor.fetchone()[0] > 0
        except sqlite3.Error:
            return False

    def update_release_groups_artist_status(self, artists_ledger: Dict[str, Dict]) -> None:
        """Efficiently update artist_cache_status in release groups based on current artist statuses"""
        with sqlite3.connect(self.db_path) as conn:
            for artist_mbid, artist_data in artists_ledger.items():
                conn.execute("""
                    UPDATE release_groups 
                    SET artist_cache_status = ?
                    WHERE artist_mbid = ?
                """, (artist_data.get("status", ""), artist_mbid))
            conn.commit()
    
    def record_canary_response(self, entity_type: str, entity_id: str, canary_target: str, 
                              status_code: str, success: bool, operation_type: str = "mbid_check") -> None:
        """Record a canary response for analytics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO canary_responses 
                (timestamp, entity_type, entity_id, canary_target, status_code, success, operation_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (iso_now(), entity_type, entity_id, canary_target, status_code, int(success), operation_type))
            conn.commit()
    
    def record_cf_cache_response(self, entity_type: str, entity_id: str, cf_cache_status: str, 
                                status_code: str, success: bool, operation_type: str = "mbid_check") -> None:
        """Record a CloudFlare cache status response for analytics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO cf_cache_responses 
                (timestamp, entity_type, entity_id, cf_cache_status, status_code, success, operation_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (iso_now(), entity_type, entity_id, cf_cache_status, status_code, int(success), operation_type))
            conn.commit()
    
    def get_canary_statistics(self) -> Dict[str, Dict]:
        """Get comprehensive canary response target statistics"""
        stats = {}
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get stats grouped by canary target and operation type
            cursor = conn.execute("""
                SELECT 
                    canary_target,
                    operation_type,
                    COUNT(*) as total_requests,
                    SUM(success) as successful_requests,
                    MIN(timestamp) as first_seen,
                    MAX(timestamp) as last_seen
                FROM canary_responses 
                WHERE canary_target != '' 
                GROUP BY canary_target, operation_type
                ORDER BY canary_target, operation_type
            """)
            
            for row in cursor:
                target = row["canary_target"]
                op_type = row["operation_type"]
                
                if target not in stats:
                    stats[target] = {
                        "first_seen": row["first_seen"],
                        "last_seen": row["last_seen"],
                        "operations": {}
                    }
                
                # Update first/last seen dates
                if row["first_seen"] < stats[target]["first_seen"]:
                    stats[target]["first_seen"] = row["first_seen"]
                if row["last_seen"] > stats[target]["last_seen"]:
                    stats[target]["last_seen"] = row["last_seen"]
                
                # Add operation stats
                stats[target]["operations"][op_type] = {
                    "total_requests": row["total_requests"],
                    "successful_requests": row["successful_requests"],
                    "success_rate": (row["successful_requests"] / row["total_requests"]) * 100 if row["total_requests"] > 0 else 0.0
                }
        
        return stats
    
    def get_cf_cache_statistics(self) -> Dict[str, Dict]:
        """Get comprehensive CloudFlare cache status statistics"""
        stats = {}
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get overall CF cache status distribution
            cursor = conn.execute("""
                SELECT 
                    cf_cache_status,
                    COUNT(*) as total_requests,
                    SUM(success) as successful_requests,
                    MIN(timestamp) as first_seen,
                    MAX(timestamp) as last_seen
                FROM cf_cache_responses 
                WHERE cf_cache_status != ''
                GROUP BY cf_cache_status
                ORDER BY total_requests DESC
            """)
            
            for row in cursor:
                cf_status = row["cf_cache_status"]
                stats[cf_status] = {
                    "total_requests": row["total_requests"],
                    "successful_requests": row["successful_requests"],
                    "success_rate": (row["successful_requests"] / row["total_requests"]) * 100 if row["total_requests"] > 0 else 0.0,
                    "first_seen": row["first_seen"],
                    "last_seen": row["last_seen"]
                }
            
            # Get breakdown by operation type
            cursor = conn.execute("""
                SELECT 
                    cf_cache_status,
                    operation_type,
                    COUNT(*) as total_requests,
                    SUM(success) as successful_requests
                FROM cf_cache_responses 
                WHERE cf_cache_status != ''
                GROUP BY cf_cache_status, operation_type
                ORDER BY cf_cache_status, operation_type
            """)
            
            for row in cursor:
                cf_status = row["cf_cache_status"]
                op_type = row["operation_type"]
                
                if cf_status in stats:
                    if "operations" not in stats[cf_status]:
                        stats[cf_status]["operations"] = {}
                    
                    stats[cf_status]["operations"][op_type] = {
                        "total_requests": row["total_requests"],
                        "successful_requests": row["successful_requests"],
                        "success_rate": (row["successful_requests"] / row["total_requests"]) * 100 if row["total_requests"] > 0 else 0.0
                    }
        
        return stats


def create_storage_backend(cfg: dict) -> StorageBackend:
    """Factory function to create appropriate storage backend based on config"""
    storage_type = cfg.get("storage_type", "csv").lower()
    
    if storage_type == "sqlite":
        return SQLiteStorage(cfg.get("db_path", "mbid_cache.db"))
    elif storage_type == "csv":
        return CSVStorage(
            cfg.get("artists_csv_path", "mbid-artists.csv"),
            cfg.get("release_groups_csv_path", "mbid-releasegroups.csv")
        )
    else:
        raise ValueError(f"Unknown storage type: {storage_type}. Use 'csv' or 'sqlite'.")
