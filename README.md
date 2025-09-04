# lidarr-cache-warmer

Cache warming tool for **Lidarr** metadata. Fetches artist and release group MBIDs from your Lidarr instance and repeatedly probes them against an API endpoint until successful, triggering cache generation in the backend.

**Multi-phase processing**: Warms artist MBID cache first, then artist text search cache, then release group cache (each phase optional and configurable).

## 🕐 Cache Freshness Management

The cache warmer intelligently manages cache freshness to handle cache expiration:

- **Smart Re-checking**: Successful cache entries are automatically re-verified after a configurable time period (default: 72 hours)
- **Minimal Impact**: Re-checks use single attempts for truly cached items
- **Configurable**: Set `cache_recheck_hours` to control freshness (0 = disabled)
- **Stale Detection**: Statistics show which entries are due for refresh

This ensures your cache warming data stays current without overwhelming the API.

## Requirements

- **Lidarr instance** with API access
- **Target API** to warm (default: `https://api.lidarr.audio/api/v0.4`)
- **Docker** (recommended) or **Python 3.8+**

---

## 🐳 Docker (Recommended)

### Quick Start

```bash
# Create data directory
mkdir -p ./data

# Run container (creates config.ini and exits)
docker run --rm -v $(pwd)/data:/app/data ghcr.io/devianteng/lidarr-cache-warmer:latest

# Edit config with your Lidarr API key
nano ./data/config.ini

# Run the cache warmer
docker run -d --name lidarr-cache-warmer -v $(pwd)/data:/app/data ghcr.io/devianteng/lidarr-cache-warmer:latest

# Monitor logs
docker logs -f lidarr-cache-warmer
```

### Docker Compose

```yaml
services:
  lidarr-cache-warmer:
    image: ghcr.io/devianteng/lidarr-cache-warmer:latest
    container_name: lidarr-cache-warmer
    restart: unless-stopped
    volumes:
      - ./data:/app/data
```

---

## 🔧 Manual Python Installation

```bash
# Clone and setup
git clone https://github.com/devianteng/lidarr-cache-warmer.git
cd lidarr-cache-warmer
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run once (creates config.ini and data/ folder)
python3 main.py --config config.ini

# Edit config.ini with your Lidarr API key, then:
python3 main.py --config config.ini

# Or run on schedule:
python3 entrypoint.py
```

---

## ⚙️ Configuration

On first run, creates `config.ini` with sensible defaults.
**Edit the API key before restarting:**

### Config options

View [config.ini.example](config.ini.example) for available options, or to pre-create your config.

### Storage Recommendations

| Library Size | Recommended Storage | Why |
|--------------|-------------------|-----|
| < 2,000 artists | `storage_type = csv` | Simple, human-readable files |
| > 2,000 artists | `storage_type = sqlite` | **Much faster**, indexed queries, atomic updates |

**SQLite Benefits:** 30MB+ CSV becomes ~1MB database, 100x faster updates, no file corruption risk, optimized text search tracking.

### File Organization

**Docker:**
```
/app/data/               # Mounted from host ./data/
├── config.ini           # Your configuration
├── mbid-artists.csv     # Artist cache status
├── mbid_cache.db        # SQLite database (if enabled)
└── results_*.log        # Run results
```

**Manual Installation:**
```
your-project/
├── config.ini           # Your configuration
├── main.py              # Application files
├── process_*.py
└── data/                # Auto-created cache directory
    ├── mbid-artists.csv
    ├── mbid_cache.db
    └── results_*.log
```

---

## 🔧 Common Configuration Scenarios

### Large Libraries (2000+ Artists)
```ini
[lidarr]
lidarr_timeout = 120        # Longer timeout for large datasets
verify_ssl = true

[ledger]
storage_type = sqlite       # Much faster for large libraries

[run]
batch_size = 50            # Larger batches for efficiency
cache_recheck_hours = 168  # Weekly freshness checks
```

### Private CA / Self-Signed Certificates
```ini
[lidarr]
verify_ssl = false         # Disable SSL verification
# WARNING: Only use in trusted private networks
```

### International Music Libraries
```ini
[run]
artist_textsearch_lowercase = true        # Mirrors Lidarr's search behavior (default)
artist_textsearch_transliterate_unicode = true  # Handle international characters (default)
```

**Why these defaults work well:**
- **Lowercase matching**: Lidarr's search is case-insensitive, so lowercase queries match better
- **Unicode transliteration**: Converts `Słoń` → `slon`, `仮BAND` → `jia band`, `Café` → `cafe`
- **Better cache hit rates**: International artists get found more reliably

### High-Performance Setup
```ini
[probe]
max_concurrent_requests = 10
rate_limit_per_second = 8

[run]
batch_size = 50
batch_write_frequency = 10
cache_recheck_hours = 48   # More frequent freshness checks
```

### Disable Cache Freshness (Legacy Behavior)
```ini
[run]
cache_recheck_hours = 0    # Never re-check successful entries
```

---

## 📊 What It Does

### Multi-Phase Cache Warming Process

The tool operates in up to three distinct phases, each targeting different API cache systems:

#### Phase 0.1: Build ledger from Lidarr (Always Enabled)
- **Purpose**: Queries local lidarr instance to get list of artists and albums
- **When**: Always runs first - foundation for all other phases
- **Output**: Generates csv or sqlite db for data that needs to be processed

#### Phase 0.2: Add manual entries into ledger (Optional, Default: Disabled)
- **Purpose**: If you want to add records to cache that are not in Lidarr, you can add them to a yaml file to be processed
- **When**: Manual list is injected into csv/sqlite db before processing starts

#### Phase 1: Artist MBID Cache Warming (Always Enabled)
- **Purpose**: Warms direct artist lookup cache using MusicBrainz IDs
- **Endpoint**: `GET /artist/{mbid}`
- **When**: Always runs
- **Retry Logic**: Up to 25 attempts per artist by default
- **Freshness**: Re-checks successful entries after configured hours
- **Output**: Updates artist `status` in storage

#### Phase 2: Artist Text Search Cache Warming (Optional, Default: Enabled)
- **Purpose**: Warms search-by-name cache for user queries like "metallica"
- **Endpoint**: `GET /search?type=all&query={artist_name}`
- **When**: After Phase 1, for all artists with names
- **Retry Logic**: Up to 25 attempts per text search by default
- **Benefits**: Faster response times for user searches in Lidarr
- **Text Processing**: Can normalize international characters and symbols
- **Freshness**: Re-checks successful searches after configured hours
- **Output**: Updates `text_search_attempted` and `text_search_success` flags

#### Phase 3: Release Group Cache Warming (Optional, Default: Disabled)
- **Purpose**: Warms album/release group cache using MusicBrainz IDs
- **Endpoint**: `GET /album/{rg_mbid}`
- **When**: Only after Phase 1 completes successfully for the parent artist
- **Dependency**: Requires successful artist cache warming first
- **Freshness**: Re-checks successful entries after configured hours
- **Output**: Updates release group `status` in storage

### First Run: Cache Discovery
On first run (no existing storage), the tool automatically enables **discovery mode**:
- **1 attempt per entity** to quickly survey what's already cached
- Creates baseline storage showing current cache state
- **Much faster** than full cache warming on potentially cached items

### Subsequent Runs: Intelligent Processing

```
🔍 First run detected - no existing storage found
   Enabling force modes for initial cache discovery (1 attempt per entity)

Lidarr API timeout: 60 seconds
=== Phase 1: Artist MBID Cache Warming ===
Will process 150 artists for MBID cache warming
   - 5 forced re-checks
   - 45 pending/failed artists  
   - 100 stale entries (older than 72 hours)

[1/25] Checking Artist Name [mbid] ... SUCCESS (code=200, attempts=1)  # Already cached!
[2/25] Checking Another Artist [mbid] ... TIMEOUT (code=503, attempts=1)  # Needs warming
Progress: 50/250 (20.0%) - Rate: 4.2 artists/sec - ETC: 14:32 - API: 3.00 req/sec - Batch: 20/25 success
```

**Intelligent processing** targets only items that need attention: new entries, failures, or stale cache entries.

---

## 📊 Statistics & Monitoring

### View Current Stats
```bash
# Get comprehensive overview with Docker
docker run --rm -v $(pwd)/data:/app/data --entrypoint python ghcr.io/devianteng/lidarr-cache-warmer:latest /app/stats.py --config /app/data/config.ini

# Manual Python installation
python3 stats.py --config config.ini
```

**Canary Header Stats (requires using SQLite storage)**
```bash
# add --canary-stats to the above calls; ex:
docker run --rm -v $(pwd)/data:/app/data --entrypoint python ghcr.io/devianteng/lidarr-cache-warmer:latest /app/stats.py --config /app/data/config.ini --canary-stats
```

**Example Output with Cache Freshness:**
```
🎵 LIDARR CACHE WARMER - STATISTICS REPORT
📋 Key Configuration Settings:
   • cache_recheck_hours: 72

🎤 ARTIST MBID STATISTICS:
   ✅ Successfully cached: 1,156 (94.2%)
   ❌ Failed/Timeout: 71 (5.8%)
   ⏰ Stale MBID cache: 45 (older than 72 hours)
   🔄 Next recheck in: 23.5 hours

🔍 ARTIST TEXT SEARCH STATISTICS:
   ✅ Text searches successful: 1,180 (98.3%)
   ⏰ Stale text searches: 15 (older than 72 hours)

💿 RELEASE GROUP STATISTICS:
   ✅ Successfully cached: 8,247 (67.1%)
   ⏰ Stale cache entries: 125 (older than 72 hours)

🚀 RECOMMENDATIONS:
   • Process 45 pending artists and 45 stale entries
   • Process 15 stale text searches
   • Process 125 stale release groups
```

The statistics now show cache freshness information, helping you understand what needs refreshing.

---

## 💡 How It Works

Cache warming is perfect for APIs where:
1. **Backend generates data on-demand** (expensive computation/database queries)
2. **Results are cached** after first successful generation
3. **Cache misses return HTTP/503** until backend completes processing
4. **Repeated requests eventually succeed** when cache is ready

### Intelligent Processing
- **First run**: Quick discovery (1 attempt each) to map current cache state
- **Subsequent runs**: Intensive warming (25+ attempts) only on items that need it
- **Cache freshness**: Automatic re-verification of successful entries after configured time
- **Phase dependencies**: Text search and release groups only processed after their dependencies succeed
- **Smart retry logic**: Different retry strategies for different types of cache misses

### Text Search Cache Warming Strategy
The text search feature targets the search-by-name cache system and mirrors Lidarr's search behavior:

1. **Text Preprocessing**: Smart normalization for international artists (enabled by default)
   - **Lowercase conversion**: "Metallica" → "metallica" (matches Lidarr's case-insensitive search)
   - **Unicode transliteration**: "Słoń" → "slon", "仮BAND" → "jia band", "Café Tacvba" → "cafe tacvba"
2. **URL Encoding**: Properly handles special characters in artist names
3. **Query Format**: Uses `?type=all&query={artist_name}` format for comprehensive results
4. **Cache Building**: Retries 503 responses as the search index builds
5. **Success Tracking**: Records both attempt status and success status for analytics

This approach minimizes wasted effort and focuses cache warming where it provides maximum user benefit.

---

## 🔧 Manual Entry YAML example (manual_entries.yml)

```yaml
6ae6a016-91d7-46cc-be7d-5e8e5d320c54:
  name: Adelitas Way
  release-groups:
    - bc4c3083-c484-4c2f-8983-dd82eb8b60c0
    - 0f74aff2-8dee-4712-80a4-b464a9e8c384

a8c1eb9a-2fb4-4f4f-8ada-62f30e27a1af:
  name: Artemis Rising
  # No release-groups needed
```

---

## ✨ What's New in Recent Versions

### v1.8.0 Features (Current)
- **🌍 Improved Unicode Support**: Better international character handling with `unidecode`
- **🎯 Optimized Defaults**: Text search now mirrors Lidarr's behavior by default
  - `artist_textsearch_lowercase = true` (matches Lidarr's case-insensitive search)
  - `artist_textsearch_transliterate_unicode = true` (converts international characters intelligently)
- **⚡ Performance**: Removed unnecessary 30-second delay when Various Artists detected
- **📝 Better Documentation**: Clearer configuration guidance for international music libraries

### v1.7.0 Features
- **🕐 Cache Freshness Management**: Automatic re-checking of successful entries after configurable hours
- **📊 Staleness Statistics**: Statistics show which entries are due for refresh and when
- **⚠️ Various Artists Protection**: Automatic detection and exclusion of Various Artists from processing
  - Detects Various Artists (MBID: `89ad4ac3-39f7-470e-963a-56509c546377`) which typically contains 100,000+ albums
  - Filters out Various Artists and all its albums to prevent severe performance issues
  - Provides protection while leaving your Lidarr library unchanged (no deletion)
- **🔧 Improved Path Resolution**: Enhanced file path handling in Docker environments
- **📈 Enhanced Statistics**: Detailed breakdown of processing reasons including staleness

### v1.6.0 Features
- **🔧 Path Resolution Fix**: Corrected double `/data/data/` path issues in Docker environments
- **📊 Enhanced Stats**: Updated statistics display with connection settings and text processing options
- **🌐 Better SSL Support**: Improved handling of `verify_ssl` and `lidarr_timeout` in stats collection

### Migration Notes
- **New Text Search Defaults**: Fresh installations now optimize for international music automatically
  - Existing configs continue working but may show recommendations to upgrade
  - Update your config for better results: `artist_textsearch_transliterate_unicode = true`
- **Unicode Processing**: Install `unidecode` for best international character support: `pip install unidecode`
- **Cache Freshness**: New installations default to 72-hour re-checking. Set `cache_recheck_hours = 0` for legacy behavior
- **Various Artists**: Now filtered out instead of deleted - your Lidarr library remains unchanged
- **File paths**: Remove `./data/` prefixes from paths in your `config.ini` if upgrading from very old versions

**Your examples will now work much better with the new defaults:**
- `Słoń` → `slon` (Polish properly transliterated + lowercase)
- `仮BAND` → `jia band` (Japanese readable + lowercase)  
- `古代祐三` → `gu dai you san` (Chinese romanized + lowercase)
- `Metallica` → `metallica` (matches Lidarr's search behavior)

The cache warmer now mirrors Lidarr's text processing by default, giving much better search success rates for international music libraries!

---

## Enjoy!
