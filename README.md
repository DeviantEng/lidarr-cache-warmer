# lidarr-cache-warmer

Cache warming tool for **Lidarr** metadata. Fetches artist and release group MBIDs from your Lidarr instance and repeatedly probes them against an API endpoint until successful, triggering cache generation in the backend.

**Multi-phase processing**: Warms artist MBID cache first, then artist text search cache, then release group cache (each phase optional and configurable).

## ⚠️ Various Artists Protection

**The cache warmer automatically detects and removes "Various Artists" from your Lidarr library.** This artist (MBID: `89ad4ac3-39f7-470e-963a-56509c546377`) typically contains 100,000+ albums and causes severe performance issues.

**Why this happens:** Lidarr has built-in protection to prevent adding Various Artists, but recent development changes occasionally allowed this artist to be added despite the protection. This cache warmer feature aligns with Lidarr's intended behavior by automatically removing Various Artists when detected.

**What happens:** If detected, the cache warmer will:
- 🚨 Alert you that Various Artists was found
- 🛠️ Automatically remove the artist and all associated albums from Lidarr
- 🚫 Add it to Lidarr's import exclusion list to prevent re-adding
- ⏱️ Wait 30 seconds for deletion to complete before continuing

This can dramatically improve performance for users experiencing timeout issues with large libraries.

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
artist_textsearch_lowercase = true        # Better search matching
artist_textsearch_remove_symbols = true  # Handle diacritics (é, ñ, ü)
```

### High-Performance Setup
```ini
[probe]
max_concurrent_requests = 10
rate_limit_per_second = 8

[run]
batch_size = 50
batch_write_frequency = 10
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
- **Output**: Updates artist `status` in storage

#### Phase 2: Artist Text Search Cache Warming (Optional, Default: Enabled)
- **Purpose**: Warms search-by-name cache for user queries like "metallica"
- **Endpoint**: `GET /search?type=all&query={artist_name}`
- **When**: After Phase 1, for all artists with names
- **Retry Logic**: Up to 25 attempts per text search by default
- **Benefits**: Faster response times for user searches in Lidarr
- **Text Processing**: Can normalize international characters and symbols
- **Output**: Updates `text_search_attempted` and `text_search_success` flags

#### Phase 3: Release Group Cache Warming (Optional, Default: Disabled)
- **Purpose**: Warms album/release group cache using MusicBrainz IDs
- **Endpoint**: `GET /album/{rg_mbid}`
- **When**: Only after Phase 1 completes successfully for the parent artist
- **Dependency**: Requires successful artist cache warming first
- **Output**: Updates release group `status` in storage

### First Run: Cache Discovery
On first run (no existing storage), the tool automatically enables **discovery mode**:
- **1 attempt per entity** to quickly survey what's already cached
- Creates baseline storage showing current cache state
- **Much faster** than full cache warming on potentially cached items

### Subsequent Runs: Targeted Cache Warming

```
🔍 First run detected - no existing storage found
   Enabling force modes for initial cache discovery (1 attempt per entity)

Lidarr API timeout: 60 seconds
=== Phase 1: Artist MBID Cache Warming ===
[1/25] Checking Artist Name [mbid] ... SUCCESS (code=200, attempts=1)  # Already cached!
[2/25] Checking Another Artist [mbid] ... TIMEOUT (code=503, attempts=1)  # Needs warming
Progress: 50/250 (20.0%) - Rate: 4.2 artists/sec - ETC: 14:32 - API: 3.00 req/sec - Batch: 20/25 success

=== Phase 2: Artist Text Search Cache Warming ===
Text processing: symbol/diacritic removal, lowercase conversion
[1/25] Text search: 'Sigur Rós' -> 'sigur ros' ... SUCCESS (code=200, attempts=1)
[2/25] Text search for 'Bob Dylan' ... TIMEOUT (code=503, attempts=1)
Progress: 50/200 (25.0%) - Rate: 3.8 searches/sec - ETC: 12:45 - API: 3.00 req/sec - Batch: 12/25 success

=== Phase 3: Release Group Cache Warming ===
[1/25] Checking Artist Name - Album Title [mbid] ... SUCCESS (code=200, attempts=1)
```

**Subsequent runs** use full attempt limits and only process pending/failed items.

---

## 📊 Statistics & Monitoring

### View Current Stats
```bash
# Get comprehensive overview with Docker
docker run --rm -v $(pwd)/data:/app/data --entrypoint python ghcr.io/devianteng/lidarr-cache-warmer:latest /app/stats.py --config /app/data/config.ini

# Manual Python installation
python333 stats.py --config config.ini
```

**Example Output:**
```
🎵 LIDARR CACHE WARMER - STATISTICS REPORT
📋 Key Configuration Settings:
   • max_concurrent_requests: 5, rate_limit_per_second: 3
   • process_artist_textsearch: true, max_attempts_per_artist_textsearch: 25
   • storage_type: sqlite, db_path: mbid_cache.db

🎤 ARTIST MBID STATISTICS:
   ✅ Successfully cached: 1,156 (94.2%)
   ❌ Failed/Timeout: 71 (5.8%)
   ⏳ Not yet processed: 0

🔍 ARTIST TEXT SEARCH STATISTICS:
   Artists with names: 1,245
   ✅ Text searches attempted: 1,200 (96.4%)
   ✅ Text searches successful: 1,180 (98.3%)
   📊 Text search coverage: 96.4% of named artists

💿 RELEASE GROUP STATISTICS:
   ✅ Successfully cached: 8,247 (67.1%)
   🎯 Eligible for processing: 12,089 (98.4% coverage)

🚀 RECOMMENDATIONS:
   • Process 45 pending text searches
   • Process 3,842 eligible release groups
   • Next run will execute: Phase 2: Text search warming, Phase 3: Release group warming
```

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
- **Phase dependencies**: Text search and release groups only processed after their dependencies succeed
- **Smart retry logic**: Different retry strategies for different types of cache misses

### Text Search Cache Warming Strategy
The text search feature specifically targets the search-by-name cache system:

1. **Text Preprocessing**: Configurable normalization for international artists
   - **Lowercase conversion**: "Metallica" → "metallica" 
   - **Symbol/diacritic removal**: "Sigur Rós" → "sigur ros", "Café Tacvba" → "cafe tacvba"
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

## 📄 Recent Changes (Latest)

### v1.6.0 Features
- **🚨 Various Artists Protection**: Automatic detection and removal of problematic Various Artists
- **⏱️ Smart Timing**: 30-second wait after Various Artists deletion to ensure proper cleanup
- **🔧 Path Resolution Fix**: Corrected double `/data/data/` path issues in Docker environments
- **📊 Enhanced Stats**: Updated statistics display with connection settings and text processing options
- **🌐 Better SSL Support**: Improved handling of `verify_ssl` and `lidarr_timeout` in stats collection

### Migration Notes
- **File paths updated**: If upgrading, remove `./data/` prefixes from paths in your `config.ini`
- **Docker users**: Update any hardcoded paths in custom configs
- **Various Artists**: Will be automatically detected and removed on next run
- **Large libraries**: Consider increasing `lidarr_timeout = 120` for 2000+ artists

---

## Enjoy!
