# Presentation Layer

## Architecture

The presentation layer is a **browser-based admin webapp** consuming a **RESTful JSON API** served by Jetty 12.1 embedded servlets. There is no server-side HTML rendering — the backend serves data only; the frontend handles all display and interaction.

This architecture serves two goals:
1. A rich, interactive admin experience with client-side charting and data management
2. An openly accessible JSON API that third parties can consume directly to build their own tools on top of the project's derived data

---

## Backend: Jetty Servlets

The server runs on port 8080 via `HpfServer.main()`. Three servlets handle all requests:

### `AdminApiServlet` — `/api/*`

REST API for data access and management. Supports paginated responses (default page size 50).

**GET endpoints:**
- `/api/boats` — paginated boat list with optional search/filter query parameters
- `/api/boats/{id}` — single boat detail (certificates, aliases, sources)
- `/api/designs` — paginated design list with boat counts
- `/api/designs/{id}` — single design detail
- `/api/races` — paginated race list with date/club filtering
- `/api/races/{id}` — single race detail (divisions, finishers)
- `/api/importers` — list configured importers and their schedule
- `/api/stats` — summary statistics (boat/design/race counts, reference factor coverage)

**POST endpoints:**
- `/api/importers/{name}/run` — trigger an importer run
- `/api/importers/stop` — request stop of running import
- `/api/config` — update admin configuration (schedule, thresholds)
- `/api/exclusions` — manage excluded boat/design/race IDs
- `/api/merge` — merge duplicate boats or designs (updates aliases.yaml)

### `AnalysisServlet` — `/api/analyse/*`

REST API for analysis results from `AnalysisCache`:

- `/api/analyse/comparisons` — list all available comparison keys
- `/api/analyse/comparisons/{id}` — get comparison result (pairs, fit, outliers) for a specific key
- `/api/analyse/reference-factors` — per-boat reference factors
- `/api/analyse/design-factors` — per-design aggregated factors

### `StaticResourceServlet` — `/*`

Serves the admin frontend HTML, JavaScript, and CSS from classpath resources (`src/main/resources/content/`).

---

## Frontend: Admin Webapp

A static single-page application (HTML + JavaScript), served as classpath resources. Charts rendered client-side using Plotly.js. No React, no build pipeline — plain JavaScript with DOM manipulation.

### Pages

**Data Browser** (`data.html` + `data.js`):
- Paginated tables for boats, designs, and races with search/filter
- Drill-down to individual entity details (certificates, race history, design members)
- Manual merge interface for duplicate boats and designs
- Exclusion management (mark boats/designs/races as excluded from analysis)

**Analysis** (`analysis.html` + `analysis.js`):
- Dropdown selection of comparison type (system×year×variant pairs)
- Plotly.js scatter plots showing paired observations with linear fit overlay
- Reference factor inspection per boat (spin, non-spin, two-handed with generation info)
- Design-level factor display

**Import Management** (`import.html` + `import.js`):
- Run individual importers (sailsys-races, orc, ams, topyacht, bwps, analysis, reference-factors, build-indexes)
- Configure scheduled runs (days of week, time)
- Live progress display for running imports
- Stop button for running imports

### Chart Types (Implemented)
- **Scatter plots** — paired handicap observations (x=system A TCF, y=system B TCF) with least-squares fit line
- **Residual highlights** — outlier pairs shown in different colour after sigma trimming

### Chart Types (Future — requires Phase 2)
- **Time series / scatter** — HPF estimates per boat per race within a series
- **Box plots or violin plots** — HPF distribution per boat or per design across a season
- **Comparison overlays** — two or more boats or designs on the same axes

---

## Configuration

Admin configuration is stored in `hpf-data/config/admin.yaml` and managed via the admin webapp
or direct file editing. Key settings:

```yaml
importers:                    # list of importers with name, mode, includeInSchedule
schedule:
  days: [MONDAY, SUNDAY]     # days to run scheduled imports
  time: "09:45:00"            # time of day for scheduled runs
nextSailSysRaceId: 32638     # resume point for SailSys race scanning
targetIrcYear: 2025           # target year for reference factor DFS
outlierSigma: null            # outlier trimming threshold (null = default 2.5)
mergeCandidateThreshold: 0.5  # JW threshold for merge candidate display
fuzzyMatchThreshold: 0.9      # JW threshold for boat/design name matching
clubCertificateWeight: 0.9    # weight multiplier for club-level certificates
sailsysCacheMaxAgeDays: 7
sailsysHttpDelayMs: 200
sailsysRecentRaceDays: 14
```
