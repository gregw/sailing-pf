# Data Sources and Formats

## Overview

This document summarises findings from exploration of the data sources available for the Australian yacht racing elapsed time database project. It covers SailSys and TopYacht data formats, external rating data sources, and key observations relevant to ingestion design.

---

## SailSys

### API Endpoints

- **Boats:** `https://api.sailsys.com.au/api/v1/boats/{id}`
- **Races:** `https://api.sailsys.com.au/api/v1/races/{id}/resultsentrants/display`

IDs are sequential integers, brute-forced from 1 upward. The API has no `robots.txt` and no expressed restriction on programmatic access. The main `www.sailsys.com.au` site has a Squarespace robots.txt that blocks AI crawlers but this does not apply to the API subdomain.

### Boat Records

Key fields:
- `id` — SailSys internal boat ID (transient use only, not stored as primary key)
- `name`, `sailNumber` — boat identity (neither guaranteed unique)
- `clubId`, `clubShortName`, `clubLongName` — home club
- `make`, `model` — maker and design respectively; these can bleed together (e.g. "Modified Mumm 36" in the `make` field with `model` null). Not always present in race JSON — the boat endpoint is authoritative.
- Physical measurements (`loa`, `lwl`, `beam`, `draft`, `displacement`) — often zeroed for older or informally registered boats
- `handicaps[]` — array of handicap entries (see below)

**Handicap entries** within a boat record:
- `definition.shortName` — handicap type: `IRC`, `IRC SH`, `AMS`, `ORCiAP`, `ORCiWL`, `PHS`, `CBH`, etc.
- `value` — the handicap number
- `spinnakerType` — 1=spinnaker, 2=non-spinnaker, 3=mixed/either
- `certificate` — present for measurement handicaps (IRC, ORC); contains `certificateNumber` and `expiryDate`. Null for AMS and performance handicaps.
- `requiresCertificate` — false for AMS and PHS; these are stored as bare numbers with no certificate document

**Important:** `make` and `model` fields are **not always present in race JSON** entries. The boat endpoint must be fetched separately for authoritative design/maker information.

### Race Records

Race records contain both entry lists (pre-race) and results (post-race) via the same endpoint. Key fields:

- `status` — race status (4 = processed/complete)
- `raceType` — race type flag
- `lastProcessedTime` — null for unprocessed races; non-null once results are in. Transition from null to non-null is the key event signalling elapsed times are available.
- `defaultHandicapId` / `handicappings[]` — handicap system(s) in use for this race
- `offsetPursuitRace` — pursuit race flag
- `series` — embedded series object (redundantly repeated)
- `club` — embedded club object (redundantly repeated)
- `competitors[]` — array of divisions, each containing:
  - `parent` — division metadata: `id`, `name`, `startTime`, `shortenedCourse`, `abandoned`
  - `items[]` — one entry per boat:
    - `boat` — boat summary (name, sailNumber, make, model, club) — make/model not always present
    - `elapsedTime` — `HH:MM:SS` string, null if DNS/DNF/DNC
    - `nonSpinnaker` — per-entry flag (entry-level decision, not a boat characteristic)
    - `seriesCasualEntry` — casual entry flag
    - `entryStatus` — 0 may indicate withdrawn/DNS
    - `handicap.currentHandicaps[]` — handicap(s) assigned for this race
    - `calculations[]` — two entries: one for each scoring method (handicap and scratch). Contains `handicapCreatedFrom` — **the handicap actually used for scoring**, which may differ from `currentHandicaps[].value`. This is the operationally important value.
    - `penalties[]` — any penalties applied

**Key finding:** `handicapCreatedFrom` in `calculations` is the handicap used for scoring in this specific race — not necessarily the same as the boat's current handicap value. For back-calculation purposes, this is the correct field to reference.

The data is highly redundant — club and division objects are repeated for every competitor entry. This is designed for front-end rendering convenience, not efficient transfer.

### Known Handicap Types in SailSys

| shortName | Description | requiresCertificate |
|---|---|---|
| IRC | IRC spinnaker | true |
| IRC SH | IRC shorthanded | true |
| AMS | Australian Measurement System | false |
| ORCiAP | ORC All Purpose | true |
| ORCiWL | ORC Windward/Leeward | true |
| PHS | SailSys Performance Handicap | false |
| CBH | Class Based Handicap | false |

CBH `spinnakerType` 1 is typically the class base (often 1.000 for one-design classes), with type 2 reflecting the individual boat's non-spinnaker adjustment.

### Current Download Approach

```bash
# Races
while :; do RACE=$(expr $RACE + 1); echo $RACE
  curl -o races/race-$RACE.json \
    https://api.sailsys.com.au/api/v1/races/$RACE/resultsentrants/display
done

# Boats
while :; do BOAT=$(expr $BOAT + 1); echo $BOAT
  curl -o boats/boat-$BOAT.json \
    https://api.sailsys.com.au/api/v1/boats/$BOAT
done
```

Large volumes of data already downloaded locally. The importer must support local file reading as first priority.

### Importer Implementation

Two importers handle SailSys data:

**`SailSysBoatImporter`** — imports boats and IRC certificates:
- **Local file mode** (`--local`) — reads from directory of downloaded `boat-{id}.json` files
- **HTTP API mode** — fetches with configurable throttle (`sailsysHttpDelayMs`, default 200ms) and cache max age (`sailsysCacheMaxAgeDays`, default 7)
- Extracts IRC certificates from `handicaps[]` where `definition.shortName` is `IRC` or `IRC SH`
- Maps `IRC SH` → `twoHanded=true`

**`SailSysRaceImporter`** — imports races and finishers:
- **Local file mode** — reads from directory of downloaded `race-{id}.json` files
- **HTTP API mode** — scans from `sailsysNextRaceId` (configured in `admin.yaml`)
- Infers certificates from `handicapCreatedFrom` in `calculations[]`
- Propagates `nonSpinnaker` flag from race entries to inferred certificates
- Recent race window: `sailsysRecentRaceDays` (default 14) for re-check of recently processed races
---

## TopYacht

### URL Patterns

TopYacht results are published either centrally or on individual club websites:

**Central hosting:**
- Club index: `https://www.topyacht.net.au/results/{clubcode}/{year}/club/index.htm`
- Event index: `https://www.topyacht.net.au/results/{year}/{eventcode}/index.htm`

**Self-hosted clubs:** Results on club's own domain, same HTML structure generated by TopYacht software.

Known club codes (partial): `sasc` (Sydney Amateur SC), `syc` (Sandringham YC), `rmys` (Royal Melbourne YS), `bss` (Brisbane Sailing Squadron), `pmyc` (Port Melbourne YC).

TopYacht claims 140+ clubs in Australia. No central directory exists — club result URLs must be manually curated and maintained as a list. Discovery is an ongoing project activity.

### Result Page Structure

A race result page (e.g. `06RGrp3.htm?ty=75533`) contains:

- Header: series name, club, race number, date, start time, handicap system used
- Table: one row per boat with columns:
  - `Place` — finishing position (or DNC/DSQ/RET etc.)
  - `Boat Name` — linked to `https://www.topyacht.com.au/mt/mt_pub.php?boid={boid}`
  - `Sail No`
  - `Skipper`
  - `From` — home club abbreviation(s); may be concatenated for multi-club boats
  - `Fin Tim` — finish time
  - `Elapsd` — elapsed time `HH:MM:SS`
  - `AHC` — allocated handicap (the value used for scoring; ignore for PHS races)
  - `Cor'd T` — corrected time
  - `Score` — points

The `?ty=` query parameter is a TopYacht internal race identifier — potentially useful as a stable reference.

**The `boid`** (TopYacht boat ID) is present per boat via the boat name link on every result page. This is the key cross-reference to the TopYacht boat register.

### Series Index Page Structure

Lists all races in a series with columns for each handicap system used (e.g. IRC, ORC AP, PHS) and an Entrants link per race. The entrants page contains only name, sail number, skipper and handicap value — no `boid`. The result page is more useful than the entrants page.

### TopYacht Boat Register

URL: `https://www.topyacht.com.au/mt/boat_list.php`

A DataTables-powered HTML table. Columns: Boat Name | Sail Number | Owner | Design | (flag) | Link to boat page (`mt_pub.php?boid=`).

- Good source for cross-referencing sail numbers, names and design names
- Design names are cleaner here than in SailSys (single field, no make/model split)
- Duplicate entries exist for the same sail number (ownership changes, multi-club)
- Individual boat pages (`mt_pub.php?boid=`) show IRC and ORC ratings where held, but there is no known bulk IRC rating list equivalent to the ORC public feed

### Scraping Notes

- TopYacht's `robots.txt` blocks the page; scraping is done from saved local copies or club-hosted pages
- HTML structure is identical regardless of central vs self-hosted publishing — one parser handles both
- Historical seasons follow predictable URL patterns (year substitution)

### Importer Implementation (`TopYachtImporter`)

The TopYacht importer:
1. Reads club URLs from `clubs.yaml` and crawls series index pages to discover race result links
2. Parses each result page HTML (JSoup) extracting boat name, sail number, elapsed time, AHC value
3. **Caption parsing**: Group captions like "Div 1 ORC NS WL results" are parsed to detect:
   - Base system keyword: `ORC`, `AMS`, or `IRC`
   - Variant tokens: `NS` → nonSpinnaker, `WL` → windwardLeeward, `DH`/`2HD`/`SH` → twoHanded
   - Underscores normalised to spaces before splitting (handles `ORC_AP`, `ORC_WL`, etc.)
4. **Multi-page merge**: When the same race appears on multiple pages (different handicap systems),
   results are merged into a single `Race` with multiple divisions. Only measurement system AHC
   values (IRC, ORC, AMS) are retained as inferred certificates.
5. Inferred certificates carry `nonSpinnaker`, `twoHanded`, and `windwardLeeward` flags from the caption

### TopYacht ORC Group Names

TopYacht encodes ORC variants in many ways. The full set from the TopYacht documentation:

| Caption pattern | System | Flags |
|---|---|---|
| ORC | ORC | (standard) |
| ORC NS | ORC | nonSpinnaker |
| ORC WL | ORC | windwardLeeward |
| ORC NS WL | ORC | nonSpinnaker, windwardLeeward |
| ORC DH | ORC | twoHanded |
| ORC LO / ORC HI | ORC | (ignored — wind range indicators) |
| ORCC (ORC Club) | ORC | club |
| ORC_AP | ORC | (all purpose — treated as standard) |
| ORC_WL | ORC | windwardLeeward |

Similarly for AMS (`AMS NS`, `AMS 2HD`) and IRC (`IRC_SH` → twoHanded).

---

## ORC Certificate Data

**URL:** `https://data.orc.org/public/WPub.dll?action=activecerts&CountryId=AUS`

Freely accessible XML feed — no terms of use, no login, no disclaimer. ORC's open-data policy is deliberate and consistent with their transparent algorithm philosophy.

**Full certificate:** `https://data.orc.org/public/WPub.dll/CC/{dxtID}`

### Certificate List Fields

| Field | Description |
|---|---|
| `dxtID` | ORC certificate ID — key for fetching full certificate |
| `RefNo` | Certificate reference number |
| `YachtName` | Boat name |
| `SailNo` | Sail number |
| `VPPYear` | Year of VPP calculation |
| `CertType` | Certificate subtype (see below) |
| `Expiry` | Certificate expiry date |
| `IsOd` | One-design flag |
| `Class` | Design name — clean, authoritative, consistent |
| `CertName` | Certificate name (e.g. "Club", "International") |
| `FamilyName` | Certificate family (ORC Standard / Double Handed / Non Spinnaker) |
| `dxtName` | Hull file reference (e.g. `2883.dxt`) — boats sharing the same `.dxt` are the same hull |

**CertType values observed:**
- 1 = IRC + ORC International (combined certificate)
- 2 = ORC International
- 3 = ORC Club
- 8 = DH (Double Handed) International
- 9 = DH Club
- 10 = NS (Non Spinnaker) International
- 11 = NS Club

**International vs Club:** CertTypes 1, 2, 8, 10 are international; all others are club.
The `OrcImporter` uses `INTL_CERT_TYPES = Set.of("1", "2", "8", "10")` and sets `club=true`
for certificates not in this set.

### Value for the Project

Even for boats that race under IRC or PHS, the ORC data provides:
- **Authoritative design names** via `Class` — far cleaner than SailSys make/model
- **Hull file grouping** via `dxtName` — identifies sister ships and design variants
- **VPP performance envelope** — characterises what a design should theoretically be capable of, useful for design-level reference quality even without individual certificates
- **Cross-design comparisons** — ORC GPH allows comparison of designs that may never have raced each other

Recommended: periodic full ingest of Australian certificate list to enrich design records, regardless of whether individual boats hold current ORC certificates.

---

## IRC Certificate Data

**URL:** `https://ircrating.org/irc-racing/online-tcc-listings/`

**Do not use.** The RORC/YCF disclaimer explicitly prohibits use of this data "for the purpose of or contributing to the creation of a handicap or rating or other time/performance adjustment factor for any boat." This is precisely what this project does.

**Alternative sources for IRC values:**
- SailSys boat records include IRC TCC values and certificate numbers where boats hold certificates (seen in boat-013143.json for "Magic")
- TopYacht race result pages publish the IRC value used in each race
- These sources represent publicly published race data rather than the proprietary RORC database, and are acceptable to use

---

## AMS Certificate Data

**Source:** raceyachts.org

AMS (Australian Measurement System) certificates are scraped from the raceyachts.org listing by `AmsImporter`. AMS is primarily used in Victoria and produces a TCF value on the same scale as IRC TCC. Includes non-spinnaker and two-handed variants.

---

## BWPS Race Data

**Source:** CYCA (Cruising Yacht Club of Australia) publicly available result pages

BWPS (BlueSail WorldWide Performance System) race results are imported by `BwpsImporter`. These are IRC-based offshore races providing boat identity, elapsed times, and IRC handicaps used for scoring. A good source of IRC AHC values from high-quality offshore racing.

---

## Cross-System Observations

### Boat Identity

- SailSys uses sequential integer `id` (transient only)
- TopYacht uses `boid` (non-sequential integer)
- Neither should be used as a primary key — canonical boat IDs are generated slugs per the id_strategy
- The same physical boat may appear in both systems with different IDs

### Design Names

Quality ranking (best to worst):
1. ORC `Class` field — controlled vocabulary, authoritative
2. TopYacht boat register design column — single clean field
3. SailSys `model` field — reasonable but inconsistent spacing/capitalisation
4. SailSys `make` field — sometimes bleeds design information (e.g. "Modified Mumm 36")

### Notable Real-World Cases

- **"Magic" (sail# 36111, RSYS)** — appears in SailSys boat records with IRC and ORC certificates, and in CYCA race results racing non-spinnaker under PHS. Good cross-club candle example.
- **"San Toy" (sail# MYC12, MYC)** — cat-rigged, never flies a spinnaker, but enters spinnaker races. The `nonSpinnaker` entry flag is set when available but not always. Her spinnaker and non-spinnaker handicaps are identical. Illustrates that `nonSpinnaker` on a race entry is an administrative classification, not a reliable indicator of what the boat actually did.
- **"Esprit" (sail# MYC32, MYC)** — appears in SailSys boat records with no handicaps, and in ORC certificate feed as "ESPRIT" SM477 (different sail number — likely a different boat of the same name).
- **"BLISS" (sail# 7702)** — appears in both CYCA race results and ORC certificate feed (Grand Soleil 44, International certificate). Instant reference quality anchor.

### Handicap System Coverage by Source

| System | SailSys boats | SailSys races | TopYacht results | ORC feed | AMS feed | BWPS | IRC feed |
|---|---|---|---|---|---|---|---|
| IRC | ✓ (certificate) | ✓ (AHC used) | ✓ (AHC used) | — | — | ✓ (AHC used) | ✗ (restricted) |
| ORC | ✓ (certificate) | ✓ (AHC used) | ✓ (AHC used) | ✓ (full feed) | — | — | — |
| AMS | ✓ (no cert) | ✓ (AHC used) | ✓ (AHC used) | — | ✓ (full list) | — | — |
| PHS | ✗ (excluded) | ✗ (excluded) | ✗ (excluded) | — | — | — | — |
| CBH | ✗ (excluded) | ✗ (excluded) | ✗ (excluded) | — | — | — | — |

### Certificate Variant Flags

All certificate records carry boolean variant flags, populated by the importer that creates them:

| Flag | Meaning | Set by |
|---|---|---|
| `nonSpinnaker` | NS certificate | OrcImporter (CertType 10/11, FamilyName), TopYacht caption (NS), SailSys race entry flag |
| `twoHanded` | DH/SH/2HD certificate | OrcImporter (CertType 8/9), TopYacht caption (DH/2HD/SH), SailSys IRC SH |
| `windwardLeeward` | WL course-specific cert | TopYacht caption (WL) |
| `club` | Club-level cert (not international) | OrcImporter (CertType not in 1,2,8,10) |
