# CLAUDE.md — Australian Yacht Racing Elapsed Time Database

This file orients Claude Code to the project. Read it before making any changes.
All linked documents are in the `.claude/` directory alongside this file.

---

## What This Project Is

An online database of elapsed times from Australian yacht racing, with statistical analysis
tools that compute an **HPF (Historical Performance Factor)** for each boat.

**HPF** is the back-calculated handicap a boat would have needed, averaged across its racing
history, to have been equal-time with a hypothetical 1.000 reference boat. It is a historical
performance measure — not a handicap system.

**Primary use case:** Informing the allocation of initial handicaps at season start, or when a
new boat joins a series. This tool explicitly does not replace how clubs age or adjust handicaps
between races day-to-day.

---

## Project Documentation

Read these files before working in any given area:

| File | What it covers |
|---|---|
| [project_description.md](.claude/project_description.md) | Full overview: data collection, reference (candle) network, HPF estimation, website |
| [object_model_architecture.md](.claude/object_model_architecture.md) | Java object model, layering principles, what NOT to replicate from the old prototype |
| [processing_pipeline.md](.claude/processing_pipeline.md) | Step-by-step pipeline from data ingestion through to HPF output |
| [id_strategy.md](.claude/id_strategy.md) | How entities are keyed and disambiguated — read before touching IDs |
| [data_sources_and_formats.md](.claude/data_sources_and_formats.md) | SailSys API, TopYacht HTML, ORC feed — field-level detail |
| [presentation_layer.md](.claude/presentation_layer.md) | REST API design and browser-based charting frontend |
| [handicaps.txt](.claude/handicaps.txt) | Domain knowledge notes on yacht racing handicap systems |

## Background

This project is being started in the repository called sailing-handicaps, which contains a prototype of this project as well as downloaded sailsys files and processed race data.
The intention is that this project will only be used for the purpose of training Claude on the problem domain and past attempts.
After analysis, the intention is to remove all the code and start again, renaming the project to sailing-history-performance-factors.
The sailsys files which have been downloaded are publicly available but probably contain proprietary data.
They are to be processed and only public data (e.g. names and elapsed times) extracted and stored in a data format for this project.
The sailsys files are to be deleted.

---

## Technology Stack

- **Language:** Java (Spring Boot - maybe?)
- **Data access:** Jackson JSON files inflated to java records.
- **Frontend:** Plain JavaScript, Plotly.js — no React, no build pipeline
- **Deployment target:** Google Cloud Run or Fly.io (scale-to-zero)

---

## Architecture: Four-Layer Model

This is the most important architectural constraint. **Never mix layers.**

```
Optimised derived  →  Deterministic derived  →  Raw
Index              →  Raw
Raw                →  (nothing)
```

### Layer 1: Raw (always persisted, immutable)
- Ingested data exactly as received from source systems
- Java **records** — immutable, no back-references, no derived fields
- Entities: `Boat`, `Race`, `Entry`, `MeasurementCertificate`, `Club`, `Design`, `Series`, `Season`

### Layer 2: Deterministic Derived (persisted, invalidated on raw change)
- Computed from raw by pure functions with no tuning parameters
- Java **records** — always the same output given the same raw input
- Entities: `BoatReference`, `DesignReference`, `RaceDispersion`
- Invalidated and recomputed when upstream raw records change

### Layer 3: Index (derived, optionally persisted for performance)
- Provides navigation that would otherwise require back-references (e.g. "all races for a boat")
- Always recomputable from raw; correctness never depends on the persisted index
- Use `entryIndex.entriesByBoatId(boat.id())` — **never** `boat.getEntries()`
- Is essentially a special case of Deterministic Derived data.

### Layer 4: Optimised Derived (never persisted, recomputed on demand)
- Output of the HPF optimisation — depends on configuration (scope, λ, convergence threshold)
- Mutable **classes** for working objects during iteration; **records** for final output snapshots
- Working types: `EntryWeight`, `RaceContext`, `BoatHpfEstimate`
- Final output: `BoatHpf` (record)

### Persistence boundary summary

| Layer | Persisted? |
|---|---|
| Raw | Always |
| Deterministic derived | Yes (invalidated on upstream change) |
| Index | Optional (performance only) |
| Optimised derived | Never |

---

## Key Domain Concepts

### HPF (Historical Performance Factor)
The central output. The back-calculated handicap a boat would have needed to be equal-time
with a 1.000 reference boat, averaged across its racing history. **Historical measure only.**

### References (aka "standard candles")
Boats whose performance can be independently estimated from measurement-based handicaps.
Used to anchor the optimisation.  They function like standard candles in Astrophysics.  Four quality tiers with associated factor weights:

| Tier | Basis | Weight |
|---|---|---|
| 1 | IRC/ORC/AMS certificate used in the race | 1.0 |
| 2 | IRC/ORC/AMS certificate held, used for the race | 0.9 |
| 3 | Holds certificate but racing under PHS | 0.8 |
| 4 | No certificate; design has many certificated boats | 0.7 |

### Measurement Handicap Systems
- **IRC** — secret algorithm, RORC/UNCL managed, produces TCC. Do NOT use the RORC online
  TCC database (restricted). Acceptable sources: SailSys boat records, published race result AHC values.
- **ORC / ORCi / ORCclub** — transparent VPP algorithm, public data feed at `data.orc.org`.
  Produces GPH (convert to TCF: `TCF = 600 / GPH`) and full VPP curves.
- **AMS** — Australian system, Victorian-primary, 4-year recalibration cycle, no public algorithm.
- **PHS** — performance (past results) based. **Exclude PHS handicap values entirely as they may reflect proprietary information of other clubs or systems.**
  Elapsed times from PHS races are valid and used; only the PHS number itself is excluded.

---

## Data Sources

### SailSys
- API base: `https://api.sailsys.com.au/api/v1/`
- Boats: `/boats/{id}` — sequential integer IDs, brute-forced
- Races: `/races/{id}/resultsentrants/display`
- **Key field:** `handicapCreatedFrom` in `calculations[]` is the handicap actually used for
  scoring — use this, not `currentHandicaps[].value`
- `make` / `model` can bleed together; prefer ORC `Class` for design names
- **Local file mode is first priority** — large volume already downloaded
- extract only data that is public record: names, sail numbers, clubs, elapsed times, etc.

### TopYacht
- Data may be on many individual club websites with similar HTML structure
- There may be `robots.txt` files that block crawling; scrape from saved local copies or club-hosted pages
- `boid` in boat name links is the cross-reference to the TopYacht boat register, however, it should not be persisted.
- `AHC` column is the handicap used for scoring and it should be ignored. Only elapsed times are used.

### Manly Yacht Club
- For many years MYC ran their own handicap processing system and much of the data is still relevant to their current fleet.

### ORC Certificate Feed
- `https://data.orc.org/public/WPub.dll?action=activecerts&CountryId=AUS`
- Open data, no restrictions. Best source for authoritative design names (`Class` field)
- `dxtName` field groups sister ships (same hull file = same design variant)

---

## Entity Identity Rules

Read [id_strategy.md](.claude/id_strategy.md) fully before touching IDs. Summary:

| Entity | Key pattern | Example |
|---|---|---|
| Club | Website domain | `manlysc.com.au` |
| Maker | Normalised name | `farr` |
| Design | Normalised name | `j24`, `farr40` |
| Season | Year label | `2024-25` |
| Series | `clubDomain/season/normalisedName` | `manlysc.com.au/2024-25/wednesday-twilight` |
| Boat | `sailnum-firstname-hex` | `aus1234-raging-3f9a` |
| Race | `clubDomain-date-hex` | `manlysc.com.au-2024-11-06-4a1f` |
| RaceEntry | `raceId+boatId` | composite |
| MeasurementCertificate | `boatId-type-year-hex` | `aus1234-raging-3f9a-irc-2024-001` |

**IDs are never derived from SailSys or TopYacht internal IDs.** Source system IDs may be
stored in alias/mapping tables for ingestion purposes only, never as primary keys.

---

## Things NOT to Do (Lessons from Old Prototype)

The old prototype is in the repo as domain knowledge only. **Do not replicate:**

1. **Static global registries** — no `static ArrayList` singletons on entity classes
2. **Mixed raw and derived fields** — raw records are immutable; derived state lives in
   separate layer objects
3. **Back-references from parent to child** — use index objects, not `boat.getEntries()`
4. **Club-specific logic in generic domain objects** — club concerns stay in importers/services
5. **Analysis logic on entity objects** — optimisation logic belongs in `HpfOptimiser` service
6. **save/restore pattern on entities** — symptom of mutable derived state on raw objects;
   working state lives in `BoatHpfEstimate` and friends, never on raw records

---

## Service Layer

Logic lives in services, not entities:

- `ReferenceNetworkBuilder` — computes `BoatReference` and `DesignReference` (pipeline steps 8–12)
- `HpfOptimiser` — alternating least squares HPF optimisation (steps 13–17)
- `SailSysImporter` — translates SailSys JSON into raw layer records
- `TopYachtImporter` — translates TopYacht HTML into raw layer records

---

## Dataset Size

Small: ~600 boats each doing ~50 races/year. No need for premature optimisation. Prefer clarity.

---

## Build Order

1. Data ingestion (SailSys importer, TopYacht scraper) → raw layer populated
2. Reference network builder → deterministic derived layer
3. HPF optimiser → optimised derived layer
4. REST API → expose derived data as JSON
5. Frontend → charts against real API output

**The frontend is last.** Do not design charts against hypothetical data shapes.

---

## Licence

- Source code: Apache 2.0
- Derived data (HPF values, reference factors): CC BY-SA 4.0
- Raw race results: no claim — public record

See [LICENSE.md](LICENSE.md) for full details.

## Communication Style

Use plain, direct language at all times. No jokes, wordplay, puns, or attempts at humour.
No dramatic flair or colourful descriptions of what you are doing. State actions and
findings factually. "Running tests" not "putting the code through its paces".