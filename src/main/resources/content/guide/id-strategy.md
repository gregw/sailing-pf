# ID Strategy

## Principles

- IDs are **never derived from source system proprietary identifiers** (e.g. SailSys internal IDs). Source system IDs may be used transiently during ingestion to interpret incoming data, but are not stored as primary keys. They may be stored in alias/mapping tables for ingestion purposes only.
- IDs are **human readable** where practical — a developer or administrator should be able to identify an entity from its ID without a database lookup.
- IDs are **stable once assigned** — they do not change if canonical names or sail numbers are later corrected.
- Where natural keys are sufficiently clean and stable, they are used directly. Where source data is dirty or ambiguous, a generated surrogate ID is used with aliases recording the raw source data.

---

## Clubs

**Key:** The club's current website domain name (e.g. `manlysc.com.au`, `mbyc.com.au`).

**Rationale:** Domain names are globally unique by definition, human readable, self-documenting, and entirely independent of any source system. They are stable — clubs rarely change domains.

**Aliasing:** If a club has changed domain names historically, the canonical key is the *current* domain. Former domains are stored as aliases.

**Source system mapping:** A small hand-maintained configuration file maps each source system's club identifier (e.g. SailSys club ID) to the canonical domain-name key. This file is built incrementally as new sources are ingested. There are few enough clubs in Australian sailing that this is manageable.

**Known collisions:** Club names are not unique nationally — there are at least two clubs called "Manly Yacht Club" (Manly NSW: `manlysc.com.au`; Wynnum QLD: `mbyc.com.au`). The domain-name key resolves this unambiguously.

---

## Boats

**Key format:** `{normalisedSailNumber}-{normalisedName}-{designId}`

Examples: `AUS1234-raging-tp52`, `5656-mondo-sydney38`, `MYC7-tensixty-radford1060`

**Construction rules** (see `IdGenerator.generateBoatId()`):

- `normalisedSailNumber`: uppercase, strip all non-alphanumeric characters, collapse spaces. E.g. `AUS-1234` → `AUS1234`, `aus 1234` → `AUS1234`.
- `normalisedName`: full name lowercased, non-alphanumeric stripped, spaces collapsed. E.g. `Raging Bull` → `raging_bull`, `TenSixty` → `tensixty`.
- `designId`: the normalised design ID (e.g. `tp52`, `sydney38`). Omitted if design is unknown.

**Rationale:** Sail numbers are mostly unique but not guaranteed so; the name and design add disambiguation. The result is compact, memorable, and source-system-independent.

**Aliasing:** Each Boat has a list of `TimedAlias` records tracking name changes over time:

```java
record TimedAlias(String name, LocalDate from, LocalDate until)
```

The `from`/`until` dates allow tracking when a boat changed names. `activeOn(date)` checks whether the alias was active at a given point.

Additionally, `altSailNumbers` tracks alternative sail numbers seen for the same boat.

**Ingestion matching** uses `normalisedSailNumber` + Jaro-Winkler name similarity (configurable threshold, default 0.90) + design context to resolve an incoming record to an existing Boat, or to create a new one. The `aliases.yaml` seed file provides manual overrides for known difficult matches and sail number redirects.

---

## Designs

**Key:** Normalised design name (lowercase, non-alphanumeric stripped, e.g. `j24`, `farr40`, `sydneyhobart34`).

**Rationale:** Design names are controlled vocabulary — they originate from manufacturers and class associations and are stable. Collisions are extremely unlikely.

**Maker linkage:** A Design references one or more Makers. Most designs have a single maker, but some (e.g. J/24) have been built by multiple manufacturers.

**Aliasing:** Designs may be known by more than one name, typically because the same hull was badged differently by different manufacturers or at different points in time. Examples: the Mumm 30 and Farr 30 are the same design; similarly the Mumm 36 and Farr 36. One name is chosen as canonical (typically the more commonly used current name) and the others are stored as aliases. Ingestion normalisation maps all known aliases to the canonical design key. The alias list is hand-maintained as new equivalences are discovered during data ingestion.

---

## Makers

**Key:** Normalised maker name (lowercase, non-alphanumeric stripped).

---

## Series

**Key:** `{clubDomain}/{normalisedSeriesName}`

Example: `myc.com.au/wednesday-twilight`

**Construction** (see `IdGenerator.generateSeriesId()`): Club domain + `/` + normalised series name. Series names are lowercased, non-alphanumeric characters replaced with hyphens, multiple hyphens collapsed.

Note: Season is not included in the series ID. Series are embedded as records within `Club` JSON files and referenced by `seriesIds` in `Race` records.

**Catch-all series:** Each club may have a pseudo-series named `events` for races that do not belong to any real series. This series is flagged `isCatchAll: true` and is excluded from series-level aggregate analysis.

---

## Races

**Key:** Generated ID — `{clubDomain}-{isoDate}-{nnnn}`

Example: `myc.com.au-2024-11-06-0001`

**Construction** (see `IdGenerator.generateRaceId()`): Club domain + `-` + ISO date + `-` + zero-padded 4-digit race number.

**Rationale:** A race's identity is not cleanly derivable from any series it belongs to, because a race can belong to multiple series. The organising club and date are stable natural attributes; the race number suffix distinguishes multiple races on the same day.

**Named vs numbered races:** Whether a race is identified within its series by a number (Race 7) or a name (Flinders Race) is stored as a race attribute, not reflected in the primary key.

**Persistence:** Races are stored as JSON files at `races/{clubId}/{seriesSlug}/{raceId}.json`.

---

## Finishers (embedded in Race → Division)

**Not a standalone entity.** A `Finisher` is embedded in a `Division` within a `Race`. It records:
- `boatId` — reference to the Boat
- `elapsedTime` — `Duration` (ISO-8601 format in JSON, e.g. `PT1H12M5S`)
- `nonSpinnaker` — per-entry flag from the race
- `certificateNumber` — reference to the certificate used for scoring (nullable)

A boat appears at most once per division.

---

## Certificates (embedded in Boat)

**Not a standalone entity.** `Certificate` records are embedded in the `certificates` list on `Boat`. They are identified by their `certificateNumber` field, which varies by source:

- **ORC certificates:** `certificateNumber` is the ORC `dxtID` (e.g. `"62738"`)
- **AMS certificates:** `certificateNumber` is the AMS cert number
- **Inferred certificates** (from race results): generated IDs like `irc-inferred-ns-4a1f...` or `ty-orc-2024-0.8420`

Each certificate carries variant flags: `nonSpinnaker`, `twoHanded`, `windwardLeeward`, `club`.

---

## Summary Table

| Entity | Key Type | Key Pattern | Storage |
|---|---|---|---|
| Club | Natural | website domain name | `clubs/{clubId}.json` |
| Maker | Natural | normalised name | `catalogue/makers.json` |
| Design | Natural | normalised name | `designs/{designId}.json` |
| Series | Natural composite | `clubDomain/normalisedName` | Embedded in Club |
| Boat | Generated slug | `sailnum-name-designid` | `boats/{boatId}.json` |
| Race | Generated slug | `clubDomain-date-nnnn` | `races/{clubId}/{seriesSlug}/{raceId}.json` |
| Certificate | Embedded | varies by source | Embedded in Boat |
| Finisher | Embedded | keyed by boatId | Embedded in Race → Division |
