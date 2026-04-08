package org.mortbay.sailing.hpf.importer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Imports race results from the Rolex Sydney Hobart Yacht Race (RSHYR) via the
 * CYCA racing feeds API at
 * {@code https://feeds.cycaracing.com/Results/Final/{raceId}/{categoryId}}.
 * <p>
 * For each year ≥ {@value #MIN_YEAR} the importer:
 * <ol>
 *   <li>Looks up (or page-scrapes) the CYCA {@code raceId} for that year's race.</li>
 *   <li>Scans category IDs to identify the Line Honours category (source of elapsed
 *       times), the IRC "All" category (handicap values + division groupings), and
 *       optionally an ORC category.</li>
 *   <li>Computes elapsed time as {@code CorrectedTime − race start}.  The race starts at
 *       13:00 AEDT on 26 December each year; {@code CorrectedTime} in the feeds API is
 *       expressed in AEDT (Australia/Sydney).  The {@code ElapsedTime} field is always the
 *       zero sentinel {@code 0001-01-01T…} and is ignored.</li>
 * </ol>
 * <p>
 * Category auto-discovery uses a linear estimate based on the two known data points
 * (raceId=178 → LH cat 1000 in 2024; raceId=185 → LH cat 1051 in 2025) and scans a
 * ±15/+40 window around the estimate.  Categories are classified by inspection:
 * <ul>
 *   <li><b>Line Honours</b>: all finished entries have TCF = 1.0 ± 0.005.</li>
 *   <li><b>IRC All</b>: entries span ≥ 2 distinct numeric {@code DivisionName} values.</li>
 *   <li><b>IRC per-division</b>: all entries share a single numeric {@code DivisionName}
 *       — skipped (duplicate of the IRC All data).</li>
 *   <li><b>ORC</b>: empty {@code DivisionName}, varying TCF, ≥ 5 entries.</li>
 * </ul>
 * <p>
 * Per CLAUDE.md:
 * <ul>
 *   <li>PHS handicap values are NOT stored.</li>
 *   <li>IRC/ORC TCF values ARE stored as inferred certificates.</li>
 *   <li>CYCA internal IDs (raceId, categoryId) are NOT persisted.</li>
 *   <li>Boat detail pages ({@code CmsUrl}) are not fetched — sail number and name are
 *       available directly in the JSON.</li>
 * </ul>
 */
public class RshyrImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(RshyrImporter.class);

    static final String SOURCE   = "RSHYR";
    static final String FEEDS_BASE   = "https://feeds.cycaracing.com";
    static final String WEBSITE_BASE = "https://rolexsydneyhobart.com";
    static final int    MIN_YEAR     = 2021;
    static final String CLUB_ID      = "cyca.com.au";
    static final String SERIES_NAME  = "Rolex Sydney Hobart Yacht Race";

    /** The race starts at 13:00 AEDT (UTC+11 in December) on 26 December every year. */
    static final ZoneId SYDNEY_TZ      = ZoneId.of("Australia/Sydney");
    static final int    RACE_START_HOUR = 13;

    /**
     * Known CYCA raceId per year, extracted from
     * {@code rolexsydneyhobart.com/race/{year}/results} ({@code const raceId = <n>;}).
     * Update this map when a new race is added.
     */
    static final Map<Integer, Integer> KNOWN_RACE_IDS = Map.of(
        2022, 165,
        2023, 176,
        2024, 178,
        2025, 185
    );

    /**
     * Hardcoded category IDs per year, identified from the race results website tabs
     * ({@code ?category=} parameter).  Fields: lhCategoryId, ircCategoryId, orcCategoryId.
     * <p>
     * Double-handed boats appear in the main IRC All and ORC All categories with "(DH)"
     * appended to their name, so separate DH tabs are not fetched.  The {@code (DH)}
     * marker is detected per-entry in {@link #processEntries} to set
     * {@link Certificate#twoHanded()} correctly.
     * <p>
     * For years not in this map the importer scans upward from
     * {@link #highestKnownCategoryId()} + 1.  Update this map each year by visiting
     * rolexsydneyhobart.com and noting the {@code ?category=} parameter for each tab.
     * <p>
     * 2021 raceId is not hardcoded; it is discovered by scraping the results page.
     */
    static final Map<Integer, RaceCategories> KNOWN_CATEGORIES = Map.of(
        2021, new RaceCategories(732,  840, 841),
        2022, new RaceCategories(862,  863, 864),
        2023, new RaceCategories(985,  986, null),
        2024, new RaceCategories(1000, 1001, null),
        2025, new RaceCategories(1051, 1052, null)
    );

    /** Matches {@code const raceId = 123;} in the website page source. */
    private static final Pattern RACE_ID_JS = Pattern.compile("const\\s+raceId\\s*=\\s*(\\d+)");

    /** Sentinel value used by the API when a field has no real datetime. */
    private static final String ZERO_DATE_PREFIX = "0001-01-01";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataStore  store;
    private final HttpClient httpClient;
    private int recentRaceReimportDays = 30;

    public RshyrImporter(DataStore store, HttpClient httpClient)
    {
        this.store = store;
        this.httpClient = httpClient;
    }

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        DataStore dataStore = new DataStore(dataRoot);
        dataStore.start();
        HttpClient client = new HttpClient();
        client.start();
        try
        {
            new RshyrImporter(dataStore, client).run();
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    // --- Entry point ---

    public void run() throws Exception { run(30); }

    public void run(int recentRaceReimportDays) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        int currentYear = LocalDate.now().getYear();
        LOG.info("RSHYR: scanning years {}–{} (recentReimportDays={})", MIN_YEAR, currentYear, recentRaceReimportDays);
        for (int year = MIN_YEAR; year <= currentYear; year++)
        {
            try
            {
                importYear(year);
            }
            catch (Exception e)
            {
                LOG.error("RSHYR: failed to import year {}: {}", year, e.getMessage(), e);
            }
        }
        LOG.info("RSHYR: done");
    }

    // --- Per-year import ---

    void importYear(int year) throws Exception
    {
        Integer raceId = KNOWN_RACE_IDS.get(year);
        if (raceId == null)
            raceId = discoverRaceId(year);
        if (raceId == null)
        {
            LOG.info("RSHYR: year {} — no raceId (race not yet run or not in feed), skipping", year);
            return;
        }

        String seriesId = IdGenerator.generateSeriesId(CLUB_ID, SERIES_NAME);
        LocalDate raceDate = LocalDate.of(year, 12, 26);
        String raceId_ = IdGenerator.generateRaceId(CLUB_ID, raceDate, 1);

        if (store.races().containsKey(raceId_))
        {
            Race existing = store.races().get(raceId_);
            boolean recent = isRecentRace(raceDate);
            if (SOURCE.equals(existing.source()) && !recent)
            {
                LOG.info("RSHYR: year {} — already imported by RSHYR ({} divisions), skipping",
                    year, existing.divisions() != null ? existing.divisions().size() : 0);
                updateClubSeries(CLUB_ID, seriesId, SERIES_NAME, raceId_);
                return;
            }
            LOG.info("RSHYR: year {} — race exists from source='{}'{}, will reimport",
                year, existing.source(), recent ? " (recent)" : "");
        }

        LOG.info("RSHYR: year {} — cyca raceId={}, starting category discovery", year, raceId);

        RaceCategories cats = discoverCategories(year, raceId);
        if (cats == null)
        {
            LOG.warn("RSHYR: year {} — could not identify Line Honours + IRC/ORC categories", year);
            return;
        }

        // Line Honours: elapsed times for all finishers (DH and non-DH alike).
        LOG.info("RSHYR: year {} — fetching Line Honours (cat {})", year, cats.lhCategoryId());
        List<Entry> lhEntries = fetchCategory(raceId, cats.lhCategoryId());
        Map<String, Duration> elapsedBySailNum = buildElapsedMap(lhEntries, year);
        LOG.info("RSHYR: year {} — {} LH finishers with valid elapsed times", year, elapsedBySailNum.size());

        // IRC All (includes DH boats with "(DH)" in NameRace — twoHanded detected per entry).
        LinkedHashMap<String, List<Finisher>> divMap = new LinkedHashMap<>();
        int count = 0;

        LOG.info("RSHYR: year {} — fetching IRC (cat {})", year, cats.ircCategoryId());
        List<Entry> ircEntries = fetchCategory(raceId, cats.ircCategoryId());
        LOG.info("RSHYR: year {} — {} IRC entries", year, ircEntries.size());
        count += processEntries(ircEntries, "IRC", year, elapsedBySailNum, divMap);

        // ORC All (optional; includes DH boats with "(DH)" in NameRace).
        List<Entry> orcEntries = List.of();
        if (cats.orcCategoryId() != null)
        {
            LOG.info("RSHYR: year {} — fetching ORC (cat {})", year, cats.orcCategoryId());
            orcEntries = fetchCategory(raceId, cats.orcCategoryId());
            LOG.info("RSHYR: year {} — {} ORC entries", year, orcEntries.size());
            count += processEntries(orcEntries, "ORC", year, elapsedBySailNum, divMap);
        }

        if (divMap.isEmpty())
        {
            LOG.warn("RSHYR: year {} — no finishers with matched elapsed times", year);
            return;
        }

        List<Division> divisions = divMap.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();

        boolean hasOrc = cats.orcCategoryId() != null;
        String handicapSystem = hasOrc ? "IRC/ORC" : "IRC";

        store.putRace(new Race(raceId_, CLUB_ID, List.of(seriesId), raceDate, 1,
            SERIES_NAME, handicapSystem, false, divisions, SOURCE, Instant.now(), null));
        LOG.info("RSHYR: year {} saved — {} finishers across {} division(s) [{}]",
            year, count, divisions.size(),
            divisions.stream().map(Division::name).toList());

        updateClubSeries(CLUB_ID, seriesId, SERIES_NAME, raceId_);
    }

    // --- Category discovery ---

    /**
     * Minimum total entry count for a category to be considered a main fleet category
     * (Line Honours, IRC All, ORC All). Filters out per-division sub-categories and
     * small unrelated CYCA races that happen to have TCF=1.0.
     */
    static final int MIN_FLEET_SIZE = 20;

    /**
     * Returns the highest category ID across all {@link #KNOWN_CATEGORIES} entries,
     * used as the starting point for scanning future years.
     */
    static int highestKnownCategoryId()
    {
        int max = 0;
        for (RaceCategories cats : KNOWN_CATEGORIES.values())
        {
            if (cats.lhCategoryId()                > max) max = cats.lhCategoryId();
            if (cats.ircCategoryId()               > max) max = cats.ircCategoryId();
            if (cats.orcCategoryId() != null && cats.orcCategoryId() > max) max = cats.orcCategoryId();
        }
        return max;
    }

    /**
     * Returns the known {@link RaceCategories} for the given year, or discovers them
     * by scanning category IDs.
     * <p>
     * For years in {@link #KNOWN_CATEGORIES} the result is returned immediately with no
     * HTTP requests.  For unknown years the scan starts from
     * {@link #highestKnownCategoryId()} + 1 and proceeds forward, stopping after 12
     * consecutive empty/error responses once at least LH and IRC have been found.
     */
    RaceCategories discoverCategories(int year, int raceId) throws Exception
    {
        RaceCategories known = KNOWN_CATEGORIES.get(year);
        if (known != null)
        {
            LOG.info("RSHYR: year {} raceId={} — using known categories: LH={} IRC={} ORC={}",
                year, raceId, known.lhCategoryId(), known.ircCategoryId(),
                known.orcCategoryId() != null ? known.orcCategoryId() : "none");
            return known;
        }

        // Unknown year: scan forward from the highest category we've seen before.
        int scanStart = highestKnownCategoryId() + 1;
        int scanEnd   = scanStart + 200;
        LOG.info("RSHYR: year {} raceId={} — scanning categories {}–{} (no known entry)",
            year, raceId, scanStart, scanEnd);

        Integer lhCat  = null;
        Integer ircCat = null;
        Integer orcCat = null;
        int consecutiveEmpty = 0;
        boolean foundAny = false;

        for (int catId = scanStart; catId <= scanEnd; catId++)
        {
            List<Entry> entries;
            try
            {
                entries = fetchCategory(raceId, catId);
            }
            catch (Exception e)
            {
                LOG.debug("RSHYR: raceId={} cat={} fetch error: {}", raceId, catId, e.getMessage());
                if (foundAny)
                {
                    consecutiveEmpty++;
                    if (consecutiveEmpty >= 12 && lhCat != null && ircCat != null)
                        break;
                }
                continue;
            }

            if (entries.isEmpty())
            {
                if (foundAny)
                {
                    consecutiveEmpty++;
                    if (consecutiveEmpty >= 12 && lhCat != null && ircCat != null)
                        break;
                }
                continue;
            }
            consecutiveEmpty = 0;
            foundAny = true;

            CategoryType type = classifyCategory(entries);
            LOG.info("RSHYR: raceId={} cat={} entries={} type={}", raceId, catId, entries.size(), type);

            switch (type)
            {
                case LINE_HONOURS -> { if (lhCat  == null) lhCat  = catId; }
                case IRC          -> { if (ircCat == null) ircCat = catId; }
                case ORC          -> { if (orcCat == null) orcCat = catId; }
                default           -> {}
            }
        }

        if (lhCat == null || ircCat == null)
        {
            LOG.warn("RSHYR: category discovery failed for year={} raceId={} (scanned {}–{}): "
                + "lhCat={} ircCat={} orcCat={}",
                year, raceId, scanStart, scanEnd, lhCat, ircCat, orcCat);
            return null;
        }
        LOG.info("RSHYR: year {} raceId={} discovered → LH={} IRC={} ORC={}",
            year, raceId, lhCat, ircCat, orcCat != null ? orcCat : "none");
        return new RaceCategories(lhCat, ircCat, orcCat);
    }

    /**
     * Classifies a category by inspecting its entries.
     * <p>
     * Rules (evaluated in order):
     * <ol>
     *   <li><b>LINE_HONOURS</b>: ≥ {@value MIN_FLEET_SIZE} entries; all finished have
     *       TCF = 1.0 ± 0.005 <em>or</em> TCF = 0.0 (older feed format).</li>
     *   <li><b>IRC</b>: entries span ≥ 2 distinct numeric DivisionName values.
     *       Double-handed boats appear here with "(DH)" in the name and are identified
     *       per-entry in {@link #processEntries}.</li>
     *   <li><b>IRC_SUB</b>: exactly 1 distinct numeric DivisionName — per-division
     *       sub-category, skip.</li>
     *   <li><b>ORC</b>: varying TCF, no numeric divisions, ≥ {@value MIN_FLEET_SIZE}
     *       entries.  DH boats appear here with "(DH)" in the name.</li>
     * </ol>
     */
    static CategoryType classifyCategory(List<Entry> entries)
    {
        if (entries.isEmpty())
            return CategoryType.UNKNOWN;

        long finished = entries.stream()
            .filter(e -> e.isFinished() || "Finished".equalsIgnoreCase(e.status()))
            .count();

        // Detect LH: all finished entries have TCF = 1.0 or (older format) TCF = 0.0.
        if (finished > 0 && entries.size() >= MIN_FLEET_SIZE)
        {
            long tcfOne = entries.stream()
                .filter(e -> e.isFinished() || "Finished".equalsIgnoreCase(e.status()))
                .filter(e -> Math.abs(e.tcf() - 1.0) < 0.005)
                .count();
            long tcfZero = entries.stream()
                .filter(e -> e.isFinished() || "Finished".equalsIgnoreCase(e.status()))
                .filter(e -> e.tcf() == 0.0)
                .count();
            if (tcfOne == finished || tcfZero == finished)
                return CategoryType.LINE_HONOURS;
        }

        // Count distinct numeric DivisionName values.
        Set<String> numericDivs = new java.util.HashSet<>();
        for (Entry e : entries)
        {
            String div = e.divisionName();
            if (div != null && div.matches("[0-9]+"))
                numericDivs.add(div);
        }

        if (numericDivs.size() >= 2)
            return CategoryType.IRC;

        if (numericDivs.size() == 1)
            return CategoryType.IRC_SUB;

        // ORC: varying TCF, no numeric divisions, large enough fleet.
        boolean hasVaryingTcf = entries.stream()
            .anyMatch(e -> Math.abs(e.tcf() - 1.0) > 0.01 && e.tcf() != 0.0);
        if (hasVaryingTcf && entries.size() >= MIN_FLEET_SIZE)
            return CategoryType.ORC;

        return CategoryType.UNKNOWN;
    }

    // --- Elapsed time computation ---

    /**
     * Builds a sail-number → elapsed Duration map from Line Honours entries.
     * <p>
     * Elapsed = {@code CorrectedTime (AEDT) − race start (Dec 26 13:00 AEDT)}.
     * Since all LH boats have TCF = 1.0, their {@code CorrectedTime} equals their
     * finish time with no correction applied.
     */
    Map<String, Duration> buildElapsedMap(List<Entry> lhEntries, int year)
    {
        ZonedDateTime raceStart = ZonedDateTime.of(year, 12, 26, RACE_START_HOUR, 0, 0, 0, SYDNEY_TZ);
        Map<String, Duration> result = new LinkedHashMap<>();

        for (Entry e : lhEntries)
        {
            if (!e.isFinished() && !"Finished".equalsIgnoreCase(e.status()))
                continue;
            if (e.correctedTime() == null || e.correctedTime().startsWith(ZERO_DATE_PREFIX))
                continue;
            String sailNum = normSailNum(e.sailNumber());
            if (sailNum.isBlank())
                continue;

            try
            {
                LocalDateTime finishLocal = LocalDateTime.parse(e.correctedTime());
                ZonedDateTime finish = finishLocal.atZone(SYDNEY_TZ);
                Duration elapsed = Duration.between(raceStart, finish);
                if (elapsed.isPositive())
                    result.put(sailNum, elapsed);
                else
                    LOG.debug("RSHYR: non-positive elapsed for {} ({}): {}", e.nameRace(), sailNum, elapsed);
            }
            catch (DateTimeParseException ex)
            {
                LOG.debug("RSHYR: could not parse CorrectedTime '{}' for {}", e.correctedTime(), e.nameRace());
            }
        }
        return result;
    }

    // --- Entry processing ---

    private int processEntries(List<Entry> entries, String system, int year,
        Map<String, Duration> elapsedBySailNum, LinkedHashMap<String, List<Finisher>> divMap)
    {
        int count = 0;
        for (Entry entry : entries)
        {
            if (!entry.isFinished() && !"Finished".equalsIgnoreCase(entry.status()))
                continue;
            if (entry.sailNumber() == null || entry.sailNumber().isBlank())
                continue;

            String sailNum = normSailNum(entry.sailNumber());
            Duration elapsed = elapsedBySailNum.get(sailNum);
            if (elapsed == null)
            {
                LOG.debug("RSHYR: no LH elapsed for {} ({}) in {} — skipping",
                    entry.nameRace(), sailNum, system);
                continue;
            }

            // CYCA appends "(DH)" or "(TH)" (Two Handed) to double-handed entries.
            // The suffix used varies by year: 2024+ uses "(DH)", earlier years use "(TH)".
            boolean dh = entry.nameRace() != null
                && (entry.nameRace().toUpperCase(Locale.ENGLISH).contains("(DH)")
                    || entry.nameRace().toUpperCase(Locale.ENGLISH).contains("(TH)"));
            String cleanName = dh
                ? entry.nameRace().replaceAll("(?i)\\((DH|TH)\\)", "").trim()
                : entry.nameRace();

            Boat boat = store.findOrCreateBoat(entry.sailNumber(), cleanName, null);
            String certNum = inferCertificate(boat, system, year, entry.tcf(), dh);

            // Division key: "IRC Div 3", "ORC", etc.
            String divKey;
            if (entry.divisionName() != null && !entry.divisionName().isBlank())
                divKey = system + " Div " + entry.divisionName();
            else
                divKey = system;

            divMap.computeIfAbsent(divKey, k -> new ArrayList<>())
                .add(new Finisher(boat.id(), elapsed, false, certNum));
            LOG.info("RSHYR: {} {} {} [{}] elapsed={} cert={}",
                system, divKey, cleanName, entry.sailNumber(),
                String.format("%dd%02dh%02dm",
                    elapsed.toDaysPart(), elapsed.toHoursPart(), elapsed.toMinutesPart()),
                certNum);
            count++;
        }
        return count;
    }

    // --- Private helpers ---

    private static String normSailNum(String s)
    {
        return s == null ? "" : s.trim().toUpperCase(Locale.ENGLISH);
    }

    private String inferCertificate(Boat boat, String system, int year, double tcf, boolean twoHanded)
    {
        for (Certificate cert : boat.certificates())
        {
            if (cert.system().equalsIgnoreCase(system)
                    && Math.abs(cert.year() - year) <= 1
                    && Math.abs(cert.value() - tcf) < 0.001
                    && cert.twoHanded() == twoHanded)
                return cert.certificateNumber();
        }
        String certNum = String.format("rshyr-%s-%d-%.4f%s",
            system.toLowerCase(Locale.ENGLISH), year, tcf, twoHanded ? "-dh" : "");
        Certificate inferred = new Certificate(system, year, tcf, false, twoHanded,
            false, false, certNum, null);
        List<Certificate> certs = new ArrayList<>(boat.certificates());
        certs.add(inferred);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(), boat.altSailNumbers(),
            List.copyOf(certs), addSource(boat.sources(), SOURCE), Instant.now(), null));
        LOG.debug("RSHYR: inferred {} cert {} (tcf={}) for boat {}", system, certNum, tcf, boat.id());
        return certNum;
    }

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }

    private void updateClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        Club club = store.clubs().get(clubId);
        if (club == null)
        {
            Club seed = store.clubSeed().get(clubId);
            if (seed == null)
                return;
            club = new Club(seed.id(), seed.shortName(), seed.longName(), seed.state(),
                seed.excluded(),
                seed.aliases() != null ? seed.aliases() : List.of(),
                seed.topyachtUrls() != null ? seed.topyachtUrls() : List.of(),
                List.of(), null);
            store.putClub(club);
        }

        List<Series> series = new ArrayList<>(club.series());
        int idx = -1;
        for (int i = 0; i < series.size(); i++)
        {
            if (seriesId.equals(series.get(i).id()))
            {
                idx = i;
                break;
            }
        }

        if (idx >= 0)
        {
            Series existing = series.get(idx);
            if (!existing.raceIds().contains(raceId))
            {
                List<String> newIds = new ArrayList<>(existing.raceIds());
                newIds.add(raceId);
                series.set(idx, new Series(existing.id(), existing.name(),
                    existing.isCatchAll(), List.copyOf(newIds)));
            }
        }
        else
        {
            series.add(new Series(seriesId, seriesName, false, List.of(raceId)));
        }

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(),
            club.excluded(), club.aliases(), club.topyachtUrls(), List.copyOf(series), null));
    }

    // --- Network ---

    /**
     * Discovers the CYCA raceId for a given year by scraping the results page and
     * extracting the {@code const raceId = <n>;} JavaScript constant.
     */
    private boolean isRecentRace(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(recentRaceReimportDays));
    }

    Integer discoverRaceId(int year) throws Exception
    {
        String url = WEBSITE_BASE + "/race/" + year + "/results";
        String html;
        try
        {
            html = fetchString(url);
        }
        catch (Exception e)
        {
            LOG.warn("RSHYR: could not fetch results page for year {}: {}", year, e.getMessage());
            return null;
        }
        Matcher m = RACE_ID_JS.matcher(html);
        if (m.find())
        {
            int id = Integer.parseInt(m.group(1));
            LOG.info("RSHYR: year {} → raceId={} (discovered from page)", year, id);
            return id;
        }
        LOG.warn("RSHYR: raceId not found in results page for year {}", year);
        return null;
    }

    List<Entry> fetchCategory(int raceId, int categoryId) throws Exception
    {
        String url = FEEDS_BASE + "/Results/Final/" + raceId + "/" + categoryId;
        String json = fetchString(url);
        Entry[] arr = MAPPER.readValue(json, Entry[].class);
        return (arr == null) ? List.of() : Arrays.asList(arr);
    }

    String fetchString(String url) throws Exception
    {
        ContentResponse response = httpClient.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // --- Inner types ---

    enum CategoryType { LINE_HONOURS, IRC, IRC_SUB, ORC, UNKNOWN }

    record RaceCategories(int lhCategoryId, int ircCategoryId, Integer orcCategoryId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Entry
    {
        @JsonProperty("NameRace")     public String  nameRace;
        @JsonProperty("SailNumber")   public String  sailNumber;
        @JsonProperty("TCF")          public double  tcf;
        @JsonProperty("DivisionName") public String  divisionName;
        @JsonProperty("Status")       public String  status;
        @JsonProperty("IsFinished")   public boolean isFinished;
        @JsonProperty("CorrectedTime") public String correctedTime;

        // accessor aliases used by the importer
        String nameRace()      { return nameRace; }
        String sailNumber()    { return sailNumber; }
        double tcf()           { return tcf; }
        String divisionName()  { return divisionName; }
        String status()        { return status; }
        boolean isFinished()   { return isFinished; }
        String correctedTime() { return correctedTime; }
    }
}
