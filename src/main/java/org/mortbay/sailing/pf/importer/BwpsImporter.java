package org.mortbay.sailing.pf.importer;

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports race results for the CYCA Blue Water Pointscore (BWPS) from three data sources:
 * <ol>
 *   <li><b>BWPS minor offshore races</b> (Bird Island, Cabbage Tree Island, Flinders Islet,
 *       etc.): listed at {@code bwps.cycaracing.com/race/<year>/results}, each race page giving
 *       its raceId for the CYCA feeds API. (The old {@code /standings/} pages are gone.)</li>
 *   <li><b>Rolex Sydney Hobart Yacht Race (RSHYR)</b> via the CYCA feeds API at
 *       {@code feeds.cycaracing.com} -- higher-quality per-division data from the dedicated
 *       race website.</li>
 *   <li><b>Noakes Sydney Gold Coast Yacht Race</b> via the same CYCA feeds API -- same
 *       platform as RSHYR.</li>
 * </ol>
 * <p>
 * All races are grouped into per-year "Blue Water Pointscore {year}" series.  The two major
 * races also belong to their own dedicated series (dual membership).
 * <p>
 * Yacht design information is fetched from the yacht listing pages on each site to supplement
 * race result data where the feeds API only provides sail number and name.
 */
public class BwpsImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(BwpsImporter.class);

    // --- Source constants ---
    static final String SOURCE = "BWPS";
    static final String RSHYR_SOURCE = "RSHYR";
    static final String GOLDCOAST_SOURCE = "GoldCoast";

    /** All sources produced by this importer; used by SailSysImporter for cert-only mode. */
    public static final Set<String> ALL_SOURCES = Set.of(SOURCE, RSHYR_SOURCE, GOLDCOAST_SOURCE);

    // --- BWPS standings constants ---
    static final String BASE_URL = "https://bwps.cycaracing.com";
    public static final int DEFAULT_MIN_YEAR = 2020;
    static final String CLUB_ID = "cyca.com.au";
    static final String SERIES_NAME_PREFIX = "Blue Water Pointscore";

    // --- CYCA feeds API constants ---
    static final String FEEDS_BASE = "https://feeds.cycaracing.com";
    static final ZoneId SYDNEY_TZ = ZoneId.of("Australia/Sydney");
    private static final String ZERO_DATE_PREFIX = "0001-01-01";
    private static final Pattern RACE_ID_JS = Pattern.compile("const\\s+raceId\\s*=\\s*(\\d+)");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Minimum fleet size for category auto-discovery. */
    static final int MIN_FLEET_SIZE = 20;

    // --- RSHYR configuration ---
    static final CycaFeedsRaceConfig RSHYR_CONFIG = new CycaFeedsRaceConfig(
        RSHYR_SOURCE,
        "Rolex Sydney Hobart Yacht Race",
        "https://rolexsydneyhobart.com",
        Map.of(2022, 165, 2023, 176, 2024, 178, 2025, 185),
        Map.of(
            2021, new RaceCategories(732,  840,  841),
            2022, new RaceCategories(862,  863,  864),
            2023, new RaceCategories(985,  986,  null),
            2024, new RaceCategories(1000, 1001, null),
            2025, new RaceCategories(1051, 1052, null)),
        Map.of(),      // no per-year overrides needed; fixed date below
        12, 26,        // always Dec 26
        13,            // 13:00 AEDT start
        2021);

    // --- Gold Coast configuration ---
    // No race in 2020 or 2021 (COVID cancellations).
    static final CycaFeedsRaceConfig GOLDCOAST_CONFIG = new CycaFeedsRaceConfig(
        GOLDCOAST_SOURCE,
        "Noakes Sydney Gold Coast Yacht Race",
        "https://goldcoast.cycaracing.com",
        Map.of(2018, 112, 2019, 145, 2022, 158, 2023, 170, 2024, 179, 2025, 186),
        Map.of(
            2018, new RaceCategories(500, 501, 503),
            2019, new RaceCategories(696, 697, 699),
            2022, new RaceCategories(761, 762, 764),
            2023, new RaceCategories(932, 933, null),
            2024, new RaceCategories(1008, 1009, null),
            2025, new RaceCategories(1060, 1061, null)),
        Map.of(
            2018, LocalDate.of(2018, 7, 28),
            2019, LocalDate.of(2019, 7, 27),
            2022, LocalDate.of(2022, 7, 30),
            2023, LocalDate.of(2023, 7, 29),
            2024, LocalDate.of(2024, 7, 27),
            2025, LocalDate.of(2025, 7, 26)),
        0, 0,          // date varies each year; use knownRaceDates
        13,            // 13:00 AEST start
        2018);

    /** Race names in the BWPS standings that are handled via the feeds API instead. */
    private static final List<String> FEEDS_RACE_KEYWORDS = List.of("SYDNEY HOBART", "GOLD COAST");

    private final DataStore store;
    private final HttpClient httpClient;
    private int recentRaceReimportDays = 30;
    private int minYear = DEFAULT_MIN_YEAR;

    public BwpsImporter(DataStore store, HttpClient httpClient)
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
            new BwpsImporter(dataStore, client).run();
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    // ========================================================================
    // Entry point
    // ========================================================================

    public void run() throws Exception { run(30, DEFAULT_MIN_YEAR); }

    public void run(int recentRaceReimportDays) throws Exception { run(recentRaceReimportDays, DEFAULT_MIN_YEAR); }

    public void run(int recentRaceReimportDays, int minYear) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        this.minYear = minYear;

        int currentYear = LocalDate.now().getYear();
        // Sail number → design, from the yacht listing pages; filled in by both phases.
        Map<String, String> yachtDesigns = new LinkedHashMap<>();

        // Phase 1: BWPS minor races from the results pages. A failure here must not stop
        // the major races below.
        try
        {
            importBwpsResults(minYear, currentYear, yachtDesigns);
        }
        catch (Exception e)
        {
            ImporterLog.error(LOG, "BWPS: minor-race import failed: {}", e.getMessage(), e);
        }

        // Phase 2: Major races via CYCA feeds API

        for (CycaFeedsRaceConfig config : List.of(RSHYR_CONFIG, GOLDCOAST_CONFIG))
        {
            for (int year = Math.max(config.minYear(), minYear); year <= currentYear; year++)
            {
                // Fetch yacht designs for this year's race website (may 404 for future years)
                yachtDesigns.putAll(fetchYachtDesigns(
                    config.websiteBase() + "/race/" + year + "/yachts"));

                try
                {
                    importCycaFeedsYear(config, year, yachtDesigns);
                }
                catch (Exception e)
                {
                    ImporterLog.error(LOG, "BWPS: failed to import {} year {}: {}",
                        config.source(), year, e.getMessage(), e);
                }
            }
        }

        LOG.info("BWPS: all phases complete");
    }

    // ========================================================================
    // Phase 1: BWPS minor races from the results pages + CYCA feeds
    // ========================================================================
    //
    // The BWPS site lists each season's races at /race/<year>/results, linking to
    // /race/<year>/results/<race-slug>; each race page embeds its CYCA feeds raceId. The feeds
    // give the start time (Race/Summary) and the results per category: elapsed times come only
    // from Line Honours (finish − start); the IRC (and ORC) results supply each boat's rating
    // for its certificate. Hobart and Gold Coast are skipped here — phase 2 imports them.

    /** Imports every minor BWPS race of the seasons fromYear..toYear. */
    void importBwpsResults(int fromYear, int toYear, Map<String, String> yachtDesigns)
    {
        for (int year = fromYear; year <= toYear; year++)
        {
            List<String> slugs;
            try
            {
                slugs = parseResultsIndex(fetchString(BASE_URL + "/race/" + year + "/results"), year);
            }
            catch (Exception e)
            {
                ImporterLog.warn(LOG, "BWPS: no results index for {}: {}", year, e.getMessage());
                continue;
            }
            LOG.info("BWPS: {} race(s) listed for {}: {}", slugs.size(), year, slugs);
            yachtDesigns.putAll(fetchYachtDesigns(BASE_URL + "/race/" + year + "/yachts"));
            for (String slug : slugs)
            {
                String raceName = raceNameFromSlug(slug);
                if (isFeedsMajorRace(raceName))
                    continue;   // phase 2
                try
                {
                    importBwpsRace(year, slug, raceName, yachtDesigns);
                }
                catch (Exception e)
                {
                    ImporterLog.error(LOG, "BWPS: failed to import '{}' {}: {}", raceName, year,
                        e.getMessage(), e);
                }
            }
        }
    }

    /** The race slugs linked from a season's results index, in page order. */
    static List<String> parseResultsIndex(String html, int year)
    {
        Matcher m = Pattern.compile("href=\"/race/" + year + "/results/([a-z0-9-]+)/?\"").matcher(html);
        LinkedHashSet<String> slugs = new LinkedHashSet<>();
        while (m.find())
            slugs.add(m.group(1));
        return List.copyOf(slugs);
    }

    /** "flinders-islet-race" → "Flinders Islet Race". */
    static String raceNameFromSlug(String slug)
    {
        return Arrays.stream(slug.split("-"))
            .filter(w -> !w.isEmpty())
            .map(w -> Character.toUpperCase(w.charAt(0)) + w.substring(1))
            .collect(Collectors.joining(" "));
    }

    private static boolean isFeedsMajorRace(String raceName)
    {
        String upper = raceName.toUpperCase(Locale.ENGLISH);
        return FEEDS_RACE_KEYWORDS.stream().anyMatch(upper::contains);
    }

    /** Imports one minor race, unless it is already held and not recent. */
    void importBwpsRace(int year, String slug, String raceName, Map<String, String> yachtDesigns)
        throws Exception
    {
        String seriesName = SERIES_NAME_PREFIX + " " + year;
        String seriesId   = IdGenerator.generateSeriesId(CLUB_ID, seriesName);

        // Held already (matched by name and year, so no request is needed) and not recent.
        Race held = store.races().values().stream()
            .filter(r -> CLUB_ID.equals(r.clubId()) && SOURCE.equals(r.source())
                && raceName.equalsIgnoreCase(r.name()) && r.date() != null && r.date().getYear() == year)
            .findFirst().orElse(null);
        if (held != null && !isRecentRace(held.date()))
        {
            updateClubSeries(CLUB_ID, seriesId, seriesName, held.id());
            return;
        }

        Matcher idMatch = RACE_ID_JS.matcher(fetchString(BASE_URL + "/race/" + year + "/results/" + slug));
        if (!idMatch.find())
        {
            ImporterLog.warn(LOG, "BWPS: no raceId on the results page for '{}' {}", raceName, year);
            return;
        }
        int cycaRaceId = Integer.parseInt(idMatch.group(1));

        RaceSummary summary = MAPPER.readValue(
            fetchString(FEEDS_BASE + "/Race/Summary/" + cycaRaceId), RaceSummary.class);
        if (summary.startDateTime() == null || summary.startDateTime().startsWith(ZERO_DATE_PREFIX))
        {
            LOG.info("BWPS: '{}' {} (raceId={}) has no start time yet -- skipping", raceName, year, cycaRaceId);
            return;
        }
        LocalDateTime start = LocalDateTime.parse(summary.startDateTime());
        if (start.atZone(SYDNEY_TZ).isAfter(ZonedDateTime.now(SYDNEY_TZ)))
        {
            LOG.info("BWPS: '{}' {} starts {} -- not sailed yet, skipping", raceName, year, start);
            return;
        }
        LocalDate raceDate = start.toLocalDate();
        String raceId = IdGenerator.generateRaceId(CLUB_ID, raceDate, 1);
        Race existing = store.races().get(raceId);
        if (existing != null && SOURCE.equals(existing.source()) && !isRecentRace(raceDate))
        {
            updateClubSeries(CLUB_ID, seriesId, seriesName, raceId);
            return;
        }

        Integer lhCat = null, ircCat = null, orcCat = null;
        for (SummaryCategory c : summary.categories() == null ? List.<SummaryCategory>of() : summary.categories())
        {
            if (c.id() == null || c.name() == null)
                continue;
            String name = c.name().trim().toUpperCase(Locale.ENGLISH);
            if (lhCat == null && (name.equals("LINE HONOURS") || "Line Honours".equalsIgnoreCase(c.type())))
                lhCat = c.id();
            else if (ircCat == null && name.equals("IRC"))
                ircCat = c.id();
            else if (orcCat == null && (name.equals("ORC") || name.equals("ORCI")))
                orcCat = c.id();
        }
        if (lhCat == null || ircCat == null)
        {
            ImporterLog.warn(LOG, "BWPS: '{}' {} (raceId={}) -- Line Honours or IRC category missing",
                raceName, year, cycaRaceId);
            return;
        }

        // Elapsed times from Line Honours only: finish (its CorrectedTime, at TCF 1) − start.
        Map<String, Duration> elapsedBySailNum =
            buildElapsedMap(fetchCategory(cycaRaceId, lhCat), start.atZone(SYDNEY_TZ));

        // IRC (and ORC) results only for each boat's rating → certificate; finishers without
        // a Line Honours elapsed time are dropped.
        LinkedHashMap<String, List<Finisher>> divMap = new LinkedHashMap<>();
        int count = processFeedsEntries(fetchCategory(cycaRaceId, ircCat), "IRC", raceDate,
            elapsedBySailNum, divMap, SOURCE, yachtDesigns);
        if (orcCat != null)
            count += processFeedsEntries(fetchCategory(cycaRaceId, orcCat), "ORC", raceDate,
                elapsedBySailNum, divMap, SOURCE, yachtDesigns);
        if (divMap.isEmpty())
        {
            ImporterLog.warn(LOG, "BWPS: '{}' {} -- no IRC/ORC finishers with Line Honours times",
                raceName, year);
            return;
        }

        List<Division> divisions = divMap.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();
        store.putRace(new Race(raceId, CLUB_ID, List.of(seriesId), raceDate, 1,
            raceName, divisions, SOURCE, Instant.now(), null));
        LOG.info("BWPS: imported race {} '{}' {} ({} finishers, {} division(s))",
            raceId, raceName, year, count, divisions.size());
        updateClubSeries(CLUB_ID, seriesId, seriesName, raceId);
    }

    /** The parts of /Race/Summary/{raceId} used: start time and the results categories. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RaceSummary(@JsonProperty("StartDateTime") String startDateTime,
                       @JsonProperty("Status") String status,
                       @JsonProperty("Categories") List<SummaryCategory> categories) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record SummaryCategory(@JsonProperty("Id") Integer id,
                           @JsonProperty("Name") String name,
                           @JsonProperty("Type") String type) {}

    // ========================================================================
    // Phase 2: CYCA feeds API races (RSHYR, Gold Coast)
    // ========================================================================

    /**
     * Imports a single year of a CYCA feeds-API race (RSHYR or Gold Coast).
     */
    void importCycaFeedsYear(CycaFeedsRaceConfig config, int year,
                             Map<String, String> yachtDesigns) throws Exception
    {
        // Determine race date
        LocalDate raceDate = config.raceDate(year);
        if (raceDate == null)
        {
            LOG.debug("{}: year {} -- no known race date, skipping", config.source(), year);
            return;
        }

        // Discover or look up the CYCA raceId
        Integer cycaRaceId = config.knownRaceIds().get(year);
        if (cycaRaceId == null)
            cycaRaceId = discoverRaceId(config, year);
        if (cycaRaceId == null)
        {
            LOG.info("{}: year {} -- no raceId (race not yet run or not in feed), skipping",
                config.source(), year);
            return;
        }

        // BWPS year series (all races share this)
        String bwpsSeriesName = SERIES_NAME_PREFIX + " " + year;
        String bwpsSeriesId   = IdGenerator.generateSeriesId(CLUB_ID, bwpsSeriesName);

        // Dedicated race series
        String raceSeriesId = IdGenerator.generateSeriesId(CLUB_ID, config.seriesName());

        String raceId = IdGenerator.generateRaceId(CLUB_ID, raceDate, 1);

        // Check for existing race
        if (store.races().containsKey(raceId))
        {
            Race existing = store.races().get(raceId);
            boolean recent = isRecentRace(raceDate);
            if (config.source().equals(existing.source()) && !recent)
            {
                LOG.info("{}: year {} -- already imported ({} divisions), skipping",
                    config.source(), year,
                    existing.divisions() != null ? existing.divisions().size() : 0);
                updateClubSeries(CLUB_ID, bwpsSeriesId, bwpsSeriesName, raceId);
                updateClubSeries(CLUB_ID, raceSeriesId, config.seriesName(), raceId);
                return;
            }
            LOG.info("{}: year {} -- race exists from source='{}'{}, will reimport",
                config.source(), year, existing.source(), recent ? " (recent)" : "");
        }

        LOG.info("{}: year {} -- cycaRaceId={}, starting category discovery",
            config.source(), year, cycaRaceId);

        RaceCategories cats = discoverCategories(config, year, cycaRaceId);
        if (cats == null)
        {
            ImporterLog.warn(LOG, "{}: year {} -- could not identify LH + IRC/ORC categories",
                config.source(), year);
            return;
        }

        // Line Honours: elapsed times for all finishers
        LOG.info("{}: year {} -- fetching Line Honours (cat {})", config.source(), year, cats.lhCategoryId());
        List<FeedsEntry> lhEntries = fetchCategory(cycaRaceId, cats.lhCategoryId());

        ZonedDateTime raceStart = ZonedDateTime.of(
            raceDate.getYear(), raceDate.getMonthValue(), raceDate.getDayOfMonth(),
            config.raceStartHour(), 0, 0, 0, SYDNEY_TZ);
        Map<String, Duration> elapsedBySailNum = buildElapsedMap(lhEntries, raceStart);
        LOG.info("{}: year {} -- {} LH finishers with valid elapsed times",
            config.source(), year, elapsedBySailNum.size());

        // IRC All
        LinkedHashMap<String, List<Finisher>> divMap = new LinkedHashMap<>();
        int count = 0;

        LOG.info("{}: year {} -- fetching IRC (cat {})", config.source(), year, cats.ircCategoryId());
        List<FeedsEntry> ircEntries = fetchCategory(cycaRaceId, cats.ircCategoryId());
        LOG.info("{}: year {} -- {} IRC entries", config.source(), year, ircEntries.size());
        count += processFeedsEntries(ircEntries, "IRC", raceDate, elapsedBySailNum, divMap,
            config.source(), yachtDesigns);

        // ORC All (optional)
        if (cats.orcCategoryId() != null)
        {
            LOG.info("{}: year {} -- fetching ORC (cat {})", config.source(), year, cats.orcCategoryId());
            List<FeedsEntry> orcEntries = fetchCategory(cycaRaceId, cats.orcCategoryId());
            LOG.info("{}: year {} -- {} ORC entries", config.source(), year, orcEntries.size());
            count += processFeedsEntries(orcEntries, "ORC", raceDate, elapsedBySailNum, divMap,
                config.source(), yachtDesigns);
        }

        if (divMap.isEmpty())
        {
            ImporterLog.warn(LOG, "{}: year {} -- no finishers with matched elapsed times",
                config.source(), year);
            return;
        }

        List<Division> divisions = divMap.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();

        // Dual series membership: BWPS year series + dedicated race series
        List<String> seriesIds = List.of(bwpsSeriesId, raceSeriesId);

        store.putRace(new Race(raceId, CLUB_ID, seriesIds, raceDate, 1,
            config.seriesName(), divisions, config.source(), Instant.now(), null));
        LOG.info("{}: year {} saved -- {} finishers across {} division(s) [{}]",
            config.source(), year, count, divisions.size(),
            divisions.stream().map(Division::name).toList());

        updateClubSeries(CLUB_ID, bwpsSeriesId, bwpsSeriesName, raceId);
        updateClubSeries(CLUB_ID, raceSeriesId, config.seriesName(), raceId);
    }

    // --- Category discovery ---

    /**
     * Returns the highest category ID across all known configs, used as the starting
     * point for scanning future years.
     */
    static int highestKnownCategoryId()
    {
        int max = 0;
        for (CycaFeedsRaceConfig config : List.of(RSHYR_CONFIG, GOLDCOAST_CONFIG))
        {
            for (RaceCategories cats : config.knownCategories().values())
            {
                if (cats.lhCategoryId() > max) max = cats.lhCategoryId();
                if (cats.ircCategoryId() > max) max = cats.ircCategoryId();
                if (cats.orcCategoryId() != null && cats.orcCategoryId() > max)
                    max = cats.orcCategoryId();
            }
        }
        return max;
    }

    /**
     * Returns known {@link RaceCategories} for the given year, or discovers them
     * via the Categories API (preferred) or by scanning category IDs (fallback).
     */
    RaceCategories discoverCategories(CycaFeedsRaceConfig config, int year, int raceId) throws Exception
    {
        RaceCategories known = config.knownCategories().get(year);
        if (known != null)
        {
            LOG.info("{}: year {} raceId={} -- using known categories: LH={} IRC={} ORC={}",
                config.source(), year, raceId, known.lhCategoryId(), known.ircCategoryId(),
                known.orcCategoryId() != null ? known.orcCategoryId() : "none");
            return known;
        }

        // Try the Categories API first -- much faster than scanning
        RaceCategories fromApi = discoverCategoriesViaApi(config, year, raceId);
        if (fromApi != null)
            return fromApi;

        // Fallback: scan forward from the highest category we've seen.
        int scanStart = highestKnownCategoryId() + 1;
        int scanEnd   = scanStart + 200;
        LOG.info("{}: year {} raceId={} -- scanning categories {}–{} (API failed, falling back)",
            config.source(), year, raceId, scanStart, scanEnd);

        Integer lhCat  = null;
        Integer ircCat = null;
        Integer orcCat = null;
        int consecutiveEmpty = 0;
        boolean foundAny = false;

        for (int catId = scanStart; catId <= scanEnd; catId++)
        {
            List<FeedsEntry> entries;
            try
            {
                entries = fetchCategory(raceId, catId);
            }
            catch (Exception e)
            {
                LOG.debug("{}: raceId={} cat={} fetch error: {}", config.source(), raceId, catId, e.getMessage());
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
            LOG.info("{}: raceId={} cat={} entries={} type={}",
                config.source(), raceId, catId, entries.size(), type);

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
            ImporterLog.warn(LOG, "{}: category discovery failed for year={} raceId={} (scanned {}–{}): "
                + "lhCat={} ircCat={} orcCat={}",
                config.source(), year, raceId, scanStart, scanEnd, lhCat, ircCat, orcCat);
            return null;
        }
        LOG.info("{}: year {} raceId={} discovered → LH={} IRC={} ORC={}",
            config.source(), year, raceId, lhCat, ircCat, orcCat != null ? orcCat : "none");
        return new RaceCategories(lhCat, ircCat, orcCat);
    }

    /**
     * Discovers categories via the {@code /Race/Categories/{raceId}} API endpoint.
     * Returns null if the API call fails or doesn't contain both LH and IRC categories.
     */
    @SuppressWarnings("unchecked")
    private RaceCategories discoverCategoriesViaApi(CycaFeedsRaceConfig config, int year, int raceId)
    {
        String url = FEEDS_BASE + "/Race/Categories/" + raceId;
        try
        {
            String json = fetchString(url);
            List<Map<String, Object>> categories = MAPPER.readValue(json,
                MAPPER.getTypeFactory().constructCollectionType(List.class, Map.class));

            Integer lhCat = null, ircCat = null, orcCat = null;
            for (Map<String, Object> cat : categories)
            {
                String name = (String) cat.get("Name");
                Number id = (Number) cat.get("Id");
                if (name == null || id == null)
                    continue;
                String upper = name.toUpperCase(Locale.ENGLISH);
                if (upper.equals("LINE HONOURS") && lhCat == null)
                    lhCat = id.intValue();
                else if (upper.equals("IRC") && ircCat == null)
                    ircCat = id.intValue();
                else if ((upper.equals("ORCI") || upper.equals("ORC")) && orcCat == null)
                    orcCat = id.intValue();
            }

            if (lhCat != null && ircCat != null)
            {
                LOG.info("{}: year {} raceId={} -- discovered via Categories API: LH={} IRC={} ORC={}",
                    config.source(), year, raceId, lhCat, ircCat, orcCat != null ? orcCat : "none");
                return new RaceCategories(lhCat, ircCat, orcCat);
            }
            LOG.debug("{}: Categories API for raceId={} incomplete: LH={} IRC={} ORC={}",
                config.source(), raceId, lhCat, ircCat, orcCat);
        }
        catch (Exception e)
        {
            LOG.debug("{}: Categories API failed for raceId={}: {}", config.source(), raceId, e.getMessage());
        }
        return null;
    }

    /**
     * Classifies a category by inspecting its entries.
     * <ul>
     *   <li><b>LINE_HONOURS</b>: ≥ {@value MIN_FLEET_SIZE} entries; all finished have TCF ≈ 1.0.</li>
     *   <li><b>IRC</b>: entries span ≥ 2 distinct numeric DivisionName values.</li>
     *   <li><b>IRC_SUB</b>: exactly 1 distinct numeric DivisionName -- per-division sub-category.</li>
     *   <li><b>ORC</b>: varying TCF, no numeric divisions, ≥ {@value MIN_FLEET_SIZE} entries.</li>
     * </ul>
     */
    static CategoryType classifyCategory(List<FeedsEntry> entries)
    {
        if (entries.isEmpty())
            return CategoryType.UNKNOWN;

        long finished = entries.stream()
            .filter(e -> e.isFinished() || "Finished".equalsIgnoreCase(e.status()))
            .count();

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

        Set<String> numericDivs = new HashSet<>();
        for (FeedsEntry e : entries)
        {
            String div = e.divisionName();
            if (div != null && div.matches("[0-9]+"))
                numericDivs.add(div);
        }

        if (numericDivs.size() >= 2)
            return CategoryType.IRC;

        if (numericDivs.size() == 1)
            return CategoryType.IRC_SUB;

        boolean hasVaryingTcf = entries.stream()
            .anyMatch(e -> Math.abs(e.tcf() - 1.0) > 0.01 && e.tcf() != 0.0);
        if (hasVaryingTcf && entries.size() >= MIN_FLEET_SIZE)
            return CategoryType.ORC;

        return CategoryType.UNKNOWN;
    }

    // --- Elapsed time computation ---

    /**
     * Builds a sail-number → elapsed Duration map from Line Honours entries.
     * Elapsed = CorrectedTime (AEDT/AEST) − race start.
     */
    Map<String, Duration> buildElapsedMap(List<FeedsEntry> lhEntries, ZonedDateTime raceStart)
    {
        Map<String, Duration> result = new LinkedHashMap<>();

        for (FeedsEntry e : lhEntries)
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
                    LOG.debug("{}: non-positive elapsed for {} ({}): {}",
                        "feeds", e.nameRace(), sailNum, elapsed);
            }
            catch (DateTimeParseException ex)
            {
                LOG.debug("{}: could not parse CorrectedTime '{}' for {}",
                    "feeds", e.correctedTime(), e.nameRace());
            }
        }
        return result;
    }

    // --- Entry processing ---

    private int processFeedsEntries(List<FeedsEntry> entries, String system, LocalDate date,
        Map<String, Duration> elapsedBySailNum, LinkedHashMap<String, List<Finisher>> divMap,
        String source, Map<String, String> yachtDesigns)
    {
        int count = 0;
        for (FeedsEntry entry : entries)
        {
            if (!entry.isFinished() && !"Finished".equalsIgnoreCase(entry.status()))
                continue;
            if (entry.sailNumber() == null || entry.sailNumber().isBlank())
                continue;

            String sailNum = normSailNum(entry.sailNumber());
            Duration elapsed = elapsedBySailNum.get(sailNum);
            if (elapsed == null)
            {
                LOG.debug("{}: no LH elapsed for {} ({}) in {} -- skipping",
                    source, entry.nameRace(), sailNum, system);
                continue;
            }

            // CYCA appends "(DH)" or "(TH)" (Two Handed) to double-handed entries.
            boolean dh = entry.nameRace() != null
                && (entry.nameRace().toUpperCase(Locale.ENGLISH).contains("(DH)")
                    || entry.nameRace().toUpperCase(Locale.ENGLISH).contains("(TH)"));
            String cleanName = dh
                ? entry.nameRace().replaceAll("(?i)\\((DH|TH)\\)", "").trim()
                : entry.nameRace();

            // Look up design from yacht listing pages
            String design = yachtDesigns.get(sailNum);

            Boat boat = store.findOrCreateBoat(entry.sailNumber(), cleanName, design, date, source);
            if (boat == null)
            {
                ImporterLog.warn(LOG, "{}: could not resolve boat {} [{}] -- ambiguous match, skipping",
                    source, cleanName, entry.sailNumber());
                continue;
            }
            String certNum = inferCertificate(boat, system, date.getYear(), entry.tcf(), dh, source);

            // Division key: "IRC Div 3", "ORC", etc. A double-handed entry goes in its own
            // "… Two-Handed" division: the finisher's variant is read from the division name
            // (EntryVariant), so this is what makes its result a two-handed one.
            String divKey;
            if (entry.divisionName() != null && !entry.divisionName().isBlank())
                divKey = system + " Div " + entry.divisionName();
            else
                divKey = system;
            if (dh)
                divKey += " Two-Handed";

            divMap.computeIfAbsent(divKey, k -> new ArrayList<>())
                .add(new Finisher(boat.id(), elapsed, false, certNum));
            LOG.info("{}: {} {} {} [{}] elapsed={} cert={}",
                source, system, divKey, cleanName, entry.sailNumber(),
                String.format("%dd%02dh%02dm",
                    elapsed.toDaysPart(), elapsed.toHoursPart(), elapsed.toMinutesPart()),
                certNum);
            count++;
        }
        return count;
    }

    // --- Race ID discovery ---

    /**
     * Discovers the CYCA raceId for a given year by scraping the results page and
     * extracting the {@code const raceId = <n>;} JavaScript constant.
     */
    Integer discoverRaceId(CycaFeedsRaceConfig config, int year) throws Exception
    {
        String url = config.websiteBase() + "/race/" + year + "/results";
        String html;
        try
        {
            html = fetchString(url);
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG, "{}: could not fetch results page for year {}: {}",
                config.source(), year, e.getMessage());
            return null;
        }
        Matcher m = RACE_ID_JS.matcher(html);
        if (m.find())
        {
            int id = Integer.parseInt(m.group(1));
            LOG.info("{}: year {} → raceId={} (discovered from page)", config.source(), year, id);
            return id;
        }
        ImporterLog.warn(LOG, "{}: raceId not found in results page for year {}", config.source(), year);
        return null;
    }

    List<FeedsEntry> fetchCategory(int raceId, int categoryId) throws Exception
    {
        String url = FEEDS_BASE + "/Results/Final/" + raceId + "/" + categoryId;
        String json = fetchString(url);
        FeedsEntry[] arr = MAPPER.readValue(json, FeedsEntry[].class);
        return (arr == null) ? List.of() : Arrays.asList(arr);
    }

    // ========================================================================
    // Yacht listing page parser
    // ========================================================================

    /**
     * Fetches and parses a yacht listing page, returning a map of normalised sail number
     * to design type.  The yacht pages on bwps.cycaracing.com, rolexsydneyhobart.com, and
     * goldcoast.cycaracing.com all share the same table format:
     * [Yacht Name, Sail Number, State/Country, Type, Category].
     */
    Map<String, String> fetchYachtDesigns(String url)
    {
        Map<String, String> result = new LinkedHashMap<>();
        try
        {
            String html = fetchHtml(url);
            Document doc = Jsoup.parse(html, url);
            for (Element row : doc.select("table tr"))
            {
                Elements cells = row.select("td");
                if (cells.size() < 4)
                    continue;

                String sailNumber = cells.get(1).text().trim();
                String type       = cells.get(3).text().trim();
                if (!sailNumber.isBlank() && !type.isBlank())
                    result.put(normSailNum(sailNumber), type);
            }
            LOG.info("BWPS: parsed {} yacht design(s) from {}", result.size(), url);
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG, "BWPS: failed to fetch yacht listing from {}: {}", url, e.getMessage());
        }
        return result;
    }

    // ========================================================================
    // Shared helpers
    // ========================================================================

    private static int parseMonthIndex(String abbr)
    {
        return switch (abbr.substring(0, 3).toLowerCase(Locale.ENGLISH))
        {
            case "jan" -> 1; case "feb" -> 2; case "mar" -> 3;
            case "apr" -> 4; case "may" -> 5; case "jun" -> 6;
            case "jul" -> 7; case "aug" -> 8; case "sep" -> 9;
            case "oct" -> 10; case "nov" -> 11; case "dec" -> 12;
            default -> throw new IllegalArgumentException("Unknown month: " + abbr);
        };
    }

    static String normSailNum(String s)
    {
        return s == null ? "" : s.trim().toUpperCase(Locale.ENGLISH);
    }

    /**
     * Checks if the boat already holds a matching certificate.  If not, creates an
     * inferred certificate and adds it to the boat.
     *
     * @return the certificateNumber of the matching or newly created certificate
     */
    private String inferCertificate(Boat boat, String system, int year, double tcf,
                                    boolean twoHanded, String source)
    {
        for (Certificate cert : boat.certificates())
        {
            if (cert.system().equalsIgnoreCase(system)
                    && Math.abs(cert.year() - year) <= 1
                    && Math.abs(cert.value() - tcf) < 0.001
                    && cert.twoHanded() == twoHanded)
                return cert.certificateNumber();
        }

        String certNumber = String.format("%s-%s-%d-%.4f%s",
            source.toLowerCase(Locale.ENGLISH),
            system.toLowerCase(Locale.ENGLISH), year, tcf, twoHanded ? "-dh" : "");
        Certificate inferred = new Certificate(system, year, tcf, false, twoHanded, false, false, certNumber, null);
        List<Certificate> certs = new ArrayList<>(boat.certificates());
        certs.add(inferred);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubIds(), List.copyOf(certs),
            addSource(boat.sources(), source), Instant.now(), null));
        LOG.debug("{}: inferred {} cert {} (TCF={} twoHanded={}) for boat {}",
            source, system, certNumber, tcf, twoHanded, boat.id());
        return certNumber;
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
            club = new Club(seed.id(), seed.shortName(), seed.longName(), seed.state(), seed.excluded(),
                seed.email(),
                seed.aliases() != null ? seed.aliases() : List.of(),
                seed.topyachtGroups() != null ? seed.topyachtGroups() : List.of(),
                seed.sailsysEvents() != null ? seed.sailsysEvents() : List.of(),
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
                List<String> newRaceIds = new ArrayList<>(existing.raceIds());
                newRaceIds.add(raceId);
                series.set(idx, new Series(existing.id(), existing.name(), existing.isCatchAll(),
                    List.copyOf(newRaceIds)));
            }
        }
        else
        {
            series.add(new Series(seriesId, seriesName, false, List.of(raceId)));
        }

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(), club.excluded(),
            club.email(), club.aliases(), club.topyachtGroups(), club.sailsysEvents(),
            List.copyOf(series), null));
    }

    private boolean isRecentRace(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(recentRaceReimportDays));
    }

    /**
     * Fetches the HTML at the given URL.  Package-private so tests can override it
     * to serve fixture content without making real HTTP requests.
     */
    String fetchHtml(String url) throws Exception
    {
        ContentResponse response = httpClient.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    /**
     * Fetches a URL as a raw string (used for JSON feeds and page-source scraping).
     * Package-private so tests can override it.
     */
    String fetchString(String url) throws Exception
    {
        ContentResponse response = httpClient.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // ========================================================================
    // Inner types -- CYCA feeds API
    // ========================================================================

    /**
     * Configuration for a CYCA feeds-API race (RSHYR or Gold Coast).
     *
     * @param source          source tag for Race records (e.g. "RSHYR", "GoldCoast")
     * @param seriesName      dedicated series name (e.g. "Rolex Sydney Hobart Yacht Race")
     * @param websiteBase     base URL of the race website (e.g. "https://rolexsydneyhobart.com")
     * @param knownRaceIds    year → CYCA raceId mapping
     * @param knownCategories year → category IDs mapping
     * @param knownRaceDates  year → start date (for races with variable dates)
     * @param fixedRaceMonth  month of race if constant (e.g. 12 for RSHYR); 0 if variable
     * @param fixedRaceDay    day of race if constant (e.g. 26 for RSHYR); 0 if variable
     * @param raceStartHour   hour of day the race starts (e.g. 13 for 1 PM)
     * @param minYear         earliest year to import
     */
    record CycaFeedsRaceConfig(
        String source,
        String seriesName,
        String websiteBase,
        Map<Integer, Integer> knownRaceIds,
        Map<Integer, RaceCategories> knownCategories,
        Map<Integer, LocalDate> knownRaceDates,
        int fixedRaceMonth,
        int fixedRaceDay,
        int raceStartHour,
        int minYear)
    {
        /** Returns the race start date for the given year, or null if unknown. */
        LocalDate raceDate(int year)
        {
            if (fixedRaceMonth > 0 && fixedRaceDay > 0)
                return LocalDate.of(year, fixedRaceMonth, fixedRaceDay);
            return knownRaceDates.get(year);
        }
    }

    enum CategoryType { LINE_HONOURS, IRC, IRC_SUB, ORC, UNKNOWN }

    record RaceCategories(int lhCategoryId, int ircCategoryId, Integer orcCategoryId) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class FeedsEntry
    {
        @JsonProperty("NameRace")      public String  nameRace;
        @JsonProperty("SailNumber")    public String  sailNumber;
        @JsonProperty("TCF")           public double  tcf;
        @JsonProperty("DivisionName")  public String  divisionName;
        @JsonProperty("Status")        public String  status;
        @JsonProperty("IsFinished")    public boolean isFinished;
        @JsonProperty("CorrectedTime") public String  correctedTime;

        String nameRace()      { return nameRace; }
        String sailNumber()    { return sailNumber; }
        double tcf()           { return tcf; }
        String divisionName()  { return divisionName; }
        String status()        { return status; }
        boolean isFinished()   { return isFinished; }
        String correctedTime() { return correctedTime; }
    }
}
