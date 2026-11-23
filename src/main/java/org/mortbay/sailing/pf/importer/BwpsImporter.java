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
import java.util.HashMap;
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

    // --- Major races, each with its own race website on the CYCA platform ---
    // The site's /race/<year>/results page embeds that year's feeds raceId; the feeds give
    // the start and the results categories. Nothing per year is configured.
    static final MajorRace RSHYR = new MajorRace(RSHYR_SOURCE,
        "Rolex Sydney Hobart Yacht Race", "https://rolexsydneyhobart.com");
    static final MajorRace GOLDCOAST = new MajorRace(GOLDCOAST_SOURCE,
        "Noakes Sydney Gold Coast Yacht Race", "https://goldcoast.cycaracing.com");
    static final List<MajorRace> MAJOR_RACES = List.of(RSHYR, GOLDCOAST);

    /**
     * In a history run, a race (or the minor-race index) missing this many years in a row
     * marks the start of its history.
     */
    static final int HISTORY_GAP_YEARS = 3;

    /** Race names in the BWPS standings that are handled via the feeds API instead. */
    private static final List<String> FEEDS_RACE_KEYWORDS = List.of("SYDNEY HOBART", "GOLD COAST");

    private final DataStore store;
    private final HttpClient httpClient;
    private int recentRaceReimportDays = 30;
    /** Pause before each yacht page fetch: one page per boat imported, so be polite. */
    int yachtPageDelayMs = 200;

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

    /** Imports the seasons minYear..now: the minor races, then the major races. */
    public void run(int recentRaceReimportDays, int minYear) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        int currentYear = LocalDate.now().getYear();
        // Sail number → yacht listing entry (design, detail page), filled in by every race.
        Map<String, YachtInfo> yachts = new LinkedHashMap<>();

        // A failure in the minor races must not stop the major races below.
        try
        {
            importBwpsResults(minYear, currentYear, yachts);
        }
        catch (Exception e)
        {
            ImporterLog.error(LOG, "BWPS: minor-race import failed: {}", e.getMessage(), e);
        }
        for (MajorRace race : MAJOR_RACES)
        {
            for (int year = minYear; year <= currentYear; year++)
                importMajorRace(race, year, yachts);
        }
        LOG.info("BWPS: all phases complete");
    }

    /**
     * Imports all the history the sites hold: walks back from this year until the
     * minor-race index, and then each major race, has been missing
     * {@value #HISTORY_GAP_YEARS} years in a row. Races already held are not fetched again.
     */
    public void runHistory(int recentRaceReimportDays) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        int currentYear = LocalDate.now().getYear();
        Map<String, YachtInfo> yachts = new LinkedHashMap<>();

        int gap = 0;
        int year = currentYear;
        for (; gap < HISTORY_GAP_YEARS; year--)
            gap = importBwpsYear(year, yachts) ? 0 : gap + 1;
        LOG.info("BWPS history: minor-race results start in {}", year + 1 + HISTORY_GAP_YEARS);

        for (MajorRace race : MAJOR_RACES)
        {
            gap = 0;
            year = currentYear;
            for (; gap < HISTORY_GAP_YEARS; year--)
                gap = importMajorRace(race, year, yachts) == Outcome.ABSENT ? gap + 1 : 0;
            LOG.info("BWPS history: {} results start in {}", race.source(),
                year + 1 + HISTORY_GAP_YEARS);
        }
        LOG.info("BWPS: history complete");
    }

    // ========================================================================
    // Minor races: the BWPS results pages
    // ========================================================================
    //
    // The BWPS site lists each season's races at /race/<year>/results, linking to
    // /race/<year>/results/<race-slug>. Hobart and Gold Coast are listed there too, but are
    // imported from their own sites (see importMajorRace).

    /** Imports every minor BWPS race of the seasons fromYear..toYear. */
    void importBwpsResults(int fromYear, int toYear, Map<String, YachtInfo> yachts)
    {
        for (int year = fromYear; year <= toYear; year++)
            importBwpsYear(year, yachts);
    }

    /** Imports one season's minor races; false if the site has no results index for it. */
    boolean importBwpsYear(int year, Map<String, YachtInfo> yachts)
    {
        List<String> slugs;
        try
        {
            slugs = parseResultsIndex(fetchString(BASE_URL + "/race/" + year + "/results"), year);
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG, "BWPS: no results index for {}: {}", year, e.getMessage());
            return false;
        }
        LOG.info("BWPS: {} race(s) listed for {}: {}", slugs.size(), year, slugs);
        String seriesName = SERIES_NAME_PREFIX + " " + year;
        for (String slug : slugs)
        {
            String raceName = raceNameFromSlug(slug);
            if (isFeedsMajorRace(raceName))
                continue;   // imported from its own site
            try
            {
                importFeedsRace(new FeedsRace(year, SOURCE, raceName,
                    BASE_URL + "/race/" + year + "/results/" + slug,
                    BASE_URL + "/race/" + year + "/yachts/" + slug,
                    List.of(seriesName)), yachts);
            }
            catch (Exception e)
            {
                ImporterLog.error(LOG, "BWPS: failed to import '{}' {}: {}", raceName, year,
                    e.getMessage(), e);
            }
        }
        return true;
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

    // ========================================================================
    // Major races: each from its own race website
    // ========================================================================

    /**
     * Imports one year of a major race. It belongs to that year's BWPS series and to the
     * race's own series.
     */
    Outcome importMajorRace(MajorRace race, int year, Map<String, YachtInfo> yachts)
    {
        try
        {
            return importFeedsRace(new FeedsRace(year, race.source(), race.seriesName(),
                race.websiteBase() + "/race/" + year + "/results",
                race.websiteBase() + "/race/" + year + "/yachts",
                List.of(SERIES_NAME_PREFIX + " " + year, race.seriesName())), yachts);
        }
        catch (Exception e)
        {
            ImporterLog.error(LOG, "BWPS: failed to import {} {}: {}", race.source(), year,
                e.getMessage(), e);
            return Outcome.ABSENT;
        }
    }

    // ========================================================================
    // Any race on the CYCA platform
    // ========================================================================
    //
    // A race's results page embeds its CYCA feeds raceId. The feeds give the start time and
    // results categories (Race/Summary) and the results per category: elapsed times come
    // only from Line Honours (finish − start); the IRC (and ORC) results supply each boat's
    // rating for its certificate.

    /** What became of one race. ABSENT: the sites have no usable results for it. */
    enum Outcome { IMPORTED, HELD, NOT_SAILED, ABSENT }

    /**
     * A race to import.
     *
     * @param year        the season (BWPS) or year (major race) it is listed under
     * @param source      source tag for the Race record
     * @param raceName    the race's name
     * @param resultsUrl  its results page, which embeds the feeds raceId
     * @param yachtsUrl   its yacht list (designs, links to each yacht's page)
     * @param seriesNames the series it belongs to; the first is its primary series
     */
    record FeedsRace(int year, String source, String raceName, String resultsUrl, String yachtsUrl,
                     List<String> seriesNames) {}

    /** Imports one race, unless it is already held and not recent. */
    Outcome importFeedsRace(FeedsRace fr, Map<String, YachtInfo> yachts) throws Exception
    {
        // Held already (matched by name and year, so no request is needed) and not recent.
        Race held = store.races().values().stream()
            .filter(r -> CLUB_ID.equals(r.clubId()) && fr.source().equals(r.source())
                && fr.raceName().equalsIgnoreCase(r.name()) && r.date() != null
                && r.date().getYear() == fr.year())
            .findFirst().orElse(null);
        if (held != null && !isRecentRace(held.date()))
        {
            addToSeries(fr, held.id());
            return Outcome.HELD;
        }

        String page;
        try
        {
            page = fetchString(fr.resultsUrl());
        }
        catch (Exception e)
        {
            LOG.info("BWPS: no results page for '{}' {}: {}", fr.raceName(), fr.year(), e.getMessage());
            return Outcome.ABSENT;
        }
        Matcher idMatch = RACE_ID_JS.matcher(page);
        if (!idMatch.find())
        {
            // e.g. a year the race was not held (Hobart 2020)
            LOG.info("BWPS: no raceId on the results page for '{}' {} ({}); not held",
                fr.raceName(), fr.year(), fr.resultsUrl());
            return Outcome.ABSENT;
        }
        int cycaRaceId = Integer.parseInt(idMatch.group(1));

        RaceSummary summary = MAPPER.readValue(
            fetchString(FEEDS_BASE + "/Race/Summary/" + cycaRaceId), RaceSummary.class);
        if (summary.year() != null && summary.year() != fr.year())
        {
            // e.g. a year the race was not held, whose page shows another year's race
            LOG.info("BWPS: '{}' {} page gives raceId={} of {}; no race that year",
                fr.raceName(), fr.year(), cycaRaceId, summary.year());
            return Outcome.ABSENT;
        }
        if (summary.startDateTime() == null || summary.startDateTime().startsWith(ZERO_DATE_PREFIX))
        {
            LOG.info("BWPS: '{}' {} (raceId={}) has no start time yet -- skipping",
                fr.raceName(), fr.year(), cycaRaceId);
            return Outcome.NOT_SAILED;
        }
        LocalDateTime start = LocalDateTime.parse(summary.startDateTime());
        if (start.atZone(SYDNEY_TZ).isAfter(ZonedDateTime.now(SYDNEY_TZ)))
        {
            LOG.info("BWPS: '{}' {} starts {} -- not sailed yet, skipping", fr.raceName(), fr.year(), start);
            return Outcome.NOT_SAILED;
        }
        LocalDate raceDate = start.toLocalDate();
        String raceId = IdGenerator.generateRaceId(CLUB_ID, raceDate, 1);
        Race existing = store.races().get(raceId);
        if (existing != null && fr.source().equals(existing.source()) && !isRecentRace(raceDate))
        {
            addToSeries(fr, raceId);
            return Outcome.HELD;
        }

        Integer lhCat = null, ircCat = null, orcCat = null, phsCat = null;
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
            else if (phsCat == null && name.equals("PHS"))
                phsCat = c.id();
        }
        if (lhCat == null || (ircCat == null && orcCat == null && phsCat == null))
        {
            ImporterLog.warn(LOG, "BWPS: '{}' {} (raceId={}) -- no Line Honours, or no IRC/ORC/PHS category",
                fr.raceName(), fr.year(), cycaRaceId);
            return Outcome.ABSENT;
        }

        List<FeedsEntry> lhEntries = fetchCategory(cycaRaceId, lhCat);
        // Results are matched across categories by sail number. Early years (e.g. the 1990s
        // Hobarts) mostly lack them, so too little of the race would survive.
        long finished = lhEntries.stream().filter(BwpsImporter::finished).count();
        long withSail = lhEntries.stream().filter(BwpsImporter::finished)
            .filter(e -> e.sailNumber() != null && !e.sailNumber().isBlank()).count();
        if (finished > 0 && withSail * 2 < finished)
        {
            ImporterLog.warn(LOG, "BWPS: '{}' {} (raceId={}) -- only {} of {} finishers have a sail number; skipping",
                fr.raceName(), fr.year(), cycaRaceId, withSail, finished);
            return Outcome.ABSENT;
        }

        // This race's yacht list: design and detail page (club) per boat.
        yachts.putAll(fetchYachtListing(fr.yachtsUrl()));

        // Elapsed times from Line Honours only: finish (its CorrectedTime) − start.
        Map<String, Duration> elapsedBySailNum = buildElapsedMap(lhEntries, start.atZone(SYDNEY_TZ));

        // IRC (and ORC) results only for each boat's rating → certificate; finishers without
        // a Line Honours elapsed time are dropped. PHS is mostly the same fleet scored again,
        // so it adds only the boats in neither IRC nor ORC (and gives them no certificate).
        LinkedHashMap<String, List<Finisher>> divMap = new LinkedHashMap<>();
        Set<String> placed = new HashSet<>();
        int count = 0;
        if (ircCat != null)
            count += processFeedsEntries(fetchCategory(cycaRaceId, ircCat), "IRC", raceDate,
                elapsedBySailNum, divMap, fr.source(), yachts, placed, false);
        if (orcCat != null)
            count += processFeedsEntries(fetchCategory(cycaRaceId, orcCat), "ORC", raceDate,
                elapsedBySailNum, divMap, fr.source(), yachts, placed, false);
        if (phsCat != null)
        {
            // PHS only adds boats, so failing to fetch it must not lose the race.
            List<FeedsEntry> phs = List.of();
            try
            {
                phs = fetchCategory(cycaRaceId, phsCat);
            }
            catch (Exception e)
            {
                ImporterLog.warn(LOG, "BWPS: '{}' {} (raceId={}) -- PHS results unavailable: {}",
                    fr.raceName(), fr.year(), cycaRaceId, e.getMessage());
            }
            count += processFeedsEntries(phs, "PHS", raceDate,
                elapsedBySailNum, divMap, fr.source(), yachts, placed, true);
        }
        if (divMap.isEmpty())
        {
            ImporterLog.warn(LOG, "BWPS: '{}' {} -- no IRC/ORC/PHS finishers with Line Honours times",
                fr.raceName(), fr.year());
            return Outcome.ABSENT;
        }

        List<Division> divisions = divMap.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();
        List<String> seriesIds = fr.seriesNames().stream()
            .map(n -> IdGenerator.generateSeriesId(CLUB_ID, n)).toList();
        store.putRace(new Race(raceId, CLUB_ID, seriesIds, raceDate, 1,
            fr.raceName(), divisions, fr.source(), Instant.now(), null));
        LOG.info("BWPS: imported race {} '{}' {} ({} finishers, {} division(s))",
            raceId, fr.raceName(), fr.year(), count, divisions.size());
        addToSeries(fr, raceId);
        return Outcome.IMPORTED;
    }

    private void addToSeries(FeedsRace fr, String raceId)
    {
        for (String name : fr.seriesNames())
            store.addRaceToClubSeries(CLUB_ID, IdGenerator.generateSeriesId(CLUB_ID, name), name, raceId);
    }

    private static boolean finished(FeedsEntry e)
    {
        return e.isFinished() || "Finished".equalsIgnoreCase(e.status());
    }

    /** The parts of /Race/Summary/{raceId} used: year, start time and the results categories. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RaceSummary(@JsonProperty("Year") Integer year,
                       @JsonProperty("StartDateTime") String startDateTime,
                       @JsonProperty("Status") String status,
                       @JsonProperty("Categories") List<SummaryCategory> categories) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record SummaryCategory(@JsonProperty("Id") Integer id,
                           @JsonProperty("Name") String name,
                           @JsonProperty("Type") String type) {}

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

    /**
     * Adds a category's finishers to {@code divMap}, recording each one's sail number in
     * {@code placed}; with {@code onlyUnplaced}, boats already placed (by an earlier
     * category of the same race) are skipped.
     */
    private int processFeedsEntries(List<FeedsEntry> entries, String system, LocalDate date,
        Map<String, Duration> elapsedBySailNum, LinkedHashMap<String, List<Finisher>> divMap,
        String source, Map<String, YachtInfo> yachts, Set<String> placed, boolean onlyUnplaced)
    {
        int count = 0;
        for (FeedsEntry entry : entries)
        {
            if (!entry.isFinished() && !"Finished".equalsIgnoreCase(entry.status()))
                continue;
            if (entry.sailNumber() == null || entry.sailNumber().isBlank())
                continue;

            String sailNum = normSailNum(entry.sailNumber());
            if (onlyUnplaced && placed.contains(sailNum))
                continue;
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

            // The yacht list gives the design and a link to the yacht's own page, which also
            // gives its club (fetched once per run for every boat imported).
            YachtInfo listed = yachts.get(sailNum);
            YachtDetails details = listed != null ? yachtDetails(listed.detailUrl()) : null;
            String design = listed != null && listed.type() != null ? listed.type()
                : details != null ? details.type() : null;

            Boat boat = store.findOrCreateBoat(entry.sailNumber(), cleanName, design, date, source);
            if (boat == null)
            {
                ImporterLog.warn(LOG, "{}: could not resolve boat {} [{}] -- ambiguous match, skipping",
                    source, cleanName, entry.sailNumber());
                continue;
            }
            boat = applyYachtDetails(boat, details, date, source);
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
            placed.add(sailNum);
            LOG.info("{}: {} {} {} [{}] elapsed={} cert={}",
                source, system, divKey, cleanName, entry.sailNumber(),
                String.format("%dd%02dh%02dm",
                    elapsed.toDaysPart(), elapsed.toHoursPart(), elapsed.toMinutesPart()),
                certNum);
            count++;
        }
        return count;
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
    /** One row of a yacht list page: its design ("Type") and the yacht's own page. */
    record YachtInfo(String name, String type, String detailUrl) {}

    /** What a yacht's own page adds: its club and design. */
    record YachtDetails(String club, String type) {}

    /**
     * Fetches and parses a yacht list page (columns Yacht Name, Sail Number, State / Country,
     * Type, …), returning normalised sail number → {@link YachtInfo}, with each row's link to
     * the yacht's own page made absolute. An unreachable page yields an empty map.
     */
    Map<String, YachtInfo> fetchYachtListing(String url)
    {
        Map<String, YachtInfo> result = new LinkedHashMap<>();
        try
        {
            result.putAll(parseYachtListing(fetchHtml(url), url));
            LOG.info("BWPS: parsed {} yacht(s) from {}", result.size(), url);
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG, "BWPS: failed to fetch yacht listing from {}: {}", url, e.getMessage());
        }
        return result;
    }

    static Map<String, YachtInfo> parseYachtListing(String html, String url)
    {
        Map<String, YachtInfo> result = new LinkedHashMap<>();
        Document doc = Jsoup.parse(html, url);
        for (Element row : doc.select("table tr"))
        {
            Elements cells = row.select("td");
            if (cells.size() < 4)
                continue;
            String sailNumber = cells.get(1).text().trim();
            if (sailNumber.isBlank())
                continue;
            String type = cells.get(3).text().trim();
            Element link = row.selectFirst("a[href]");
            String detailUrl = link != null ? link.absUrl("href") : null;
            String name = link != null ? link.text().trim() : cells.get(0).text().trim();
            result.put(normSailNum(sailNumber), new YachtInfo(name,
                type.isBlank() ? null : type, detailUrl == null || detailUrl.isBlank() ? null : detailUrl));
        }
        return result;
    }

    /** Parses a yacht's own page: its table of label → value rows (Club, Type, …). */
    static YachtDetails parseYachtDetails(String html)
    {
        Map<String, String> fields = new LinkedHashMap<>();
        for (Element row : Jsoup.parse(html).select("table tr"))
        {
            Element th = row.selectFirst("th");
            Element td = row.selectFirst("td");
            if (th != null && td != null)
                fields.putIfAbsent(th.text().trim().toLowerCase(Locale.ENGLISH), td.text().trim());
        }
        String club = fields.get("club");
        String type = fields.get("type");
        return new YachtDetails(club == null || club.isBlank() ? null : club,
            type == null || type.isBlank() ? null : type);
    }

    // Yacht pages fetched this run, by URL (null when the page could not be read).
    private final Map<String, YachtDetails> yachtDetailsCache = new HashMap<>();

    /** The yacht's own page, fetched once per run (politely spaced); null if unavailable. */
    YachtDetails yachtDetails(String detailUrl)
    {
        if (detailUrl == null)
            return null;
        if (yachtDetailsCache.containsKey(detailUrl))
            return yachtDetailsCache.get(detailUrl);
        YachtDetails details = null;
        try
        {
            if (yachtPageDelayMs > 0)
                Thread.sleep(yachtPageDelayMs);
            details = parseYachtDetails(fetchHtml(detailUrl));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (Exception e)
        {
            LOG.debug("BWPS: could not read yacht page {}: {}", detailUrl, e.getMessage());
        }
        yachtDetailsCache.put(detailUrl, details);
        return details;
    }

    /**
     * Completes a boat from its yacht page: a missing club is filled in (unless the boat is
     * marked as having no club), and a club or design that disagrees with what is held is
     * logged, not changed. Returns the boat as now stored.
     */
    Boat applyYachtDetails(Boat boat, YachtDetails details, LocalDate date, String source)
    {
        if (details == null)
            return boat;
        if (details.club() != null)
        {
            Club club = store.findUniqueClubByShortName(details.club(), null,
                source + " yacht page for " + boat.id());
            if (club == null)
                LOG.debug("{}: yacht page club '{}' for {} matches no club", source, details.club(), boat.id());
            else if (boat.clubIds().isEmpty())
            {
                if (!store.isExplicitlyNoClub(boat.id()))
                {
                    boat = new Boat(boat.id(), boat.sailNumber(), boat.name(), boat.designId(),
                        List.of(club.id()), boat.certificates(), addSource(boat.sources(), source), boat.links(),
                        Instant.now(), null);
                    store.putBoat(boat);
                    LOG.info("{}: club of {} set to {} from its yacht page", source, boat.id(), club.id());
                }
            }
            else if (!boat.clubIds().contains(club.id()))
                LOG.info("{}: yacht page gives club {} ('{}') for {}, held as {} -- not changed",
                    source, club.id(), details.club(), boat.id(), boat.clubIds());
        }
        if (details.type() != null && boat.designId() != null && !boat.designId().isBlank()
            && !boat.designId().equals(store.resolveDesignId(details.type(), boat, date)))
            LOG.info("{}: yacht page gives design '{}' for {}, held as {} -- not changed",
                source, details.type(), boat.id(), boat.designId());
        return boat;
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
     * inferred certificate and adds it to the boat.  ORC handicaps are not kept
     * (see {@link Certificate#isRaceDerivedOrc()}).
     *
     * @return the certificateNumber of the matching or newly created certificate, or null for ORC
     */
    private String inferCertificate(Boat boat, String system, int year, double tcf,
                                    boolean twoHanded, String source)
    {
        // ORC ratings come only from official certificates; PHS is a club handicap, not a certificate.
        if ("ORC".equalsIgnoreCase(system) || "PHS".equalsIgnoreCase(system))
            return null;
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
            addSource(boat.sources(), source), boat.links(), Instant.now(), null));
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
     * A major race with its own website on the CYCA platform.
     *
     * @param source      source tag for its Race records (e.g. "RSHYR")
     * @param seriesName  its own series, and its races' name
     * @param websiteBase its website (e.g. "https://rolexsydneyhobart.com")
     */
    record MajorRace(String source, String seriesName, String websiteBase) {}

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
