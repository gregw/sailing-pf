package org.mortbay.sailing.pf.importer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.SailSysEvent;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports {@link Race} records from SailSys race JSON, creating {@link Boat},
 * {@link Design}, and {@link Certificate} records from the race data itself.
 * <p>
 * Replaces the former {@code SailSysRaceImporter} + {@code SailSysBoatImporter} pair.
 * All data needed to create a boat (name, sail number, make, model, club) is present
 * in the race JSON; no separate boat-API fetches are required.
 * <p>
 * For races with measurement-based handicap systems (IRC, ORC*, AMS) the importer:
 * <ul>
 *   <li>Finds all measurement systems in {@code data.handicappings} (any entry that is
 *       not PHS, TPR, YRD, or other performance-based system).</li>
 *   <li>For each system, extracts the actual race-time handicap from
 *       {@code calculations[].handicapCreatedFrom} matched by {@code handicapDefinitionId}
 *       -- this is the value actually used for scoring, not the current stored certificate
 *       value which may be from a later year.</li>
 *   <li>Creates an inferred certificate linked to the race finisher.  When a certificate
 *       already exists on the boat with the same system, year (±1), value, nonSpinnaker,
 *       twoHanded, and club flags, that certificate's number is reused rather than
 *       duplicating.</li>
 *   <li>Races with multiple measurement systems (e.g. IRC + ORC) produce separate
 *       divisions per system (e.g. "Division 1 IRC", "Division 1 ORC").</li>
 * </ul>
 * <p>
 * Organising club: resolved from {@code data.club.shortName}/{@code longName} against
 * {@code clubs.yaml}.  A log warning is emitted if the club cannot be identified.
 * <p>
 * Boat club assignment: if {@code boat.club} matches the organising club's shortName,
 * the organising club is used directly.  Otherwise {@code findUniqueClubByShortName} is
 * tried, and if that is ambiguous, the club with the same shortName and same state as
 * the organising club is chosen (if unique).
 */
public class SailSysImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(SailSysImporter.class);

    static final String SOURCE = "SailSys";

    /** Sources from dedicated importers whose races SailSys should not overwrite with empty results. */
    private static final Set<String> DEDICATED_SOURCES = BwpsImporter.ALL_SOURCES;

    private static final String API_BASE   = "https://api.sailsys.com.au/api/v1/races/";
    private static final String API_SUFFIX = "/resultsentrants/display";
    /** Lists a club's current series; past seasons are not returned. */
    private static final String API_CLUB_PROFILE_BASE   = "https://api.sailsys.com.au/api/v1/clubs/";
    private static final String API_CLUB_PROFILE_SUFFIX = "/profile";
    /** Lists a series' races -- metadata only, no elapsed times. */
    private static final String API_SERIES_BASE   = "https://api.sailsys.com.au/api/v1/series/";
    private static final String API_SERIES_SUFFIX = "/display/races";
    private static final int SAVE_INTERVAL  = 500;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataStore  store;
    private final HttpClient client; // null in local mode

    // Tuneable parameters set at start of run()
    private int youngCacheMaxAgeDays = 7;
    private int oldCacheMaxAgeDays   = 352;
    private int youngRaceMaxAgeDays  = 365;
    private int httpDelayMs          = 200;
    private int recentRaceDays       = 14;
    /** SailSys integer ID of the race currently being processed (for source tagging). */
    private int currentSailSysRaceId = 0;
    private Path racesCacheDir;
    private int recentRaceReimportDays = 30;

    // Per-run tallies, reset by run(); reported back through RunResult.
    private int clubsVisited;
    private int seriesVisited;
    private int racesImported;
    private int racesAlreadyPresent;
    private int finishersImported;
    private int seriesCollisions;

    public SailSysImporter(DataStore store, HttpClient client)
    {
        this.store  = store;
        this.client = client;
    }

    // --- Entry point (standalone) ---

    public static void main(String[] args) throws Exception
    {
        Path dataRoot  = DataStore.resolveDataRoot(args);
        DataStore dataStore = new DataStore(dataRoot);
        dataStore.start();

        // Local replay only. Fetching from SailSys is opt-in per club and runs through the
        // server's club-scoped import, never as an unbounded scan from the command line.
        Path racesDir = args.length > 1 ? Path.of(args[1])
            : dataRoot.resolve("cache/sailsys/races");
        try
        {
            new SailSysImporter(dataStore, null).runFromDirectory(racesDir);
        }
        finally
        {
            dataStore.stop();
        }
    }

    // --- Run modes ---

    /**
     * Processes all {@code race-*.json} files in {@code dir} in sorted filename order.
     */
    public void runFromDirectory(Path dir) throws IOException
    {
        LOG.info("Loading races from directory {}", dir.toAbsolutePath());
        List<Path> files;
        try (var stream = Files.list(dir))
        {
            files = stream
                .filter(p -> p.getFileName().toString().matches("race-\\d+\\.json"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }
        LOG.info("Found {} race files", files.size());

        int processed = 0;
        for (Path file : files)
        {
            try
            {
                processRaceJson(Files.readString(file));
            }
            catch (Exception e)
            {
                ImporterLog.warn(LOG,"Error processing {}: {}", file.getFileName(), e.getMessage());
            }
            processed++;
            if (processed % SAVE_INTERVAL == 0)
            {
                LOG.info("Processed {} files -- saving", processed);
                store.save();
            }
        }
        store.save();
        LOG.info("Done. Processed {} files.", processed);
    }

    /**
     * Tallies for one club-scoped run, matching the contract the per-club import expects.
     *
     * @param clubs               clubs whose SailSys events were visited
     * @param series              SailSys series fetched across those clubs
     * @param racesImported       races written to the store
     * @param racesAlreadyPresent races already held for this series and not recent, so never fetched
     * @param finishers           finisher records written
     * @param seriesCollisions    races whose ID was already taken by a *different* series, so
     *                            their divisions were merged into it rather than stored apart
     */
    public record RunResult(int clubs, int series, int racesImported, int racesAlreadyPresent,
                            int finishers, int seriesCollisions) {}

    /**
     * Configures the on-disk response cache and rate limiting. Returns {@code this} so it can
     * be chained onto the constructor.
     */
    public SailSysImporter withCache(Path racesDir, int youngCacheMaxAgeDays, int oldCacheMaxAgeDays,
                                     int youngRaceMaxAgeDays, int httpDelayMs, int recentRaceDays)
    {
        this.racesCacheDir        = racesDir;
        this.youngCacheMaxAgeDays = youngCacheMaxAgeDays;
        this.oldCacheMaxAgeDays   = oldCacheMaxAgeDays;
        this.youngRaceMaxAgeDays  = youngRaceMaxAgeDays;
        this.httpDelayMs          = httpDelayMs;
        this.recentRaceDays       = recentRaceDays;
        return this;
    }

    /**
     * Imports the SailSys events configured on each club.
     * <p>
     * Races used to be found by walking global race IDs from 1 upwards, fetching the whole
     * platform to reach the few clubs we care about. Clubs now opt in: each carries
     * {@link SailSysEvent}s naming either a SailSys club (whose current series are discovered
     * each run) or one specific series. Only those are fetched.
     *
     * @param recentRaceReimportDays races at least this recent are re-fetched even when held
     * @param clubIds when non-null and non-empty, restricts the run to those club ids
     */
    public RunResult run(int recentRaceReimportDays, Set<String> clubIds) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        clubsVisited = 0;
        seriesVisited = 0;
        racesImported = 0;
        racesAlreadyPresent = 0;
        finishersImported = 0;
        seriesCollisions = 0;
        boolean restricted = clubIds != null && !clubIds.isEmpty();

        // Merge seed + persisted so a club configured but never imported is still visited
        List<Club> allClubs = Stream.concat(
            store.clubs().values().stream(),
            store.clubSeed().values().stream().filter(c -> !store.clubs().containsKey(c.id()))
        ).filter(c -> !restricted || clubIds.contains(c.id())).toList();

        if (restricted)
        {
            Set<String> found = allClubs.stream().map(Club::id).collect(Collectors.toSet());
            for (String requested : clubIds)
            {
                if (!found.contains(requested))
                    ImporterLog.warn(LOG, "SailSys: club={} is not a known club; skipping", requested);
            }
        }

        for (Club club : allClubs)
        {
            List<SailSysEvent> events = club.sailsysEvents();
            if (events == null || events.isEmpty())
            {
                if (restricted)
                    ImporterLog.warn(LOG, "SailSys: club={} has no SailSys events configured; "
                        + "nothing to import", club.id());
                continue;
            }
            clubsVisited++;
            for (SailSysEvent event : events)
            {
                for (int seriesId : resolveSeriesIds(club, event))
                {
                    seriesVisited++;
                    try
                    {
                        importSeries(club, event, seriesId);
                    }
                    catch (Exception e)
                    {
                        ImporterLog.error(LOG, "SailSys: failed to import series {} for club={}: {}",
                            seriesId, club.id(), e.getMessage());
                    }
                }
            }
        }

        store.save();
        ImporterLog.info(LOG, "SailSys: run complete -- {} club(s), {} series, {} race(s) imported, "
                + "{} already present, {} finishers",
            clubsVisited, seriesVisited, racesImported, racesAlreadyPresent, finishersImported);
        if (seriesCollisions > 0)
            ImporterLog.warn(LOG, "SailSys: {} race(s) shared an ID with a race from a different "
                + "series and were merged into it", seriesCollisions);
        return new RunResult(clubsVisited, seriesVisited, racesImported, racesAlreadyPresent,
            finishersImported, seriesCollisions);
    }

    /** Run tallies, for tests to assert the counters are actually wired. */
    int racesImportedForTest()
    {
        return racesImported;
    }

    int finishersImportedForTest()
    {
        return finishersImported;
    }

    /**
     * The SailSys series IDs an event contributes: the one it names, or -- for a club event --
     * whatever the club profile currently lists. That endpoint returns only current-season
     * series, so a club event never reaches past seasons; those need their own series events.
     */
    private List<Integer> resolveSeriesIds(Club club, SailSysEvent event) throws Exception
    {
        if (event.isSeries())
            return List.of(event.seriesId());
        if (!event.isClub())
            return List.of();
        String json = fetch(API_CLUB_PROFILE_BASE + event.clubId() + API_CLUB_PROFILE_SUFFIX);
        List<Integer> ids = parseClubSeriesIds(json);
        ImporterLog.info(LOG, "SailSys: club={} event='{}' -- SailSys club {} lists {} current series {}",
            club.id(), event.displayName(), event.clubId(), ids.size(), ids);
        if (ids.isEmpty())
            ImporterLog.warn(LOG, "SailSys: SailSys club {} lists no current series (club={}); "
                + "add explicit series events for past seasons", event.clubId(), club.id());
        return ids;
    }

    /**
     * Series IDs from a {@code clubs/{id}/profile} response, taking the union of the upcoming
     * and the with-results lists, de-duplicated and in first-seen order.
     */
    List<Integer> parseClubSeriesIds(String json) throws IOException
    {
        ClubProfileResponse response = MAPPER.readValue(json, ClubProfileResponse.class);
        if (response == null || response.data == null)
            return List.of();
        List<Integer> ids = new ArrayList<>();
        for (List<EventGroup> groups : List.of(
            response.data.nextEvents == null ? List.<EventGroup>of() : response.data.nextEvents,
            response.data.eventsWithEntrantsOrResults == null
                ? List.<EventGroup>of() : response.data.eventsWithEntrantsOrResults))
        {
            for (EventGroup group : groups)
            {
                if (group == null || group.items == null)
                    continue;
                for (EventItem item : group.items)
                {
                    if (item != null && item.seriesId != null && !ids.contains(item.seriesId))
                        ids.add(item.seriesId);
                }
            }
        }
        return List.copyOf(ids);
    }

    /**
     * Imports one SailSys series: lists its races, then fetches the results of the ones we do
     * not already hold. The race list carries the date and number, which is the whole race ID,
     * so an already-held race costs no request at all.
     */
    private void importSeries(Club club, SailSysEvent event, int sailsysSeriesId) throws Exception
    {
        String json = fetch(API_SERIES_BASE + sailsysSeriesId + API_SERIES_SUFFIX);
        SeriesRacesResponse response = MAPPER.readValue(json, SeriesRacesResponse.class);
        if (response == null || response.data == null || response.data.races == null)
        {
            ImporterLog.warn(LOG, "SailSys: series {} returned no race list (club={})",
                sailsysSeriesId, club.id());
            return;
        }
        String seriesName = response.data.name;
        String seriesId = IdGenerator.generateSeriesId(club.id(), seriesName);
        ImporterLog.info(LOG, "SailSys: club={} series {} '{}' has {} race(s)",
            club.id(), sailsysSeriesId, seriesName, response.data.races.size());

        for (SeriesRace race : response.data.races)
        {
            if (race == null || race.id == null || race.dateTime == null)
                continue;
            LocalDate date = peekDate(race.dateTime);
            if (date == null)
                continue;
            int number = race.number != null ? race.number : 0;
            String raceId = IdGenerator.generateRaceId(club.id(), date, number);

            // Already held for this series and not recent -- no request needed at all
            Race existing = store.races().get(raceId);
            if (existing != null && !isRecentRace(date)
                && existing.seriesIds() != null && existing.seriesIds().contains(seriesId))
            {
                racesAlreadyPresent++;
                continue;
            }
            if (existing != null && (existing.seriesIds() == null
                || !existing.seriesIds().contains(seriesId)))
            {
                seriesCollisions++;
                ImporterLog.warn(LOG, "SailSys: race {} already exists under {} but series '{}' "
                        + "also claims it; merging", raceId, existing.seriesIds(), seriesName);
            }

            String raceJson = fetchRaceJson(race.id);
            if (raceJson != null && isApiFound(raceJson))
                processRaceJson(raceJson, club);
        }
    }

    /**
     * Fetches one race's results, honouring the on-disk cache.
     * <p>
     * Cache staleness rules, unchanged from the scanning importer:
     * <ul>
     *   <li>Successful responses: re-fetch if recent (within {@code recentRaceDays}); otherwise
     *       use file last-modified vs {@code youngCacheMaxAgeDays} (young race) or
     *       {@code oldCacheMaxAgeDays} (old race).</li>
     *   <li>Error responses (series locked / not yet published): refetch while the cached error
     *       is younger than {@code youngCacheMaxAgeDays}; once older, treat it as settled.</li>
     * </ul>
     */
    private String fetchRaceJson(int id) throws Exception
    {
        currentSailSysRaceId = id;
        Path cachedFile = racesCacheDir != null
            ? racesCacheDir.resolve(String.format("race-%06d.json", id)) : null;

        String cachedJson = null;
        if (cachedFile != null && Files.exists(cachedFile))
            cachedJson = Files.readString(cachedFile);

        boolean useCache = false;
        if (cachedJson != null)
        {
            if (isApiFound(cachedJson))
            {
                LocalDate raceDate = peekRaceDate(cachedJson);
                if (!isRecent(raceDate))
                {
                    int maxAge = isYoung(raceDate) ? youngCacheMaxAgeDays : oldCacheMaxAgeDays;
                    useCache = !isStale(cachedFile, maxAge);
                }
                // recent success -> always refetch so live results are picked up
            }
            else
            {
                useCache = isStale(cachedFile, youngCacheMaxAgeDays);
            }
        }
        if (useCache)
            return cachedJson;

        try
        {
            String json = fetch(API_BASE + id + API_SUFFIX);
            if (cachedFile != null)
            {
                Files.createDirectories(racesCacheDir);
                Files.writeString(cachedFile, json);
            }
            return json;
        }
        catch (Exception e)
        {
            if (cachedJson != null)
            {
                LOG.debug("Network refresh failed for id={}, using cached: {}", id, e.getMessage());
                return cachedJson;
            }
            ImporterLog.warn(LOG, "SailSys: error fetching race id={}: {}", id, e.getMessage());
            return null;
        }
    }

    /** A throttled GET returning the body; the delay is the politeness the scanner lacked. */
    private String fetch(String url) throws Exception
    {
        if (httpDelayMs > 0)
            Thread.sleep(httpDelayMs);
        ContentResponse response = client.GET(url);
        if (response.getStatus() != 200)
            throw new IOException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    /** True if the race is recent enough to be re-imported even when already held. */
    private boolean isRecentRace(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(recentRaceReimportDays));
    }

    /** Parses the leading {@code yyyy-MM-dd} of a SailSys ISO date-time. */
    static LocalDate peekDate(String dateTime)
    {
        if (dateTime == null || dateTime.length() < 10)
            return null;
        try
        {
            return LocalDate.parse(dateTime.substring(0, 10));
        }
        catch (Exception e)
        {
            return null;
        }
    }


    // --- Parse / import layer (package-private for testing) ---

    LocalDate peekRaceDate(String json)
    {
        try
        {
            RaceResponse response = MAPPER.readValue(json, RaceResponse.class);
            return (response.data != null) ? parseDate(response.data.dateTime) : null;
        }
        catch (Exception e) { return null; }
    }

    boolean isApiFound(String json)
    {
        try
        {
            RaceResponse response = MAPPER.readValue(json, RaceResponse.class);
            return "success".equals(response.result) && response.data != null;
        }
        catch (Exception e) { return false; }
    }

    boolean processRaceJson(String json)
    {
        return processRaceJson(json, null);
    }

    /**
     * @param owningClub the club whose configuration led us to this race, or null when the
     *                   race was replayed from a cache directory with no club context
     */
    boolean processRaceJson(String json, Club owningClub)
    {
        RaceResponse response;
        try
        {
            response = MAPPER.readValue(json, RaceResponse.class);
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG,"Cannot parse race JSON: {}", e.getMessage());
            return false;
        }

        if (!"success".equals(response.result) || response.data == null)
        {
            LOG.debug("Skipping non-success response: result={} error={}", response.result, response.errorMessage);
            return false;
        }

        RaceData data = response.data;
        if (data.status == null || data.status != 4)
        {
            LOG.debug("Skipping race id={}: status={}", data.id, data.status);
            return false;
        }
        if (data.lastProcessedTime == null)
        {
            LOG.debug("Skipping race id={}: lastProcessedTime is null", data.id);
            return false;
        }

        processRace(data, owningClub);
        return true;
    }

    private void processRace(RaceData data, Club owningClub)
    {
        currentSailSysRaceId = data.id != null ? data.id : 0;

        LocalDate raceDate = parseDate(data.dateTime);
        if (raceDate == null)
        {
            ImporterLog.warn(LOG,"Skipping race id={}: cannot parse dateTime={}", data.id, data.dateTime);
            return;
        }

        int number = data.number != null ? data.number : 0;

        // Organising club -- required for race ID and series registration.
        // Excluded clubs are filtered out by findUniqueClubByShortName; if the club name
        // resolves only to excluded clubs, skip the race entirely.
        Club organizingClub = owningClub;
        if (data.club != null && data.club.shortName != null)
        {
            String context = "SailSys race id=" + data.id + " series=" + (data.series != null ? data.series.name : "?");
            Club byName = store.findUniqueClubByShortName(data.club.shortName, data.club.longName, context);
            if (owningClub == null)
            {
                // No club context (cache replay): fall back to the name lookup, and skip the
                // race when the name resolves only to excluded clubs.
                organizingClub = byName;
                if (byName == null && store.isClubNameExcluded(data.club.shortName))
                {
                    LOG.debug("SailSys: skipping race id={} -- organising club '{}' is excluded",
                        data.id, data.club.shortName);
                    return;
                }
            }
            else if (byName != null && !byName.id().equals(owningClub.id()))
            {
                // The club opted in explicitly, so its own ID wins — but a disagreement means
                // either a mis-entered SailSys ID or two clubs sharing a short name.
                ImporterLog.warn(LOG, "SailSys: race id={} is configured under club={} but its "
                        + "short name '{}' resolves to {}; keeping the configured club",
                    data.id, owningClub.id(), data.club.shortName, byName.id());
            }
        }

        String clubId     = organizingClub != null ? organizingClub.id() : null;
        String seriesName = data.series != null ? data.series.name : null;
        String seriesId   = (clubId != null && seriesName != null)
            ? IdGenerator.generateSeriesId(clubId, seriesName) : null;
        String raceId     = clubId != null
            ? IdGenerator.generateRaceId(clubId, raceDate, number)
            : "unknown-" + raceDate + String.format("-%04d", number);

        // Find measurement systems that actually have calculation data on competitors
        List<HandicappingSummary> measurementSystems = resolveMeasurementSystems(data.handicappings);
        List<HandicappingSummary> actualSystems = resolveActualSystems(data.competitors, measurementSystems);

        List<Division> divisions = buildDivisions(
            data.competitors, actualSystems, raceDate, organizingClub);

        // If SailSys produced no finishers and a race for the same club + date already
        // exists from BWPS or RSHYR, don't create/merge -- just extract certificates.
        // This avoids creating empty duplicates of races whose results come from
        // dedicated importers (BWPS blue water pointscore, RSHYR Sydney Hobart).
        if (divisions.isEmpty() && hasDedicatedImporterRace(clubId, raceDate))
        {
            extractCertificatesOnly(data.competitors, actualSystems, raceDate, organizingClub);
            return;
        }

        String source = SOURCE + (currentSailSysRaceId > 0 ? "-" + currentSailSysRaceId : "");
        List<String> newSeriesIds = seriesId != null ? List.of(seriesId) : List.of();

        // Check whether this race matches any configured series exclusion pattern before storing.
        boolean autoExclude = store.matchesSeriesExclusion(data.name, seriesName);

        // If a race with this ID already exists (e.g. same physical race imported from
        // both a PHS series and an ORC series), merge rather than overwrite.
        List<Division> storedDivisions;
        Race existing = store.races().get(raceId);
        if (existing != null)
        {
            List<String> mergedSeriesIds = mergeSeriesIds(existing.seriesIds(), newSeriesIds);
            List<Division> mergedDivisions = mergeDivisions(existing.divisions(), divisions);
            String mergedSource = existing.source().contains(source) ? existing.source()
                : existing.source() + "," + source;

            store.putRace(new Race(
                raceId, clubId, mergedSeriesIds, raceDate, number, data.name,
                mergedDivisions, mergedSource, Instant.now(), null));
            storedDivisions = mergedDivisions;
        }
        else
        {
            store.putRace(new Race(
                raceId, clubId, newSeriesIds, raceDate, number, data.name,
                divisions, source, Instant.now(), null));
            storedDivisions = divisions;
        }
        racesImported++;
        for (Division d : divisions)
            finishersImported += d.finishers().size();

        if (autoExclude)
        {
            String reason = "series-pattern: name='" + data.name + "' series='" + seriesName + "'";
            store.setRaceExcluded(raceId, true, reason);
            LOG.info("SailSys: auto-excluded race '{}' (id={}) -- name/series matched exclusion pattern",
                data.name, data.id);
        }

        if (clubId != null && seriesId != null && seriesName != null)
            updateClubSeries(clubId, seriesId, seriesName, raceId);
    }

    // --- Division / finisher building ---

    private List<Division> buildDivisions(List<DivisionData> competitors,
                                          List<HandicappingSummary> measurementSystems,
                                          LocalDate raceDate, Club organizingClub)
    {
        if (competitors == null)
            return List.of();

        List<Division> divisions = new ArrayList<>();
        for (DivisionData divData : competitors)
        {
            String name = divData.parent != null ? divData.parent.name : "Unknown";
            List<Finisher> finishers = buildFinishers(divData.items, measurementSystems, raceDate, organizingClub);
            if (!finishers.isEmpty())
                divisions.add(new Division(name, finishers));
        }
        return List.copyOf(divisions);
    }

    /**
     * Builds finishers from a division's entries, attaching the first matching measurement
     * certificate when available.  Boats are never excluded for lacking cert data -- every
     * entrant with a valid elapsed time becomes a finisher.
     */
    private List<Finisher> buildFinishers(List<EntryData> items,
                                          List<HandicappingSummary> measurementSystems,
                                          LocalDate raceDate, Club organizingClub)
    {
        if (items == null)
            return List.of();

        List<Finisher> finishers = new ArrayList<>();
        for (EntryData entry : items)
        {
            if (entry.elapsedTime == null || entry.elapsedTime.isBlank())
                continue; // DNS/DNF/DNC

            Duration elapsed = parseElapsedTime(entry.elapsedTime);
            if (elapsed == null || elapsed.isNegative() || elapsed.isZero())
                continue;

            boolean nonSpinnaker = entry.nonSpinnaker != null && entry.nonSpinnaker;

            Boat boat = resolveBoat(entry.boat, organizingClub, raceDate);
            if (boat == null)
                continue;

            String certNumber = null;
            for (HandicappingSummary sys : measurementSystems)
            {
                if (sys.id == null) continue;
                Double value = extractHandicapValue(entry.calculations, sys.id);
                if (value != null)
                {
                    certNumber = resolveCertificate(boat, normalizeSystem(sys.shortName),
                        value, raceDate.getYear(), nonSpinnaker, false, isClubCert(sys.shortName));
                    break; // use first matching system
                }
            }

            finishers.add(new Finisher(boat.id(), elapsed, nonSpinnaker, certNumber));
        }
        return List.copyOf(finishers);
    }

    // --- Dedicated-importer detection and certificate-only mode ---

    /** Returns true if any existing race for the given club + date was imported by BWPS or RSHYR. */
    private boolean hasDedicatedImporterRace(String clubId, LocalDate raceDate)
    {
        if (clubId == null) return false;
        String prefix = IdGenerator.sanitizeIdForFilesystem(clubId) + "-" + raceDate + "-";
        for (Map.Entry<String, Race> entry : store.races().entrySet())
        {
            if (entry.getKey().startsWith(prefix))
            {
                String source = entry.getValue().source();
                if (source != null)
                {
                    for (String ds : DEDICATED_SOURCES)
                    {
                        if (source.contains(ds))
                            return true;
                    }
                }
            }
        }
        return false;
    }

    /** Resolves boats and certificates from competitor data without creating a race record. */
    private void extractCertificatesOnly(List<DivisionData> competitors,
                                         List<HandicappingSummary> measurementSystems,
                                         LocalDate raceDate, Club organizingClub)
    {
        if (competitors == null) return;
        int count = 0;
        for (DivisionData divData : competitors)
        {
            if (divData.items == null) continue;
            for (EntryData entry : divData.items)
            {
                Boat boat = resolveBoat(entry.boat, organizingClub, raceDate);
                if (boat == null) continue;
                count++;

                boolean nonSpinnaker = entry.nonSpinnaker != null && entry.nonSpinnaker;
                for (HandicappingSummary sys : measurementSystems)
                {
                    if (sys.id == null) continue;
                    Double value = extractHandicapValue(entry.calculations, sys.id);
                    if (value != null)
                    {
                        resolveCertificate(boat, normalizeSystem(sys.shortName),
                            value, raceDate.getYear(), nonSpinnaker, false,
                            isClubCert(sys.shortName));
                        break;
                    }
                }
            }
        }
        LOG.info("SailSys: cert-only mode for race at {} on {} -- {} boat(s) processed " +
            "(race already imported by dedicated importer)",
            organizingClub != null ? organizingClub.id() : "?", raceDate, count);
    }

    // --- Race merging (same physical race imported from multiple series/systems) ---

    /** Union two seriesId lists, preserving order, no duplicates. */
    private static List<String> mergeSeriesIds(List<String> existing, List<String> incoming)
    {
        List<String> merged = new ArrayList<>(existing);
        for (String s : incoming)
        {
            if (!merged.contains(s))
                merged.add(s);
        }
        return List.copyOf(merged);
    }

    /**
     * Merges two division lists from the same physical race.  For divisions with matching
     * names, finishers are merged: new boats are added, and existing finishers gain a
     * certificate number if the incoming data provides one they lacked.
     * Divisions that exist only in one list are included as-is.
     */
    private static List<Division> mergeDivisions(List<Division> existing, List<Division> incoming)
    {
        List<Division> merged = new ArrayList<>();

        for (Division eDiv : existing)
        {
            Division iDiv = incoming.stream()
                .filter(d -> Objects.equals(d.name(), eDiv.name()))
                .findFirst().orElse(null);

            if (iDiv == null)
            {
                merged.add(eDiv);
                continue;
            }

            // Merge finishers within the matching division
            List<Finisher> mergedFinishers = new ArrayList<>(eDiv.finishers());
            for (Finisher iFinisher : iDiv.finishers())
            {
                int idx = -1;
                for (int i = 0; i < mergedFinishers.size(); i++)
                {
                    if (mergedFinishers.get(i).boatId().equals(iFinisher.boatId()))
                    {
                        idx = i;
                        break;
                    }
                }

                if (idx < 0)
                {
                    // New boat not in existing division -- add it
                    mergedFinishers.add(iFinisher);
                }
                else if (mergedFinishers.get(idx).certificateNumber() == null
                    && iFinisher.certificateNumber() != null)
                {
                    // Existing finisher has no cert, incoming has one -- upgrade
                    mergedFinishers.set(idx, iFinisher);
                }
                // else: existing already has cert data or incoming has nothing new -- keep existing
            }
            merged.add(new Division(eDiv.name(), List.copyOf(mergedFinishers)));
        }

        // Add any incoming divisions that had no match in existing
        for (Division iDiv : incoming)
        {
            if (existing.stream().noneMatch(d -> Objects.equals(d.name(), iDiv.name())))
                merged.add(iDiv);
        }

        return List.copyOf(merged);
    }

    // --- Boat resolution ---

    /**
     * Finds or creates the boat using data from the race entry.
     * Design is derived from {@code make}/{@code model}; club is resolved from the boat's
     * {@code club} shortName using the organising club as a tiebreaker.
     *
     * @return the resolved/created Boat, or {@code null} if sail number or name is blank.
     */
    private Boat resolveBoat(BoatSummary boatSummary, Club organizingClub, LocalDate raceDate)
    {
        if (boatSummary == null
                || boatSummary.sailNumber == null || boatSummary.sailNumber.isBlank()
                || boatSummary.name == null || boatSummary.name.isBlank())
            return null;

        String sailNo = boatSummary.sailNumber.trim();
        String name   = boatSummary.name.trim();

        // Design from make+model (same logic as former SailSysBoatImporter)
        String make  = boatSummary.make  != null ? boatSummary.make.trim()  : "";
        String model = boatSummary.model != null ? boatSummary.model.trim() : "";
        String designName = (!make.isBlank() && !model.isBlank()) ? make + " " + model
            : (!make.isBlank() ? make : (!model.isBlank() ? model : null));
        if (designName != null && isGenericBoatClass(designName))
            designName = null;

        Boat boat = store.findOrCreateBoat(sailNo, name, designName, raceDate, SOURCE);
        if (boat == null)
        {
            ImporterLog.warn(LOG, "Skipping ambiguous boat sailNo={} name={} -- multiple designs in store", sailNo, name);
            return null;
        }

        // Assign club if missing and not explicitly marked no-club in config
        Club boatClub = resolveBoatClub(boatSummary.club, organizingClub);
        if (boatClub != null && boat.clubIds().isEmpty() && !store.isExplicitlyNoClub(boat.id()))
        {
            store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                boat.designId(), List.of(boatClub.id()),
                boat.certificates(), addSource(boat.sources(), SOURCE), Instant.now(), null));
            boat = store.boats().get(boat.id());
        }

        return boat;
    }

    /**
     * Resolves the club for a boat given its shortName and the organising club.
     * Priority:
     * <ol>
     *   <li>If shortName matches the organising club's shortName → use organising club.</li>
     *   <li>If shortName uniquely identifies a club → use it.</li>
     *   <li>If ambiguous, pick the club with matching shortName and same state as the
     *       organising club (if exactly one such club exists).</li>
     * </ol>
     */
    private Club resolveBoatClub(String shortName, Club organizingClub)
    {
        if (shortName == null || shortName.isBlank())
            return null;
        if (organizingClub != null && shortName.equalsIgnoreCase(organizingClub.shortName()))
            return organizingClub;
        Club unique = store.findUniqueClubByShortName(shortName, null, "SailSys boat club");
        if (unique != null)
            return unique;
        // Ambiguous shortName: prefer the club with the same state as the organising club
        if (organizingClub != null && organizingClub.state() != null)
        {
            String targetState = organizingClub.state();
            List<Club> matches = store.clubs().values().stream()
                .filter(c -> shortName.equalsIgnoreCase(c.shortName())
                    && targetState.equals(c.state()))
                .toList();
            if (matches.size() == 1)
                return matches.get(0);
        }
        return null;
    }

    // --- Certificate resolution ---

    /**
     * Finds or creates a certificate on the given boat.
     * <p>
     * Matching criteria: system + year (±1) + value (±0.001) + nonSpinnaker + twoHanded
     * + club flag.  If a match is found, that certificate number is returned without
     * creating a duplicate.  Otherwise an inferred certificate is created and added.
     */
    private String resolveCertificate(Boat boat, String system, double value, int year,
                                      boolean nonSpinnaker, boolean twoHanded, boolean clubCert)
    {
        // Always read the freshest version of the boat in case another cert was just added
        Boat current = store.boats().get(boat.id());
        if (current == null) current = boat;

        for (Certificate c : current.certificates())
        {
            if (c.system().equals(system)
                    && Math.abs(c.year() - year) <= 1
                    && Math.abs(c.value() - value) < 0.001
                    && c.nonSpinnaker() == nonSpinnaker
                    && c.twoHanded() == twoHanded
                    && c.club() == clubCert)
                return c.certificateNumber();
        }

        // Create inferred certificate
        String certNum = String.format("%s-inferred-%d-%.4f%s%s%s",
            system.toLowerCase(), year, value,
            nonSpinnaker ? "-ns" : "",
            twoHanded    ? "-dh" : "",
            clubCert     ? "-c"  : "");
        Certificate cert = new Certificate(system, year, value,
            nonSpinnaker, twoHanded, false, clubCert, certNum, null);

        List<Certificate> certs = new ArrayList<>(current.certificates());
        certs.add(cert);
        store.putBoat(new Boat(current.id(), current.sailNumber(), current.name(),
            current.designId(), current.clubIds(),
            List.copyOf(certs),
            addSource(current.sources(), SOURCE + (currentSailSysRaceId > 0 ? "-" + currentSailSysRaceId : "")),
            Instant.now(), null));

        LOG.debug("SailSys: inferred {} cert {} (value={} year={}) for boat {}",
            system, certNum, value, year, current.id());
        return certNum;
    }

    // --- Handicap system helpers ---

    /**
     * Returns all measurement-based handicap systems from the race's {@code handicappings}
     * list, preserving order.  PHS, TPR, YRD, and other performance-based systems are
     * excluded.
     */
    private static List<HandicappingSummary> resolveMeasurementSystems(
        List<HandicappingSummary> handicappings)
    {
        if (handicappings == null)
            return List.of();
        return handicappings.stream()
            .filter(h -> h != null && h.shortName != null && isMeasurementSystem(h.shortName))
            .toList();
    }

    /**
     * Scans competitor calculations to find which claimed measurement systems actually have
     * data.  A system is "actual" if at least one boat has a non-null handicap value for it.
     * This filters out systems that are listed in the race handicappings but have no real
     * calculation data (e.g. race 34328 claims ORCc but no boat has ORCc calculations).
     */
    private static List<HandicappingSummary> resolveActualSystems(
        List<DivisionData> competitors, List<HandicappingSummary> claimedSystems)
    {
        if (competitors == null || claimedSystems.isEmpty())
            return List.of();

        Set<Integer> activeIds = new HashSet<>();
        for (DivisionData div : competitors)
        {
            if (div.items == null) continue;
            for (EntryData entry : div.items)
            {
                for (HandicappingSummary sys : claimedSystems)
                {
                    if (sys.id != null && extractHandicapValue(entry.calculations, sys.id) != null)
                        activeIds.add(sys.id);
                }
            }
        }

        return claimedSystems.stream()
            .filter(s -> s.id != null && activeIds.contains(s.id))
            .toList();
    }

    /**
     * Returns true for any handicap system shortName that is measurement-based.
     * Covers IRC, ORC (all variants: ORCi, ORCc, ORCGP), and AMS.
     */
    private static boolean isMeasurementSystem(String shortName)
    {
        if (shortName == null) return false;
        return shortName.equals("IRC")
            || shortName.startsWith("ORC")   // ORC, ORCc, ORCi, ORCGP, …
            || shortName.equals("AMS");
    }

    /**
     * Normalises variant ORC shortNames ({@code ORCc}, {@code ORCi}, {@code ORCGP}) to
     * {@code "ORC"}.  {@code "IRC"} and {@code "AMS"} pass through unchanged.
     */
    private static String normalizeSystem(String shortName)
    {
        if (shortName != null && shortName.startsWith("ORC")) return "ORC";
        return shortName;
    }

    /**
     * Returns true for ORC Club ({@code ORCc}) certificates, which carry a configurable
     * weight penalty in the analysis.
     */
    private static boolean isClubCert(String shortName)
    {
        return "ORCc".equalsIgnoreCase(shortName);
    }

    /**
     * Finds the {@code handicapCreatedFrom} value in the calculations list whose
     * {@code handicapDefinitionId} matches the given system id.
     * Returns {@code null} if no matching, non-null value is found.
     */
    private static Double extractHandicapValue(List<Calculation> calculations, int definitionId)
    {
        if (calculations == null) return null;
        for (Calculation c : calculations)
        {
            if (c.handicapDefinitionId != null
                    && c.handicapDefinitionId == definitionId
                    && c.handicapCreatedFrom != null)
                return c.handicapCreatedFrom;
        }
        return null;
    }

    // --- Club / series helpers ---

    private void updateClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        Club club = store.clubs().get(clubId);
        if (club == null)
            club = store.clubSeed().get(clubId);
        if (club == null)
            return;

        List<Series> series = new ArrayList<>(club.series() != null ? club.series() : List.of());
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

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(),
            club.excluded(), club.email(), club.aliases(), club.topyachtGroups(), club.sailsysEvents(),
            List.copyOf(series), null));
    }

    // --- Utilities ---

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }

    private boolean isRecent(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(recentRaceDays));
    }

    private boolean isYoung(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(youngRaceMaxAgeDays));
    }

    /**
     * Returns true if the file's last-modified time is older than {@code maxAgeDays}.
     * Used to decide whether to re-fetch a cached race JSON.
     */
    static boolean isStale(Path file, int maxAgeDays)
    {
        try
        {
            return Files.getLastModifiedTime(file).toInstant()
                .isBefore(Instant.now().minus(maxAgeDays, ChronoUnit.DAYS));
        }
        catch (Exception e) { return true; }
    }

    private static boolean isGenericBoatClass(String name)
    {
        return "Keelboat".equalsIgnoreCase(name)
            || "Trailable".equalsIgnoreCase(name)
            || "Hong Kong - Big Boats".equalsIgnoreCase(name);
    }

    private LocalDate parseDate(String dateTime)
    {
        if (dateTime == null || dateTime.isBlank()) return null;
        try
        {
            String datePart = dateTime.contains("T")
                ? dateTime.substring(0, dateTime.indexOf('T')) : dateTime.trim();
            return LocalDate.parse(datePart);
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse dateTime: {}", dateTime);
            return null;
        }
    }

    private Duration parseElapsedTime(String raw)
    {
        if (raw == null || raw.isBlank()) return null;
        try
        {
            String[] parts = raw.trim().split(":");
            if (parts.length != 3) return null;
            return Duration.ofHours(Integer.parseInt(parts[0]))
                .plusMinutes(Integer.parseInt(parts[1]))
                .plusSeconds(Integer.parseInt(parts[2]));
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse elapsedTime: {}", raw);
            return null;
        }
    }

    // --- Jackson DTOs ---

    // --- Discovery DTOs (club profile, series race list) ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ClubProfileResponse
    {
        public ClubProfileData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ClubProfileData
    {
        public String shortName;
        public String longName;
        /** Upcoming series. */
        public List<EventGroup> nextEvents;
        /** Series that already have entrants or results. Overlaps nextEvents. */
        public List<EventGroup> eventsWithEntrantsOrResults;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EventGroup
    {
        public String parent;
        public List<EventItem> items;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EventItem
    {
        public String name;
        public Integer seriesId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SeriesRacesResponse
    {
        public SeriesRacesData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SeriesRacesData
    {
        public Integer id;
        public String name;
        public ClubSummary club;
        public List<SeriesRace> races;
    }

    /**
     * One race in a series listing: metadata only. Elapsed times live behind
     * {@code races/{id}/resultsentrants/display}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SeriesRace
    {
        public Integer id;
        public String dateTime;
        public Integer number;
        public String name;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RaceResponse
    {
        public String   result;
        public String   errorMessage;
        public RaceData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RaceData
    {
        public Integer id;
        public Integer status;
        public String  lastProcessedTime;
        public String  dateTime;
        public Integer number;
        public String  name;
        public ClubSummary            club;
        public SeriesSummary          series;
        public List<HandicappingSummary> handicappings;
        public List<DivisionData>     competitors;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ClubSummary
    {
        public String shortName;
        public String longName;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SeriesSummary
    {
        public String name;
    }

    /**
     * A handicap system defined for the race.  {@code id} is used to correlate with
     * {@link Calculation#handicapDefinitionId}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class HandicappingSummary
    {
        public Integer id;
        public String  shortName;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class DivisionData
    {
        public DivisionParent  parent;
        public List<EntryData> items;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class DivisionParent
    {
        public String name;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EntryData
    {
        public BoatSummary       boat;
        public String            elapsedTime;   // "H:MM:SS" or null for DNS/DNF/DNC
        public Boolean           nonSpinnaker;
        public List<Calculation> calculations;
    }

    /**
     * Summary of a boat as it appears in a race entry.
     * Contains enough data to create a {@link Boat} and {@link Design} without
     * fetching the separate boat API endpoint.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BoatSummary
    {
        public String name;
        public String sailNumber;
        public String make;
        public String model;
        public String club;    // club shortName as displayed in SailSys
    }

    /**
     * One scoring calculation for an entrant.
     * {@code handicapDefinitionId} matches {@link HandicappingSummary#id};
     * {@code handicapCreatedFrom} is the actual handicap used for scoring.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Calculation
    {
        public Integer handicapDefinitionId; // null for the "overall" (non-system) row
        public Double  handicapCreatedFrom;  // null when not scored under this system
    }
}
