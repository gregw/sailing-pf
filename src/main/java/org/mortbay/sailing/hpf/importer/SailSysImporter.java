package org.mortbay.sailing.hpf.importer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.store.DataStore;
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
 *       — this is the value actually used for scoring, not the current stored certificate
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

    private static final String API_BASE   = "https://api.sailsys.com.au/api/v1/races/";
    private static final String API_SUFFIX = "/resultsentrants/display";
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

        String mode = args.length > 1 ? args[1] : "--local";
        Path defaultRacesDir = dataRoot.resolve("cache/sailsys/races");

        if ("--api".equals(mode))
        {
            int startId = args.length > 2 ? Integer.parseInt(args[2]) : 1;
            HttpClient client = new HttpClient();
            client.start();
            try
            {
                new SailSysImporter(dataStore, client)
                    .runFromApi(startId, id -> {}, () -> false, defaultRacesDir);
            }
            finally
            {
                dataStore.stop();
                client.stop();
            }
        }
        else
        {
            Path racesDir = args.length > 2 ? Path.of(args[2]) : defaultRacesDir;
            try
            {
                new SailSysImporter(dataStore, null).runFromDirectory(racesDir);
            }
            finally
            {
                dataStore.stop();
            }
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
                LOG.warn("Error processing {}: {}", file.getFileName(), e.getMessage());
            }
            processed++;
            if (processed % SAVE_INTERVAL == 0)
            {
                LOG.info("Processed {} files — saving", processed);
                store.save();
            }
        }
        store.save();
        LOG.info("Done. Processed {} files.", processed);
    }

    /**
     * Fetches races sequentially from the SailSys API starting at {@code startId},
     * caching each response as {@code race-{id}.json} under {@code cacheDir}.
     * Stops after 200 consecutive not-found responses or when {@code stop} returns true.
     */
    public void runFromApi(int startId, IntConsumer onId, BooleanSupplier stop, Path cacheDir)
        throws Exception
    {
        LOG.info("Fetching races from SailSys API starting at id={}", startId);
        int id = startId;
        int consecutiveNotFound = 0;
        int processed = 0;

        while (consecutiveNotFound < 200)
        {
            LOG.info("Fetching race id={}", id);
            String url = API_BASE + id + API_SUFFIX;
            String json;
            Path cachedFile = cacheDir != null
                ? cacheDir.resolve(String.format("race-%06d.json", id)) : null;
            try
            {
                if (cachedFile != null && Files.exists(cachedFile))
                    json = Files.readString(cachedFile);
                else
                {
                    Thread.sleep(httpDelayMs);
                    ContentResponse response = client.GET(url);
                    json = response.getContentAsString();
                    if (cachedFile != null)
                    {
                        Files.createDirectories(cacheDir);
                        Files.writeString(cachedFile, json);
                    }
                }
            }
            catch (Exception e)
            {
                LOG.warn("HTTP error fetching race id={}: {}", id, e.getMessage());
                id++;
                continue;
            }

            onId.accept(id);
            boolean found = processRaceJson(json);
            if (found) consecutiveNotFound = 0;
            else consecutiveNotFound++;

            processed++;
            if (processed % SAVE_INTERVAL == 0)
            {
                LOG.info("Fetched {} races (id={}) — saving", processed, id);
                store.save();
            }

            if (stop.getAsBoolean())
            {
                LOG.info("Stop requested — stopping after race id={}", id);
                break;
            }
            id++;
        }

        store.save();
        LOG.info("Done. Last id={}, processed={}.", id, processed);
    }

    /**
     * Unified run method: reads from local cache when fresh; fetches from network when
     * absent or stale; always re-fetches recent races so results are picked up promptly.
     *
     * @return the minimum SailSys race ID whose date fell within the recent window,
     *         or 0 if no races were in the recent window.
     */
    public int run(int startId, IntConsumer onId, BooleanSupplier stop,
                   Path racesDir,
                   int youngCacheMaxAgeDays, int oldCacheMaxAgeDays,
                   int youngRaceMaxAgeDays, int httpDelayMs,
                   int recentRaceDays, int notFoundThreshold)
        throws Exception
    {
        this.youngCacheMaxAgeDays = youngCacheMaxAgeDays;
        this.oldCacheMaxAgeDays   = oldCacheMaxAgeDays;
        this.youngRaceMaxAgeDays  = youngRaceMaxAgeDays;
        this.httpDelayMs          = httpDelayMs;
        this.recentRaceDays       = recentRaceDays;

        LOG.info("Importing SailSys races starting at id={}, notFoundThreshold={}",
            startId, notFoundThreshold);
        int id = startId;
        int consecutiveNotFound = 0;
        int processed = 0;
        int minRecentId = Integer.MAX_VALUE;

        while (consecutiveNotFound < notFoundThreshold)
        {
            LOG.info("Fetching race id={}", id);
            Path cachedFile = racesDir != null
                ? racesDir.resolve(String.format("race-%06d.json", id)) : null;

            String cachedJson = null;
            if (cachedFile != null && Files.exists(cachedFile))
                cachedJson = Files.readString(cachedFile);

            boolean useCache = false;
            if (cachedJson != null)
            {
                LocalDate raceDate = peekRaceDate(cachedJson);
                if (!isRecent(raceDate))
                {
                    int maxAge = isYoung(raceDate) ? youngCacheMaxAgeDays : oldCacheMaxAgeDays;
                    useCache = !isStale(cachedFile, maxAge);
                }
            }

            String json;
            if (useCache)
            {
                json = cachedJson;
            }
            else
            {
                try
                {
                    Thread.sleep(httpDelayMs);
                    ContentResponse response = client.GET(API_BASE + id + API_SUFFIX);
                    json = response.getContentAsString();
                    if (cachedFile != null)
                    {
                        Files.createDirectories(racesDir);
                        Files.writeString(cachedFile, json);
                    }
                }
                catch (Exception e)
                {
                    if (cachedJson != null)
                    {
                        LOG.debug("Network refresh failed for id={}, using cached: {}", id, e.getMessage());
                        json = cachedJson;
                    }
                    else
                    {
                        LOG.warn("Error fetching race id={}: {}", id, e.getMessage());
                        id++;
                        continue;
                    }
                }
            }

            LocalDate raceDate = peekRaceDate(json);
            if (isRecent(raceDate))
                minRecentId = Math.min(minRecentId, id);

            onId.accept(id);
            boolean apiFound = isApiFound(json);
            if (apiFound) consecutiveNotFound = 0;
            else consecutiveNotFound++;
            if (apiFound) processRaceJson(json);

            processed++;
            if (processed % SAVE_INTERVAL == 0)
            {
                LOG.info("Fetched {} races (id={}) — saving", processed, id);
                store.save();
            }
            if (stop.getAsBoolean())
            {
                LOG.info("Stop requested after id={}", id);
                break;
            }
            id++;
        }

        store.save();
        LOG.info("Done. Last id={}, processed={}.", id, processed);
        return (minRecentId == Integer.MAX_VALUE) ? 0 : minRecentId;
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
        RaceResponse response;
        try
        {
            response = MAPPER.readValue(json, RaceResponse.class);
        }
        catch (Exception e)
        {
            LOG.warn("Cannot parse race JSON: {}", e.getMessage());
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

        processRace(data);
        return true;
    }

    private void processRace(RaceData data)
    {
        currentSailSysRaceId = data.id != null ? data.id : 0;

        LocalDate date = parseDate(data.dateTime);
        if (date == null)
        {
            LOG.warn("Skipping race id={}: cannot parse dateTime={}", data.id, data.dateTime);
            return;
        }

        int number = data.number != null ? data.number : 0;

        // Organising club — required for race ID and series registration.
        // Log a warning if not found (club.yaml may need updating) but continue.
        Club organizingClub = null;
        if (data.club != null && data.club.shortName != null)
            organizingClub = store.findUniqueClubByShortName(data.club.shortName, data.club.longName,
                "SailSys race id=" + data.id + " series=" + (data.series != null ? data.series.name : "?"));

        String clubId     = organizingClub != null ? organizingClub.id() : null;
        String seriesName = data.series != null ? data.series.name : null;
        String seriesId   = (clubId != null && seriesName != null)
            ? IdGenerator.generateSeriesId(clubId, seriesName) : null;
        String raceId     = clubId != null
            ? IdGenerator.generateRaceId(clubId, date, number)
            : "unknown-" + date + String.format("-%04d", number);

        // Find all measurement systems.  Races commonly list PHS first then IRC/ORC;
        // we process every measurement system independently, creating separate divisions.
        List<HandicappingSummary> measurementSystems = resolveMeasurementSystems(data.handicappings);
        String handicapSystem = measurementSystems.isEmpty()
            ? "PHS"
            : normalizeSystem(measurementSystems.get(0).shortName);
        if (measurementSystems.size() > 1)
            handicapSystem = String.join("/", measurementSystems.stream()
                .map(h -> normalizeSystem(h.shortName)).distinct().toList());

        List<Division> divisions = buildDivisions(
            data.competitors, measurementSystems, date.getYear(), organizingClub);

        store.putRace(new Race(
            raceId, clubId,
            seriesId != null ? List.of(seriesId) : List.of(),
            date, number, data.name, handicapSystem,
            data.offsetPursuitRace != null && data.offsetPursuitRace,
            divisions,
            SOURCE + (currentSailSysRaceId > 0 ? "-" + currentSailSysRaceId : ""),
            Instant.now(), null));

        if (clubId != null && seriesId != null && seriesName != null)
            updateClubSeries(clubId, seriesId, seriesName, raceId);
    }

    // --- Division / finisher building ---

    private List<Division> buildDivisions(List<DivisionData> competitors,
                                          List<HandicappingSummary> measurementSystems,
                                          int raceYear, Club organizingClub)
    {
        if (competitors == null)
            return List.of();

        boolean multiSystem = measurementSystems.size() > 1;
        List<Division> divisions = new ArrayList<>();

        for (DivisionData divData : competitors)
        {
            String baseName = divData.parent != null ? divData.parent.name : "Unknown";

            if (measurementSystems.isEmpty())
            {
                // PHS-only race: create the division without cert links
                List<Finisher> finishers = buildFinishers(divData.items, null, raceYear, organizingClub);
                if (!finishers.isEmpty())
                    divisions.add(new Division(baseName, finishers));
            }
            else
            {
                for (HandicappingSummary sys : measurementSystems)
                {
                    String divName = multiSystem
                        ? baseName + " " + normalizeSystem(sys.shortName)
                        : baseName;
                    List<Finisher> finishers = buildFinishers(divData.items, sys, raceYear, organizingClub);
                    if (!finishers.isEmpty())
                        divisions.add(new Division(divName, finishers));
                }
            }
        }
        return List.copyOf(divisions);
    }

    /**
     * Builds finishers for one measurement system (or null for PHS-only).
     * For each entrant, the cert value is taken from the {@code calculations[]} entry
     * whose {@code handicapDefinitionId} matches the system's id — this is the handicap
     * actually used for scoring in this race, not the current stored certificate value.
     */
    private List<Finisher> buildFinishers(List<EntryData> items,
                                          HandicappingSummary system,
                                          int raceYear, Club organizingClub)
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

            Boat boat = resolveBoat(entry.boat, organizingClub);
            if (boat == null)
                continue;

            String certNumber = null;
            if (system != null && system.id != null)
            {
                Double value = extractHandicapValue(entry.calculations, system.id);
                if (value == null)
                    continue; // no handicap for this system — exclude from this division
                certNumber = resolveCertificate(boat, normalizeSystem(system.shortName),
                    value, raceYear, nonSpinnaker, false, isClubCert(system.shortName));
            }

            // Re-read boat after cert may have been added by resolveCertificate
            // (boat object passed to resolveCertificate may be stale; the ID is stable)
            finishers.add(new Finisher(boat.id(), elapsed, nonSpinnaker, certNumber));
        }
        return List.copyOf(finishers);
    }

    // --- Boat resolution ---

    /**
     * Finds or creates the boat using data from the race entry.
     * Design is derived from {@code make}/{@code model}; club is resolved from the boat's
     * {@code club} shortName using the organising club as a tiebreaker.
     *
     * @return the resolved/created Boat, or {@code null} if sail number or name is blank.
     */
    private Boat resolveBoat(BoatSummary boatSummary, Club organizingClub)
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

        Boat boat = store.findOrCreateBoat(sailNo, name, designName, SOURCE);

        // Assign club if missing
        Club boatClub = resolveBoatClub(boatSummary.club, organizingClub);
        if (boatClub != null && boat.clubId() == null)
        {
            store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                boat.designId(), boatClub.id(), boat.aliases(), boat.altSailNumbers(),
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
            current.designId(), current.clubId(), current.aliases(), current.altSailNumbers(),
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
            club.excluded(), club.aliases(), club.topyachtUrls(), List.copyOf(series), null));
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
        public Boolean offsetPursuitRace;
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
