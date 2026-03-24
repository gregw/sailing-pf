package org.mortbay.sailing.hpf.importer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Duration;
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
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports {@link Race} records from SailSys race JSON.
 * <p>
 * Only races with {@code status == 4} and non-null {@code lastProcessedTime} are imported
 * (i.e., results have been processed). Finishers with null {@code elapsedTime} (DNS/DNF/DNC)
 * are excluded.
 * <p>
 * For non-PHS races, each finisher is linked to a {@link Certificate} via
 * {@code certificateNumber}. If the boat already holds a certificate with the same system and
 * value, that certificate's number is reused. Otherwise an inferred certificate is created.
 * <p>
 * Supports two run modes:
 * <ul>
 *   <li><b>Local:</b> reads pre-downloaded {@code race-*.json} files from a directory.</li>
 *   <li><b>HTTP:</b> fetches races sequentially from the SailSys API.</li>
 * </ul>
 * <p>
 * CLI usage:
 * <pre>
 *   SailSysRaceImporter [dataRoot] --local &lt;racesDir&gt;
 *   SailSysRaceImporter [dataRoot] --api [startId]
 * </pre>
 */
public class SailSysRaceImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(SailSysRaceImporter.class);

    private static final String API_BASE = "https://api.sailsys.com.au/api/v1/races/";
    private static final String API_SUFFIX = "/resultsentrants/display";
    private static final int NOT_FOUND_THRESHOLD = 200;
    private static final int SAVE_INTERVAL = 500;
    private static final int HTTP_DELAY_MS = 200;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataStore store;
    private final HttpClient client; // null in local mode

    public SailSysRaceImporter(DataStore store, HttpClient client)
    {
        this.store = store;
        this.client = client;
    }

    // --- Entry point ---

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        DataStore dataStore = new DataStore(dataRoot);
        dataStore.start();

        String mode = args.length > 1 ? args[1] : "--local";

        Path defaultRacesDir = dataRoot.resolve("sailsys/races");

        if ("--api".equals(mode))
        {
            int startId = args.length > 2 ? Integer.parseInt(args[2]) : 1;
            HttpClient client = new HttpClient();
            client.start();
            try
            {
                new SailSysRaceImporter(dataStore, client).runFromApi(startId, id -> {}, () -> false, defaultRacesDir);
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
                new SailSysRaceImporter(dataStore, null).runFromDirectory(racesDir);
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
     * Saves every {@value #SAVE_INTERVAL} files processed and once at the end.
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
                String json = Files.readString(file);
                processRaceJson(json);
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
     * caching each response as {@code race-{id}.json} under {@code cacheDir} (if non-null).
     * On subsequent runs, cached files are read directly without an HTTP request.
     * Stops after {@value #NOT_FOUND_THRESHOLD} consecutive not-found responses, or when
     * {@code stop} returns true (checked after each successfully fetched race).
     * {@code onId} is called with the current SailSys ID on each successful fetch.
     */
    public void runFromApi(int startId, IntConsumer onId, BooleanSupplier stop, Path cacheDir)
        throws Exception
    {
        LOG.info("Fetching races from SailSys API starting at id={}", startId);
        int id = startId;
        int consecutiveNotFound = 0;
        int processed = 0;

        while (consecutiveNotFound < NOT_FOUND_THRESHOLD)
        {
            LOG.info("Fetching race id={}", id);
            String url = API_BASE + id + API_SUFFIX;
            String json;
            Path cachedFile = (cacheDir != null) ? cacheDir.resolve(String.format("race-%06d.json", id)) : null;
            try
            {
                if (cachedFile != null && Files.exists(cachedFile))
                {
                    json = Files.readString(cachedFile);
                }
                else
                {
                    Thread.sleep(HTTP_DELAY_MS);
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

            boolean found = processRaceJson(json);
            if (found)
            {
                consecutiveNotFound = 0;
                onId.accept(id);
            }
            else
                consecutiveNotFound++;

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

    // --- Parse / import layer (package-private for testing) ---

    /**
     * Parses one SailSys race JSON response and imports it into the store.
     *
     * @return {@code true} if the response contained a valid, processed race,
     *         {@code false} if it was a not-found, error, or unprocessed response.
     */
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
        // Date from dateTime field (ISO-8601 "2018-09-01T00:00:00.000")
        LocalDate date = parseDate(data.dateTime);
        if (date == null)
        {
            LOG.warn("Skipping race id={}: cannot parse dateTime={}", data.id, data.dateTime);
            return;
        }

        int number = data.number != null ? data.number : 0;

        // Club resolution
        Club club = null;
        if (data.club != null && data.club.shortName != null)
            club = store.findUniqueClubByShortName(data.club.shortName, data.club.longName);

        String clubId = club != null ? club.id() : null;

        // Series
        String seriesName = data.series != null ? data.series.name : null;
        String seriesId = (clubId != null && seriesName != null)
            ? IdGenerator.generateSeriesId(clubId, seriesName)
            : null;

        // Race ID
        String raceId = clubId != null
            ? IdGenerator.generateRaceId(clubId, date, number)
            : "unknown-" + date + String.format("-%04d", number);

        // Handicap system
        String handicapSystem = resolveHandicapSystem(data.handicappings);

        // Finishers
        boolean measurementSystem = isMeasurementHandicapSystem(handicapSystem);
        List<Division> divisions = buildDivisions(data.competitors, handicapSystem, measurementSystem, date.getYear());

        Race race = new Race(
            raceId,
            clubId,
            seriesId != null ? List.of(seriesId) : List.of(),
            date,
            number,
            data.name,
            handicapSystem,
            data.offsetPursuitRace != null && data.offsetPursuitRace,
            divisions,
            null
        );

        store.putRace(race);

        if (clubId != null && seriesId != null && seriesName != null)
            updateClubSeries(clubId, seriesId, seriesName, raceId);
    }

    private List<Division> buildDivisions(List<DivisionData> competitors, String handicapSystem,
                                          boolean measurementSystem, int raceYear)
    {
        if (competitors == null)
            return List.of();

        List<Division> divisions = new ArrayList<>();
        for (DivisionData divData : competitors)
        {
            String divName = divData.parent != null ? divData.parent.name : "Unknown";
            List<Finisher> finishers = buildFinishers(divData.items, handicapSystem, measurementSystem, raceYear);
            if (!finishers.isEmpty())
                divisions.add(new Division(divName, finishers));
        }
        return List.copyOf(divisions);
    }

    private List<Finisher> buildFinishers(List<EntryData> items, String handicapSystem,
                                          boolean measurementSystem, int raceYear)
    {
        if (items == null)
            return List.of();

        List<Finisher> finishers = new ArrayList<>();
        for (EntryData entry : items)
        {
            if (entry.elapsedTime == null || entry.elapsedTime.isBlank())
                continue; // DNS/DNF/DNC

            Duration elapsed = parseElapsedTime(entry.elapsedTime);
            if (elapsed == null)
            {
                LOG.debug("Skipping entry: cannot parse elapsedTime={}", entry.elapsedTime);
                continue;
            }

            boolean nonSpinnaker = entry.nonSpinnaker != null && entry.nonSpinnaker;

            String certNumber = null;
            if (measurementSystem && entry.boat != null)
            {
                Double hcFrom = extractHandicapCreatedFrom(entry.calculations);
                if (hcFrom != null)
                {
                    String sailNo = entry.boat.sailNumber != null ? entry.boat.sailNumber : "";
                    String name = entry.boat.name != null ? entry.boat.name : "";
                    if (!sailNo.isBlank() && !name.isBlank())
                        certNumber = resolveCertificate(sailNo, name, handicapSystem, hcFrom, raceYear);
                }
            }

            String boatId = resolveBoatId(entry.boat);
            if (boatId == null)
                continue;

            finishers.add(new Finisher(boatId, elapsed, nonSpinnaker, certNumber));
        }
        return List.copyOf(finishers);
    }

    private String resolveBoatId(BoatSummary boat)
    {
        if (boat == null || boat.sailNumber == null || boat.sailNumber.isBlank()
                || boat.name == null || boat.name.isBlank())
            return null;
        return store.findOrCreateBoat(boat.sailNumber.trim(), boat.name.trim(), null).id();
    }

    /**
     * Finds or creates a certificate for the given system and value on the boat,
     * returning the certificate number.
     */
    private String resolveCertificate(String sailNo, String name, String system,
                                      double value, int year)
    {
        Boat boat = store.findOrCreateBoat(sailNo.trim(), name.trim(), null);

        // Search for an existing cert with matching system and value
        for (Certificate c : boat.certificates())
        {
            if (c.system().equals(system) && c.value() == value)
                return c.certificateNumber();
        }

        // Create an inferred certificate
        String certNum = system.toLowerCase() + "-inferred-"
            + Long.toHexString(Double.doubleToLongBits(value));
        Certificate cert = new Certificate(system, year, value, false, false, false, certNum, null);

        List<Certificate> updatedCerts = new ArrayList<>(boat.certificates());
        updatedCerts.add(cert);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(), List.copyOf(updatedCerts), null));

        return certNum;
    }

    /**
     * Updates the club's series list to include the raceId under the given seriesId.
     * Creates the series if it does not yet exist in the club.
     */
    private void updateClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        Club club = store.clubs().get(clubId);
        if (club == null)
            return;

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

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(),
            club.aliases(), club.topyachtUrls(), List.copyOf(series), null));
    }

    /**
     * Returns true only for measurement-based handicap systems (IRC, ORC, AMS).
     * Any other system — PHS, TCF, CBH, or unknown — is treated as performance-based
     * and must not produce inferred certificates.
     */
    private static boolean isMeasurementHandicapSystem(String system)
    {
        return "IRC".equalsIgnoreCase(system)
            || "ORC".equalsIgnoreCase(system)
            || "AMS".equalsIgnoreCase(system);
    }

    private String resolveHandicapSystem(List<HandicappingSummary> handicappings)
    {
        if (handicappings == null || handicappings.isEmpty())
            return "UNKNOWN";
        for (HandicappingSummary h : handicappings)
        {
            if (h != null && h.shortName != null)
                return h.shortName;
        }
        return "UNKNOWN";
    }

    private Double extractHandicapCreatedFrom(List<Calculation> calculations)
    {
        if (calculations == null || calculations.isEmpty())
            return null;
        Calculation first = calculations.getFirst();
        return first != null ? first.handicapCreatedFrom : null;
    }

    private LocalDate parseDate(String dateTime)
    {
        if (dateTime == null || dateTime.isBlank())
            return null;
        try
        {
            String datePart = dateTime.contains("T") ? dateTime.substring(0, dateTime.indexOf('T')) : dateTime.trim();
            return LocalDate.parse(datePart);
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse dateTime: {}", dateTime);
            return null;
        }
    }

    /**
     * Parses "H:MM:SS" or "HH:MM:SS" elapsed time strings to Duration.
     */
    private Duration parseElapsedTime(String raw)
    {
        if (raw == null || raw.isBlank())
            return null;
        try
        {
            String[] parts = raw.trim().split(":");
            if (parts.length != 3)
                return null;
            int hours = Integer.parseInt(parts[0]);
            int minutes = Integer.parseInt(parts[1]);
            int seconds = Integer.parseInt(parts[2]);
            return Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds);
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse elapsedTime: {}", raw);
            return null;
        }
    }

    // --- Jackson DTOs (package-private for testing) ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RaceResponse
    {
        public String result;
        public String errorMessage;
        public RaceData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class RaceData
    {
        public Integer id;
        public Integer status;
        public String lastProcessedTime;
        public String dateTime;         // e.g. "2018-09-01T00:00:00.000"
        public Integer number;
        public String name;
        public Boolean offsetPursuitRace;
        public ClubSummary club;
        public SeriesSummary series;
        public List<HandicappingSummary> handicappings;
        public List<DivisionData> competitors;
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class HandicappingSummary
    {
        public String shortName;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class DivisionData
    {
        public DivisionParent parent;
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
        public BoatSummary boat;
        public String elapsedTime;      // "H:MM:SS" or null for DNS/DNF/DNC
        public Boolean nonSpinnaker;
        public List<Calculation> calculations;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BoatSummary
    {
        public String name;
        public String sailNumber;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class Calculation
    {
        public Double handicapCreatedFrom;  // nullable
    }
}
