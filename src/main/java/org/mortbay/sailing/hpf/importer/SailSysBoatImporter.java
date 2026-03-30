package org.mortbay.sailing.hpf.importer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports {@link Boat} and {@link Certificate} records from SailSys boat JSON.
 * <p>
 * Supports two run modes:
 * <ul>
 *   <li><b>Local:</b> reads pre-downloaded {@code boat-*.json} files from a directory
 *       (first priority — large volume already downloaded to {@code OLD/sailsys/boats}).</li>
 *   <li><b>HTTP:</b> fetches boats sequentially from the SailSys API, stopping after
 *       {@value #NOT_FOUND_THRESHOLD} consecutive 404/not-found responses.</li>
 * </ul>
 * <p>
 * Certificates imported: IRC only (spinnaker and non-spinnaker variants), keyed by
 * certificate number for idempotent upsert. ORC is handled by {@link OrcImporter}.
 * AMS is handled by {@link AmsImporter}. PHS and CBH are excluded by policy.
 * <p>
 * CLI usage:
 * <pre>
 *   SailSysBoatImporter [dataRoot] --local &lt;boatsDir&gt;
 *   SailSysBoatImporter [dataRoot] --api [startId]
 * </pre>
 */
public class SailSysBoatImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(SailSysBoatImporter.class);

    static final String SOURCE = "SailSys";

    private static final String API_BASE = "https://api.sailsys.com.au/api/v1/boats/";
    private static final int NOT_FOUND_THRESHOLD = 200;
    private static final int SAVE_INTERVAL = 500;
    private static final int HTTP_DELAY_MS = 200;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataStore store;
    private final HttpClient client; // null in local mode

    public SailSysBoatImporter(DataStore store, HttpClient client)
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

        Path defaultBoatsDir = dataRoot.resolve("sailsys/boats");

        if ("--api".equals(mode))
        {
            int startId = args.length > 2 ? Integer.parseInt(args[2]) : 1;
            HttpClient client = new HttpClient();
            client.start();
            try
            {
                new SailSysBoatImporter(dataStore, client).runFromApi(startId, id -> {}, () -> false, defaultBoatsDir);
            }
            finally
            {
                dataStore.stop();
                client.stop();
            }
        }
        else
        {
            Path boatsDir = args.length > 2 ? Path.of(args[2]) : defaultBoatsDir;
            try
            {
                new SailSysBoatImporter(dataStore, null).runFromDirectory(boatsDir);
            }
            finally
            {
                dataStore.stop();
            }
        }
    }

    // --- Run modes ---

    /**
     * Processes all {@code boat-*.json} files in {@code dir} in sorted filename order.
     * Saves every {@value #SAVE_INTERVAL} files processed and once at the end.
     */
    public void runFromDirectory(Path dir) throws IOException
    {
        LOG.info("Loading boats from directory {}", dir.toAbsolutePath());
        List<Path> files;
        try (var stream = Files.list(dir))
        {
            files = stream
                .filter(p -> p.getFileName().toString().matches("boat-\\d+\\.json"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        }
        LOG.info("Found {} boat files", files.size());

        int processed = 0;
        for (Path file : files)
        {
            try
            {
                String json = Files.readString(file);
                processBoatJson(json);
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
     * Fetches boats sequentially from the SailSys API starting at {@code startId},
     * caching each response as {@code boat-{id}.json} under {@code cacheDir} (if non-null).
     * On subsequent runs, cached files are read directly without an HTTP request.
     * Stops after {@value #NOT_FOUND_THRESHOLD} consecutive not-found responses, or when
     * {@code stop} returns true (checked after each successfully fetched boat).
     * {@code onId} is called with the current SailSys ID on each successful fetch.
     */
    public void runFromApi(int startId, IntConsumer onId, BooleanSupplier stop, Path cacheDir)
        throws Exception
    {
        LOG.info("Fetching boats from SailSys API starting at id={}", startId);
        int id = startId;
        int consecutiveNotFound = 0;
        int processed = 0;

        while (consecutiveNotFound < NOT_FOUND_THRESHOLD)
        {
            LOG.info("Fetching boat id={}", id);
            String url = API_BASE + id;
            String json;
            Path cachedFile = (cacheDir != null) ? cacheDir.resolve(String.format("boat-%06d.json", id)) : null;
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
                LOG.warn("HTTP error fetching boat id={}: {}", id, e.getMessage());
                id++;
                continue;
            }

            boolean found = processBoatJson(json);
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
                LOG.info("Fetched {} boats (id={}) — saving", processed, id);
                store.save();
            }

            if (stop.getAsBoolean())
            {
                LOG.info("Stop requested — stopping after boat id={}", id);
                break;
            }

            id++;
        }

        store.save();
        LOG.info("Done. Last id={}, processed={}.", id, processed);
    }

    /**
     * Fetches (or reads from cache) a single boat by SailSys ID and imports it.
     * Used by {@link SailSysRaceImporter} for on-demand boat enrichment.
     * If a fresh cached file exists, no HTTP request is made.
     *
     * @return {@code true} if the boat was found and imported
     */
    boolean fetchAndImport(int boatId, Path cacheDir, int cacheMaxAgeDays, int httpDelayMs)
        throws Exception
    {
        Path cachedFile = cacheDir != null
            ? cacheDir.resolve(String.format("boat-%06d.json", boatId)) : null;
        String json;
        if (cachedFile != null && Files.exists(cachedFile) && !isStale(cachedFile, cacheMaxAgeDays))
        {
            json = Files.readString(cachedFile);
        }
        else
        {
            Thread.sleep(httpDelayMs);
            ContentResponse response = client.GET(API_BASE + boatId);
            json = response.getContentAsString();
            if (cachedFile != null)
            {
                Files.createDirectories(cacheDir);
                Files.writeString(cachedFile, json);
            }
        }
        return processBoatJson(json);
    }

    static boolean isStale(Path file, int maxAgeDays)
    {
        try
        {
            return Files.getLastModifiedTime(file).toInstant()
                .isBefore(Instant.now().minus(maxAgeDays, ChronoUnit.DAYS));
        }
        catch (IOException e) { return true; }
    }

    // --- Parse / import layer (package-private for testing) ---

    /**
     * Parses one SailSys boat JSON response and imports it into the store.
     *
     * @return {@code true} if the response contained a valid boat, {@code false} if it
     *         was a not-found or error response.
     */
    boolean processBoatJson(String json)
    {
        SailSysResponse response;
        try
        {
            response = MAPPER.readValue(json, SailSysResponse.class);
        }
        catch (Exception e)
        {
            LOG.warn("Cannot parse boat JSON: {}", e.getMessage());
            return false;
        }

        if (!"success".equals(response.result) || response.data == null)
        {
            LOG.debug("Skipping non-success response: result={} error={}", response.result, response.errorMessage);
            return false;
        }

        processBoat(response.data);
        return true;
    }

    private void processBoat(BoatData boat)
    {
        if (boat.name == null || boat.name.isBlank() || boat.sailNumber == null || boat.sailNumber.isBlank())
        {
            LOG.warn("Skipping boat id={}: missing name or sailNumber", boat.id);
            return;
        }

        // Design: combine make and model. If only one is present, use that alone.
        // Fall back to boatClass.name when make/model are both blank, but only for
        // specific class names (not generic categories like "Keelboat").
        String make = boat.make != null ? boat.make.trim() : "";
        String model = boat.model != null ? boat.model.trim() : "";
        String designName = (!make.isBlank() && !model.isBlank()) ? make + " " + model
            : (!make.isBlank() ? make : (!model.isBlank() ? model : null));
        if (designName == null && boat.boatClass != null
                && boat.boatClass.name != null
                && !boat.boatClass.name.isBlank()
                && !isGenericBoatClass(boat.boatClass.name))
        {
            designName = boat.boatClass.name;
        }
        Design design = store.findOrCreateDesign(designName, SOURCE);

        // Club: SailSys gives shortName + longName; use longName to disambiguate if needed.
        Club club = resolveClub(boat.clubShortName, boat.clubLongName);

        // Find or create the boat
        Boat existing = store.findOrCreateBoat(boat.sailNumber.trim(), boat.name.trim(), design);

        // Set clubId if resolved and not already set
        String clubId = existing.clubId() != null ? existing.clubId()
            : (club != null ? club.id() : null);

        // Upsert IRC certificates
        List<Certificate> certs = upsertIrcCerts(existing.certificates(), boat.handicaps);

        store.putBoat(new Boat(existing.id(), existing.sailNumber(), existing.name(),
            existing.designId(), clubId, existing.aliases(), existing.altSailNumbers(), List.copyOf(certs),
            addSource(existing.sources(), SOURCE + (boat.id != null ? "-" + boat.id : "")),
            Instant.now(), null));
    }

    /**
     * Returns an updated certificate list with IRC certs from {@code incoming} merged in.
     * For each distinct certificate number in the incoming IRC certs, all existing certs
     * with that number are removed and replaced with the new values.
     * Certificates from other systems (ORC, AMS) are left untouched.
     */
    private List<Certificate> upsertIrcCerts(List<Certificate> existing, List<HandicapEntry> incoming)
    {
        if (incoming == null || incoming.isEmpty())
            return new ArrayList<>(existing);

        // Collect incoming IRC entries (skip IRC SH, PHS, CBH, AMS, ORC)
        List<Certificate> newIrcCerts = new ArrayList<>();
        for (HandicapEntry h : incoming)
        {
            if (h.definition == null || !"IRC".equals(h.definition.shortName))
                continue;
            if (h.certificate == null || h.certificate.certificateNumber == null)
            {
                LOG.debug("Skipping IRC entry with no certificate object");
                continue;
            }
            boolean nonSpinnaker = h.spinnakerType == 2;
            LocalDate expiry = parseExpiry(h.certificate.expiryDate);
            int year = certYear(expiry);
            newIrcCerts.add(new Certificate("IRC", year, h.value, nonSpinnaker, false, false, false,
                h.certificate.certificateNumber, expiry));
        }

        if (newIrcCerts.isEmpty())
            return new ArrayList<>(existing);

        // Cert numbers being replaced
        List<String> replacedNumbers = newIrcCerts.stream()
            .map(Certificate::certificateNumber)
            .distinct()
            .toList();

        List<Certificate> result = new ArrayList<>(existing);
        result.removeIf(c -> "IRC".equals(c.system()) && replacedNumbers.contains(c.certificateNumber()));
        result.addAll(newIrcCerts);
        result.sort(Comparator.comparingInt(Certificate::year).reversed());
        return result;
    }

    /**
     * Looks up a club by shortName, using longName as a tiebreaker when shortName is ambiguous.
     * Returns the club if there is exactly one match; null if none or still ambiguous.
     */
    private Club resolveClub(String shortName, String longName)
    {
        if (shortName == null || shortName.isBlank())
            return null;
        return store.findUniqueClubByShortName(shortName, longName);
    }

    private LocalDate parseExpiry(String raw)
    {
        if (raw == null || raw.isBlank())
            return null;
        try
        {
            // Format: "2023-05-31T23:59:59.000" — take only the date part
            String datePart = raw.contains("T") ? raw.substring(0, raw.indexOf('T')) : raw.trim();
            return LocalDate.parse(datePart);
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse expiryDate: {}", raw);
            return null;
        }
    }

    /**
     * Derives a certificate year from the expiry date.
     * IRC certificates in Australian racing expire on 31 May; a cert expiring May 2023
     * was issued for the 2022-23 season, so the cert year is 2022.
     */
    private int certYear(LocalDate expiry)
    {
        if (expiry == null)
            return LocalDate.now().getYear();
        return expiry.getMonthValue() <= 6 ? expiry.getYear() - 1 : expiry.getYear();
    }

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }

    private static boolean isGenericBoatClass(String name)
    {
        return "Keelboat".equalsIgnoreCase(name)
            || "Trailable".equalsIgnoreCase(name)
            || "Hong Kong - Big Boats".equalsIgnoreCase(name);
    }

    // --- Jackson DTOs (package-private for testing) ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class SailSysResponse
    {
        public String result;
        public String errorMessage;
        public BoatData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BoatData
    {
        public Integer id;
        public String name;
        public String sailNumber;
        public String clubShortName;
        public String clubLongName;
        public String make;
        public String model;
        public BoatClass boatClass;
        public List<HandicapEntry> handicaps;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class BoatClass
    {
        public String name;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class HandicapEntry
    {
        public HandicapDefinition definition;
        public double value;
        public int spinnakerType;
        public CertData certificate;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class HandicapDefinition
    {
        public String shortName;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    static class CertData
    {
        public String certificateNumber;
        public String expiryDate;
    }
}
