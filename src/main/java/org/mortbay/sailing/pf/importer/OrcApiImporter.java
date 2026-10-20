package org.mortbay.sailing.pf.importer;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the ORC Australian certificate list from the ORC API (sas.orc.org) and persists
 * Boat, Design and Certificate records via DataStore. Replaces {@link OrcImporter}, whose
 * WPub.dll feed stops serving on 31 January 2027.
 * <p>
 * List: {@code GET /api/{key}/activecerts?CountryId=AUS} (JSON, rows in {@code data.rows}).
 * Per cert: {@code GET /api/{key}/DownBoatRMS_byrefno?RefNo={RefNo}&ext=json} (GPH in {@code rms[0].GPH}).
 * See <a href="https://sas.orc.org/api-docs.php">ORC API docs</a>.
 * <p>
 * The API key is part of the URL path, so request URLs are never logged — see {@link #redact}.
 * It is read from the {@code ORC_API_KEY} environment variable, else from
 * {@code config/orc-api-key} (gitignored) — see {@link #loadApiKey}.
 * <p>
 * Certificates are keyed by dxtID exactly as {@link OrcImporter} does, so the two importers
 * recognise each other's certificates and never duplicate them.
 */
public class OrcApiImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(OrcApiImporter.class);

    public static final String ENV_API_KEY = "ORC_API_KEY";
    public static final String API_KEY_FILE = "orc-api-key";

    private static final String API_BASE = "https://sas.orc.org/api/";
    private static final String LIST_PATH = "activecerts?CountryId=AUS";
    private static final String CERT_PATH_PREFIX = "DownBoatRMS_byrefno?ext=json&RefNo=";

    // Keys are "orc_" followed by 64 hex digits
    private static final Pattern KEY_PATTERN = Pattern.compile("orc_[0-9a-fA-F]{64}");

    private static final Pattern REFNO_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataStore store;
    private final HttpClient client;
    private final String apiKey;

    public OrcApiImporter(DataStore store, HttpClient client, String apiKey)
    {
        this.store = store;
        this.client = client;
        this.apiKey = apiKey;
    }

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        String apiKey = loadApiKey(dataRoot.resolve("config"));
        DataStore dataStore = new DataStore(dataRoot);
        dataStore.start();

        HttpClient client = new HttpClient();
        client.start();
        try
        {
            Path cacheDir = dataRoot.resolve("cache/orc/api");
            new OrcApiImporter(dataStore, client, apiKey).run(cacheDir, 1);
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    /** Whether an API key is configured, without reading or validating it. See {@link #loadApiKey}. */
    public static boolean hasApiKey(Path configDir)
    {
        String key = System.getenv(ENV_API_KEY);
        if (key != null && !key.isBlank())
            return true;
        Path file = configDir.resolve(API_KEY_FILE);
        try
        {
            return Files.isRegularFile(file) && Files.size(file) > 0;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    /**
     * Returns the ORC API key from the {@code ORC_API_KEY} environment variable, else from
     * {@code configDir/orc-api-key}. Throws if neither is set.
     */
    public static String loadApiKey(Path configDir) throws IOException
    {
        String key = System.getenv(ENV_API_KEY);
        Path file = configDir.resolve(API_KEY_FILE);
        if ((key == null || key.isBlank()) && Files.isRegularFile(file))
            key = Files.readString(file, StandardCharsets.UTF_8);
        if (key == null || key.isBlank())
            throw new IllegalStateException("No ORC API key: set " + ENV_API_KEY + " or put the key in " + file);
        key = key.trim();
        if (!KEY_PATTERN.matcher(key).matches())
            LOG.warn("ORC API key does not look like orc_ followed by 64 hex digits -- using it anyway");
        return key;
    }

    // --- Run ---

    /**
     * Fetches the ORC Australian active-certificate list and imports all new certificates.
     *
     * @param cacheDir       directory for caching the list and cert JSON, or null to disable caching
     * @param listMaxAgeDays re-fetch the list if the cached copy is older than this many days (ignored if cacheDir is null)
     */
    public void run(Path cacheDir, int listMaxAgeDays) throws Exception
    {
        List<JsonNode> rows = parseRows(fetchList(cacheDir, listMaxAgeDays));
        if (rows.isEmpty())
            ImporterLog.warn(LOG, "No rows in ORC activecerts response -- check data.rows");
        LOG.info("Processing {} certificate entries", rows.size());

        Path certsDir = cacheDir != null ? cacheDir.resolve("certs") : null;
        Set<String> imported = OrcCertRow.importedDxtIds(store);

        for (int i = 0; i < rows.size(); i++)
        {
            JsonNode row = rows.get(i);
            String dxtId = text(row, "dxtID");
            String refNo = text(row, "RefNo");
            LOG.info("Process {} {} {}", i, dxtId, refNo);
            if (dxtId == null)
            {
                ImporterLog.warn(LOG, "Skipping cert with missing dxtID (RefNo={})", refNo);
                continue;
            }
            if (imported.contains(dxtId))
            {
                LOG.debug("Skipping already-imported dxtID={}", dxtId);
                continue;
            }
            // RefNo names the cache file, so only plain identifiers are accepted
            if (refNo == null || !REFNO_PATTERN.matcher(refNo).matches())
            {
                ImporterLog.warn(LOG, "Skipping cert dxtID={}: missing or invalid RefNo={}", dxtId, refNo);
                continue;
            }

            String certJson;
            try
            {
                certJson = fetchCert(refNo, certsDir);
            }
            catch (Exception e)
            {
                ImporterLog.warn(LOG, "Skipping cert dxtID={} RefNo={}: failed to fetch cert data -- {}",
                    dxtId, refNo, e.getMessage());
                continue;
            }

            processRow(row, certJson);
        }

        store.save();
        LOG.info("Done.");
    }

    /**
     * Fetches (or loads from cache) the activecerts list JSON.
     * Cached at {@code cacheDir/list-AUS.json}; re-fetched if the cached copy is older than
     * {@code listMaxAgeDays} days, or if no cache dir is provided.
     */
    private String fetchList(Path cacheDir, int listMaxAgeDays) throws Exception
    {
        if (cacheDir == null)
        {
            LOG.info("Fetching ORC certificate list {} (no cache)", LIST_PATH);
            return fetch(LIST_PATH);
        }
        Files.createDirectories(cacheDir);
        Path listFile = cacheDir.resolve("list-AUS.json");
        if (Files.exists(listFile))
        {
            long agedays = ChronoUnit.DAYS.between(
                Files.getLastModifiedTime(listFile).toInstant(), Instant.now());
            if (agedays < listMaxAgeDays)
            {
                LOG.info("Using cached ORC list (age={}d, max={}d)", agedays, listMaxAgeDays);
                return Files.readString(listFile, StandardCharsets.UTF_8);
            }
            LOG.info("ORC list cache is stale (age={}d >= max={}d) -- re-fetching", agedays, listMaxAgeDays);
        }
        else
        {
            LOG.info("No cached ORC list found -- fetching {}", LIST_PATH);
        }
        String json = fetch(LIST_PATH);
        Files.writeString(listFile, json, StandardCharsets.UTF_8);
        return json;
    }

    /**
     * Fetches (or loads from cache) the rating data for one certificate.
     * Cached at {@code certsDir/{RefNo}.json}; issued certificates do not change,
     * so cached copies are used indefinitely.
     */
    private String fetchCert(String refNo, Path certsDir) throws Exception
    {
        Path certFile = certsDir != null ? certsDir.resolve(refNo + ".json") : null;
        if (certFile != null && Files.exists(certFile))
        {
            LOG.debug("Using cached cert data for RefNo={}", refNo);
            return Files.readString(certFile, StandardCharsets.UTF_8);
        }
        Thread.sleep(100); // be gentle on the server
        String json = fetch(CERT_PATH_PREFIX + URLEncoder.encode(refNo, StandardCharsets.UTF_8));
        if (certFile != null)
        {
            Files.createDirectories(certsDir);
            Files.writeString(certFile, json, StandardCharsets.UTF_8);
        }
        return json;
    }

    /**
     * GETs an API path (relative to {@code /api/{key}/}). Only 200 responses without an
     * {@code errors} array are returned, so error bodies are never cached.
     */
    private String fetch(String path) throws Exception
    {
        ContentResponse response;
        try
        {
            response = client.GET(API_BASE + apiKey + "/" + path);
        }
        catch (Exception e)
        {
            // The message may contain the URL, and so the key: rethrow without it
            String message = e.getMessage() == null ? "" : ": " + e.getMessage().replace(apiKey, "{key}");
            throw new IOException(e.getClass().getSimpleName() + " for " + redact(path) + message);
        }
        String body = response.getContentAsString();
        String errors = errors(body);
        if (response.getStatus() != 200 || errors != null)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + redact(path)
                + (errors != null ? ": " + errors : ""));
        return body;
    }

    /** A loggable description of an API path: the key is never included. */
    private static String redact(String path)
    {
        return API_BASE + "{key}/" + path;
    }

    // --- Parse layer (no HTTP, package-private for testing) ---

    /** The {@code errors} array of an API response joined into one string, or null if none. */
    static String errors(String json)
    {
        try
        {
            JsonNode errors = MAPPER.readTree(json).path("errors");
            if (!errors.isArray() || errors.isEmpty())
                return null;
            List<String> messages = new ArrayList<>();
            errors.forEach(e -> messages.add(e.asText()));
            return String.join("; ", messages);
        }
        catch (Exception e)
        {
            return null; // not JSON: let the caller's parse report it
        }
    }

    /** The rows of an activecerts response ({@code data.rows}). */
    List<JsonNode> parseRows(String json) throws IOException
    {
        List<JsonNode> rows = new ArrayList<>();
        MAPPER.readTree(json).path("data").path("rows").forEach(rows::add);
        return rows;
    }

    /**
     * GPH from a DownBoatRMS_byrefno JSON response ({@code rms[0].GPH}). Returns NaN if not found.
     */
    double parseGph(String json, String refNo)
    {
        try
        {
            JsonNode gph = MAPPER.readTree(json).path("rms").path(0).path("GPH");
            if (gph.isNumber())
                return gph.asDouble();
            if (gph.isTextual())
                return Double.parseDouble(gph.asText().trim());
        }
        catch (Exception e)
        {
            ImporterLog.warn(LOG, "Cannot parse GPH for RefNo={}: {}", refNo, e.getMessage());
        }
        return Double.NaN;
    }

    /**
     * Process one activecerts row using the already-fetched cert JSON.
     * See {@link OrcCertRow#importInto}.
     */
    void processRow(JsonNode row, String certJson)
    {
        String dxtId = text(row, "dxtID");
        if (dxtId == null)
            return;
        OrcCertRow cert = new OrcCertRow(dxtId,
            text(row, "YachtName"), text(row, "SailNo"), text(row, "VPPYear"),
            text(row, "CertType"), text(row, "Expiry"), text(row, "Class"), text(row, "FamilyName"));
        cert.importInto(store, parseGph(certJson, text(row, "RefNo")));
    }

    /** A field as trimmed text (numbers included), or null when absent, null or blank. */
    private static String text(JsonNode node, String field)
    {
        JsonNode v = node.get(field);
        if (v == null || v.isNull())
            return null;
        String s = v.asText();
        return s.isBlank() ? null : s.trim();
    }
}
