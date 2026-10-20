package org.mortbay.sailing.pf.importer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Fetches the ORC Australian certificate feed and persists Boat, Design and
 * Certificate records via DataStore.
 * <p>
 * Feed URL: <a href="https://data.orc.org/public/WPub.dll?action=activecerts&amp;CountryId=AUS">active certs feed</a>
 * Individual cert: <a href="https://data.orc.org/public/WPub.dll/CC/">WPub.dll/CC/{dxtID}</a>
 * <p>
 * GPH is parsed from the certificate page and converted to TCF (600/GPH) before storing.
 * The certificateNumber field stores the ORC dxtID for idempotency checking.
 */
public class OrcImporter
{

    private static final Logger LOG = LoggerFactory.getLogger(OrcImporter.class);

    private static final String LIST_URL =
        "https://data.orc.org/public/WPub.dll?action=activecerts&CountryId=AUS";
    private static final String CERT_URL_PREFIX =
        "https://data.orc.org/public/WPub.dll/CC/";

    // Matches the GPH table cell: <td ... filecode="GPH">521.7</td>
    // This attribute is present on all ORC cert page variants (club, NS, international, DH).
    private static final java.util.regex.Pattern GPH_PATTERN =
        java.util.regex.Pattern.compile(
            "filecode=\"GPH\">([0-9]{3,4}(?:\\.[0-9]+)?)</td>",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    private final DataStore store;
    private final HttpClient client;

    public OrcImporter(DataStore store, HttpClient client)
    {
        this.store = store;
        this.client = client;
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
            Path cacheDir = dataRoot.resolve("cache/orc");
            new OrcImporter(dataStore, client).run(cacheDir, 1);
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    // --- Run ---

    /**
     * Fetches the ORC Australian certificate list and imports all new certificates.
     *
     * @param cacheDir       directory for caching list XML and cert HTML pages, or null to disable caching
     * @param listMaxAgeDays re-fetch the list if the cached copy is older than this many days (ignored if cacheDir is null)
     */
    public void run(Path cacheDir, int listMaxAgeDays) throws Exception
    {
        String listXml = fetchList(cacheDir, listMaxAgeDays);
        Document listDoc = parseXml(listXml);

        NodeList certNodes = listDoc.getElementsByTagName("ROW");
        if (certNodes.getLength() == 0)
            ImporterLog.warn(LOG, "No ROW nodes found -- check XML for correct tag name");
        LOG.info("Processing {} certificate entries", certNodes.getLength());

        Path certsDir = cacheDir != null ? cacheDir.resolve("certs") : null;
        Set<String> imported = OrcCertRow.importedDxtIds(store);

        for (int i = 0; i < certNodes.getLength(); i++)
        {
            Element el = (Element)certNodes.item(i);

            String dxtId = child(el, "dxtID");
            LOG.info("Process {} {}", i, dxtId);
            if (dxtId == null)
                dxtId = getDxtID(el);
            if (dxtId == null || dxtId.isBlank())
            {
                ImporterLog.warn(LOG,"Skipping cert with missing dxtID");
                continue;
            }
            if (imported.contains(dxtId))
            {
                LOG.debug("Skipping already-imported dxtID={}", dxtId);
                continue;
            }

            String certHtml;
            try
            {
                certHtml = fetchCert(dxtId, certsDir);
            }
            catch (Exception e)
            {
                ImporterLog.warn(LOG, "Skipping cert dxtID={}: failed to fetch cert page -- {}", dxtId, e.getMessage());
                continue;
            }

            processCertElement(el, certHtml);
        }

        store.save();
        LOG.info("Done.");
    }

    /**
     * Fetches (or loads from cache) the ORC active-certs list XML.
     * Cached at {@code cacheDir/list-AUS.xml}; re-fetched if the cached copy is older than
     * {@code listMaxAgeDays} days, or if no cache dir is provided.
     */
    private String fetchList(Path cacheDir, int listMaxAgeDays) throws Exception
    {
        if (cacheDir != null)
        {
            Files.createDirectories(cacheDir);
            Path listFile = cacheDir.resolve("list-AUS.xml");
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
                LOG.info("No cached ORC list found -- fetching from {}", LIST_URL);
            }
            String xml = fetch(LIST_URL);
            Files.writeString(listFile, xml, StandardCharsets.UTF_8);
            return xml;
        }
        LOG.info("Fetching ORC certificate list from {} (no cache)", LIST_URL);
        return fetch(LIST_URL);
    }

    /**
     * Fetches (or loads from cache) the HTML page for one ORC certificate.
     * Cached at {@code certsDir/{dxtId}.html}; individual cert pages are immutable
     * once issued so cached copies are used indefinitely.
     */
    private String fetchCert(String dxtId, Path certsDir) throws Exception
    {
        if (certsDir != null)
        {
            Files.createDirectories(certsDir);
            Path certFile = certsDir.resolve(dxtId + ".html");
            if (Files.exists(certFile))
            {
                LOG.debug("Using cached cert page for dxtID={}", dxtId);
                return Files.readString(certFile, StandardCharsets.UTF_8);
            }
        }
        Thread.sleep(100); // be gentle on the server
        String html = fetch(CERT_URL_PREFIX + dxtId);
        if (certsDir != null)
            Files.writeString(certsDir.resolve(dxtId + ".html"), html, StandardCharsets.UTF_8);
        return html;
    }

    private String fetch(String url) throws Exception
    {
        ContentResponse response = client.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // --- Parse layer (no HTTP, package-private for testing) ---

    /**
     * Process one ROW element using the already-fetched cert HTML page.
     * Parses GPH, then creates or updates Design, Boat and Certificate in the store
     * (see {@link OrcCertRow#importInto}).
     */
    void processCertElement(Element el, String certHtml)
    {
        String dxtId = child(el, "dxtID");
        if (dxtId == null)
            dxtId = getDxtID(el);
        if (dxtId == null || dxtId.isBlank())
            return;
        OrcCertRow row = new OrcCertRow(dxtId,
            child(el, "YachtName"), child(el, "SailNo"), child(el, "VPPYear"),
            child(el, "CertType"), child(el, "Expiry"), child(el, "Class"), child(el, "FamilyName"));
        row.importInto(store, parseGph(certHtml, dxtId));
    }

    /**
     * Parse GPH from an ORC cert page (HTML). Returns NaN if not found.
     */
    double parseGph(String html, String dxtId)
    {
        java.util.regex.Matcher m = GPH_PATTERN.matcher(html);
        if (m.find())
        {
            try
            {
                return Double.parseDouble(m.group(1));
            }
            catch (NumberFormatException e)
            {
                ImporterLog.warn(LOG,"Cannot parse GPH value for dxtID={}: {}", dxtId, m.group(1));
            }
        }
        return Double.NaN;
    }

    private Document parseXml(String xml) throws Exception
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        return factory.newDocumentBuilder()
            .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private String child(Element parent, String tag)
    {
        NodeList nodes = parent.getElementsByTagName(tag);
        if (nodes.getLength() == 0)
            return null;
        String text = nodes.item(0).getTextContent();
        return text == null || text.isBlank() ? null : text.trim();
    }

    private String getDxtID(Element el)
    {
        String val = el.getAttribute("dxtID");
        return val.isBlank() ? null : val.trim();
    }
}
