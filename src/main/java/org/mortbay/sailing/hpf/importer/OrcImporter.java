package org.mortbay.sailing.hpf.importer;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.store.DataStore;
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

    // CertType values indicating non-spinnaker
    private static final Set<String> NON_SPIN_CERT_TYPES = Set.of("10", "11");

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
            new OrcImporter(dataStore, client).run();
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    // --- Fetch layer (HTTP) ---

    public void run() throws Exception
    {
        LOG.info("Fetching ORC certificate list from {}", LIST_URL);
        String listXml = fetch(LIST_URL);
        Document listDoc = parseXml(listXml);

        NodeList certNodes = listDoc.getElementsByTagName("ROW");
        if (certNodes.getLength() == 0)
            LOG.warn("No ROW nodes found — check XML for correct tag name");
        LOG.info("Processing {} certificate entries", certNodes.getLength());

        for (int i = 0; i < certNodes.getLength(); i++)
        {
            Element el = (Element)certNodes.item(i);

            String dxtId = child(el, "dxtID");
            LOG.info("Process {} {}", i, dxtId);
            if (dxtId == null)
                dxtId = getDxtID(el);
            if (dxtId == null || dxtId.isBlank())
            {
                LOG.warn("Skipping cert with missing dxtID");
                continue;
            }
            final String finalDxtId = dxtId;
            if (store.boats().values().stream()
                .flatMap(b -> b.certificates().stream())
                .anyMatch(c -> finalDxtId.equals(c.certificateNumber())))
            {
                LOG.debug("Skipping already-imported dxtID={}", dxtId);
                continue;
            }

            String certHtml;
            try
            {
                Thread.sleep(100); // be gentle on the server
                certHtml = fetch(CERT_URL_PREFIX + dxtId);
            }
            catch (Exception e)
            {
                LOG.warn("Skipping cert dxtID={}: failed to fetch cert page — {}", dxtId, e.getMessage());
                continue;
            }

            processCertElement(el, certHtml);
        }

        store.save();
        LOG.info("Done.");
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
     * Validates required fields, parses GPH, then creates or updates Design, Boat and Certificate
     * in the store.
     */
    void processCertElement(Element el, String certHtml)
    {
        String dxtId = child(el, "dxtID");
        if (dxtId == null)
            dxtId = getDxtID(el);
        if (dxtId == null || dxtId.isBlank())
            return;
        String yachtName = child(el, "YachtName");
        String sailNo = child(el, "SailNo");
        String vppYearStr = child(el, "VPPYear");
        String certType = child(el, "CertType");
        String expiryStr = child(el, "Expiry");
        String className = child(el, "Class");
        String familyName = child(el, "FamilyName");
        LOG.info("Process {} {} {} {} {} {} {} {}", dxtId, yachtName, sailNo, vppYearStr, certType, expiryStr, className, familyName);

        if (yachtName == null || sailNo == null || vppYearStr == null)
        {
            LOG.warn("Skipping cert dxtID={}: missing required fields", dxtId);
            return;
        }

        int year;
        try
        {
            year = Integer.parseInt(vppYearStr.trim());
        }
        catch (NumberFormatException e)
        {
            LOG.warn("Skipping cert dxtID={}: invalid VPPYear={}", dxtId, vppYearStr);
            return;
        }

        double gph = parseGph(certHtml, dxtId);
        if (Double.isNaN(gph))
        {
            LOG.warn("Skipping cert dxtID={}: GPH not found in cert HTML", dxtId);
            return;
        }
        if (gph < 400 || gph > 900)
        {
            LOG.warn("Skipping cert dxtID={}: implausible GPH={} (expected 400-900, regex mismatch?)", dxtId, gph);
            return;
        }
        double tcf = 600.0 / gph;

        boolean nonSpinnaker = (familyName != null && familyName.contains("Non Spinnaker"))
            || NON_SPIN_CERT_TYPES.contains(certType == null ? "" : certType.trim());

        LocalDate expiry = null;
        if (expiryStr != null && !expiryStr.isBlank())
        {
            try
            {
                expiry = LocalDate.parse(expiryStr.trim());
            }
            catch (Exception e)
            {
                LOG.debug("Cannot parse expiry date for dxtID={}: {}", dxtId, expiryStr);
            }
        }

        // Find-or-create Design
        Design design = store.findOrCreateDesign(className);

        // Find-or-create Boat
        Boat boat = store.findOrCreateBoat(sailNo, yachtName.trim(), design);

        // Upsert certificate: remove old cert with same dxtId first, then add new one
        final String finalDxtId = dxtId;
        List<Certificate> updatedCerts = new ArrayList<>(boat.certificates());
        updatedCerts.removeIf(c -> finalDxtId.equals(c.certificateNumber()));

        // CertType 1 = IRC+ORC international, 2 = ORC international; all others are club
        boolean orcClub = certType == null || (!"1".equals(certType.trim()) && !"2".equals(certType.trim()));
        Certificate cert = new Certificate("ORC",
            year, tcf, nonSpinnaker, false, orcClub, dxtId, expiry);
        updatedCerts.add(cert);
        updatedCerts.sort(Comparator.comparingInt(Certificate::year).reversed());

        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(),
            List.copyOf(updatedCerts), null));
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
                LOG.warn("Cannot parse GPH value for dxtID={}: {}", dxtId, m.group(1));
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
