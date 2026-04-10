package org.mortbay.sailing.hpf.importer;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the AMS (Australian Measurement System) rating list from TopYacht and
 * persists Boat and Certificate records via DataStore.
 * <p>
 * Source: <a href="https://topyacht.com.au/ams/ams_list.php">AMS Ratings</a>
 * <p>
 * Three Certificate records are created per AMS cert number: spinnaker, non-spinnaker,
 * and two-handed (where values are present and parseable).
 */
public class AmsImporter
{

    private static final Logger LOG = LoggerFactory.getLogger(AmsImporter.class);

    static final String SOURCE = "AMS";

    private static final String LIST_URL = "https://topyacht.com.au/ams/ams_list.php";

    // Matches <tr ...>…</tr> — captures the row content (header rows are filtered out
    // downstream when certDate or rating values fail to parse)
    private static final Pattern ROW_PATTERN =
        Pattern.compile("<tr[^>]*>(.*?)</tr>",
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    // Matches <td ...>content</td> — captures cell content (may contain nested tags)
    private static final Pattern TD_PATTERN =
        Pattern.compile("<td[^>]*>(.*?)</td>",
                        Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final DataStore store;
    private final HttpClient client;

    public AmsImporter(DataStore store, HttpClient client)
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
            new AmsImporter(dataStore, client).run();
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    public void run() throws Exception
    {
        LOG.info("Fetching AMS rating list from {}", LIST_URL);
        String html = fetch(LIST_URL);
        parseAndImport(html);
        store.save();
        LOG.info("Done.");
    }

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }

    private String fetch(String url) throws Exception
    {
        ContentResponse response = client.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // --- Parse layer (no HTTP, package-private for testing) ---

    void parseAndImport(String html)
    {
        Matcher rows = ROW_PATTERN.matcher(html);
        int count = 0;
        while (rows.find())
        {
            List<String> cols = extractCols(rows.group(1));
            if (cols.size() < 7)
                continue;
            LOG.info("Process row: {}", cols);
            String club = cols.size() > 7 ? cols.get(7) : "";
            String state = cols.size() > 8 ? cols.get(8) : "";
            processRow(cols.get(0), cols.get(1), cols.get(2),
                       cols.get(3), cols.get(4), cols.get(5), cols.get(6),
                       club, state);
            count++;
        }
        LOG.info("Processed {} data rows", count);
    }

    void processRow(String boatName, String sailNo, String certNo,
                    String certDate, String spin, String noSpin, String twoHnd,
                    String club, String state)
    {
        if (boatName.isBlank() || sailNo.isBlank())
        {
            LOG.warn("Skipping row: missing boat name or sail number");
            return;
        }
        if (certNo.isBlank())
        {
            LOG.warn("Skipping row boat={}: missing cert number", boatName);
            return;
        }

        int year;
        try
        {
            year = LocalDate.parse(certDate.trim()).getYear();
        }
        catch (Exception e)
        {
            LOG.debug("Skipping row boat={}: cannot parse certDate={}", boatName, certDate);
            return;
        }

        if (club.isBlank())
            LOG.error("Missing club for boat={}", boatName);
        else if (store.findClubByShortName(club, state, "AMS boat=" + boatName) == null)
            LOG.error("Unknown club shortName={} state={} for boat={}", club, state, boatName);

        Boat boat = store.findOrCreateBoat(sailNo.trim(), boatName.trim(), null, SOURCE);

        // Upsert: remove all existing AMS certs whose certNo starts with this base number
        // (covers the bare certNo plus the -ns and -2h suffixed variants)
        final String trimmedCertNo = certNo.trim();
        List<Certificate> updatedCerts = new ArrayList<>(boat.certificates());
        updatedCerts.removeIf(c -> "AMS".equals(c.system())
                                   && c.year() == year
                                   && c.certificateNumber() != null
                                   && (c.certificateNumber().equals(trimmedCertNo)
                                       || c.certificateNumber().startsWith(trimmedCertNo + "-")));

        // Add up to 3 certs (skip any whose value is blank or unparseable)
        Double spinVal   = parseRating(spin,   boatName, "spinnaker");
        Double noSpinVal = parseRating(noSpin, boatName, "non-spinnaker");
        Double twoHndVal = parseRating(twoHnd, boatName, "two-handed");

        if (spinVal != null)
            updatedCerts.add(makeCert(year, spinVal, false, false, trimmedCertNo));
        if (noSpinVal != null)
            updatedCerts.add(makeCert(year, noSpinVal, true, false, trimmedCertNo + "-ns"));
        if (twoHndVal != null)
            updatedCerts.add(makeCert(year, twoHndVal, false, true, trimmedCertNo + "-2h"));

        updatedCerts.sort(Comparator.comparingInt(Certificate::year).reversed());

        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                boat.designId(), boat.clubId(), boat.aliases(), boat.altSailNumbers(),
                List.copyOf(updatedCerts), addSource(boat.sources(), SOURCE), Instant.now(), null));
    }

    private Double parseRating(String value, String boatName, String kind)
    {
        if (value == null || value.isBlank())
            return null;
        try
        {
            return Double.parseDouble(value.trim());
        }
        catch (NumberFormatException e)
        {
            LOG.debug("Skipping {} cert for boat={}: cannot parse value={}", kind, boatName, value);
            return null;
        }
    }

    private List<String> extractCols(String rowHtml)
    {
        List<String> cols = new ArrayList<>();
        Matcher td = TD_PATTERN.matcher(rowHtml);
        while (td.find())
            cols.add(stripTags(td.group(1)));
        return cols;
    }

    private static String stripTags(String html)
    {
        return html.replaceAll("<[^>]+>", "").trim();
    }

    private Certificate makeCert(int year, double value,
                                  boolean nonSpinnaker, boolean twoHanded, String certNo)
    {
        return new Certificate("AMS", year, value, nonSpinnaker, twoHanded, false, false, certNo, null);
    }
}
