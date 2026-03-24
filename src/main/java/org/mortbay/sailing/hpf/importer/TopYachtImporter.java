package org.mortbay.sailing.hpf.importer;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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

import java.net.URI;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Imports race results from TopYacht club pages.
 * <p>
 * Walks: club index page → series pages → results pages → Race + Finisher records.
 * <p>
 * Entry points for each club are the {@code topyachtUrls} list on the {@link Club} record,
 * populated from {@code hpf-data/config/clubs.yaml}.
 * <p>
 * Per CLAUDE.md:
 * - AHC, corrected time and PHS handicap values are NOT stored
 * - TopYacht {@code boid} (boat register ID) is NOT stored
 * - Skipper names are NOT stored
 * - Elapsed times from PHS races ARE stored
 * - AHC values from non-PHS races ARE used to infer/verify boat certificates
 * <p>
 * A series race row may have multiple results links (e.g. IRC + ORC AP). When that
 * happens all pages are fetched and finishers merged by sail number into a single
 * unnamed Division. Duplicate sail numbers with matching elapsed times produce one
 * Finisher; mismatched elapsed times are logged as errors and discarded.
 * <p>
 * TODO: Two-handed detection — TopYacht may indicate two-handed races in the series name,
 * caption, or a column. No example found yet. A LOG.warn is emitted when the series name
 * contains "two", "2h", or "2-handed" as a breadcrumb for future investigation.
 */
public class TopYachtImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(TopYachtImporter.class);

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("d/M/yyyy");
    private static final Pattern RACE_LABEL = Pattern.compile(
        "Race\\s+(\\d+)\\s*-\\s*(\\d{1,2}/\\d{1,2}/\\d{4})", Pattern.CASE_INSENSITIVE);

    /**
     * Maps known handicap system name fragments (upper-cased, possibly multi-word)
     * to normalised system identifiers. Used during caption parsing to handle
     * names like "ORC AP" → "ORC".
     */
    private static final Map<String, String> KNOWN_SYSTEMS = Map.of(
        "IRC", "IRC",
        "ORC", "ORC", "ORC AP", "ORC", "ORC CLUB", "ORC",
        "ORC I", "ORC", "ORCI", "ORC",
        "AMS", "AMS",
        "PHS", "PHS");

    private final DataStore store;
    private final HttpClient httpClient;

    public TopYachtImporter(DataStore store, HttpClient httpClient)
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
            new TopYachtImporter(dataStore, client).run();
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }
    }

    // --- Entry point ---

    public void run() throws Exception
    {
        // topyachtUrls are configured in the seed (clubs.yaml); merge seed + persisted
        // so we don't miss clubs that haven't been imported yet.
        List<Club> allClubs = Stream.concat(
            store.clubs().values().stream(),
            store.clubSeed().values().stream().filter(c -> !store.clubs().containsKey(c.id()))
        ).toList();

        for (Club club : allClubs)
        {
            for (String indexUrl : club.topyachtUrls())
            {
                LOG.info("TopYacht: fetching index for club={} url={}", club.id(), indexUrl);
                try
                {
                    String html = fetch(indexUrl);
                    processIndexPage(club, indexUrl, html);
                }
                catch (Exception e)
                {
                    LOG.error("Failed to fetch index for club={} url={}: {}", club.id(), indexUrl, e.getMessage());
                }
            }
        }
    }

    // --- Page processors (package-private for testing) ---

    void processIndexPage(Club club, String indexUrl, String html)
    {
        List<SeriesLink> series = parseIndexPage(html, indexUrl);
        LOG.info("TopYacht: club={} found {} series on index page", club.id(), series.size());

        for (SeriesLink sl : series)
        {
            // TODO: log a warning if series name contains two-handed hints
            String lower = sl.name().toLowerCase();
            if (lower.contains("two") || lower.contains("2h") || lower.contains("2-handed"))
                LOG.warn("TopYacht: possible two-handed series '{}' — two-handed detection not yet implemented", sl.name());

            try
            {
                String html2 = fetch(sl.url());
                processSeriesPage(club, sl.name(), sl.url(), html2);
            }
            catch (Exception e)
            {
                LOG.error("Failed to fetch series url={}: {}", sl.url(), e.getMessage());
            }
        }
    }

    void processSeriesPage(Club club, String seriesName, String seriesUrl, String html)
    {
        List<RaceRow> races = parseSeriesPage(html, seriesUrl);
        LOG.info("TopYacht: club={} series='{}' found {} races", club.id(), seriesName, races.size());

        for (RaceRow row : races)
        {
            if (row.resultsUrls().size() == 1)
            {
                // Single results page: fetch and process directly (preserves multi-division pages)
                try
                {
                    String html2 = fetch(row.resultsUrls().get(0));
                    processResultsPage(club, seriesName, row.number(), row.date(), html2);
                }
                catch (Exception e)
                {
                    LOG.error("Failed to fetch results url={}: {}", row.resultsUrls().get(0), e.getMessage());
                }
            }
            else
            {
                // Multiple results pages (e.g. IRC + ORC): fetch all, merge finishers
                List<ParsedRace> parsedList = new ArrayList<>();
                for (String url : row.resultsUrls())
                {
                    try
                    {
                        ParsedRace pr = parseResultsPage(fetch(url));
                        if (pr != null)
                            parsedList.add(pr);
                    }
                    catch (Exception e)
                    {
                        LOG.error("Failed to fetch results url={}: {}", url, e.getMessage());
                    }
                }
                if (!parsedList.isEmpty())
                    processResultsPages(club, seriesName, row.number(), row.date(), parsedList);
            }
        }
    }

    /**
     * Processes a single results HTML page. Preserves multiple divisions when the page
     * contains multiple {@code centre_results_table} tables (e.g. CYCSA division pages).
     * Package-private for testing with inline HTML.
     */
    void processResultsPage(Club club, String seriesName, int raceNumber, LocalDate date, String html)
    {
        ParsedRace parsed = parseResultsPage(html);
        if (parsed == null)
            return;

        String seriesId = IdGenerator.generateSeriesId(club.id(), seriesName);
        String raceId = IdGenerator.generateRaceId(club.id(), date, raceNumber);

        if (store.races().containsKey(raceId))
        {
            LOG.debug("TopYacht: race {} already imported, updating series membership only", raceId);
            updateClubSeries(club.id(), seriesId, seriesName, raceId);
            return;
        }

        List<Division> divisions = new ArrayList<>();
        int totalFinishers = 0;
        String handicapSystem = parsed.divisions().get(0).handicapSystem();

        for (ParsedDivision parsedDiv : parsed.divisions())
        {
            List<Finisher> finishers = buildFinishers(parsedDiv.rows(), parsedDiv.handicapSystem(), date.getYear());
            divisions.add(new Division(parsedDiv.name(), List.copyOf(finishers)));
            totalFinishers += finishers.size();
        }

        store.putRace(new Race(raceId, club.id(), List.of(seriesId), date, raceNumber,
            null, handicapSystem, false, List.copyOf(divisions), null));
        LOG.info("TopYacht: imported race {} ({} finishers, {} division(s), system={})",
            raceId, totalFinishers, divisions.size(), handicapSystem);

        updateClubSeries(club.id(), seriesId, seriesName, raceId);
    }

    /**
     * Processes multiple results pages for the same race (e.g. IRC + ORC AP).
     * Merges all finishers from all pages into a single unnamed Division, deduplicating
     * by sail number. Infers certificates from AHC column values for non-PHS systems.
     * Package-private for testing.
     */
    void processResultsPages(Club club, String seriesName, int raceNumber,
                              LocalDate date, List<ParsedRace> parsedList)
    {
        String seriesId = IdGenerator.generateSeriesId(club.id(), seriesName);
        String raceId = IdGenerator.generateRaceId(club.id(), date, raceNumber);

        if (store.races().containsKey(raceId))
        {
            LOG.debug("TopYacht: race {} already imported, updating series membership only", raceId);
            updateClubSeries(club.id(), seriesId, seriesName, raceId);
            return;
        }

        // Merge all rows from all pages, keyed by sail number.
        // The first occurrence establishes boatName/elapsed/clubCode/designName.
        // Subsequent occurrences from other pages add their {system, ahcValue} pair
        // if the elapsed time matches; mismatches are logged and discarded.
        LinkedHashMap<String, MergeEntry> merged = new LinkedHashMap<>();
        for (ParsedRace pr : parsedList)
        {
            for (ParsedDivision div : pr.divisions())
            {
                for (ParsedRow row : div.rows())
                {
                    MergeEntry existing = merged.get(row.sailNo());
                    if (existing == null)
                    {
                        MergeEntry entry = new MergeEntry(row.boatName(), row.elapsed(),
                            row.clubCode(), row.designName());
                        entry.sysAhcs.add(new SysAhc(div.handicapSystem(), row.ahcValue()));
                        merged.put(row.sailNo(), entry);
                    }
                    else
                    {
                        long diffSeconds = Math.abs(
                            existing.elapsed.minus(row.elapsed()).abs().toSeconds());
                        if (diffSeconds > 1)
                        {
                            LOG.error("TopYacht: sail {} '{}' has conflicting elapsed times " +
                                "{} vs {} in race {} — discarding duplicate",
                                row.sailNo(), row.boatName(),
                                existing.elapsed, row.elapsed(), raceId);
                        }
                        else
                        {
                            existing.sysAhcs.add(new SysAhc(div.handicapSystem(), row.ahcValue()));
                        }
                    }
                }
            }
        }

        // Derive race handicapSystem from the distinct systems seen across all pages
        LinkedHashSet<String> systems = new LinkedHashSet<>();
        for (ParsedRace pr : parsedList)
            for (ParsedDivision div : pr.divisions())
                systems.add(div.handicapSystem());
        String handicapSystem = String.join("/", systems);

        // Build finishers
        List<Finisher> finishers = new ArrayList<>();
        for (Map.Entry<String, MergeEntry> e : merged.entrySet())
        {
            String sailNo = e.getKey();
            MergeEntry me = e.getValue();

            Design design = me.designName != null
                ? store.findOrCreateDesign(me.designName) : null;
            Boat boat = store.findOrCreateBoat(sailNo, me.boatName, design);

            if (me.clubCode != null && !me.clubCode.isBlank() && boat.clubId() == null)
            {
                Club fromClub = store.findUniqueClubByShortName(me.clubCode, null);
                if (fromClub != null)
                {
                    store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                        boat.designId(), fromClub.id(), boat.aliases(), boat.certificates(), null));
                }
            }

            // Infer certificates from AHC values for non-PHS systems; use first cert number
            String certNumber = null;
            for (SysAhc sa : me.sysAhcs)
            {
                if (!"PHS".equals(sa.system()))
                {
                    // Re-read boat in case a previous iteration updated it
                    boat = store.boats().get(boat.id());
                    String cn = inferCertificate(boat, sa.system(), date.getYear(), sa.ahcValue());
                    if (certNumber == null)
                        certNumber = cn;
                }
            }

            finishers.add(new Finisher(boat.id(), me.elapsed, false, certNumber));
        }

        store.putRace(new Race(raceId, club.id(), List.of(seriesId), date, raceNumber,
            null, handicapSystem, false, List.of(new Division(null, List.copyOf(finishers))), null));
        LOG.info("TopYacht: imported race {} ({} finishers, system={})",
            raceId, finishers.size(), handicapSystem);

        updateClubSeries(club.id(), seriesId, seriesName, raceId);
    }

    // --- Parsers (package-private for testing) ---

    List<SeriesLink> parseIndexPage(String html, String baseUrl)
    {
        Document doc = Jsoup.parse(html, baseUrl);
        List<SeriesLink> result = new ArrayList<>();

        for (Element a : doc.select("a[href]"))
        {
            String href = a.attr("href");
            String hrefNoQuery = stripQuery(href);
            if (!hrefNoQuery.endsWith("series.htm"))
                continue;

            String name = a.text().trim();
            if (name.isBlank())
                continue;

            String resolved = resolveUrl(baseUrl, href);
            if (resolved != null)
                result.add(new SeriesLink(name, resolved));
        }
        return result;
    }

    List<RaceRow> parseSeriesPage(String html, String baseUrl)
    {
        Document doc = Jsoup.parse(html, baseUrl);
        List<RaceRow> result = new ArrayList<>();

        Elements rows = doc.select("table.centre_index_table tr");
        for (Element row : rows)
        {
            Elements cells = row.select("td");
            if (cells.isEmpty())
                continue;

            String label = cells.get(0).text().trim();

            // Skip Series Scores, headers, and other non-race rows
            String labelLower = label.toLowerCase();
            if (labelLower.contains("score") || labelLower.contains("series") || labelLower.contains("place"))
                continue;

            Matcher m = RACE_LABEL.matcher(label);
            if (!m.find())
                continue;

            int number;
            LocalDate date;
            try
            {
                number = Integer.parseInt(m.group(1));
                date = LocalDate.parse(m.group(2), DATE_FMT);
            }
            catch (NumberFormatException | DateTimeParseException e)
            {
                LOG.debug("TopYacht: could not parse race label '{}': {}", label, e.getMessage());
                continue;
            }

            // Collect ALL results links (not entrant or series/score links)
            List<String> resultsUrls = new ArrayList<>();
            for (Element a : row.select("a[href]"))
            {
                String href = a.attr("href").toLowerCase();
                if (!href.contains("entr") && !href.contains("series") && !href.contains("score"))
                {
                    String resolved = resolveUrl(baseUrl, a.attr("href"));
                    if (resolved != null)
                        resultsUrls.add(resolved);
                }
            }

            if (!resultsUrls.isEmpty())
                result.add(new RaceRow(number, date, resultsUrls));
        }
        return result;
    }

    ParsedRace parseResultsPage(String html)
    {
        Document doc = Jsoup.parse(html);
        Elements tables = doc.select("table.centre_results_table");
        if (tables.isEmpty())
        {
            LOG.warn("TopYacht: no centre_results_table found in results page");
            return null;
        }

        List<ParsedDivision> divisions = new ArrayList<>();

        for (Element table : tables)
        {
            // Extract division name and handicap system from caption.
            // Caption format: "[DivisionName] HandicapSystem results [rest]"
            // e.g. "PHS results  Start : 12:25"
            //      "Cruising A PHS results Start : 12:25"
            //      "ORC AP results Start : 10:00"
            Element caption = table.selectFirst("caption");
            String handicapSystem = "UNKNOWN";
            String divisionName = null;
            if (caption != null)
            {
                String captionText = caption.text().trim();
                String[] words = captionText.split("\\s+");

                // Find the index of "results" keyword
                int resultsIdx = -1;
                for (int i = 0; i < words.length; i++)
                {
                    if (words[i].equalsIgnoreCase("results"))
                    {
                        resultsIdx = i;
                        break;
                    }
                }

                if (resultsIdx > 0)
                {
                    // Try 2-word suffix then 1-word suffix against KNOWN_SYSTEMS.
                    // This handles "ORC AP results" → system="ORC" as well as "PHS results".
                    List<String> before = List.of(words).subList(0, resultsIdx);
                    String matched = null;
                    for (int len = Math.min(2, before.size()); len >= 1; len--)
                    {
                        String candidate = String.join(" ",
                            before.subList(before.size() - len, before.size())).toUpperCase();
                        if (KNOWN_SYSTEMS.containsKey(candidate))
                        {
                            matched = KNOWN_SYSTEMS.get(candidate);
                            if (before.size() > len)
                                divisionName = String.join(" ", before.subList(0, before.size() - len));
                            break;
                        }
                    }
                    handicapSystem = matched != null ? matched : before.get(before.size() - 1).toUpperCase();
                }
                else
                {
                    // "results" not found or is first word — fall back to first word
                    handicapSystem = words.length > 0 ? words[0].toUpperCase() : "UNKNOWN";
                }
            }

            // Find header row (type1) and detect column indices
            Element headerRow = table.selectFirst("tr.type1");
            if (headerRow == null)
            {
                LOG.warn("TopYacht: no header row (type1) found in results table; skipping division");
                continue;
            }

            int sailIdx = -1, nameIdx = -1, elapsedIdx = -1, clubIdx = -1, designIdx = -1, ahcIdx = -1;
            Elements headers = headerRow.select("td");
            for (int i = 0; i < headers.size(); i++)
            {
                String h = headers.get(i).text().trim().toLowerCase();
                if (h.contains("sail"))
                    sailIdx = i;
                else if ((h.contains("boat") || h.contains("name")) && nameIdx < 0)
                    nameIdx = i;
                else if (h.contains("elapsd") || h.contains("elapsed"))
                    elapsedIdx = i;
                else if (h.contains("from") || h.equals("club"))
                    clubIdx = i;
                else if (h.contains("class") || h.contains("design") || h.equals("type"))
                    designIdx = i;
                else if (h.equals("ahc"))
                    ahcIdx = i;
            }

            if (sailIdx < 0 || nameIdx < 0 || elapsedIdx < 0)
            {
                LOG.warn("TopYacht: required columns not found (sail={}, name={}, elapsed={}); skipping division",
                    sailIdx, nameIdx, elapsedIdx);
                continue;
            }

            // Parse data rows
            List<ParsedRow> rows = new ArrayList<>();
            for (Element row : table.select("tr.type3, tr.type4"))
            {
                Elements cells = row.select("td");
                if (cells.size() <= Math.max(sailIdx, Math.max(nameIdx, elapsedIdx)))
                    continue;

                String sailNo = cells.get(sailIdx).text().trim();
                String boatName = cells.get(nameIdx).text().trim();  // Jsoup strips <a> and gives text
                String elapsedText = cells.get(elapsedIdx).text().trim();

                if (sailNo.isBlank() || boatName.isBlank())
                    continue;

                Duration elapsed = parseElapsed(elapsedText);
                if (elapsed == null)
                    continue;  // DNF, DNS, RET, or unparseable — skip this finisher

                String clubCode = (clubIdx >= 0 && clubIdx < cells.size())
                    ? cells.get(clubIdx).text().trim() : null;
                String designName = (designIdx >= 0 && designIdx < cells.size())
                    ? cells.get(designIdx).text().trim() : null;
                String ahcValue = (ahcIdx >= 0 && ahcIdx < cells.size())
                    ? cells.get(ahcIdx).text().trim() : null;

                if (clubCode != null && clubCode.isBlank())   clubCode = null;
                if (designName != null && designName.isBlank()) designName = null;
                if (ahcValue != null && ahcValue.isBlank())   ahcValue = null;

                rows.add(new ParsedRow(sailNo, boatName, elapsed, clubCode, designName, ahcValue));
            }

            divisions.add(new ParsedDivision(divisionName, handicapSystem, rows));
        }

        return divisions.isEmpty() ? null : new ParsedRace(divisions);
    }

    // --- Internal helpers ---

    /**
     * Builds a list of Finisher records from parsed rows, inferring certificates
     * from AHC values for non-PHS systems.
     */
    private List<Finisher> buildFinishers(List<ParsedRow> rows, String handicapSystem, int year)
    {
        List<Finisher> finishers = new ArrayList<>();
        for (ParsedRow row : rows)
        {
            Design design = row.designName() != null
                ? store.findOrCreateDesign(row.designName()) : null;
            Boat boat = store.findOrCreateBoat(row.sailNo(), row.boatName(), design);

            if (row.clubCode() != null && !row.clubCode().isBlank() && boat.clubId() == null)
            {
                Club fromClub = store.findUniqueClubByShortName(row.clubCode(), null);
                if (fromClub != null)
                {
                    store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                        boat.designId(), fromClub.id(), boat.aliases(), boat.certificates(), null));
                }
            }

            String certNumber = null;
            if (isMeasurementHandicapSystem(handicapSystem) && row.ahcValue() != null)
            {
                boat = store.boats().get(boat.id());
                certNumber = inferCertificate(boat, handicapSystem, year, row.ahcValue());
            }

            finishers.add(new Finisher(boat.id(), row.elapsed(), false, certNumber));
        }
        return finishers;
    }

    /**
     * Checks if the boat already has a certificate matching the given system, year, and
     * TCF value. If not, creates an inferred certificate and adds it to the boat.
     *
     * @return the {@code certificateNumber} of the matching or newly created certificate,
     *         or {@code null} if {@code ahcValue} cannot be parsed as a number.
     */
    private String inferCertificate(Boat boat, String system, int year, String ahcValue)
    {
        if (ahcValue == null)
            return null;
        double tcf;
        try
        {
            tcf = Double.parseDouble(ahcValue);
        }
        catch (NumberFormatException e)
        {
            return null;
        }

        // Check existing certs: system match + year within 1 + value within tolerance
        for (Certificate cert : boat.certificates())
        {
            if (cert.system().equalsIgnoreCase(system)
                    && Math.abs(cert.year() - year) <= 1
                    && Math.abs(cert.value() - tcf) < 0.001)
                return cert.certificateNumber();
        }

        // Create inferred certificate with a deterministic number (idempotent on re-runs)
        String certNumber = String.format("ty-%s-%d-%.4f", system.toLowerCase(), year, tcf);
        Certificate inferred = new Certificate(system, year, tcf, false, false, false, certNumber, null);
        List<Certificate> certs = new ArrayList<>(boat.certificates());
        certs.add(inferred);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(), List.copyOf(certs), null));
        LOG.debug("TopYacht: inferred {} cert {} (TCF={}) for boat {}", system, certNumber, tcf, boat.id());
        return certNumber;
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

    private void updateClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        Club club = store.clubs().get(clubId);
        if (club == null)
        {
            // Club not yet persisted — initialise from seed so series can be recorded
            Club seed = store.clubSeed().get(clubId);
            if (seed == null)
                return;
            club = new Club(seed.id(), seed.shortName(), seed.longName(), seed.state(),
                seed.aliases() != null ? seed.aliases() : List.of(),
                seed.topyachtUrls() != null ? seed.topyachtUrls() : List.of(),
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

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(),
            club.aliases(), club.topyachtUrls(), List.copyOf(series), null));
    }

    private static Duration parseElapsed(String text)
    {
        if (text == null || text.isBlank())
            return null;
        String upper = text.trim().toUpperCase();
        if (upper.equals("DNF") || upper.equals("DNS") || upper.equals("RET")
            || upper.equals("DNC") || upper.equals("OCS") || upper.equals("DSQ"))
            return null;

        String[] parts = text.split(":");
        if (parts.length != 3)
            return null;
        try
        {
            int h = Integer.parseInt(parts[0].trim());
            int min = Integer.parseInt(parts[1].trim());
            int sec = Integer.parseInt(parts[2].trim());
            return Duration.ofHours(h).plusMinutes(min).plusSeconds(sec);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    private static String stripQuery(String href)
    {
        int q = href.indexOf('?');
        return q >= 0 ? href.substring(0, q) : href;
    }

    private static String resolveUrl(String baseUrl, String href)
    {
        try
        {
            return URI.create(baseUrl).resolve(href).toString();
        }
        catch (Exception e)
        {
            LOG.warn("TopYacht: could not resolve href '{}' against base '{}': {}", href, baseUrl, e.getMessage());
            return null;
        }
    }

    private String fetch(String url) throws Exception
    {
        ContentResponse response = httpClient.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // --- Internal records and helpers ---

    record SeriesLink(String name, String url) {}

    record RaceRow(int number, LocalDate date, List<String> resultsUrls) {}

    record ParsedRace(List<ParsedDivision> divisions) {}

    record ParsedDivision(String name, String handicapSystem, List<ParsedRow> rows) {}

    record ParsedRow(String sailNo, String boatName, Duration elapsed,
                     String clubCode, String designName, String ahcValue) {}

    /** Accumulates data from multiple results pages for the same race, keyed by sail number. */
    private static class MergeEntry
    {
        final String boatName;
        final Duration elapsed;
        final String clubCode;
        final String designName;
        final List<SysAhc> sysAhcs = new ArrayList<>();

        MergeEntry(String boatName, Duration elapsed, String clubCode, String designName)
        {
            this.boatName = boatName;
            this.elapsed = elapsed;
            this.clubCode = clubCode;
            this.designName = designName;
        }
    }

    private record SysAhc(String system, String ahcValue) {}
}
