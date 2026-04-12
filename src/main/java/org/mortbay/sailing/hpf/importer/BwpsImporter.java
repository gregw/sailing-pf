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
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Imports race results from the BWPS (Blue Water Pointscore) standings site at
 * {@code https://bwps.cycaracing.com/standings/}.
 * <p>
 * Iterates all races in the race selector and all years ≥ {@value #MIN_YEAR} in the year
 * selector. For each race/year combination, fetches the IRC standings (and ORC if present)
 * to extract handicap values, and the Line Honours standings to extract elapsed times.
 * Boat detail pages are fetched individually to obtain sail number, state, and design.
 * <p>
 * Per CLAUDE.md:
 * - PHS handicap values are NOT stored (PHS tab is skipped entirely)
 * - IRC/ORC HCAP values from race results ARE used to infer boat certificates
 * - Elapsed times from boats in IRC/ORC categories ARE stored
 * - BWPS-internal IDs (categoryId, raceId, seriesId) are NOT stored
 * - The boat detail page {@code boid} parameter is NOT stored
 * <p>
 * Elapsed times on this site use a {@code DD:HH:MM:SS} format (days, hours, minutes, seconds),
 * which differs from the {@code H:MM:SS} format used by SailSys and TopYacht.
 */
public class BwpsImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(BwpsImporter.class);

    static final String SOURCE = "BWPS";
    static final String BASE_URL = "https://bwps.cycaracing.com";
    static final int MIN_YEAR = 2020;
    static final String CLUB_ID = "cyca.com.au";

    /** Formatter for BWPS finish time strings: e.g. "29 Dec 2025 02:39:32 PM". */
    private static final DateTimeFormatter FINISH_FMT =
        DateTimeFormatter.ofPattern("d MMM yyyy h:mm:ss a", Locale.ENGLISH);

    private final DataStore store;
    private final HttpClient httpClient;
    private int recentRaceReimportDays = 30;

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

    // --- Entry point ---

    public void run() throws Exception { run(30); }

    public void run(int recentRaceReimportDays) throws Exception
    {
        this.recentRaceReimportDays = recentRaceReimportDays;
        LOG.info("BWPS: fetching race list from {}/standings/", BASE_URL);
        String mainHtml = fetchHtml(BASE_URL + "/standings/");
        List<RaceOption> races = parseRaceSelector(mainHtml);
        LOG.info("BWPS: found {} races", races.size());

        for (RaceOption race : races)
        {
            LOG.info("BWPS: fetching years for race '{}'", race.name());
            String raceHtml;
            try
            {
                raceHtml = fetchHtml(BASE_URL + race.url());
            }
            catch (Exception e)
            {
                LOG.error("BWPS: failed to fetch race page {}: {}", race.url(), e.getMessage());
                continue;
            }

            List<YearOption> years = parseYearSelector(raceHtml);
            for (YearOption year : years)
            {
                if (year.year() < MIN_YEAR)
                    continue;
                LOG.info("BWPS: processing race='{}' year={}", race.name(), year.yearLabel());
                try
                {
                    processRaceEdition(race.name(), year.year(), year.url());
                }
                catch (Exception e)
                {
                    LOG.error("BWPS: failed to process race='{}' year={}: {}",
                        race.name(), year.yearLabel(), e.getMessage(), e);
                }
            }
        }
    }

    // --- Race edition processor (package-private for testing) ---

    void processRaceEdition(String raceName, int year, String standingsUrl) throws Exception
    {
        // Sydney Hobart is imported by RshyrImporter with higher-quality elapsed times
        if (raceName.toUpperCase(Locale.ENGLISH).contains("SYDNEY HOBART"))
        {
            LOG.debug("BWPS: skipping '{}' {} — handled by RshyrImporter", raceName, year);
            return;
        }
        String standingsHtml = fetchHtml(BASE_URL + standingsUrl);
        Map<String, String> tabs = parseCategoryTabs(standingsHtml);

        String ircTabUrl = tabs.get("IRC");
        String lhTabUrl  = tabs.get("Line Honours");
        if (ircTabUrl == null || lhTabUrl == null)
        {
            LOG.warn("BWPS: race='{}' year={} — IRC or Line Honours tab not found; tabs={}",
                raceName, year, tabs.keySet());
            return;
        }

        // Collect IRC rows; add ORC rows if an ORC tab exists
        List<StandingsRow> standingsRows = new ArrayList<>(
            parseStandingsTable(fetchHtml(BASE_URL + ircTabUrl), "IRC"));
        for (Map.Entry<String, String> tab : tabs.entrySet())
        {
            String label = tab.getKey();
            if (label.equalsIgnoreCase("ORC") || label.equalsIgnoreCase("ORCi")
                    || label.equalsIgnoreCase("ORC Club"))
            {
                standingsRows.addAll(parseStandingsTable(fetchHtml(BASE_URL + tab.getValue()), "ORC"));
            }
        }

        List<LhRow> lhRows = parseLineHonoursTable(fetchHtml(BASE_URL + lhTabUrl));

        LocalDate raceDate = computeRaceDate(lhRows, year);
        if (raceDate == null)
        {
            LOG.warn("BWPS: race='{}' year={} — could not compute race date", raceName, year);
            return;
        }

        String seriesId = IdGenerator.generateSeriesId(CLUB_ID, raceName);
        String raceId   = IdGenerator.generateRaceId(CLUB_ID, raceDate, 1);

        if (store.races().containsKey(raceId) && !isRecentRace(raceDate))
        {
            LOG.debug("BWPS: race {} already imported, updating series membership only", raceId);
            updateClubSeries(CLUB_ID, seriesId, raceName, raceId);
            return;
        }

        // Index LH rows by boatDetailUrl for fast lookup
        Map<String, LhRow> lhByUrl = new LinkedHashMap<>();
        for (LhRow lh : lhRows)
        {
            if ("FINISHED".equalsIgnoreCase(lh.status()))
                lhByUrl.put(lh.boatDetailUrl(), lh);
        }

        // Build finishers grouped by division
        LinkedHashMap<String, List<Finisher>> divMap = new LinkedHashMap<>();
        int finisherCount = 0;

        for (StandingsRow row : standingsRows)
        {
            if (!"FINISHED".equalsIgnoreCase(row.status()))
                continue;

            LhRow lh = lhByUrl.get(row.boatDetailUrl());
            if (lh == null)
                continue;  // boat not in Line Honours (retired before finishing?)

            BoatDetail detail;
            try
            {
                detail = parseBoatDetail(fetchHtml(BASE_URL + row.boatDetailUrl()));
            }
            catch (Exception e)
            {
                LOG.warn("BWPS: failed to fetch boat detail {}: {}", row.boatDetailUrl(), e.getMessage());
                continue;
            }

            // CYCA appends "(TH)" or "(DH)" (Two Handed / Double Handed) to entries
            // in the two-handed division.  Strip the suffix before creating the boat so
            // that the same physical boat is not stored under two different identities,
            // and record the flag so the certificate is correctly marked twoHanded.
            String rawName = detail.yachtName();
            boolean twoHanded = rawName != null
                && (rawName.toUpperCase(Locale.ENGLISH).contains("(TH)")
                    || rawName.toUpperCase(Locale.ENGLISH).contains("(DH)"));
            String boatName = twoHanded
                ? rawName.replaceAll("(?i)\\((TH|DH)\\)", "").trim()
                : rawName;

            String designName = (detail.type() != null && !detail.type().isBlank())
                ? detail.type() : null;
            Boat boat = store.findOrCreateBoat(detail.sailNumber(), boatName, designName, raceDate, SOURCE);

            if (detail.club() != null && !detail.club().isBlank() && boat.clubId() == null)
            {
                Club fromClub = store.findUniqueClubByShortName(detail.club(), null,
                    "BWPS boat sailNumber=" + detail.sailNumber() + " name=" + boatName);
                if (fromClub != null)
                {
                    store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                        boat.designId(), fromClub.id(), boat.aliases(), boat.altSailNumbers(), boat.certificates(),
                        addSource(boat.sources(), SOURCE), Instant.now(), null));
                }
            }

            // Re-read boat after potential club update
            boat = store.boats().get(boat.id());
            String certNum = inferCertificate(boat, row.system(), year, row.hcap(), twoHanded);

            Duration lhElapsed = lh.elapsed();
            if (lhElapsed == null || lhElapsed.isNegative() || lhElapsed.isZero())
            {
                LOG.warn("BWPS: skipping finisher '{}' in race '{}' year={}: non-positive elapsed {}",
                    boat.id(), raceName, year, lhElapsed);
                continue;
            }
            Finisher finisher = new Finisher(boat.id(), lhElapsed, false, certNum);
            divMap.computeIfAbsent(row.div(), k -> new ArrayList<>()).add(finisher);
            finisherCount++;
        }

        if (divMap.isEmpty())
        {
            LOG.warn("BWPS: race='{}' year={} — no finished IRC/ORC boats with elapsed times", raceName, year);
            return;
        }

        List<Division> divisions = divMap.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();

        // Derive handicap system label from the systems present
        boolean hasIrc = standingsRows.stream().anyMatch(r -> "IRC".equals(r.system()));
        boolean hasOrc = standingsRows.stream().anyMatch(r -> "ORC".equals(r.system()));
        String handicapSystem = hasIrc && hasOrc ? "IRC/ORC" : hasIrc ? "IRC" : "ORC";

        store.putRace(new Race(raceId, CLUB_ID, List.of(seriesId), raceDate, 1,
            raceName, handicapSystem, false, divisions, SOURCE, Instant.now(), null));
        LOG.info("BWPS: imported race {} '{}' {} ({} finishers, {} division(s), system={})",
            raceId, raceName, year, finisherCount, divisions.size(), handicapSystem);

        updateClubSeries(CLUB_ID, seriesId, raceName, raceId);
    }

    // --- Parsers (package-private for testing) ---

    List<RaceOption> parseRaceSelector(String html)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        List<RaceOption> result = new ArrayList<>();
        Element select = doc.selectFirst("select[aria-labelledby=standings-filters-race-label]");
        if (select == null)
            return result;
        for (Element option : select.select("option"))
        {
            String url  = option.attr("value").trim();
            String name = option.text().trim();
            if (!url.isBlank() && !name.isBlank())
                result.add(new RaceOption(name, url));
        }
        return result;
    }

    List<YearOption> parseYearSelector(String html)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        List<YearOption> result = new ArrayList<>();
        Element select = doc.selectFirst("select[aria-labelledby=standings-filters-year-label]");
        if (select == null)
            return result;
        for (Element option : select.select("option"))
        {
            String url   = option.attr("value").trim();
            String label = option.text().trim();
            if (url.isBlank() || label.isBlank())
                continue;
            try
            {
                int year = Integer.parseInt(label);
                result.add(new YearOption(label, year, url));
            }
            catch (NumberFormatException e)
            {
                LOG.debug("BWPS: skipping year option with non-integer label '{}'", label);
            }
        }
        return result;
    }

    /**
     * Returns a map of category tab labels to their href values (relative URLs).
     * De-duplicated by label — the same tab may appear twice in the HTML (mobile + desktop).
     */
    Map<String, String> parseCategoryTabs(String html)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        Map<String, String> result = new LinkedHashMap<>();
        // Category tabs are <a href="/Standings?categoryId=...">LABEL</a>
        for (Element a : doc.select("a[href*='/Standings?categoryId']"))
        {
            String label = a.text().trim();
            String href  = a.attr("href").trim();
            if (!label.isBlank() && !href.isBlank())
                result.putIfAbsent(label, href);
        }
        return result;
    }

    /**
     * Parses an IRC or ORC standings table.
     * <p>
     * Expected column order: [index, yacht+link, DIV, status, HCAP, corrected time]
     */
    List<StandingsRow> parseStandingsTable(String html, String system)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        List<StandingsRow> result = new ArrayList<>();
        Element table = doc.selectFirst("table.standings");
        if (table == null)
        {
            LOG.warn("BWPS: no standings table found (system={})", system);
            return result;
        }

        // Verify HCAP column is present by checking headers
        Element headerRow = table.selectFirst("thead tr");
        if (headerRow != null)
        {
            boolean hasHcap = headerRow.select("th").stream()
                .anyMatch(th -> th.text().trim().equalsIgnoreCase("HCAP"));
            if (!hasHcap)
            {
                LOG.warn("BWPS: standings table for system={} has no HCAP column — skipping", system);
                return result;
            }
        }

        for (Element row : table.select("tbody tr"))
        {
            Elements cells = row.select("td");
            if (cells.size() < 5)
                continue;

            // [0]=index, [1]=yacht, [2]=div, [3]=status, [4]=hcap
            Element yachtCell = cells.get(1);
            Element link = yachtCell.selectFirst("a[href]");
            if (link == null)
                continue;

            String boatDetailUrl = link.attr("href").trim();
            String boatName      = link.text().trim();
            String div           = cells.get(2).text().trim();
            String status        = cells.get(3).text().trim();
            String hcapText      = cells.get(4).text().trim();

            if (boatDetailUrl.isBlank() || boatName.isBlank() || hcapText.isBlank())
                continue;

            double hcap;
            try
            {
                hcap = Double.parseDouble(hcapText);
            }
            catch (NumberFormatException e)
            {
                LOG.debug("BWPS: could not parse HCAP '{}' for boat '{}'; skipping", hcapText, boatName);
                continue;
            }

            result.add(new StandingsRow(boatDetailUrl, boatName, div, status, hcap, system));
        }
        return result;
    }

    /**
     * Parses the Line Honours standings table.
     * <p>
     * Expected column order: [index, yacht+link, status, elapsed+finish_datetime]
     */
    List<LhRow> parseLineHonoursTable(String html)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        List<LhRow> result = new ArrayList<>();
        Element table = doc.selectFirst("table.standings");
        if (table == null)
        {
            LOG.warn("BWPS: no standings table found on Line Honours page");
            return result;
        }

        for (Element row : table.select("tbody tr"))
        {
            Elements cells = row.select("td");
            if (cells.size() < 4)
                continue;

            // [0]=index, [1]=yacht, [2]=status, [3]=elapsed+finish
            Element yachtCell = cells.get(1);
            Element link = yachtCell.selectFirst("a[href]");
            if (link == null)
                continue;

            String boatDetailUrl = link.attr("href").trim();
            String boatName      = link.text().trim();
            String status        = cells.get(2).text().trim();
            Element timeCell     = cells.get(3);

            // Elapsed time is the direct text of the cell (before the <span> and <div> children)
            String elapsedText = timeCell.ownText().trim();
            Duration elapsed = parseElapsed(elapsedText);
            if (elapsed == null)
                continue;

            // Finish date/time is in the nested <div class="small">
            Element smallDiv = timeCell.selectFirst("div.small");
            String finishText = smallDiv != null ? smallDiv.text().trim() : null;

            result.add(new LhRow(boatDetailUrl, boatName, status, elapsed, finishText));
        }
        return result;
    }

    /**
     * Parses a BWPS boat detail page.
     * <p>
     * Extracts from the {@code <td>LABEL</td><td>VALUE</td>} table rows:
     * Sail Number, State, Club, Type (design), and Yacht Name.
     */
    BoatDetail parseBoatDetail(String html)
    {
        Document doc = Jsoup.parse(html, BASE_URL);
        Map<String, String> fields = new LinkedHashMap<>();

        for (Element row : doc.select("table tr"))
        {
            Elements cells = row.select("td");
            if (cells.size() >= 2)
            {
                String label = cells.get(0).text().trim();
                String value = cells.get(1).text().trim();
                if (!label.isBlank())
                    fields.putIfAbsent(label, value);
            }
        }

        return new BoatDetail(
            fields.get("Yacht Name"),
            fields.get("Sail Number"),
            fields.get("Owner"),
            fields.get("State"),
            fields.get("Club"),
            fields.get("Type")
        );
    }

    /**
     * Parses an elapsed time in {@code DD:HH:MM:SS} format.
     *
     * @param text raw cell text, e.g. {@code "03:01:39:32"} or {@code "00:06:15:44"}
     * @return parsed Duration, or {@code null} if the format is unrecognised
     */
    static Duration parseElapsed(String text)
    {
        if (text == null || text.isBlank())
            return null;
        // Take only the first whitespace-delimited token (ignores speed and finish time)
        String token = text.trim().split("\\s+")[0];
        String[] parts = token.split(":");
        if (parts.length != 4)
            return null;
        try
        {
            int days    = Integer.parseInt(parts[0]);
            int hours   = Integer.parseInt(parts[1]);
            int minutes = Integer.parseInt(parts[2]);
            int seconds = Integer.parseInt(parts[3]);
            return Duration.ofDays(days).plusHours(hours).plusMinutes(minutes).plusSeconds(seconds);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
     * Computes the race start date from the first Line Honours finisher.
     * <p>
     * Parses the finish date/time from the first finished LH row, subtracts the
     * elapsed time, and returns the date component of the result.
     * <p>
     * Handles year wraparound: if the finish month is Jan–Mar, the finish is assumed to
     * be in {@code year + 1} (i.e. race started in Oct–Dec of {@code year}).
     *
     * @return the race start date, or {@code null} if it cannot be determined
     */
    static LocalDate computeRaceDate(List<LhRow> lhRows, int year)
    {
        // Use the first FINISHED entry
        LhRow first = lhRows.stream()
            .filter(r -> "FINISHED".equalsIgnoreCase(r.status()))
            .findFirst().orElse(null);
        if (first == null || first.finishText() == null)
            return null;

        // finishText: e.g. "29 Dec 02:39:32 PM" or "20 Sep 04:15:44 PM"
        String[] tokens = first.finishText().trim().split("\\s+");
        if (tokens.length < 4)
            return null;

        // Determine year to use for the finish datetime
        // If finish month is Jan–Mar, the race started in the previous calendar year
        // (e.g. a race starting in Dec 2025 can finish in Jan 2026)
        int finishYear = year;
        try
        {
            String monthStr = tokens[1];
            int month = parseMonthIndex(monthStr);
            if (month <= 3)
                finishYear = year + 1;
        }
        catch (Exception e)
        {
            // leave finishYear = year
        }

        String fullDateTimeStr = tokens[0] + " " + tokens[1] + " " + finishYear
            + " " + tokens[2] + " " + tokens[3];
        try
        {
            LocalDateTime finishDt = LocalDateTime.parse(fullDateTimeStr, FINISH_FMT);
            return finishDt.minus(first.elapsed()).toLocalDate();
        }
        catch (Exception e)
        {
            LOG.warn("BWPS: could not parse finish datetime '{}': {}", fullDateTimeStr, e.getMessage());
            return null;
        }
    }

    // --- Private helpers ---

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

    /**
     * Checks if the boat already holds a certificate matching the given system, year, TCF,
     * and twoHanded flag.  If not, creates an inferred certificate and adds it to the boat.
     *
     * @return the certificateNumber of the matching or newly created certificate
     */
    private String inferCertificate(Boat boat, String system, int year, double tcf, boolean twoHanded)
    {
        for (Certificate cert : boat.certificates())
        {
            if (cert.system().equalsIgnoreCase(system)
                    && Math.abs(cert.year() - year) <= 1
                    && Math.abs(cert.value() - tcf) < 0.001
                    && cert.twoHanded() == twoHanded)
                return cert.certificateNumber();
        }

        String certNumber = String.format("bwps-%s-%d-%.4f%s",
            system.toLowerCase(Locale.ENGLISH), year, tcf, twoHanded ? "-dh" : "");
        Certificate inferred = new Certificate(system, year, tcf, false, twoHanded, false, false, certNumber, null);
        List<Certificate> certs = new ArrayList<>(boat.certificates());
        certs.add(inferred);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(), boat.altSailNumbers(), List.copyOf(certs),
            addSource(boat.sources(), SOURCE), Instant.now(), null));
        LOG.debug("BWPS: inferred {} cert {} (TCF={} twoHanded={}) for boat {}", system, certNumber, tcf, twoHanded, boat.id());
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

    private void updateClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        Club club = store.clubs().get(clubId);
        if (club == null)
        {
            Club seed = store.clubSeed().get(clubId);
            if (seed == null)
                return;
            club = new Club(seed.id(), seed.shortName(), seed.longName(), seed.state(), seed.excluded(),
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

        store.putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(), club.excluded(),
            club.aliases(), club.topyachtUrls(), List.copyOf(series), null));
    }

    private boolean isRecentRace(LocalDate date)
    {
        return date != null && !date.isBefore(LocalDate.now().minusDays(recentRaceReimportDays));
    }

    /**
     * Fetches the HTML at the given URL. Package-private so tests can override it
     * to serve fixture content without making real HTTP requests.
     */
    String fetchHtml(String url) throws Exception
    {
        ContentResponse response = httpClient.GET(url);
        if (response.getStatus() != 200)
            throw new RuntimeException("HTTP " + response.getStatus() + " for " + url);
        return response.getContentAsString();
    }

    // --- Inner records ---

    record RaceOption(String name, String url) {}

    record YearOption(String yearLabel, int year, String url) {}

    record StandingsRow(String boatDetailUrl, String boatName, String div,
                        String status, double hcap, String system) {}

    record LhRow(String boatDetailUrl, String boatName, String status,
                 Duration elapsed, String finishText) {}

    record BoatDetail(String yachtName, String sailNumber, String owner,
                      String state, String club, String type) {}
}
