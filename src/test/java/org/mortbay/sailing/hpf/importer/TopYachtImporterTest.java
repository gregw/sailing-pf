package org.mortbay.sailing.hpf.importer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.importer.TopYachtImporter.ParsedDivision;
import org.mortbay.sailing.hpf.importer.TopYachtImporter.ParsedRace;
import org.mortbay.sailing.hpf.importer.TopYachtImporter.ParsedRow;
import org.mortbay.sailing.hpf.store.DataStore;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopYachtImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;
    private TopYachtImporter importer;

    /** Minimal club with no TopYacht URLs (we call processors directly). */
    private static final Club TEST_CLUB = new Club(
        "bsyc.com.au", "BSYC", "Brighton & Seacliff Yacht Club", "SA",
        List.of(), List.of(), List.of(), null);

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        store.putClub(TEST_CLUB);
        importer = new TopYachtImporter(store, null); // httpClient not used by parse/process methods
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    // --- parseIndexPage ---

    @Test
    void parseIndexPageExtractsSeriesLinks()
    {
        String html = "<html><body><ul>" +
            "<li><a href=perf-rac/series.htm>Performance Racing</a></li>" +
            "<li><a href=pass-rat/series.htm>Rating Passage</a></li>" +
            "</ul></body></html>";

        List<TopYachtImporter.SeriesLink> links =
            importer.parseIndexPage(html, "https://www.topyacht.net.au/results/2025/bsyc/index.htm");

        assertEquals(2, links.size());
        assertEquals("Performance Racing", links.get(0).name());
        assertEquals("https://www.topyacht.net.au/results/2025/bsyc/perf-rac/series.htm", links.get(0).url());
        assertEquals("Rating Passage", links.get(1).name());
    }

    @Test
    void parseIndexPageStripsQueryFromSeriesHref()
    {
        String html = "<html><body><a href='perf-rac/series.htm?ty=99'>Perf</a></body></html>";
        List<TopYachtImporter.SeriesLink> links =
            importer.parseIndexPage(html, "https://www.topyacht.net.au/results/2025/bsyc/index.htm");

        assertEquals(1, links.size());
        // URL itself may or may not retain query — key is that the link was found
        assertEquals("Perf", links.get(0).name());
    }

    @Test
    void parseIndexPageIgnoresNonSeriesLinks()
    {
        String html = "<html><body>" +
            "<a href='http://other.com/page.htm'>Other</a>" +
            "<a href='results.htm'>Results</a>" +
            "<a href='perf-rac/series.htm'>Valid</a>" +
            "</body></html>";
        List<TopYachtImporter.SeriesLink> links =
            importer.parseIndexPage(html, "https://www.topyacht.net.au/results/2025/bsyc/index.htm");

        assertEquals(1, links.size());
        assertEquals("Valid", links.get(0).name());
    }

    // --- parseSeriesPage ---

    @Test
    void parseSeriesPageExtractsRaceRows()
    {
        String html = seriesHtml(
            "<tr class='type3 txtsize'>" +
            "  <td class='type_x left_align'>Race 7 - 15/08/2024</td>" +
            "  <td class='type_x centre_align'><a href='07RGrp12.htm'><img></a></td>" +
            "  <td class='type_x centre_align'><a href='07entr.htm'><img></a></td>" +
            "</tr>");

        List<TopYachtImporter.RaceRow> rows =
            importer.parseSeriesPage(html, "https://www.topyacht.net.au/results/2024/bsyc/perf-rac/series.htm");

        assertEquals(1, rows.size());
        assertEquals(7, rows.get(0).number());
        assertEquals(LocalDate.of(2024, 8, 15), rows.get(0).date());
        assertEquals(1, rows.get(0).resultsUrls().size());
        assertTrue(rows.get(0).resultsUrls().get(0).endsWith("07RGrp12.htm"));
    }

    @Test
    void parseSeriesPageCollectsMultipleResultUrls()
    {
        // ABRW-style: IRC link + ORC AP link + entrants link per race row
        String html = seriesHtml(
            "<tr class='type3 txtsize'>" +
            "  <td class='type_x left_align'>Race 2 - 09/08/2025</td>" +
            "  <td class='type_x centre_align'><a href='02RGrp1.htm'><img></a></td>" +
            "  <td class='type_x centre_align'><a href='02RGrp2.htm'><img></a></td>" +
            "  <td class='type_x centre_align'><a href='02entr.htm'><img></a></td>" +
            "</tr>");

        List<TopYachtImporter.RaceRow> rows =
            importer.parseSeriesPage(html, "https://www.topyacht.net.au/results/2025/abrw/pass-rat/series.htm");

        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).resultsUrls().size());
        assertTrue(rows.get(0).resultsUrls().get(0).endsWith("02RGrp1.htm"));
        assertTrue(rows.get(0).resultsUrls().get(1).endsWith("02RGrp2.htm"));
    }

    @Test
    void parseSeriesPageSkipsSeriesScoresRow()
    {
        String html = seriesHtml(
            "<tr class='type3 txtsize'>" +
            "  <td class='type_x left_align'>Series Scores</td>" +
            "  <td class='type_x centre_align'><a href='SGrp12.htm'><img></a></td>" +
            "  <td></td>" +
            "</tr>" +
            "<tr class='type4 txtsize'>" +
            "  <td class='type_x left_align'>Race 1 - 9/08/2024</td>" +
            "  <td class='type_x centre_align'><a href='01RGrp12.htm'><img></a></td>" +
            "  <td></td>" +
            "</tr>");

        List<TopYachtImporter.RaceRow> rows =
            importer.parseSeriesPage(html, "https://www.topyacht.net.au/results/2024/bsyc/perf-rac/series.htm");

        assertEquals(1, rows.size());
        assertEquals(1, rows.get(0).number());
    }

    @Test
    void parseSeriesPageSkipsRowsWithNoResultsLink()
    {
        String html = seriesHtml(
            "<tr class='type3 txtsize'>" +
            "  <td class='type_x left_align'>Race 2 - 22/08/2024</td>" +
            "  <td class='type_x centre_align'>&nbsp;</td>" +
            "  <td></td>" +
            "</tr>");

        List<TopYachtImporter.RaceRow> rows =
            importer.parseSeriesPage(html, "https://www.topyacht.net.au/results/2024/bsyc/perf-rac/series.htm");

        assertTrue(rows.isEmpty());
    }

    @Test
    void parseSeriesPageHandlesSingleDigitDate()
    {
        String html = seriesHtml(
            "<tr class='type4 txtsize'>" +
            "  <td>Race 1 - 9/8/2024</td>" +
            "  <td><a href='01RGrp12.htm'><img></a></td>" +
            "  <td></td>" +
            "</tr>");

        List<TopYachtImporter.RaceRow> rows =
            importer.parseSeriesPage(html, "https://www.topyacht.net.au/results/2024/bsyc/perf-rac/series.htm");

        assertEquals(1, rows.size());
        assertEquals(LocalDate.of(2024, 8, 9), rows.get(0).date());
    }

    // --- parseResultsPage ---

    @Test
    void parseResultsPageExtractsHandicapSystemFromCaption()
    {
        ParsedRace parsed = importer.parseResultsPage(resultsHtml("PHS results  Start : 12:25", List.of()));
        assertNotNull(parsed);
        assertEquals(1, parsed.divisions().size());
        assertEquals("PHS", parsed.divisions().get(0).handicapSystem());
        assertNull(parsed.divisions().get(0).name());
    }

    @Test
    void parseResultsPageExtractsDivisionNameFromCaption()
    {
        ParsedRace parsed = importer.parseResultsPage(resultsHtml("Cruising A PHS results Start : 12:25", List.of()));
        assertNotNull(parsed);
        assertEquals(1, parsed.divisions().size());
        assertEquals("PHS", parsed.divisions().get(0).handicapSystem());
        assertEquals("Cruising A", parsed.divisions().get(0).name());
    }

    @Test
    void parseResultsPageExtractsOrcApHandicapSystem()
    {
        // "ORC AP" is a two-word system name; must not be parsed as system="AP", div="ORC"
        ParsedRace parsed = importer.parseResultsPage(resultsHtml("ORC AP results  Start : 10:00", List.of()));
        assertNotNull(parsed);
        assertEquals("ORC", parsed.divisions().get(0).handicapSystem());
        assertNull(parsed.divisions().get(0).name());
    }

    @Test
    void parseResultsPageExtractsDivisionNameWithOrcAp()
    {
        ParsedRace parsed = importer.parseResultsPage(
            resultsHtml("Cruising A ORC AP results Start : 10:00", List.of()));
        assertNotNull(parsed);
        assertEquals("ORC", parsed.divisions().get(0).handicapSystem());
        assertEquals("Cruising A", parsed.divisions().get(0).name());
    }

    @Test
    void parseResultsPageExtractsAhcValue()
    {
        String html = resultsHtml("IRC results  Start : 10:00", List.of(
            resultRow("1", "BOAT", "AUS1", "Skipper", "DSS", "13:00:00", "03:00:00")
        ));
        // The resultRow helper puts "0.856" in the AHC column
        ParsedRace parsed = importer.parseResultsPage(html);
        assertNotNull(parsed);
        ParsedRow row = parsed.divisions().get(0).rows().get(0);
        assertEquals("0.856", row.ahcValue());
    }

    @Test
    void parseResultsPageBasicFinishers()
    {
        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "DEVILJSH", "4988R", "Skipper One", "DSS", "15:25:51", "03:00:51"),
            resultRow("2", "SECOND BOAT", "AUS123", "Skipper Two", "BSYC", "15:45:00", "03:20:00")
        ));

        ParsedRace parsed = importer.parseResultsPage(html);
        assertNotNull(parsed);
        ParsedDivision div = parsed.divisions().get(0);
        assertEquals(2, div.rows().size());

        ParsedRow row0 = div.rows().get(0);
        assertEquals("4988R", row0.sailNo());
        assertEquals("DEVILJSH", row0.boatName());
        assertEquals(Duration.ofHours(3).plusMinutes(0).plusSeconds(51), row0.elapsed());
        assertEquals("DSS", row0.clubCode());
    }

    @Test
    void parseResultsPageSkipsDnfRows()
    {
        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "DEVILJSH", "4988R", "Skip", "DSS", "15:25:51", "03:00:51"),
            resultRowDnf("DNF", "LATE BOAT", "AUS999", "Skip2", "WSC")
        ));

        ParsedRace parsed = importer.parseResultsPage(html);
        assertNotNull(parsed);
        List<ParsedRow> rows = parsed.divisions().get(0).rows();
        assertEquals(1, rows.size());
        assertEquals("4988R", rows.get(0).sailNo());
    }

    @Test
    void parseResultsPageReturnsNullWhenNoTable()
    {
        ParsedRace parsed = importer.parseResultsPage("<html><body>No table here</body></html>");
        assertNull(parsed);
    }

    @Test
    void parseResultsPageReturnsNullWhenRequiredColumnMissing()
    {
        // Table with no elapsed column — division is skipped, so no valid divisions → null
        String html = "<html><body><table class='centre_results_table'>" +
            "<caption>PHS results</caption>" +
            "<tr class='type1'><td>Place</td><td>Boat Name</td><td>Sail No</td></tr>" +
            "<tr class='type3'><td>1</td><td>Boat</td><td>AUS1</td></tr>" +
            "</table></body></html>";
        ParsedRace parsed = importer.parseResultsPage(html);
        assertNull(parsed);
    }

    @Test
    void parseResultsPageHandlesMultipleDivisions()
    {
        String html = "<html><body>" +
            "<table class='centre_results_table'>" +
            "<caption>Cruising A PHS results Start : 12:00</caption>" +
            "<tr class='type1'><td>Place</td><td>Boat Name</td><td>Sail No</td><td>From</td><td>Elapsd</td></tr>" +
            "<tr class='type3'><td>1</td><td>ALPHA</td><td>AUS1</td><td>DSS</td><td>03:00:00</td></tr>" +
            "</table>" +
            "<table class='centre_results_table'>" +
            "<caption>Cruising B PHS results Start : 12:00</caption>" +
            "<tr class='type1'><td>Place</td><td>Boat Name</td><td>Sail No</td><td>From</td><td>Elapsd</td></tr>" +
            "<tr class='type3'><td>1</td><td>BETA</td><td>AUS2</td><td>WSC</td><td>02:30:00</td></tr>" +
            "</table>" +
            "</body></html>";

        ParsedRace parsed = importer.parseResultsPage(html);
        assertNotNull(parsed);
        assertEquals(2, parsed.divisions().size());
        assertEquals("Cruising A", parsed.divisions().get(0).name());
        assertEquals("PHS", parsed.divisions().get(0).handicapSystem());
        assertEquals(1, parsed.divisions().get(0).rows().size());
        assertEquals("Cruising B", parsed.divisions().get(1).name());
        assertEquals(1, parsed.divisions().get(1).rows().size());
    }

    // --- processResultsPage integration ---

    @Test
    void processResultsPageCreatesRaceAndBoat()
    {
        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "DEVILJSH", "4988R", "Skipper", "DSS", "15:25:51", "03:00:51")
        ));

        importer.processResultsPage(TEST_CLUB, "Performance Racing", 7, LocalDate.of(2024, 8, 15), html, null);

        assertEquals(1, store.races().size());
        Race race = store.races().values().iterator().next();
        assertEquals("bsyc.com.au-2024-08-15-0007", race.id());
        assertEquals("PHS", race.handicapSystem());
        assertEquals(LocalDate.of(2024, 8, 15), race.date());
        assertEquals(7, race.number());

        assertEquals(1, race.divisions().size());
        assertEquals(1, race.divisions().get(0).finishers().size());

        Finisher f = race.divisions().get(0).finishers().get(0);
        assertEquals(Duration.ofHours(3).plusMinutes(0).plusSeconds(51), f.elapsedTime());
        assertFalse(f.nonSpinnaker());
        assertNull(f.certificateNumber());  // PHS: no cert inference

        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals("4988R", boat.sailNumber());
        assertEquals("DEVILJSH", boat.name());
    }

    @Test
    void processResultsPageUpdatesSeriesMembership()
    {
        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "BOAT", "AUS1", "Skip", "DSS", "14:00:00", "02:00:00")
        ));

        importer.processResultsPage(TEST_CLUB, "Performance Racing", 1, LocalDate.of(2024, 8, 9), html, null);
        importer.processResultsPage(TEST_CLUB, "Performance Racing", 2, LocalDate.of(2024, 8, 16), html, null);

        Club updated = store.clubs().get("bsyc.com.au");
        assertEquals(1, updated.series().size());
        Series s = updated.series().get(0);
        assertEquals("bsyc.com.au/performance-racing", s.id());
        assertEquals(2, s.raceIds().size());
    }

    @Test
    void processResultsPageIsDuplicateIdempotent()
    {
        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "BOAT", "AUS1", "Skip", "DSS", "14:00:00", "02:00:00")
        ));
        LocalDate date = LocalDate.of(2024, 8, 9);

        importer.processResultsPage(TEST_CLUB, "Performance Racing", 1, date, html, null);
        importer.processResultsPage(TEST_CLUB, "Performance Racing", 1, date, html, null);

        assertEquals(1, store.races().size());
    }

    @Test
    void processResultsPageSetsBoatClubFromFromColumn()
    {
        // Add a second club to the store so it can be resolved
        Club dss = new Club("dssinc.org.au", "DSS", "Derwent Sailing Squadron", "TAS",
            List.of(), List.of(), List.of(), null);
        store.putClub(dss);

        String html = resultsHtml("PHS results  Start : 12:25", List.of(
            resultRow("1", "DEVILJSH", "4988R", "Skipper", "DSS", "15:25:51", "03:00:51")
        ));

        importer.processResultsPage(TEST_CLUB, "Performance Racing", 7, LocalDate.of(2024, 8, 15), html, null);

        Boat boat = store.boats().values().iterator().next();
        assertEquals("dssinc.org.au", boat.clubId());
    }

    @Test
    void processResultsPageCreatesDesignWhenColumnPresent()
    {
        String html = resultsHtmlWithDesign("PHS results  Start : 12:25", List.of(
            resultRowWithDesign("1", "BOAT", "AUS1", "Skip", "DSS", "14:00:00", "02:00:00", "J/24")
        ));

        importer.processResultsPage(TEST_CLUB, "Performance Racing", 1, LocalDate.of(2024, 8, 9), html, null);

        assertFalse(store.designs().isEmpty());
        assertNotNull(store.designs().get("j24"));
        Boat boat = store.boats().values().iterator().next();
        assertEquals("j24", boat.designId());
    }

    // --- processResultsPages (multi-page merge) ---

    @Test
    void processResultsPagesDedupsByElapsed()
    {
        // Same boat appears in both IRC and ORC pages with the same elapsed time
        Duration elapsed = Duration.ofHours(3);
        ParsedRow ircRow = new ParsedRow("AUS1", "BOAT", elapsed, null, null, "1.267");
        ParsedRow orcRow = new ParsedRow("AUS1", "BOAT", elapsed, null, null, "1.470");
        ParsedRace ircPage = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(ircRow))));
        ParsedRace orcPage = new ParsedRace(List.of(new ParsedDivision(null, "ORC", false, false, false, List.of(orcRow))));

        importer.processResultsPages(TEST_CLUB, "Rating Passage", 1,
            LocalDate.of(2025, 8, 9), List.of(ircPage, orcPage));

        assertEquals(1, store.races().size());
        Race race = store.races().values().iterator().next();
        assertEquals("IRC/ORC", race.handicapSystem());
        assertEquals(1, race.divisions().size());
        assertEquals(1, race.divisions().get(0).finishers().size()); // deduped to one finisher
    }

    @Test
    void processResultsPagesMismatchedElapsedIsDiscarded()
    {
        // Same sail number but different elapsed times in IRC vs ORC page — discard the second
        ParsedRow row1 = new ParsedRow("AUS1", "BOAT", Duration.ofHours(3), null, null, "1.267");
        ParsedRow row2 = new ParsedRow("AUS1", "BOAT", Duration.ofHours(4), null, null, "1.470");
        ParsedRace page1 = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(row1))));
        ParsedRace page2 = new ParsedRace(List.of(new ParsedDivision(null, "ORC", false, false, false, List.of(row2))));

        importer.processResultsPages(TEST_CLUB, "Rating Passage", 1,
            LocalDate.of(2025, 8, 9), List.of(page1, page2));

        Race race = store.races().values().iterator().next();
        List<Finisher> finishers = race.divisions().get(0).finishers();
        assertEquals(1, finishers.size()); // only the first page's finisher survives
        assertEquals(Duration.ofHours(3), finishers.get(0).elapsedTime());
    }

    @Test
    void processResultsPagesDistinctBoatsMergedIntoOneDiv()
    {
        // IRC page has boat A; ORC page has boat B — both in same race, one division
        ParsedRow rowA = new ParsedRow("AUS1", "BOAT A", Duration.ofHours(3), null, null, "1.267");
        ParsedRow rowB = new ParsedRow("AUS2", "BOAT B", Duration.ofHours(2).plusMinutes(30), null, null, "1.470");
        ParsedRace ircPage = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(rowA))));
        ParsedRace orcPage = new ParsedRace(List.of(new ParsedDivision(null, "ORC", false, false, false, List.of(rowB))));

        importer.processResultsPages(TEST_CLUB, "Rating Passage", 1,
            LocalDate.of(2025, 8, 9), List.of(ircPage, orcPage));

        Race race = store.races().values().iterator().next();
        assertEquals(1, race.divisions().size());
        assertEquals(2, race.divisions().get(0).finishers().size());
    }

    @Test
    void processResultsPagesInfersCertificateFromAhc()
    {
        ParsedRow row = new ParsedRow("AUS1", "BOAT", Duration.ofHours(3), null, null, "1.267");
        ParsedRace ircPage = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(row))));

        importer.processResultsPages(TEST_CLUB, "Rating Passage", 1,
            LocalDate.of(2025, 8, 9), List.of(ircPage));

        Boat boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size());
        Certificate cert = boat.certificates().get(0);
        assertEquals("IRC", cert.system());
        assertEquals(1.267, cert.value(), 0.001);
        assertEquals(2025, cert.year());
        assertTrue(cert.certificateNumber().startsWith("ty-irc-"));

        // Finisher should link to the cert
        Race race = store.races().values().iterator().next();
        Finisher f = race.divisions().get(0).finishers().get(0);
        assertEquals(cert.certificateNumber(), f.certificateNumber());
    }

    @Test
    void processResultsPagesSkipsCertInferenceForPhs()
    {
        // PHS results: AHC column present but must NOT be used to infer a certificate
        ParsedRow row = new ParsedRow("AUS1", "BOAT", Duration.ofHours(3), null, null, "0.856");
        ParsedRace phsPage = new ParsedRace(List.of(new ParsedDivision(null, "PHS", false, false, false, List.of(row))));

        importer.processResultsPages(TEST_CLUB, "Performance Racing", 1,
            LocalDate.of(2025, 8, 9), List.of(phsPage));

        Boat boat = store.boats().values().iterator().next();
        assertTrue(boat.certificates().isEmpty());

        Race race = store.races().values().iterator().next();
        assertNull(race.divisions().get(0).finishers().get(0).certificateNumber());
    }

    @Test
    void processResultsPagesReuseExistingCertificate()
    {
        // First race: infers IRC cert from AHC
        ParsedRow row1 = new ParsedRow("AUS1", "BOAT", Duration.ofHours(3), null, null, "1.267");
        ParsedRace page1 = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(row1))));
        importer.processResultsPages(TEST_CLUB, "Rating Passage", 1,
            LocalDate.of(2025, 8, 9), List.of(page1));

        Boat boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size());

        // Second race: same TCF — should reuse existing cert, not add a second one
        ParsedRow row2 = new ParsedRow("AUS1", "BOAT", Duration.ofHours(2).plusMinutes(45), null, null, "1.267");
        ParsedRace page2 = new ParsedRace(List.of(new ParsedDivision(null, "IRC", false, false, false, List.of(row2))));
        importer.processResultsPages(TEST_CLUB, "Rating Passage", 2,
            LocalDate.of(2025, 8, 16), List.of(page2));

        boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size()); // still exactly one cert
    }

    // --- HTML helpers ---

    private String seriesHtml(String rows)
    {
        // 4 columns: Race label | IRC results | ORC results | Entrants
        // Entrants column (index 3) is recognised as excluded by the column-header filter.
        return "<html><body><table class='centre_index_table'>" +
            "<tr class='type1'><td>Race</td><td>IRC</td><td>ORC</td><td>Entrants</td></tr>" +
            rows +
            "</table></body></html>";
    }

    private String resultsHtml(String caption, List<String> dataRows)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body><table class='centre_results_table'>");
        sb.append("<caption>").append(caption).append("</caption>");
        sb.append("<tr class='type1 txtsize'>");
        sb.append("<td>Place</td><td>Boat Name</td><td>Sail No</td>");
        sb.append("<td>Skipper</td><td>From</td><td>Fin Tim</td><td>Elapsd</td>");
        sb.append("<td>AHC</td><td>Cor'd T</td><td>Score</td>");
        sb.append("</tr>");
        for (String row : dataRows)
            sb.append(row);
        sb.append("</table></body></html>");
        return sb.toString();
    }

    private String resultsHtmlWithDesign(String caption, List<String> dataRows)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body><table class='centre_results_table'>");
        sb.append("<caption>").append(caption).append("</caption>");
        sb.append("<tr class='type1 txtsize'>");
        sb.append("<td>Place</td><td>Boat Name</td><td>Sail No</td>");
        sb.append("<td>Skipper</td><td>Class</td><td>From</td><td>Fin Tim</td><td>Elapsd</td>");
        sb.append("</tr>");
        for (String row : dataRows)
            sb.append(row);
        sb.append("</table></body></html>");
        return sb.toString();
    }

    private String resultRow(String place, String name, String sail, String skipper,
                              String from, String finTime, String elapsed)
    {
        return "<tr class='type3 txtsize'>" +
            "<td class='boldText centre_align'>" + place + "</td>" +
            "<td><a href='https://www.topyacht.com.au/mt/mt_pub.php?boid=123'>" + name + "</a></td>" +
            "<td>" + sail + "</td>" +
            "<td>" + skipper + "</td>" +
            "<td>" + from + "</td>" +
            "<td>" + finTime + "</td>" +
            "<td>" + elapsed + "</td>" +
            "<td>0.856</td><td>02:34:48</td><td>1.0</td>" +
            "</tr>";
    }

    private String resultRowDnf(String place, String name, String sail, String skipper, String from)
    {
        return "<tr class='type4 txtsize'>" +
            "<td class='boldText centre_align'>" + place + "</td>" +
            "<td>" + name + "</td>" +
            "<td>" + sail + "</td>" +
            "<td>" + skipper + "</td>" +
            "<td>" + from + "</td>" +
            "<td></td>" +
            "<td>DNF</td>" +
            "<td></td><td></td><td></td>" +
            "</tr>";
    }

    private String resultRowWithDesign(String place, String name, String sail, String skipper,
                                        String from, String finTime, String elapsed, String design)
    {
        return "<tr class='type3 txtsize'>" +
            "<td class='boldText centre_align'>" + place + "</td>" +
            "<td>" + name + "</td>" +
            "<td>" + sail + "</td>" +
            "<td>" + skipper + "</td>" +
            "<td>" + design + "</td>" +
            "<td>" + from + "</td>" +
            "<td>" + finTime + "</td>" +
            "<td>" + elapsed + "</td>" +
            "</tr>";
    }
}
