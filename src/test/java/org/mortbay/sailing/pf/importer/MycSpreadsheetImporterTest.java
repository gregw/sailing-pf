package org.mortbay.sailing.pf.importer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MycSpreadsheetImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;
    private MycSpreadsheetImporter importer;

    private static Club club(String id, String shortName, String state)
    {
        return new Club(id, shortName, id, state, false, null,
            List.of(), List.of(), List.of(), List.of(), null);
    }

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        // "MYC" is shared by clubs in several states.
        store.putClub(club("myc.org.au", "MYC", "NSW"));
        store.putClub(club("mooloolabayachtclub.com.au", "MYC", "QLD"));
        store.putClub(club("mhyc.com.au", "MHYC", "NSW"));
        importer = new MycSpreadsheetImporter(store);
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    // Excel day numbers
    private static final String D_2019_09_08 = "43716.0";
    private static final String D_2012_10_19 = "41201.0";

    // --- Race sheet layouts ---

    /** 2015-on layout: "Race: n date" row, "State:" label, BOAT first, details on the right. */
    private static List<List<String>> newLayoutSheet(String state, List<List<String>> boats)
    {
        List<List<String>> rows = new ArrayList<>();
        rows.add(List.of("Race:", "1.0", D_2019_09_08, "", "Summer Series 2019-2020"));
        rows.add(List.of("Division:", "1.0", "2.0"));
        rows.add(List.of("Start Time:", "0.52", "", "State:", state));
        rows.add(List.of("BOAT NAME", "SAIL", "DIV", "CAS", "START", "STARTED", "FINISH", "CODE",
            "COMMENT", "ELAPSED", "PurPHS", "POINTS", "SKIPPER", "CLUB", "DESIGN", "EMAIL"));
        rows.addAll(boats);
        return rows;
    }

    private static List<String> newRow(String boat, String sail, String div, String code,
                                       String elapsed, String club, String design)
    {
        return List.of(boat, sail, div, "", "0.53", "0.533", "0.62", code, "", elapsed, "0.8", "1.0",
            "A Skipper", club, design, "someone@example.com");
    }

    private XlsxReader.Workbook workbook(Map<String, List<List<String>>> sheets)
    {
        Map<String, XlsxReader.Sheet> m = new LinkedHashMap<>();
        sheets.forEach((k, v) -> m.put(k, new XlsxReader.Sheet(k, v)));
        return new XlsxReader.Workbook(m, false);
    }

    @Test
    void parsesNewLayoutRaceSheet()
    {
        var sheet = new XlsxReader.Sheet("Race 1", newLayoutSheet("RESULTS", List.of(
            newRow("PINTA 11", "5865.0", "1.0", "", "0.09236111111", "PBYC", "Beneteau First 31.7"),
            newRow("ESRA TEW", "MYC27", "2.0", "DNC", "", "MYC", "Northshore 27"),
            newRow("RACER", "MYC1", "1.0", "OCS", "0.09", "MYC", "J24"))));

        var rs = importer.parseRaceSheet("test", 1, sheet, workbook(Map.of()));

        assertNotNull(rs);
        assertEquals(LocalDate.of(2019, 9, 8), rs.date());
        assertEquals(1, rs.rows().size(), "DNC and coded (OCS) rows are not finishers");
        var row = rs.rows().get(0);
        assertEquals("PINTA 11", row.boatName());
        assertEquals("5865", row.sailNo());
        assertEquals("Division 1", row.division());
        assertEquals("PBYC", row.club());
        assertEquals("Beneteau First 31.7", row.design());
        assertEquals(Duration.ofSeconds(Math.round(0.09236111111 * 86400)), row.elapsed());
    }

    @Test
    void parsesOldLayoutRaceSheet()
    {
        // 2012-2014 layout: bare RESULTS in the first cell, "Date:" label, BID first.
        List<List<String>> rows = List.of(
            List.of("RESULTS", "Series:", "Twilight Series"),
            List.of("", "Season:", "2012-2013"),
            List.of("", "Race:", "1.0"),
            List.of("", "Date:", D_2012_10_19),
            List.of("BID", "BOAT", "SAIL", "DIV", "CAS", "DESIGN", "CLUB", "SKIPPER", "START", "STARTED",
                "FINISH", "CODE", "COMMENT", "ELAPSED"),
            List.of("4323.0", "GOOD INTENT", "MYC820", "1.0", "", "Clansman 30", "MYC", "P B",
                "0.75", "0.751", "0.79", "", "", "0.04296296296"));

        var rs = importer.parseRaceSheet("test", 1, new XlsxReader.Sheet("Race 1", rows), workbook(Map.of()));

        assertNotNull(rs);
        assertEquals(LocalDate.of(2012, 10, 19), rs.date());
        assertEquals("GOOD INTENT", rs.rows().get(0).boatName());
        assertEquals("Clansman 30", rs.rows().get(0).design());
    }

    @Test
    void unsailedRaceSheetIsSkipped()
    {
        var sheet = new XlsxReader.Sheet("Race 1", newLayoutSheet("ENTRIES", List.of(
            newRow("PINTA 11", "5865.0", "1.0", "", "0.09", "PBYC", "Beneteau First 31.7"))));
        assertNull(importer.parseRaceSheet("test", 1, sheet, workbook(Map.of())));
    }

    @Test
    void dateFoundWhenRaceLabelOvertyped()
    {
        List<List<String>> rows = new ArrayList<>(newLayoutSheet("RESULTS", List.of(
            newRow("PINTA 11", "5865.0", "1.0", "", "0.09", "PBYC", "Beneteau First 31.7"))));
        rows.set(0, List.of("2.0", "7.0", "42771.0", "", "Summer Series 2016-2017"));
        var rs = importer.parseRaceSheet("test", 7, new XlsxReader.Sheet("Race 7", rows), workbook(Map.of()));
        assertNotNull(rs);
        assertEquals(LocalDate.of(2017, 2, 5), rs.date());
    }

    @Test
    void handEditedHeadersFallBackToPosition()
    {
        // "x" over BOAT, "State:" over CODE
        List<List<String>> rows = new ArrayList<>(newLayoutSheet("RESULTS", List.of()));
        rows.set(3, List.of("x", "SAIL", "DIV", "CAS", "START", "STARTED", "FINISH", "State:",
            "COMMENT", "ELAPSED"));
        rows.add(List.of("TENSIXTY", "MYC7", "1.0", "", "0.54", "0.54", "0.63", "", "", "0.086"));
        rows.add(List.of("SAN TOY", "MYC12", "1.0", "", "0.54", "", "", "DNF", "", "0.086"));
        var rs = importer.parseRaceSheet("test", 1, new XlsxReader.Sheet("Race 1", rows), workbook(Map.of()));
        assertNotNull(rs);
        assertEquals(1, rs.rows().size());
        assertEquals("TENSIXTY", rs.rows().get(0).boatName());
    }

    @Test
    void clubAndDesignRealignedPerRow()
    {
        // TL2 Race 20: a finisher row lacks the skipper cell, so CLUB/DESIGN sit one column left.
        List<String> shifted = new ArrayList<>(newRow("AIDA", "MYC6", "1.0", "", "0.063", "", ""));
        shifted.subList(12, shifted.size()).clear();
        shifted.addAll(List.of("MYC", "S80", "2094"));
        var sheet = new XlsxReader.Sheet("Race 1", newLayoutSheet("RESULTS", List.of(shifted,
            newRow("EOS", "MYC5", "1.0", "", "0.07", "MHYC", "Brittany Sloop"))));

        var rs = importer.parseRaceSheet("test", 1, sheet, workbook(Map.of()));

        assertEquals("MYC", rs.rows().get(0).club());
        assertEquals("S80", rs.rows().get(0).design());
        assertEquals("MHYC", rs.rows().get(1).club());
        assertEquals("Brittany Sloop", rs.rows().get(1).design());
    }

    @Test
    void correctsDatesAYearOut()
    {
        var row = List.<MycSpreadsheetImporter.Row>of();
        List<MycSpreadsheetImporter.RaceSheet> races = List.of(
            new MycSpreadsheetImporter.RaceSheet(1, LocalDate.of(2016, 9, 11), row),
            new MycSpreadsheetImporter.RaceSheet(2, LocalDate.of(2016, 10, 16), row),
            new MycSpreadsheetImporter.RaceSheet(3, LocalDate.of(2016, 1, 22), row),   // typo for 2017
            new MycSpreadsheetImporter.RaceSheet(4, LocalDate.of(2017, 2, 12), row),
            new MycSpreadsheetImporter.RaceSheet(5, LocalDate.of(2017, 3, 26), row));

        var fixed = importer.correctDateTypos("test", races);

        assertEquals(LocalDate.of(2017, 1, 22), fixed.get(2).date());
        assertEquals(LocalDate.of(2016, 9, 11), fixed.get(0).date());
    }

    // --- Workbook-level rules ---

    private static List<List<String>> setup(String series, String season)
    {
        return List.of(List.of("Series:", series), List.of("Season:", season));
    }

    private Map<String, List<List<String>>> book(String series, String season, List<List<String>> race1)
    {
        Map<String, List<List<String>>> m = new LinkedHashMap<>();
        m.put("Setup", setup(series, season));
        m.put("Race 1", race1);
        return m;
    }

    private static List<List<String>> oneFinisher()
    {
        return newLayoutSheet("RESULTS", List.of(
            newRow("PINTA 11", "5865.0", "1.0", "", "0.09", "PBYC", "Beneteau First 31.7")));
    }

    @Test
    void seasonSeriesNamedBySeason()
    {
        var wb = importer.parseWorkbook("t", workbook(book("Summer Series", "2019-2020", oneFinisher())));
        assertEquals("2019-2020 Summer Series", wb.seriesName());
        assertEquals("myc.org.au/2019-2020-summer-series", wb.seriesId());
        assertFalse(wb.nonSpinnaker());
    }

    @Test
    void calendarYearSeriesNamedByYear()
    {
        var wb = importer.parseWorkbook("t", workbook(book("Winter Series", "2018-2019", oneFinisher())));
        assertEquals("myc.org.au/2019-winter-series", wb.seriesId());
    }

    @Test
    void twilightSeriesIsNonSpinnaker()
    {
        var wb = importer.parseWorkbook("t", workbook(book("Twilight Series", "2019-2020", oneFinisher())));
        assertTrue(wb.nonSpinnaker());
    }

    @Test
    void onlyCombinedWomensChallengeImported()
    {
        assertNull(importer.parseWorkbook("t",
            workbook(book("Womens Challenge - Female Helm", "2018-2019", oneFinisher()))));
        var wb = importer.parseWorkbook("t",
            workbook(book("Womens challenge - combined", "2018-2019", oneFinisher())));
        assertNotNull(wb);
        assertEquals("myc.org.au/2019-helly-hansen-women-s-challenge", wb.seriesId());
    }

    @Test
    void workbookWithoutSetupIsNotResults()
    {
        assertNull(importer.parseWorkbook("t", workbook(Map.of("Sheet1", List.of(List.of("Roster"))))));
    }

    @Test
    void seriesNameFromFileNameWhenSetupHasNone()
    {
        assertEquals("Twilight Series", MycSpreadsheetImporter.seriesFromFileName("Twilight Series 2016-2017.xlsx"));
        assertEquals("Marathon Series", MycSpreadsheetImporter.seriesFromFileName("a.zip!Marathon Series.xlsx"));
    }

    // --- End to end through real xlsx bytes ---

    @Test
    void runImportsArchiveAndReplacesSailSysDemo() throws Exception
    {
        // A SailSys trial race of the same racing, which the workbook replaces.
        store.putRace(new Race("myc.org.au-2019-03-01-0001", "myc.org.au",
            List.of("myc.org.au/friday-twilights-single-race-demo"), LocalDate.of(2019, 3, 1), 1, null,
            List.of(new Division("Overall", List.of())), "SailSys-2880", Instant.now(), null));

        Path dir = Files.createDirectories(tempDir.resolve(MycSpreadsheetImporter.ARCHIVE_DIR));
        byte[] xlsx = xlsx(book("Summer Series", "2019-2020", newLayoutSheet("RESULTS", List.of(
            newRow("PINTA 11", "5865.0", "1.0", "", "0.09236111111", "PBYC", "Beneteau First 31.7"),
            newRow("TENSIXTY", "MYC7", "2.0", "", "0.08619212963", "MYC", "Radford 10.6")))));
        Files.write(dir.resolve("Summer Series 2019-2020.xlsx"), xlsx);
        // The same workbook again inside a zip is read once.
        try (ZipOutputStream z = new ZipOutputStream(Files.newOutputStream(dir.resolve("drive.zip"))))
        {
            z.putNextEntry(new ZipEntry("Summer Series 2019-2020.xlsx"));
            z.write(xlsx);
            z.closeEntry();
        }

        var result = importer.run();

        assertEquals(1, result.workbooks());
        assertEquals(1, result.races());
        assertEquals(2, result.finishers());
        assertEquals(1, result.sailsysRacesExcluded());
        assertTrue(store.isRaceExcluded("myc.org.au-2019-03-01-0001"));

        String raceId = "myc.org.au-2019-2020-summer-series-2019-09-08-0001";
        Race race = store.races().get(raceId);
        assertNotNull(race);
        assertEquals(List.of("myc.org.au/2019-2020-summer-series"), race.seriesIds());
        assertEquals(List.of("Division 1", "Division 2"), race.divisions().stream().map(Division::name).toList());

        Finisher tensixty = race.divisions().get(1).finishers().get(0);
        Boat boat = store.boats().get(tensixty.boatId());
        assertEquals("MYC7", boat.sailNumber());
        assertEquals(List.of("myc.org.au"), boat.clubIds(), "'MYC' resolves to the organising club");

        Series series = store.clubs().get("myc.org.au").series().stream()
            .filter(s -> s.id().equals("myc.org.au/2019-2020-summer-series")).findFirst().orElseThrow();
        assertEquals("2019-2020 Summer Series", series.name());
        assertEquals(List.of(raceId), series.raceIds());

        // Idempotent
        importer.run();
        assertEquals(1, store.clubs().get("myc.org.au").series().stream()
            .filter(s -> s.id().equals("myc.org.au/2019-2020-summer-series"))
            .findFirst().orElseThrow().raceIds().size());
    }

    @Test
    void reusesExistingBoatDespitePrefixedSailAndDesignSpelling() throws Exception
    {
        // PAM is held (from SailSys) as 55KA16 / Einar Ohlson International 5.5; the sheets
        // write KA16 / "5.5 Metre Class".
        Boat pam = store.findOrCreateBoat("55KA16", "Pam", "Einar Ohlson International 5.5",
            LocalDate.of(2025, 1, 1), "SailSys");
        // Two boats share sail 34 with this name: ambiguous, so neither is reused.
        store.findOrCreateBoat("34", "Twin", "J24", LocalDate.of(2025, 1, 1), "SailSys");
        store.findOrCreateBoat("34", "Twin", "Etchells", LocalDate.of(2025, 1, 1), "SailSys");

        Path dir = Files.createDirectories(tempDir.resolve(MycSpreadsheetImporter.ARCHIVE_DIR));
        Files.write(dir.resolve("Summer Series 2019-2020.xlsx"), xlsx(book("Summer Series", "2019-2020",
            newLayoutSheet("RESULTS", List.of(
                newRow("PAM", "KA16", "1.0", "", "0.09", "MYC", "5.5 Metre Class"),
                newRow("TWIN", "MYC34", "1.0", "", "0.08", "MYC", "Farr 30"))))));

        importer.run();

        Race race = store.races().get("myc.org.au-2019-2020-summer-series-2019-09-08-0001");
        List<String> ids = race.divisions().get(0).finishers().stream().map(Finisher::boatId).toList();
        assertEquals(pam.id(), ids.get(0));
        assertFalse(ids.get(1).startsWith("34-"), "ambiguous match creates a new boat: " + ids.get(1));
    }

    @Test
    void xlsxReaderReadsCachedValuesAndDates() throws Exception
    {
        var wb = XlsxReader.read(xlsx(Map.of("Setup", List.of(
            List.of("Series:", "Summer Series"), List.of("", "Dates:", D_2019_09_08)))));
        var rows = wb.sheet("Setup").rows();
        assertEquals(List.of("Series:", "Summer Series"), rows.get(0));
        assertEquals(List.of("", "Dates:", D_2019_09_08), rows.get(1));
        assertEquals(LocalDate.of(2019, 9, 8), wb.toDate(rows.get(1).get(2)));
    }

    /** A minimal xlsx: numeric-looking cells as numbers, the rest as shared strings. */
    private static byte[] xlsx(Map<String, List<List<String>>> sheets) throws IOException
    {
        List<String> shared = new ArrayList<>();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream z = new ZipOutputStream(bytes))
        {
            StringBuilder wb = new StringBuilder("<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\""
                + " xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets>");
            StringBuilder rels = new StringBuilder("<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">");
            int n = 0;
            for (var e : sheets.entrySet())
            {
                n++;
                wb.append("<sheet name=\"").append(e.getKey()).append("\" sheetId=\"").append(n)
                    .append("\" r:id=\"rId").append(n).append("\"/>");
                rels.append("<Relationship Id=\"rId").append(n).append("\" Target=\"worksheets/sheet").append(n).append(".xml\"/>");
                StringBuilder sh = new StringBuilder("<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData>");
                int r = 0;
                for (List<String> row : e.getValue())
                {
                    r++;
                    sh.append("<row r=\"").append(r).append("\">");
                    for (int c = 0; c < row.size(); c++)
                    {
                        String v = row.get(c);
                        if (v.isEmpty())
                            continue;
                        String ref = colName(c) + r;
                        if (v.matches("-?\\d+(\\.\\d+)?"))
                            sh.append("<c r=\"").append(ref).append("\"><v>").append(v).append("</v></c>");
                        else
                        {
                            shared.add(v);
                            sh.append("<c r=\"").append(ref).append("\" t=\"s\"><v>").append(shared.size() - 1).append("</v></c>");
                        }
                    }
                    sh.append("</row>");
                }
                sh.append("</sheetData></worksheet>");
                put(z, "xl/worksheets/sheet" + n + ".xml", sh.toString());
            }
            put(z, "xl/workbook.xml", wb.append("</sheets></workbook>").toString());
            put(z, "xl/_rels/workbook.xml.rels", rels.append("</Relationships>").toString());
            StringBuilder ss = new StringBuilder("<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">");
            for (String s : shared)
                ss.append("<si><t>").append(s.replace("&", "&amp;").replace("<", "&lt;")).append("</t></si>");
            put(z, "xl/sharedStrings.xml", ss.append("</sst>").toString());
        }
        return bytes.toByteArray();
    }

    private static String colName(int c)
    {
        StringBuilder sb = new StringBuilder();
        for (c++; c > 0; c = (c - 1) / 26)
            sb.insert(0, (char)('A' + (c - 1) % 26));
        return sb.toString();
    }

    private static void put(ZipOutputStream z, String name, String content) throws IOException
    {
        z.putNextEntry(new ZipEntry(name));
        z.write(content.getBytes(StandardCharsets.UTF_8));
        z.closeEntry();
    }
}
