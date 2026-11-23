package org.mortbay.sailing.pf.importer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The BWPS minor-race import: results index → race page raceId → CYCA feeds (Race/Summary for
 * the start, Line Honours for elapsed times, IRC for ratings). Responses are canned.
 */
class BwpsImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    /** An importer answering from a URL-substring → body map, recording every URL fetched. */
    static class CannedImporter extends BwpsImporter
    {
        final Map<String, String> responses = new LinkedHashMap<>();
        final List<String> fetched = new ArrayList<>();

        CannedImporter(DataStore store)
        {
            super(store, null);
            yachtPageDelayMs = 0;
        }

        @Override
        String fetchString(String url) throws Exception
        {
            fetched.add(url);
            for (Map.Entry<String, String> e : responses.entrySet())
            {
                if (url.endsWith(e.getKey()))
                    return e.getValue();
            }
            throw new RuntimeException("HTTP 404 for " + url);
        }

        @Override
        String fetchHtml(String url) throws Exception
        {
            return fetchString(url);
        }
    }

    private static final String INDEX_2025 = """
        <a href="/race/2025/results">Results</a>
        <a href="/race/2025/results/flinders-islet-race">Flinders Islet Race</a>
        <a href="/race/2025/results/rolex-sydney-hobart-yacht-race">Hobart</a>
        <a href="/race/2025/results/flinders-islet-race">again</a>
        <a href="/race/2024/results/flinders-islet-race">last year</a>
        """;

    private static final String SUMMARY_187 = """
        {"RaceId":187,"Status":"Race completed","StartDateTime":"2025-09-20T10:00:00",
         "Categories":[
          {"Id":1071,"Name":"IRC","Type":"Handicap"},
          {"Id":1072,"Name":"PHS","Type":"Handicap"},
          {"Id":1068,"Name":"Line Honours","Type":"Line Honours"}]}
        """;

    // Line Honours: CorrectedTime is the finish (TCF 1). Speedy retired.
    private static final String LH_187 = """
        [{"NameRace":"Moneypenny","SailNumber":"AUS10","Status":"Finished","IsFinished":true,
          "TCF":1.0,"DivisionName":"","CorrectedTime":"2025-09-20T16:15:44"},
         {"NameRace":"Disko Trooper (DH)","SailNumber":"AUS99","Status":"Finished","IsFinished":true,
          "TCF":1.0,"DivisionName":"","CorrectedTime":"2025-09-20T21:30:00"},
         {"NameRace":"Speedy","SailNumber":"1234","Status":"Retired","IsFinished":false,
          "TCF":1.0,"DivisionName":"","CorrectedTime":"0001-01-01T00:00:00"}]
        """;

    // IRC: ratings only; the times here are corrected and must not be used.
    private static final String IRC_187 = """
        [{"NameRace":"Moneypenny","SailNumber":"AUS10","Status":"Finished","IsFinished":true,
          "TCF":1.6,"DivisionName":"1","CorrectedTime":"2025-09-21T02:00:00"},
         {"NameRace":"Disko Trooper (DH)","SailNumber":"AUS99","Status":"Finished","IsFinished":true,
          "TCF":0.996,"DivisionName":"2","CorrectedTime":"2025-09-20T21:26:52"},
         {"NameRace":"Speedy","SailNumber":"1234","Status":"Retired","IsFinished":false,
          "TCF":1.1,"DivisionName":"2","CorrectedTime":"0001-01-01T00:00:00"}]
        """;

    // The race's yacht list: Moneypenny links its own page; Disko Trooper is not listed.
    private static final String YACHTS_187 = """
        <table><tr><th>Yacht Name</th><th>Sail Number</th><th>State</th><th>Type</th></tr>
        <tr><td><a href="/race/2025/yachts/flinders-islet-race/moneypenny/">Moneypenny</a> AUS10</td>
            <td>AUS10</td><td>NSW</td><td>TP52</td></tr></table>
        """;

    private void putClub(String id, String shortName, String longName)
    {
        store.putClub(new Club(id, shortName, longName, "NSW", false, null, List.of(), List.of(),
            List.of(), List.of(), null));
    }

    private CannedImporter canned()
    {
        CannedImporter imp = new CannedImporter(store);
        imp.responses.put("/race/2025/results", INDEX_2025);
        imp.responses.put("/race/2025/results/flinders-islet-race",
            "<script>(function(){const raceId = 187; window.raceConfig = {};})();</script>");
        imp.responses.put("/Race/Summary/187", SUMMARY_187);
        imp.responses.put("/Results/Final/187/1068", LH_187);
        imp.responses.put("/Results/Final/187/1071", IRC_187);
        imp.responses.put("/race/2025/yachts/flinders-islet-race", YACHTS_187);
        imp.responses.put("/race/2025/yachts/flinders-islet-race/moneypenny/",
            "<table><tr><th>Yacht Name</th><td>Moneypenny</td></tr>"
            + "<tr><th>Club</th><td>MHYC</td></tr><tr><th>Type</th><td>TP52</td></tr></table>");
        return imp;
    }

    @Test
    void parsesResultsIndexAndRaceNames()
    {
        assertEquals(List.of("flinders-islet-race", "rolex-sydney-hobart-yacht-race"),
            BwpsImporter.parseResultsIndex(INDEX_2025, 2025));
        assertEquals("Flinders Islet Race", BwpsImporter.raceNameFromSlug("flinders-islet-race"));
        assertEquals("Bird Island Race", BwpsImporter.raceNameFromSlug("bird-island-race"));
    }

    @Test
    void importsMinorRaceWithLineHonoursElapsedAndIrcRatings()
    {
        CannedImporter imp = canned();
        imp.importBwpsResults(2025, 2025, new LinkedHashMap<>());

        assertEquals(1, store.races().size());
        Race race = store.races().get("cyca.com.au-2025-09-20-0001");
        assertNotNull(race, store.races().keySet().toString());
        assertEquals("Flinders Islet Race", race.name());
        assertEquals(BwpsImporter.SOURCE, race.source());
        assertEquals(List.of(IdGenerator.generateSeriesId(BwpsImporter.CLUB_ID, "Blue Water Pointscore 2025")),
            race.seriesIds());

        Map<String, Division> divs = new LinkedHashMap<>();
        race.divisions().forEach(d -> divs.put(d.name(), d));
        assertEquals(List.of("IRC Div 1", "IRC Div 2 Two-Handed"), List.copyOf(divs.keySet()),
            "the (DH) entry is a two-handed result; the retired boat is dropped");

        Finisher moneypenny = divs.get("IRC Div 1").finishers().getFirst();
        assertEquals(Duration.ofHours(6).plusMinutes(15).plusSeconds(44), moneypenny.elapsedTime(),
            "elapsed = Line Honours finish − start, not the IRC corrected time");

        Finisher disko = divs.get("IRC Div 2 Two-Handed").finishers().getFirst();
        assertEquals(Duration.ofHours(11).plusMinutes(30), disko.elapsedTime());
        Boat diskoBoat = store.boats().get(disko.boatId());
        assertEquals("Disko Trooper", diskoBoat.name(), "(DH) stripped from the name");
        Certificate cert = diskoBoat.certificates().getFirst();
        assertEquals("IRC", cert.system());
        assertEquals(0.996, cert.value(), 1e-9);
        assertTrue(cert.twoHanded());

        assertFalse(imp.fetched.stream().anyMatch(u -> u.contains("hobart")),
            "Hobart is left to phase 2: " + imp.fetched);
    }

    @Test
    void phsAddsOnlyBoatsNotInIrc()
    {
        CannedImporter imp = canned();
        // Line Honours also has a PHS-only boat.
        imp.responses.put("/Results/Final/187/1068", LH_187.replace("[{", """
            [{"NameRace":"Cruiser","SailNumber":"5555","Status":"Finished","IsFinished":true,
              "TCF":1.0,"DivisionName":"","CorrectedTime":"2025-09-20T22:00:00"},
             {"""));
        // PHS rescores Moneypenny (already in IRC) and adds Cruiser.
        imp.responses.put("/Results/Final/187/1072", """
            [{"NameRace":"Moneypenny","SailNumber":"AUS10","Status":"Finished","IsFinished":true,
              "TCF":1.2,"DivisionName":"","CorrectedTime":"2025-09-20T23:00:00"},
             {"NameRace":"Cruiser","SailNumber":"5555","Status":"Finished","IsFinished":true,
              "TCF":0.8,"DivisionName":"","CorrectedTime":"2025-09-20T19:00:00"}]
            """);
        imp.importBwpsResults(2025, 2025, new LinkedHashMap<>());

        Race race = store.races().get("cyca.com.au-2025-09-20-0001");
        Map<String, Division> divs = new LinkedHashMap<>();
        race.divisions().forEach(d -> divs.put(d.name(), d));
        assertEquals(List.of("IRC Div 1", "IRC Div 2 Two-Handed", "PHS"), List.copyOf(divs.keySet()));
        List<Finisher> phs = divs.get("PHS").finishers();
        assertEquals(1, phs.size(), "Moneypenny is not counted again");
        assertEquals(Duration.ofHours(12), phs.getFirst().elapsedTime(), "elapsed from Line Honours");
        assertTrue(store.boats().get(phs.getFirst().boatId()).certificates().isEmpty(),
            "PHS is not a certificate");
    }

    @Test
    void yachtPageFillsInClubAndListGivesDesign()
    {
        putClub("mhyc.com.au", "MHYC", "Middle Harbour Yacht Club");
        canned().importBwpsResults(2025, 2025, new LinkedHashMap<>());

        Boat moneypenny = store.boats().values().stream()
            .filter(b -> "Moneypenny".equals(b.name())).findFirst().orElseThrow();
        assertEquals(List.of("mhyc.com.au"), moneypenny.clubIds());
        assertEquals("tp52", moneypenny.designId());

        Boat disko = store.boats().values().stream()
            .filter(b -> "Disko Trooper".equals(b.name())).findFirst().orElseThrow();
        assertTrue(disko.clubIds().isEmpty(), "not on the yacht list: nothing to fill in");
    }

    @Test
    void yachtPageDoesNotOverwriteAHeldClub()
    {
        putClub("mhyc.com.au", "MHYC", "Middle Harbour Yacht Club");
        putClub("rpayc.com.au", "RPAYC", "Royal Prince Alfred Yacht Club");
        store.putBoat(new Boat("AUS10-moneypenny-tp52", "AUS10", "Moneypenny", "tp52",
            List.of("rpayc.com.au"), List.of(), List.of("Test"), null, null));
        canned().importBwpsResults(2025, 2025, new LinkedHashMap<>());
        List<Boat> moneypennys = store.boats().values().stream()
            .filter(b -> "Moneypenny".equals(b.name())).toList();
        assertEquals(1, moneypennys.size(), "the held boat is used, not a new one");
        assertEquals(List.of("rpayc.com.au"), moneypennys.getFirst().clubIds());
    }

    @Test
    void heldRaceIsNotFetchedAgain()
    {
        canned().importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertEquals(1, store.races().size());

        // 2025-09-20 is long past the recent-reimport window: only the index is fetched.
        CannedImporter again = canned();
        again.importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertEquals(List.of(BwpsImporter.BASE_URL + "/race/2025/results"), again.fetched);
        assertEquals(1, store.races().size());
    }

    @Test
    void raceWithoutStartIsSkipped()
    {
        CannedImporter imp = canned();
        imp.responses.put("/Race/Summary/187",
            "{\"RaceId\":187,\"StartDateTime\":\"0001-01-01T00:00:00\",\"Categories\":[]}");
        imp.importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertTrue(store.races().isEmpty());
    }

    @Test
    void raceNotYetSailedIsSkippedWithoutFetchingResults()
    {
        CannedImporter imp = canned();
        imp.responses.put("/Race/Summary/187", SUMMARY_187.replace("2025-09-20T10:00:00",
            LocalDate.now().plusYears(1) + "T10:00:00"));
        imp.importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertTrue(store.races().isEmpty());
        assertFalse(imp.fetched.stream().anyMatch(u -> u.contains("/Results/")), imp.fetched.toString());
    }

    // --- Major races, from their own sites ---

    private static final String HOBART = "https://rolexsydneyhobart.com";

    /** Canned Hobart responses for a year: results page → raceId, summary, LH and IRC. */
    private static void putHobart(CannedImporter imp, int year, int raceId, String start,
                                  String lhFinish, String sailNumber)
    {
        imp.responses.put(HOBART + "/race/" + year + "/results",
            "<script>const raceId = " + raceId + ";</script>");
        imp.responses.put("/Race/Summary/" + raceId, """
            {"RaceId":%d,"Year":%d,"StartDateTime":"%s","Categories":[
              {"Id":%d1,"Name":"Line Honours","Type":"Line Honours"},
              {"Id":%d2,"Name":"IRC","Type":"Handicap"}]}
            """.formatted(raceId, year, start, raceId, raceId));
        imp.responses.put("/Results/Final/" + raceId + "/" + raceId + "1", """
            [{"NameRace":"Wild Oats XI","SailNumber":"%s","Status":"Finished","TCF":0.0,
              "DivisionName":"","CorrectedTime":"%s"}]
            """.formatted(sailNumber, lhFinish));
        imp.responses.put("/Results/Final/" + raceId + "/" + raceId + "2", """
            [{"NameRace":"Wild Oats XI","SailNumber":"%s","Status":"Finished","TCF":1.9,
              "DivisionName":"0","CorrectedTime":"%s"}]
            """.formatted(sailNumber, lhFinish));
    }

    @Test
    void majorRaceTakesItsStartAndCategoriesFromTheFeed()
    {
        CannedImporter imp = new CannedImporter(store);
        // The 2005 Hobart started at 13:20, not the usual 13:00.
        putHobart(imp, 2005, 73, "2005-12-26T13:20:00", "2005-12-28T07:20:00", "AUS10001");

        assertEquals(BwpsImporter.Outcome.IMPORTED,
            imp.importMajorRace(BwpsImporter.RSHYR, 2005, new LinkedHashMap<>()));

        Race race = store.races().get("cyca.com.au-2005-12-26-0001");
        assertNotNull(race, store.races().keySet().toString());
        assertEquals(BwpsImporter.RSHYR_SOURCE, race.source());
        assertEquals("Rolex Sydney Hobart Yacht Race", race.name());
        assertEquals(List.of(
                IdGenerator.generateSeriesId(BwpsImporter.CLUB_ID, "Blue Water Pointscore 2005"),
                IdGenerator.generateSeriesId(BwpsImporter.CLUB_ID, "Rolex Sydney Hobart Yacht Race")),
            race.seriesIds());
        assertEquals(Duration.ofHours(42), race.divisions().getFirst().finishers().getFirst().elapsedTime());

        // Held now: a second run fetches nothing.
        CannedImporter again = new CannedImporter(store);
        assertEquals(BwpsImporter.Outcome.HELD,
            again.importMajorRace(BwpsImporter.RSHYR, 2005, new LinkedHashMap<>()));
        assertTrue(again.fetched.isEmpty(), again.fetched.toString());
    }

    @Test
    void yearWithoutARaceIsAbsent()
    {
        CannedImporter imp = new CannedImporter(store);
        // Hobart 2020 (cancelled): the page exists but has no raceId.
        imp.responses.put(HOBART + "/race/2020/results", "<html>No results</html>");
        assertEquals(BwpsImporter.Outcome.ABSENT,
            imp.importMajorRace(BwpsImporter.RSHYR, 2020, new LinkedHashMap<>()));
        // A page whose raceId is another year's race.
        putHobart(imp, 2019, 144, "2019-12-26T13:00:00", "2019-12-28T13:00:00", "AUS1");
        imp.responses.put(HOBART + "/race/2021/results", "<script>const raceId = 144;</script>");
        assertEquals(BwpsImporter.Outcome.ABSENT,
            imp.importMajorRace(BwpsImporter.RSHYR, 2021, new LinkedHashMap<>()));
        assertTrue(store.races().isEmpty());
    }

    @Test
    void raceMostlyWithoutSailNumbersIsSkipped()
    {
        CannedImporter imp = new CannedImporter(store);
        putHobart(imp, 1995, 65, "1995-12-26T13:00:00", "1995-12-29T13:00:00", "");
        assertEquals(BwpsImporter.Outcome.ABSENT,
            imp.importMajorRace(BwpsImporter.RSHYR, 1995, new LinkedHashMap<>()));
        assertTrue(store.races().isEmpty());
    }

    @Test
    void historyWalksBackUntilThreeMissingYears() throws Exception
    {
        int y = LocalDate.now().getYear();
        CannedImporter imp = new CannedImporter(store);
        // Hobart held two and four years ago, not three years ago (one-year gap, kept going).
        putHobart(imp, y - 2, 902, (y - 2) + "-12-26T13:00:00", (y - 2) + "-12-28T13:00:00", "AUS1");
        putHobart(imp, y - 4, 904, (y - 4) + "-12-26T13:00:00", (y - 4) + "-12-28T13:00:00", "AUS1");

        imp.runHistory(30);

        assertNotNull(store.races().get("cyca.com.au-" + (y - 2) + "-12-26-0001"));
        assertNotNull(store.races().get("cyca.com.au-" + (y - 4) + "-12-26-0001"));
        List<String> hobartPages = imp.fetched.stream().filter(u -> u.startsWith(HOBART)).toList();
        assertTrue(hobartPages.contains(HOBART + "/race/" + (y - 7) + "/results"), hobartPages.toString());
        assertFalse(hobartPages.contains(HOBART + "/race/" + (y - 8) + "/results"),
            "stops after three missing years: " + hobartPages);
        // The BWPS index is missing every year: three tries, then the major races.
        assertEquals(3, imp.fetched.stream().filter(u -> u.startsWith(BwpsImporter.BASE_URL + "/race/")).count());
    }

    @Test
    void missingIndexYearIsSkipped()
    {
        CannedImporter imp = canned();
        imp.importBwpsResults(2025, 2026, new LinkedHashMap<>());   // 2026 index is a 404
        assertEquals(1, store.races().size(), "2025 still imported");
        assertEquals(LocalDate.of(2025, 9, 20), store.races().values().iterator().next().date());
    }
}
