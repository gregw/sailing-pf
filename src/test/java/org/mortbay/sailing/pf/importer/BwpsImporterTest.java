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

    private CannedImporter canned()
    {
        CannedImporter imp = new CannedImporter(store);
        imp.responses.put("/race/2025/results", INDEX_2025);
        imp.responses.put("/race/2025/results/flinders-islet-race",
            "<script>(function(){const raceId = 187; window.raceConfig = {};})();</script>");
        imp.responses.put("/Race/Summary/187", SUMMARY_187);
        imp.responses.put("/Results/Final/187/1068", LH_187);
        imp.responses.put("/Results/Final/187/1071", IRC_187);
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
    void heldRaceIsNotFetchedAgain()
    {
        canned().importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertEquals(1, store.races().size());

        // 2025-09-20 is long past the recent-reimport window: only the index is fetched.
        CannedImporter again = canned();
        again.importBwpsResults(2025, 2025, new LinkedHashMap<>());
        assertEquals(List.of(BwpsImporter.BASE_URL + "/race/2025/results",
                BwpsImporter.BASE_URL + "/race/2025/yachts"), again.fetched);
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

    @Test
    void missingIndexYearIsSkipped()
    {
        CannedImporter imp = canned();
        imp.importBwpsResults(2025, 2026, new LinkedHashMap<>());   // 2026 index is a 404
        assertEquals(1, store.races().size(), "2025 still imported");
        assertEquals(LocalDate.of(2025, 9, 20), store.races().values().iterator().next().date());
    }
}
