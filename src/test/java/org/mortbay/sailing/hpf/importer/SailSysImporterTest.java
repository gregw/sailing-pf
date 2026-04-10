package org.mortbay.sailing.hpf.importer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SailSysImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;
    private SailSysImporter importer;

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        importer = new SailSysImporter(store, null);
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    // --- processRaceJson ---

    @Test
    void notFoundResponseReturnsFalseAndCreatesNoRace()
    {
        boolean result = importer.processRaceJson(
            """
            {"data":null,"errorMessage":"Race not found","result":"error","httpCode":400}
            """);

        assertFalse(result);
        assertTrue(store.races().isEmpty());
    }

    @Test
    void statusNot4IsSkipped()
    {
        boolean result = importer.processRaceJson(raceJson(1, 2, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship", "PHS", false, List.of()));

        assertFalse(result);
        assertTrue(store.races().isEmpty());
    }

    @Test
    void nullLastProcessedTimeIsSkipped()
    {
        boolean result = importer.processRaceJson(raceJsonNullProcessed(1, 4, "2020-09-13T00:00:00.000",
            1, "MYC", "Manly Yacht Club", "Club Championship", "PHS", false, List.of()));

        assertFalse(result);
        assertTrue(store.races().isEmpty());
    }

    @Test
    void phsRaceImportedWithoutCertificateNumber()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        boolean result = importer.processRaceJson(raceJson(1, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship", "PHS", false,
            List.of(entry("Shear Magic", "MYC100", "1:09:42", false, null))));

        assertTrue(result);
        assertEquals(1, store.races().size());

        Race race = store.races().values().iterator().next();
        assertEquals("PHS", race.handicapSystem());
        assertEquals(1, race.divisions().size());

        Finisher finisher = race.divisions().getFirst().finishers().getFirst();
        assertEquals(Duration.ofHours(1).plusMinutes(9).plusSeconds(42), finisher.elapsedTime());
        assertNull(finisher.certificateNumber(), "PHS finisher should have null certificateNumber");
    }

    @Test
    void ircRaceImportedWithCertificateNumber()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        boolean result = importer.processRaceJson(raceJson(2, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship", "IRC", false,
            List.of(entry("Raging Bull", "AUS1234", "1:09:42", false, 1.071))));

        assertTrue(result);
        assertEquals(1, store.races().size());

        Race race = store.races().values().iterator().next();
        assertEquals("IRC", race.handicapSystem());

        Finisher finisher = race.divisions().getFirst().finishers().getFirst();
        assertNotNull(finisher.certificateNumber(), "IRC finisher should have a certificateNumber");
        assertTrue(finisher.certificateNumber().startsWith("irc-inferred-"),
            "Unknown cert should be inferred");

        Boat boat = store.boats().values().stream()
            .filter(b -> "AUS1234".equals(b.sailNumber()))
            .findFirst().orElseThrow();
        assertEquals(1, boat.certificates().size());
        Certificate cert = boat.certificates().getFirst();
        assertEquals("IRC", cert.system());
        assertEquals(1.071, cert.value());
        assertEquals(finisher.certificateNumber(), cert.certificateNumber());
    }

    @Test
    void ircRaceReusesExistingCertificateBySystemAndValue()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        Certificate existingCert = new Certificate("IRC", 2020, 1.071, false, false, false, false, "CERT-12345", null);
        Boat boat = new Boat("AUS1234-raging_bull", "AUS1234", "Raging Bull", null, "myc.com.au",
            List.of(), List.of(), List.of(existingCert), List.of(), null, null);
        store.putBoat(boat);

        importer.processRaceJson(raceJson(2, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship", "IRC", false,
            List.of(entry("Raging Bull", "AUS1234", "1:09:42", false, 1.071))));

        Race race = store.races().values().iterator().next();
        Finisher finisher = race.divisions().getFirst().finishers().getFirst();
        assertEquals("CERT-12345", finisher.certificateNumber(),
            "Should reuse existing cert number when system and value match");
    }

    @Test
    void unknownClubLeavesClubIdNull()
    {
        importer.processRaceJson(raceJson(1, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "XYZ", "Unknown Yacht Club",
            "Some Series", "PHS", false,
            List.of(entry("Shear Magic", "MYC100", "1:09:42", false, null))));

        assertEquals(1, store.races().size());
        Race race = store.races().values().iterator().next();
        assertNull(race.clubId(), "Unresolved club should leave clubId null");
    }

    @Test
    void seriesCreatedOnFirstRace()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        importer.processRaceJson(raceJson(1, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship 2020-21", "PHS", false,
            List.of(entry("Shear Magic", "MYC100", "1:09:42", false, null))));

        Club updated = store.clubs().get("myc.org.au");
        assertEquals(1, updated.series().size());
        Series series = updated.series().getFirst();
        assertEquals("myc.org.au/club-championship-2020-21", series.id());
        assertEquals("Club Championship 2020-21", series.name());
        assertEquals(1, series.raceIds().size());
    }

    @Test
    void seriesUpdatedOnSubsequentRace()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        importer.processRaceJson(raceJson(1, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship 2020-21", "PHS", false,
            List.of(entry("Shear Magic", "MYC100", "1:09:42", false, null))));

        importer.processRaceJson(raceJson(2, 4, "2020-09-20T00:00:00.000",
            "2020-09-20T15:00:00.000", 2, "MYC", "Manly Yacht Club",
            "Club Championship 2020-21", "PHS", false,
            List.of(entry("Tensixty", "MYC7", "1:07:37", false, null))));

        Club updated = store.clubs().get("myc.org.au");
        assertEquals(1, updated.series().size());
        assertEquals(2, updated.series().getFirst().raceIds().size(),
            "Both races should appear in the series raceIds");
    }

    @Test
    void dnsFinishersExcluded()
    {
        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        importer.processRaceJson(raceJson(1, 4, "2020-09-13T00:00:00.000",
            "2020-09-13T15:00:00.000", 1, "MYC", "Manly Yacht Club",
            "Club Championship", "PHS", false,
            List.of(
                entry("Shear Magic", "MYC100", "1:09:42", false, null),
                entryDns("Tensixty", "MYC7")
            )));

        Race race = store.races().values().iterator().next();
        assertEquals(1, race.divisions().getFirst().finishers().size(),
            "DNS entry should not appear as a finisher");
    }

    @Test
    void runFromDirectoryProcessesAllEligibleFiles() throws IOException
    {
        Path racesDir = tempDir.resolve("races-input");
        Files.createDirectories(racesDir);

        Club myc = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(myc);

        Files.writeString(racesDir.resolve("race-000001.json"),
            raceJson(1, 4, "2020-09-13T00:00:00.000", "2020-09-13T15:00:00.000",
                1, "MYC", "Manly Yacht Club", "Club Championship", "PHS", false,
                List.of(entry("Shear Magic", "MYC100", "1:09:42", false, null))));
        Files.writeString(racesDir.resolve("race-000002.json"),
            raceJson(2, 4, "2020-09-20T00:00:00.000", "2020-09-20T15:00:00.000",
                2, "MYC", "Manly Yacht Club", "Club Championship", "PHS", false,
                List.of(entry("Tensixty", "MYC7", "1:07:37", false, null))));
        // Status 2 — should be skipped
        Files.writeString(racesDir.resolve("race-000003.json"),
            raceJson(3, 2, "2020-10-01T00:00:00.000", "2020-10-01T15:00:00.000",
                3, "MYC", "Manly Yacht Club", "Club Championship", "PHS", false,
                List.of(entry("Mondo", "5656", "1:10:00", false, null))));

        DataStore testStore = new DataStore(tempDir.resolve("hpf-data"));
        testStore.start();
        Club myc2 = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", false, List.of(), List.of(), List.of(), null);
        testStore.putClub(myc2);
        SailSysImporter testImporter = new SailSysImporter(testStore, null);

        testImporter.runFromDirectory(racesDir);

        assertEquals(2, testStore.races().size(), "Only status=4 races should be imported");
        testStore.stop();
    }

    // --- peekRaceDate ---

    @Test
    void peekRaceDateReturnsCorrectDate()
    {
        String json = raceJson(1, 4, "2020-09-13T00:00:00.000", "2020-09-13T15:00:00.000",
            1, "MYC", "Manly Yacht Club", "Series", "PHS", false, List.of());

        LocalDate date = importer.peekRaceDate(json);

        assertEquals(LocalDate.of(2020, 9, 13), date);
    }

    // --- run() ---

    @Test
    void runReturnsMinRecentIdFromCachedFiles() throws Exception
    {
        Path racesDir = tempDir.resolve("races-recent");
        Files.createDirectories(racesDir);

        LocalDate recentDate1 = LocalDate.now();
        LocalDate recentDate2 = LocalDate.now().minusDays(10);
        LocalDate oldDate     = LocalDate.now().minusDays(60);

        Files.writeString(racesDir.resolve("race-000001.json"),
            raceJson(1, 4, recentDate1 + "T00:00:00.000", "2026-01-01T00:00:00.000",
                1, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));
        Files.writeString(racesDir.resolve("race-000002.json"),
            raceJson(2, 4, recentDate2 + "T00:00:00.000", "2026-01-01T00:00:00.000",
                2, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));
        Files.writeString(racesDir.resolve("race-000003.json"),
            raceJson(3, 4, oldDate + "T00:00:00.000", "2025-01-01T00:00:00.000",
                3, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));

        int[] count = {0};
        int minRecentId = importer.run(1, id -> {}, () -> ++count[0] >= 3,
            racesDir, 7, 352, 365, 0, 30, 1000);

        assertEquals(1, minRecentId, "Should return the lowest recent ID");
    }

    @Test
    void runReturnsZeroWhenNoRecentRaces() throws Exception
    {
        Path racesDir = tempDir.resolve("races-old");
        Files.createDirectories(racesDir);

        LocalDate oldDate1 = LocalDate.now().minusDays(60);
        LocalDate oldDate2 = LocalDate.now().minusDays(90);
        LocalDate oldDate3 = LocalDate.now().minusDays(120);

        Files.writeString(racesDir.resolve("race-000001.json"),
            raceJson(1, 4, oldDate1 + "T00:00:00.000", "2025-01-01T00:00:00.000",
                1, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));
        Files.writeString(racesDir.resolve("race-000002.json"),
            raceJson(2, 4, oldDate2 + "T00:00:00.000", "2025-01-01T00:00:00.000",
                2, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));
        Files.writeString(racesDir.resolve("race-000003.json"),
            raceJson(3, 4, oldDate3 + "T00:00:00.000", "2025-01-01T00:00:00.000",
                3, "MYC", "Manly Yacht Club", "Series A", "PHS", false, List.of()));

        int[] count = {0};
        int minRecentId = importer.run(1, id -> {}, () -> ++count[0] >= 3,
            racesDir, 7, 352, 365, 0, 30, 1000);

        assertEquals(0, minRecentId, "No recent races: should return 0");
    }

    // --- Helpers ---

    private String raceJson(int id, int status, String dateTime, String lastProcessedTime,
                            int number, String clubShort, String clubLong,
                            String seriesName, String handicapSystem, boolean pursuit,
                            List<String> items)
    {
        String itemsJson = String.join(",", items);
        return """
            {"result":"success","data":{
              "id":%d,"status":%d,"dateTime":"%s","lastProcessedTime":"%s",
              "number":%d,"name":null,"offsetPursuitRace":%b,
              "club":{"shortName":"%s","longName":"%s"},
              "series":{"name":"%s"},
              "handicappings":[{"id":1,"shortName":"%s"}],
              "competitors":[{"parent":{"name":"Division 1"},"items":[%s]}]
            }}
            """.formatted(id, status, dateTime, lastProcessedTime, number, pursuit,
                clubShort, clubLong, seriesName, handicapSystem, itemsJson);
    }

    private String raceJsonNullProcessed(int id, int status, String dateTime,
                                         int number, String clubShort, String clubLong,
                                         String seriesName, String handicapSystem, boolean pursuit,
                                         List<String> items)
    {
        String itemsJson = String.join(",", items);
        return """
            {"result":"success","data":{
              "id":%d,"status":%d,"dateTime":"%s","lastProcessedTime":null,
              "number":%d,"name":null,"offsetPursuitRace":%b,
              "club":{"shortName":"%s","longName":"%s"},
              "series":{"name":"%s"},
              "handicappings":[{"id":1,"shortName":"%s"}],
              "competitors":[{"parent":{"name":"Division 1"},"items":[%s]}]
            }}
            """.formatted(id, status, dateTime, number, pursuit,
                clubShort, clubLong, seriesName, handicapSystem, itemsJson);
    }

    private String entry(String name, String sailNo, String elapsed, boolean nonSpin,
                         Double handicapCreatedFrom)
    {
        String hcFrom = handicapCreatedFrom != null ? handicapCreatedFrom.toString() : "null";
        return """
            {"boat":{"name":"%s","sailNumber":"%s"},
             "elapsedTime":"%s","nonSpinnaker":%b,
             "calculations":[{"handicapDefinitionId":1,"handicapCreatedFrom":%s}]}
            """.formatted(name, sailNo, elapsed, nonSpin, hcFrom);
    }

    private String entryDns(String name, String sailNo)
    {
        return """
            {"boat":{"name":"%s","sailNumber":"%s"},
             "elapsedTime":null,"nonSpinnaker":false,
             "calculations":[]}
            """.formatted(name, sailNo);
    }
}
