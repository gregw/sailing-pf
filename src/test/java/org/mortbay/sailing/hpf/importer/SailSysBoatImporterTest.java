package org.mortbay.sailing.hpf.importer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SailSysBoatImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;
    private SailSysBoatImporter importer;

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        importer = new SailSysBoatImporter(store, null);
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    // --- processBoatJson ---

    @Test
    void notFoundResponseReturnsFalseAndCreatesNoBoat()
    {
        boolean result = importer.processBoatJson(
            """
            {"data":null,"errorMessage":"Boat not found","result":"error","httpCode":400,"errorObject":null}
            """);

        assertFalse(result);
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void successfulBoatWithNoHandicapsCreatesBoat()
    {
        boolean result = importer.processBoatJson(boatJson(1, "Ranger", "A1", "SASC", "Ranger", "", List.of()));

        assertTrue(result);
        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals("Ranger", boat.name());
        assertEquals("A1", boat.sailNumber());
        assertTrue(boat.certificates().isEmpty());
    }

    @Test
    void ircCertsImportedWithSpinAndNonSpinVariants()
    {
        importer.processBoatJson(boatJson(6, "Papillon", "6841", "RSAYS", "Archambault", "40",
            List.of(
                handicap("IRC", 1.0710, 1, "37582", "2023-05-31T23:59:59.000"),
                handicap("IRC", 1.0510, 2, "37582", "2023-05-31T23:59:59.000")
            )));

        Boat boat = store.boats().values().iterator().next();
        assertEquals(2, boat.certificates().size());

        Certificate spin = boat.certificates().stream()
            .filter(c -> !c.nonSpinnaker()).findFirst().orElseThrow();
        assertEquals("IRC", spin.system());
        assertEquals(1.0710, spin.value());
        assertFalse(spin.nonSpinnaker());
        assertEquals("37582", spin.certificateNumber());
        assertEquals(2022, spin.year()); // cert expiring May 2023 → cert year 2022

        Certificate noSpin = boat.certificates().stream()
            .filter(Certificate::nonSpinnaker).findFirst().orElseThrow();
        assertEquals(1.0510, noSpin.value());
        assertTrue(noSpin.nonSpinnaker());
    }

    @Test
    void ircShAndPhsAndAmsAreSkipped()
    {
        importer.processBoatJson(boatJson(7, "Test Boat", "AUS1", "CYCA", "Maker", "Model",
            List.of(
                handicap("IRC SH", 1.05, 1, "99001", "2024-05-31T23:59:59.000"),
                handicap("PHS", 0.95, 1, null, null),
                handicap("AMS", 0.88, 1, null, null),
                handicap("IRC", 1.07, 1, "99001", "2024-05-31T23:59:59.000")
            )));

        Boat boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size(), "Only IRC (not IRC SH, PHS, AMS) should be imported");
        assertEquals("IRC", boat.certificates().getFirst().system());
    }

    @Test
    void ircUpsertReplacesExistingCertsBySameCertNumber()
    {
        importer.processBoatJson(boatJson(6, "Papillon", "6841", "RSAYS", "Archambault", "40",
            List.of(handicap("IRC", 1.0710, 1, "37582", "2023-05-31T23:59:59.000"))));

        importer.processBoatJson(boatJson(6, "Papillon", "6841", "RSAYS", "Archambault", "40",
            List.of(handicap("IRC", 1.0750, 1, "37582", "2023-05-31T23:59:59.000"))));

        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size(), "Upsert should retain only the new cert");
        assertEquals(1.0750, boat.certificates().getFirst().value());
    }

    @Test
    void ircUpsertPreservesExistingOrcCerts()
    {
        // Simulate ORC cert already present (from OrcImporter)
        importer.processBoatJson(boatJson(6, "Papillon", "6841", "RSAYS", "Archambault", "40",
            List.of(handicap("IRC", 1.0710, 1, "37582", "2023-05-31T23:59:59.000"))));

        Boat boat = store.boats().values().iterator().next();
        // Manually add an ORC cert as if OrcImporter had run
        Certificate orcCert = new Certificate("ORC", 2023, 588.4, false, false, false, false, "AUS-2023-XYZ", null);
        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubId(), boat.aliases(), boat.altSailNumbers(),
            List.of(boat.certificates().getFirst(), orcCert), boat.sources(), boat.lastUpdated(), null));

        // Re-run SailSys importer with updated IRC
        importer.processBoatJson(boatJson(6, "Papillon", "6841", "RSAYS", "Archambault", "40",
            List.of(handicap("IRC", 1.0720, 1, "37582", "2024-05-31T23:59:59.000"))));

        boat = store.boats().values().iterator().next();
        assertEquals(2, boat.certificates().size(), "ORC cert should be preserved");
        assertTrue(boat.certificates().stream().anyMatch(c -> "ORC".equals(c.system())));
        assertTrue(boat.certificates().stream().anyMatch(c -> "IRC".equals(c.system()) && c.value() == 1.0720));
    }

    @Test
    void makeAndModelCombinedForDesign()
    {
        importer.processBoatJson(boatJson(1, "Test", "AUS1", "CYCA", "Archambault", "40", List.of()));

        assertEquals(1, store.designs().size());
        assertEquals("Archambault 40", store.designs().values().iterator().next().canonicalName());
    }

    @Test
    void makeUsedAloneWhenModelIsBlank()
    {
        importer.processBoatJson(boatJson(20, "Ranger", "A1", "SASC", "Ranger", "", List.of()));

        assertEquals(1, store.designs().size());
        assertEquals("Ranger", store.designs().values().iterator().next().canonicalName());
    }

    @Test
    void blankNameOrSailNumberIsSkipped()
    {
        importer.processBoatJson(boatJson(99, "", "AUS1", "CYCA", "Make", "Model", List.of()));
        assertTrue(store.boats().isEmpty(), "Blank name should be skipped");

        importer.processBoatJson(boatJson(98, "Boat", "", "CYCA", "Make", "Model", List.of()));
        assertTrue(store.boats().isEmpty(), "Blank sail number should be skipped");
    }

    @Test
    void certYearIsOneLessThanExpiryYearForMayExpiry()
    {
        importer.processBoatJson(boatJson(1, "Delta", "AUS10", "CYCA", "Make", "Model",
            List.of(handicap("IRC", 1.05, 1, "C001", "2025-05-31T23:59:59.000"))));

        Certificate cert = store.boats().values().iterator().next().certificates().getFirst();
        assertEquals(2024, cert.year(), "May expiry → cert year = expiryYear - 1");
    }

    @Test
    void certYearEqualsExpiryYearForDecemberExpiry()
    {
        importer.processBoatJson(boatJson(1, "Echo", "AUS11", "CYCA", "Make", "Model",
            List.of(handicap("IRC", 1.05, 1, "C002", "2024-12-31T23:59:59.000"))));

        Certificate cert = store.boats().values().iterator().next().certificates().getFirst();
        assertEquals(2024, cert.year(), "December expiry → cert year = expiryYear");
    }

    @Test
    void runFromDirectoryProcessesAllFiles() throws IOException
    {
        Path boatsDir = tempDir.resolve("boats-input");
        Files.createDirectories(boatsDir);

        Files.writeString(boatsDir.resolve("boat-000001.json"),
            boatJson(1, "Alpha", "AUS1", "CYCA", "Make", "Model", List.of()));
        Files.writeString(boatsDir.resolve("boat-000002.json"),
            boatJson(2, "Beta", "AUS2", "CYCA", "Make", "Model", List.of()));
        Files.writeString(boatsDir.resolve("boat-000003.json"),
            """
            {"data":null,"errorMessage":"Boat not found","result":"error","httpCode":400}
            """);

        // Use a separate DataStore pointing to a different subdir for the store itself
        DataStore testStore = new DataStore(tempDir.resolve("hpf-data"));
        testStore.start();
        SailSysBoatImporter testImporter = new SailSysBoatImporter(testStore, null);

        testImporter.runFromDirectory(boatsDir);

        assertEquals(2, testStore.boats().size());
        assertTrue(testStore.boats().values().stream().anyMatch(b -> "AUS1".equals(b.sailNumber())));
        assertTrue(testStore.boats().values().stream().anyMatch(b -> "AUS2".equals(b.sailNumber())));
        testStore.stop();
    }

    @Test
    void longNameDisambiguatesWhenShortNameIsAmbiguous()
    {
        // Seed two clubs with the same shortName but different longNames
        Club mycNsw = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", List.of(), List.of(), List.of(), null);
        Club mycVic = new Club("morningtonyc.net.au", "MYC", "Mornington Yacht Club", "VIC", List.of(), List.of(), List.of(), null);
        store.putClub(mycNsw);
        store.putClub(mycVic);

        // Boat with MYC / Manly Yacht Club — should resolve to mycNsw
        importer.processBoatJson(boatJson(767, "Esprit", "MYC32", "MYC", "Manly Yacht Club", "Archambault", "Grand Surprise", List.of()));

        Boat boat = store.boats().values().iterator().next();
        assertEquals("myc.org.au", boat.clubId(), "Should resolve via longName tiebreaker");
    }

    @Test
    void ambiguousShortNameWithNoLongNameLeavesClubNull()
    {
        Club mycNsw = new Club("myc.org.au", "MYC", "Manly Yacht Club", "NSW", List.of(), List.of(), List.of(), null);
        Club mycVic = new Club("morningtonyc.net.au", "MYC", "Mornington Yacht Club", "VIC", List.of(), List.of(), List.of(), null);
        store.putClub(mycNsw);
        store.putClub(mycVic);

        // No longName provided — can't disambiguate
        importer.processBoatJson(boatJson(1, "Unknown", "AUS1", "MYC", "", "Make", "Model", List.of()));

        Boat boat = store.boats().values().iterator().next();
        assertNull(boat.clubId(), "Ambiguous shortName with no longName should leave clubId null");
    }

    // --- Helpers ---

    /** Convenience overload with empty clubLongName. */
    private String boatJson(int id, String name, String sailNo, String clubShort,
                             String make, String model, List<String> handicaps)
    {
        return boatJson(id, name, sailNo, clubShort, "", make, model, handicaps);
    }

    private String boatJson(int id, String name, String sailNo, String clubShort, String clubLong,
                             String make, String model, List<String> handicaps)
    {
        String hcapJson = String.join(",", handicaps);
        return """
            {"data":{"id":%d,"name":"%s","sailNumber":"%s",
             "clubShortName":"%s","clubLongName":"%s","make":"%s","model":"%s",
             "handicaps":[%s]},
             "result":"success","errorMessage":null}
            """.formatted(id, name, sailNo, clubShort, clubLong, make, model, hcapJson);
    }

    private String handicap(String system, double value, int spinnakerType,
                             String certNumber, String expiryDate)
    {
        String certJson = certNumber != null
            ? """
              {"certificateNumber":"%s","expiryDate":"%s"}
              """.formatted(certNumber, expiryDate != null ? expiryDate : "")
            : "null";
        return """
            {"definition":{"shortName":"%s"},"value":%s,"spinnakerType":%d,"certificate":%s}
            """.formatted(system, value, spinnakerType, certJson);
    }
}
