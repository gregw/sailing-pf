package org.mortbay.sailing.hpf.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.data.Series;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataStoreTest {

    // --- Load from pre-built test data ---

    @Test
    void loadAllRacesFromTestData() throws URISyntaxException {
        DataStore store = testDataStore();
        store.start();

        assertEquals(2, store.races().size());
        Race race = store.races().get("myc.com.au-2020-09-13-0001");
        assertNotNull(race);

        assertEquals("myc.com.au", race.clubId());
        assertEquals(LocalDate.of(2020, 9, 13), race.date());
        assertEquals(2, race.divisions().size());

        Division div1 = race.divisions().getFirst();
        assertEquals("Division 1", div1.name());
        assertEquals(4, div1.finishers().size());

        Finisher shearMagic = div1.finishers().getFirst();
        assertEquals("MYC100-shearmagic-adams10", shearMagic.boatId());
        assertEquals(Duration.ofMinutes(69).plusSeconds(42), shearMagic.elapsedTime());
        assertFalse(shearMagic.nonSpinnaker());

        Finisher sanToy = div1.finishers().get(2);
        assertEquals("MYC12-santoy", sanToy.boatId());
        assertTrue(sanToy.nonSpinnaker());
    }

    @Test
    void loadCatalogueFromTestData() throws URISyntaxException {
        DataStore store = testDataStore();
        store.start();

        assertEquals(7, store.boats().size());
        Boat tensixty = store.boats().get("MYC7-tensixty-radford1060");
        assertNotNull(tensixty);
        assertEquals("Tensixty", tensixty.name());
        assertEquals("radford1060", tensixty.designId());

        assertEquals(1, store.clubs().size());
        Club club = store.clubs().values().iterator().next();
        assertEquals("MYC", club.shortName());
        assertEquals("NSW", club.state());

        assertEquals(1, club.series().size());
        Series series = club.series().getFirst();
        assertEquals("myc.com.au/club-championship", series.id());
        assertFalse(series.isCatchAll());
        assertEquals(List.of("myc.com.au-2020-09-13-0001", "myc.com.au-2020-09-20-0002"), series.raceIds());

        assertEquals(4, store.designs().size());
        Design radford = store.designs().get("radford1060");
        assertNotNull(radford);
        assertEquals(List.of("1060"), radford.aliases());

        Boat mondo = store.boats().get("5656-mondo-sydney38");
        assertNotNull(mondo);
        assertEquals(1, mondo.certificates().size());
        Certificate cert = mondo.certificates().getFirst();
        assertEquals("ORC", cert.system());
        assertEquals(588.4, cert.value());
        assertEquals(LocalDate.of(2021, 6, 30), cert.expiryDate());
    }

    // --- Round-trip save/load ---

    @Test
    void roundTripRace(@TempDir Path tempDir) {
        Race race = buildRace();

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putRace(race);
        store.stop();

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        Race loaded = store2.races().get(race.id());

        assertEquals(race, loaded);
    }

    @Test
    void roundTripCatalogue(@TempDir Path tempDir) {
        List<Boat> boats = List.of(
                new Boat("MYC100-shearmagic-adams10", "MYC100", "Shear Magic", "adams10", "myc.com.au", List.of(), List.of(), null, null),
                new Boat("MYC7-tensixty-radford1060", "MYC7", "Tensixty", "radford1060", "myc.com.au", List.of(), List.of(), null, null)
        );

        DataStore store = new DataStore(tempDir);
        store.start();
        boats.forEach(store::putBoat);
        store.save();

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        assertEquals(boats, store2.boats().values().stream()
                .sorted(java.util.Comparator.comparing(Boat::id)).toList());

        Series series = new Series("myc.com.au/club-championship", "Club Championship", false,
                List.of("myc.com.au-2020-09-13-0001"));
        Club club = new Club("myc.com.au", "MYC", "Manly Yacht Club", null, false, List.of(), List.of(), List.of(series), null);
        store2.putClub(club);
        store2.stop();

        DataStore store3 = new DataStore(tempDir);
        store3.start();
        assertEquals(List.of(club), List.copyOf(store3.clubs().values()));
    }

    @Test
    void emptyListWhenFileAbsent(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        assertTrue(store.boats().isEmpty());
        assertTrue(store.races().isEmpty());
    }

    @Test
    void eachRaceInOwnFile(@TempDir Path tempDir) {
        Race race1 = buildRace();
        Race race2 = new Race("myc.com.au-2020-09-20-0002", "myc.com.au",
                List.of("myc.com.au/club-championship"),
                LocalDate.of(2020, 9, 20), 2, null,
                List.of(new Division("Division 1", List.of(
                        new Finisher("MYC7-tensixty-radford1060", Duration.ofMinutes(72).plusSeconds(5), false, null)
                ))), null, null, null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putRace(race1);
        store.putRace(race2);
        store.stop();

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        assertEquals(race1, store2.races().get(race1.id()));
        assertEquals(race2, store2.races().get(race2.id()));
        assertEquals(2, store2.races().size());
    }

    @Test
    void eachBoatInOwnFile(@TempDir Path tempDir) {
        Boat boat1 = new Boat("MYC100-shearmagic-adams10", "MYC100", "Shear Magic", "adams10", "myc.com.au", List.of(), List.of(), null, null);
        Boat boat2 = new Boat("MYC7-tensixty-radford1060", "MYC7", "Tensixty", "radford1060", "myc.com.au", List.of(), List.of(), null, null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(boat1);
        store.putBoat(boat2);
        store.stop();

        assertTrue(tempDir.resolve("imported/boats/MYC100-shearmagic-adams10.json").toFile().exists());
        assertTrue(tempDir.resolve("imported/boats/MYC7-tensixty-radford1060.json").toFile().exists());

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        List<Boat> loaded = store2.boats().values().stream()
                .sorted(java.util.Comparator.comparing(Boat::id)).toList();
        assertEquals(List.of(boat1, boat2), loaded);
    }

    @Test
    void boatWithCertificatesRoundTrip(@TempDir Path tempDir) {
        Certificate cert = new Certificate(
                "ORC", 2020, 588.4, false, false, false, false, "AUS-2020-1234",
                LocalDate.of(2021, 6, 30));
        Boat boat = new Boat("5656-mondo-sydney38", "5656", "Mondo", "sydney38", "myc.com.au",
                List.of(cert), List.of(), null, null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(boat);
        store.stop();

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        assertEquals(1, store2.boats().size());
        Boat loaded = store2.boats().get(boat.id());
        assertEquals(boat, loaded);
        assertEquals(1, loaded.certificates().size());
        assertEquals(cert, loaded.certificates().getFirst());
    }

    @Test
    void clubWithSeriesRoundTrip(@TempDir Path tempDir) {
        Series series = new Series("myc.com.au/club-championship", "Club Championship", false,
                List.of("myc.com.au-2020-09-13-0001", "myc.com.au-2020-09-20-0002"));
        Club club = new Club("myc.com.au", "MYC", "Manly Yacht Club", null, false, List.of(), List.of(), List.of(series), null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putClub(club);
        store.stop();

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        assertEquals(1, store2.clubs().size());
        Club loadedClub = store2.clubs().get(club.id());
        assertEquals(club, loadedClub);
        assertEquals(1, loadedClub.series().size());
        Series loadedSeries = loadedClub.series().getFirst();
        assertEquals("myc.com.au/club-championship", loadedSeries.id());
        assertFalse(loadedSeries.isCatchAll());
        assertEquals(List.of("myc.com.au-2020-09-13-0001", "myc.com.au-2020-09-20-0002"), loadedSeries.raceIds());
    }

    @Test
    void clubWithPathIdRoundTrip(@TempDir Path tempDir) {
        Club club = new Club("rycv.com.au/ppnyc", "PPNYC", "Port Phillip North Yacht Clubs",
                "VIC", false, List.of(), List.of(), List.of(), null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putClub(club);
        store.stop();

        // File should use '--' as filesystem separator
        assertTrue(tempDir.resolve("imported/clubs/rycv.com.au--ppnyc.json").toFile().exists());

        DataStore store2 = new DataStore(tempDir);
        store2.start();
        Club loaded = store2.clubs().get("rycv.com.au/ppnyc");
        assertNotNull(loaded);
        assertEquals(club, loaded);
    }

    // --- findOrCreateBoat ---

    @Test
    void findOrCreateBoatCreatesNewBoat(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design design = new Design("j24", "J/24", List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat boat = store.findOrCreateBoat("AUS1234", "Raging Bull", "J/24");

        assertEquals("1234-ragingbull-j24", boat.id());
        assertEquals("1234", boat.sailNumber());
        assertEquals("Raging Bull", boat.name());
        assertEquals("j24", boat.designId());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatReturnsSameBoatOnExactId(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design design = new Design("j24", "J/24", List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat boat1 = store.findOrCreateBoat("AUS1234", "Raging Bull", "J/24");
        Boat boat2 = store.findOrCreateBoat("AUS1234", "Raging Bull", "J/24");

        assertEquals(boat1.id(), boat2.id());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatUpgradesNoDesignBoat(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat noDesign = new Boat("1234-ragingbull", "1234", "Raging Bull", null, null,
                List.of(), List.of(), null, null);
        store.putBoat(noDesign);

        Design design = new Design("j24", "J/24", List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat upgraded = store.findOrCreateBoat("AUS1234", "Raging Bull", "J/24");

        assertEquals("1234-ragingbull-j24", upgraded.id());
        assertEquals("j24", upgraded.designId());
        assertFalse(store.boats().containsKey("1234-ragingbull"));
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatCreatesNewDesign(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Boat boat = store.findOrCreateBoat("AUS99", "TestBoat", "J/24");

        assertEquals("j24", boat.designId());
        Design design = store.designs().get("j24");
        assertNotNull(design);
        assertEquals("J/24", design.canonicalName());
    }

    @Test
    void findOrCreateBoatMatchesDesignAlias(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design existing = new Design("j24", "J/24", List.of("J 24"), List.of(), null, null);
        store.putDesign(existing);

        Boat boat = store.findOrCreateBoat("AUS99", "TestBoat", "J 24");

        assertEquals("j24", boat.designId());
        assertEquals(1, store.designs().size());
    }

    // --- Alias seed integration ---

    @Test
    void findOrCreateBoatResolvesDesignAliasFromSeed(@TempDir Path tempDir) {
        // aliases.yaml maps "Sydney 36 OD" → sydney36cr
        DataStore store = new DataStore(tempDir);
        store.start();
        // Pre-populate the canonical design
        Design canonical = new Design("sydney36cr", "Sydney 36 CR", List.of(), List.of(), null, null);
        store.putDesign(canonical);

        Boat boat = store.findOrCreateBoat("AUS99", "TestBoat", "Sydney 36 OD");

        assertEquals("sydney36cr", boat.designId());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateBoatCreatesCanonicalDesignViaSeedWhenAbsent(@TempDir Path tempDir) {
        // aliases.yaml maps "Sydney 36 OD" → sydney36cr with canonicalName "Sydney 36 CR"
        // If sydney36cr doesn't exist yet, it should be created with the seed's canonical name
        DataStore store = new DataStore(tempDir);
        store.start();

        Boat boat = store.findOrCreateBoat("AUS99", "TestBoat", "Sydney 36 OD");

        assertEquals("sydney36cr", boat.designId());
        Design created = store.designs().get("sydney36cr");
        assertNotNull(created);
        assertEquals("Sydney 36 CR", created.canonicalName());
    }

    @Test
    void findOrCreateBoatResolvesAliasFromSeedToExistingCanonicalBoat(@TempDir Path tempDir) {
        // aliases.yaml maps MYC7 / "1060" → canonical name "Day Dreaming"
        // Pre-populate the canonical boat; searching by "1060" should find it
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat canonical = new Boat("MYC7-daydreaming", "MYC7", "Day Dreaming", null, null, List.of(), List.of(), null, null);
        store.putBoat(canonical);

        Boat found = store.findOrCreateBoat("MYC7", "1060", null);

        assertEquals("MYC7-daydreaming", found.id());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatCreatesWithCanonicalNameWhenAliasEncounteredFirst(@TempDir Path tempDir) {
        // aliases.yaml maps MYC7 / "1060" → canonical "Day Dreaming"
        // No boat yet — first encounter via an alias name should create with canonical name + alias recorded
        DataStore store = new DataStore(tempDir);
        store.start();

        Boat boat = store.findOrCreateBoat("MYC7", "1060", null);

        assertEquals("MYC7-daydreaming", boat.id());
        assertEquals("Day Dreaming", boat.name());
        assertEquals(1, store.boats().size());
    }

    // --- findUniqueClubByShortName ---

    @Test
    void findUniqueClubByShortNameMatchesLongName(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Club club = new Club("test.example.com", "TYC", "Test Yacht Club", "NSW", false,
                List.of(), List.of(), List.of(), null);
        store.putClub(club);

        Club found = store.findUniqueClubByShortName("Test Yacht Club", null, "test");

        assertNotNull(found);
        assertEquals("test.example.com", found.id());
    }

    @Test
    void findUniqueClubByShortNameMatchesAlias(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Club club = new Club("test.example.com", "TYC", "Test Yacht Club", "NSW", false,
                List.of("TYC/OTHER"), List.of(), List.of(), null);
        store.putClub(club);

        Club found = store.findUniqueClubByShortName("TYC/OTHER", null, "test");

        assertNotNull(found);
        assertEquals("test.example.com", found.id());
    }

    @Test
    void findUniqueClubByShortNamePrefersShortNameOverLongName(@TempDir Path tempDir) {
        // Club A: shortName="X", longName="Y"
        // Club B: shortName="Y", longName="Z"
        // Searching "Y" should return B (shortName match wins over longName match of A)
        DataStore store = new DataStore(tempDir);
        store.start();
        Club clubA = new Club("a.example.com", "X", "Y", "NSW", false, List.of(), List.of(), List.of(), null);
        Club clubB = new Club("b.example.com", "Y", "Z", "NSW", false, List.of(), List.of(), List.of(), null);
        store.putClub(clubA);
        store.putClub(clubB);

        Club found = store.findUniqueClubByShortName("Y", null, "test");

        assertNotNull(found);
        assertEquals("b.example.com", found.id());
    }

    @Test
    void findUniqueClubByShortNameMatchesFirstTokenOfCompoundName(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Club club = new Club("test.example.com", "TYC", "Test Yacht Club", "NSW", false,
                List.of(), List.of(), List.of(), null);
        store.putClub(club);

        Club found = store.findUniqueClubByShortName("TYC/OTHER", null, "test");

        assertNotNull(found);
        assertEquals("test.example.com", found.id());
    }

    @Test
    void findUniqueClubByShortNameReturnsNullWhenNoMatch(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Club found = store.findUniqueClubByShortName("Nonexistent Club", null, "test");

        assertNull(found);
    }

    // --- isDesignExcluded ---

    @Test
    void isDesignExcludedReturnsTrueForKnownDinghy(@TempDir Path tempDir) throws Exception {
        // test classpath design.yaml lists "Optimist" as excluded
        DataStore store = new DataStore(tempDir);
        store.start();

        assertTrue(store.isDesignExcluded("optimist"));
    }

    @Test
    void isDesignExcludedReturnsFalseForKeelboat(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        assertFalse(store.isDesignExcluded("j24"));
    }

    @Test
    void isDesignExcludedCaseInsensitive(@TempDir Path tempDir) {
        // design.yaml lists "Optimist"; the normalised form of "OPTIMIST" should also match
        DataStore store = new DataStore(tempDir);
        store.start();

        assertTrue(store.isDesignExcluded("optimist"));
    }

    // --- mergeDesigns ---

    @Test
    void mergeDesignsUpdatesBoatIdsAndRaceFinishers(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Design oldDesign  = new Design("old",  "Old Class", List.of(), List.of(), null, null);
        Design keepDesign = new Design("keep", "Keep Class", List.of(), List.of(), null, null);
        store.putDesign(oldDesign);
        store.putDesign(keepDesign);

        // Boat whose ID currently encodes the "old" design
        Boat boat = new Boat("AUS1-foo-old", "AUS1", "Foo", "old", null, List.of(), List.of(), null, null);
        store.putBoat(boat);

        // Race with a finisher referencing the old boat ID
        Race race = new Race("club-2024-01-01-0001", "club", List.of("club/series"),
                LocalDate.of(2024, 1, 1), 1, null,
                List.of(new Division("A", List.of(
                        new Finisher("AUS1-foo-old", Duration.ofMinutes(60), false, null)
                ))), null, null, null);
        store.putRace(race);

        DataStore.DesignMergeResult result = store.mergeDesigns("keep", List.of("old"));

        assertFalse(store.boats().containsKey("AUS1-foo-old"),   "old boat ID should be gone");
        assertTrue(store.boats().containsKey("AUS1-foo-keep"),   "new boat ID should exist");
        assertEquals("keep", store.boats().get("AUS1-foo-keep").designId());

        Finisher f = store.races().get("club-2024-01-01-0001").divisions().getFirst().finishers().getFirst();
        assertEquals("AUS1-foo-keep", f.boatId(), "finisher should reference new boat ID");

        assertEquals(1, result.updatedBoats());
        assertEquals(1, result.updatedRaces());
        assertEquals(1, result.updatedFinishers());
    }

    @Test
    void mergeDesignsCollisionMergesBoats(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Design oldDesign  = new Design("old",  "Old Class", List.of(), List.of(), null, null);
        Design keepDesign = new Design("keep", "Keep Class", List.of(), List.of(), null, null);
        store.putDesign(oldDesign);
        store.putDesign(keepDesign);

        // Boat A: under the old design — will be renamed to AUS1-foo-keep
        Boat boatA = new Boat("AUS1-foo-old", "AUS1", "Foo", "old", null,
                List.of(), List.of("SailSys"), null, null);
        store.putBoat(boatA);

        // Boat B: already exists under the keep design with same sail+name — will collide
        Certificate cert = new Certificate("ORC", 2023, 0.95, false, false, false, false, null, null);
        Boat boatB = new Boat("AUS1-foo-keep", "AUS1", "Foo", "keep", "myclub.com",
                List.of(cert), List.of("ORC"), null, null);
        store.putBoat(boatB);

        // Race with finisher referencing boatA
        Race race = new Race("club-2024-01-01-0001", "club", List.of("club/s"),
                LocalDate.of(2024, 1, 1), 1, null,
                List.of(new Division("A", List.of(
                        new Finisher("AUS1-foo-old", Duration.ofMinutes(60), false, null)
                ))), null, null, null);
        store.putRace(race);

        store.mergeDesigns("keep", List.of("old"));

        // Old id gone; merged boat at new id
        assertFalse(store.boats().containsKey("AUS1-foo-old"));
        assertTrue(store.boats().containsKey("AUS1-foo-keep"));
        Boat merged = store.boats().get("AUS1-foo-keep");
        assertEquals("keep", merged.designId());
        // clubId from keep boat
        assertEquals("myclub.com", merged.clubId());
        // Certificate from keep boat
        assertEquals(1, merged.certificates().size());
        // Sources merged
        assertTrue(merged.sources().contains("ORC"));
        assertTrue(merged.sources().contains("SailSys"));
        // Finisher repointed
        assertEquals("AUS1-foo-keep",
                store.races().get("club-2024-01-01-0001").divisions().getFirst().finishers().getFirst().boatId());
    }

    @Test
    void mergeDesignsBoatWithNullDesignIsUnchanged(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Design d1 = new Design("d1", "D1", List.of(), List.of(), null, null);
        Design d2 = new Design("d2", "D2", List.of(), List.of(), null, null);
        store.putDesign(d1);
        store.putDesign(d2);

        Boat noDesign = new Boat("AUS1-foo", "AUS1", "Foo", null, null, List.of(), List.of(), null, null);
        store.putBoat(noDesign);

        store.mergeDesigns("d1", List.of("d2"));

        assertTrue(store.boats().containsKey("AUS1-foo"), "boat with null designId must be untouched");
        assertNull(store.boats().get("AUS1-foo").designId());
        assertEquals(1, store.boats().size());
    }

    // --- Design override ---

    @Test
    void findOrCreateBoatAppliesDesignOverride(@TempDir Path tempDir) throws Exception {
        // Create config/design.yaml with an override: 6499/Supernova → sydney36mkii
        Path configDir = tempDir.resolve("config");
        Files.createDirectories(configDir);
        Files.writeString(configDir.resolve("design.yaml"),
            """
            excluded: []
            ignored: []
            boatDesignOverrides:
              - designId: "sydney36mkii"
                canonicalName: "Sydney 36 MkII"
                boats:
                  - sailNumber: "6499"
                    name: "Supernova"
            """);

        DataStore store = new DataStore(tempDir);
        store.start();

        // Create an existing "sydney36" design (the wrong one, from raw data)
        store.putDesign(new Design("sydney36", "Sydney 36", List.of(), List.of(), null, null));

        // The override should have auto-created "sydney36mkii" at startup
        assertNotNull(store.designs().get("sydney36mkii"), "override design should be auto-created");

        // Call findOrCreateBoat with raw design "Sydney 36" — override should kick in
        Boat boat = store.findOrCreateBoat("6499", "Supernova", "Sydney 36");

        assertEquals("sydney36mkii", boat.designId(),
            "design override should replace sydney36 with sydney36mkii");
        assertEquals("6499-supernova-sydney36mkii", boat.id(),
            "boat ID should use the overridden design");
    }

    // --- Helpers ---

    private DataStore testDataStore() throws URISyntaxException {
        Path testData = Path.of(getClass().getClassLoader().getResource("testdata").toURI());
        return new DataStore(testData);
    }

    /**
     * When findOrCreateBoat upgrades a design-less boat to one with a design,
     * all existing race finisher references to the old ID must be rewritten.
     */
    @Test
    void upgradeBoatDesignRewritesFinisherReferences(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        // Create a boat without design (as RSHYR would)
        Boat noDesign = store.findOrCreateBoat("8108", "Highly Sprung", null);
        assertEquals("8108-highlysprung", noDesign.id());

        // Create a race with this boat as a finisher
        Race race = new Race("cyca.com.au-2025-12-26-0001", "cyca.com.au",
            List.of(), LocalDate.of(2025, 12, 26), 1, "Sydney Hobart",
            List.of(new Division("IRC Div 1", List.of(
                new Finisher("8108-highlysprung", Duration.ofHours(3), false, null),
                new Finisher("other-boat", Duration.ofHours(4), false, null)
            ))),
            "RSHYR", null, null);
        store.putRace(race);

        // Now another importer provides the design (as ORC/SailSys would)
        Boat withDesign = store.findOrCreateBoat("8108", "Highly Sprung", "TP 52");
        assertEquals("8108-highlysprung-tp52", withDesign.id());

        // The old boat should be gone
        assertNull(store.boats().get("8108-highlysprung"));
        assertNotNull(store.boats().get("8108-highlysprung-tp52"));

        // The finisher reference in the race should be updated
        Race updated = store.races().get("cyca.com.au-2025-12-26-0001");
        Finisher f0 = updated.divisions().getFirst().finishers().get(0);
        assertEquals("8108-highlysprung-tp52", f0.boatId(),
            "Finisher boatId should be rewritten to the upgraded boat ID");

        // Other finishers should be unchanged
        Finisher f1 = updated.divisions().getFirst().finishers().get(1);
        assertEquals("other-boat", f1.boatId());
    }

    private Race buildRace() {
        return new Race(
                "myc.com.au-2020-09-13-0001",
                "myc.com.au",
                List.of("myc.com.au/club-championship"),
                LocalDate.of(2020, 9, 13),
                1,
                null,
                List.of(
                        new Division("Division 1", List.of(
                                new Finisher("MYC100-shearmagic-adams10", Duration.ofMinutes(69).plusSeconds(42), false, null),
                                new Finisher("MYC7-tensixty-radford1060",  Duration.ofMinutes(67).plusSeconds(37), false, null),
                                new Finisher("MYC12-santoy",              Duration.ofMinutes(67).plusSeconds(22), true,  null),
                                new Finisher("5656-mondo-sydney38",        Duration.ofMinutes(67).plusSeconds(19), false, null)
                        )),
                        new Division("Division 2", List.of(
                                new Finisher("1152-bokarra-santana22",     Duration.ofMinutes(80).plusSeconds(26), false, null),
                                new Finisher("1255-melody",               Duration.ofMinutes(77).plusSeconds(32), false, null),
                                new Finisher("6295-rattytooey",          Duration.ofMinutes(77).plusSeconds(59), false, null)
                        ))
                ),
                null, null, null
        );
    }
}
