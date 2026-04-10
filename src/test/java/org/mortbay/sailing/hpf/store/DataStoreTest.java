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
        assertEquals("PHS", race.handicapSystem());
        assertEquals(2, race.divisions().size());

        Division div1 = race.divisions().getFirst();
        assertEquals("Division 1", div1.name());
        assertEquals(4, div1.finishers().size());

        Finisher shearMagic = div1.finishers().getFirst();
        assertEquals("MYC100-shear_magic-adams10", shearMagic.boatId());
        assertEquals(Duration.ofMinutes(69).plusSeconds(42), shearMagic.elapsedTime());
        assertFalse(shearMagic.nonSpinnaker());

        Finisher sanToy = div1.finishers().get(2);
        assertEquals("MYC12-san_toy", sanToy.boatId());
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
        assertEquals(List.of("TenSixty", "1060"), tensixty.aliases());

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
                new Boat("MYC100-shear_magic-adams10", "MYC100", "Shear Magic", "adams10", "myc.com.au", List.of(), List.of(), List.of(), List.of(), null, null),
                new Boat("MYC7-tensixty-radford1060", "MYC7", "Tensixty", "radford1060", "myc.com.au", List.of("TenSixty", "1060"), List.of(), List.of(), List.of(), null, null)
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
                LocalDate.of(2020, 9, 20), 2, null, "PHS", false,
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
        Boat boat1 = new Boat("MYC100-shear_magic-adams10", "MYC100", "Shear Magic", "adams10", "myc.com.au", List.of(), List.of(), List.of(), List.of(), null, null);
        Boat boat2 = new Boat("MYC7-tensixty-radford1060", "MYC7", "Tensixty", "radford1060", "myc.com.au", List.of("TenSixty", "1060"), List.of(), List.of(), List.of(), null, null);

        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(boat1);
        store.putBoat(boat2);
        store.stop();

        assertTrue(tempDir.resolve("imported/boats/MYC100-shear_magic-adams10.json").toFile().exists());
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
                List.of(), List.of(), List.of(cert), List.of(), null, null);

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

    // --- findOrCreateBoat / findOrCreateDesign ---

    @Test
    void findOrCreateBoatCreatesNewBoat(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design design = new Design("j24", "J/24", List.of(), List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat boat = store.findOrCreateBoat("AUS1234", "Raging Bull", design);

        assertEquals("AUS1234-raging_bull-j24", boat.id());
        assertEquals("AUS1234", boat.sailNumber());
        assertEquals("Raging Bull", boat.name());
        assertEquals("j24", boat.designId());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatReturnsSameBoatOnExactId(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design design = new Design("j24", "J/24", List.of(), List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat boat1 = store.findOrCreateBoat("AUS1234", "Raging Bull", design);
        Boat boat2 = store.findOrCreateBoat("AUS1234", "Raging Bull", design);

        assertSame(boat1, boat2);
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatFuzzyMatchesByAlias(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat existing = new Boat("AUS1234-raging_bull", "AUS1234", "Raging Bull", null, null,
                List.of("RagingBull"), List.of(), List.of(), List.of(), null, null);
        store.putBoat(existing);

        Boat found = store.findOrCreateBoat("AUS1234", "RagingBull", null);

        assertEquals("AUS1234-raging_bull", found.id());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatUpgradesNoDesignBoat(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat noDesign = new Boat("AUS1234-raging_bull", "AUS1234", "Raging Bull", null, null,
                List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(noDesign);

        Design design = new Design("j24", "J/24", List.of(), List.of(), List.of(), null, null);
        store.putDesign(design);

        Boat upgraded = store.findOrCreateBoat("AUS1234", "Raging Bull", design);

        assertEquals("AUS1234-raging_bull-j24", upgraded.id());
        assertEquals("j24", upgraded.designId());
        assertFalse(store.boats().containsKey("AUS1234-raging_bull"));
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateDesignCreatesNewDesign(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();

        Design design = store.findOrCreateDesign("J/24");

        assertEquals("j24", design.id());
        assertEquals("J/24", design.canonicalName());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateDesignMatchesAlias(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Design existing = new Design("j24", "J/24", List.of(), List.of("J 24"), List.of(), null, null);
        store.putDesign(existing);

        Design found = store.findOrCreateDesign("J 24");

        assertEquals("j24", found.id());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateDesignFuzzyMatchesTypoInName(@TempDir Path tempDir) {
        // "Sydney 38" → "sydney38"; "Sydeny 38" (transposed n/e) → "sydeny38" — same digits,
        // JW score ~0.97, above threshold.
        DataStore store = new DataStore(tempDir);
        store.start();
        Design existing = new Design("sydney38", "Sydney 38", List.of(), List.of(), List.of(), null, null);
        store.putDesign(existing);

        Design found = store.findOrCreateDesign("Sydeny 38");

        assertEquals("sydney38", found.id());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateDesignDoesNotFuzzyMatchAdjacentModelNumber(@TempDir Path tempDir) {
        // "Sydney 38" → "sydney38" and "Sydney 39" → "sydney39": digits differ so must not match
        // even though the JW score is high.
        DataStore store = new DataStore(tempDir);
        store.start();
        Design existing = new Design("sydney38", "Sydney 38", List.of(), List.of(), List.of(), null, null);
        store.putDesign(existing);

        Design found = store.findOrCreateDesign("Sydney 39");

        assertEquals("sydney39", found.id());
        assertEquals(2, store.designs().size());
    }

    @Test
    void findOrCreateDesignFuzzyMatchesTransposedName(@TempDir Path tempDir) {
        // "Radford 1060" → "radford1060"; "Radfrod 1060" (transposed r/o) → "radfrod1060" —
        // same digits, JW score ~0.98, above threshold.
        DataStore store = new DataStore(tempDir);
        store.start();
        Design existing = new Design("radford1060", "Radford 1060", List.of(), List.of(), List.of(), null, null);
        store.putDesign(existing);

        Design found = store.findOrCreateDesign("Radfrod 1060");

        assertEquals("radford1060", found.id());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateBoatFuzzyMatchesSimilarAlias(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat existing = new Boat("AUS1234-shear_magic", "AUS1234", "Shear Magic",
                null, null, List.of("ShearMagic"), List.of(), List.of(), List.of(), null, null);
        store.putBoat(existing);

        Boat found = store.findOrCreateBoat("AUS1234", "SheerMagic", null);

        assertEquals("AUS1234-shear_magic", found.id());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatFuzzyMatchesSimilarName(@TempDir Path tempDir) {
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat existing = new Boat("AUS1234-shear_magic", "AUS1234", "Shear Magic",
                null, null, List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(existing);

        Boat found = store.findOrCreateBoat("AUS1234", "Sheer Magic", null);

        assertEquals("AUS1234-shear_magic", found.id());
        assertEquals(1, store.boats().size());
    }

    // --- Alias seed integration ---

    @Test
    void findOrCreateDesignResolvesAliasFromSeed(@TempDir Path tempDir) {
        // aliases.yaml maps "Sydney 36 OD" → sydney36cr
        DataStore store = new DataStore(tempDir);
        store.start();
        // Pre-populate the canonical design
        Design canonical = new Design("sydney36cr", "Sydney 36 CR", List.of(), List.of(), List.of(), null, null);
        store.putDesign(canonical);

        Design found = store.findOrCreateDesign("Sydney 36 OD");

        assertEquals("sydney36cr", found.id());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateDesignCreatesCanonicalDesignViaSeedWhenAbsent(@TempDir Path tempDir) {
        // aliases.yaml maps "Sydney 36 OD" → sydney36cr with canonicalName "Sydney 36 CR"
        // If sydney36cr doesn't exist yet, it should be created with the seed's canonical name
        DataStore store = new DataStore(tempDir);
        store.start();

        Design found = store.findOrCreateDesign("Sydney 36 OD");

        assertEquals("sydney36cr", found.id());
        assertEquals("Sydney 36 CR", found.canonicalName());
        assertEquals(1, store.designs().size());
    }

    @Test
    void findOrCreateBoatResolvesAliasFromSeedToExistingCanonicalBoat(@TempDir Path tempDir) {
        // aliases.yaml maps MYC7 / "1060" → canonical name "Day Dreaming"
        // Pre-populate the canonical boat; searching by "1060" should find it
        DataStore store = new DataStore(tempDir);
        store.start();
        Boat canonical = new Boat("MYC7-day_dreaming", "MYC7", "Day Dreaming", null, null, List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(canonical);

        Boat found = store.findOrCreateBoat("MYC7", "1060", null);

        assertEquals("MYC7-day_dreaming", found.id());
        assertEquals(1, store.boats().size());
    }

    @Test
    void findOrCreateBoatCreatesWithCanonicalNameWhenAliasEncounteredFirst(@TempDir Path tempDir) {
        // aliases.yaml maps MYC7 / "1060" → canonical "Day Dreaming"
        // No boat yet — first encounter via an alias name should create with canonical name + alias recorded
        DataStore store = new DataStore(tempDir);
        store.start();

        Boat boat = store.findOrCreateBoat("MYC7", "1060", null);

        assertEquals("MYC7-day_dreaming", boat.id());
        assertEquals("Day Dreaming", boat.name());
        assertEquals(List.of("1060"), boat.aliases());
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

        Design oldDesign  = new Design("old",  "Old Class",  List.of(), List.of(), List.of(), null, null);
        Design keepDesign = new Design("keep", "Keep Class", List.of(), List.of(), List.of(), null, null);
        store.putDesign(oldDesign);
        store.putDesign(keepDesign);

        // Boat whose ID currently encodes the "old" design
        Boat boat = new Boat("AUS1-foo-old", "AUS1", "Foo", "old", null, List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(boat);

        // Race with a finisher referencing the old boat ID
        Race race = new Race("club-2024-01-01-0001", "club", List.of("club/series"),
                LocalDate.of(2024, 1, 1), 1, null, "IRC", false,
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

        Design oldDesign  = new Design("old",  "Old Class",  List.of(), List.of(), List.of(), null, null);
        Design keepDesign = new Design("keep", "Keep Class", List.of(), List.of(), List.of(), null, null);
        store.putDesign(oldDesign);
        store.putDesign(keepDesign);

        // Boat A: under the old design — will be renamed to AUS1-foo-keep
        Boat boatA = new Boat("AUS1-foo-old", "AUS1", "Foo", "old", null,
                List.of("AliasFromOld"), List.of(), List.of(), List.of("SailSys"), null, null);
        store.putBoat(boatA);

        // Boat B: already exists under the keep design with same sail+name — will collide
        Certificate cert = new Certificate("ORC", 2023, 0.95, false, false, false, false, null, null);
        Boat boatB = new Boat("AUS1-foo-keep", "AUS1", "Foo", "keep", "myclub.com",
                List.of("AliasFromKeep"), List.of(), List.of(cert), List.of("ORC"), null, null);
        store.putBoat(boatB);

        // Race with finisher referencing boatA
        Race race = new Race("club-2024-01-01-0001", "club", List.of("club/s"),
                LocalDate.of(2024, 1, 1), 1, null, "IRC", false,
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
        // Aliases merged from both
        assertTrue(merged.aliases().contains("AliasFromOld"));
        assertTrue(merged.aliases().contains("AliasFromKeep"));
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

        Design d1 = new Design("d1", "D1", List.of(), List.of(), List.of(), null, null);
        Design d2 = new Design("d2", "D2", List.of(), List.of(), List.of(), null, null);
        store.putDesign(d1);
        store.putDesign(d2);

        Boat noDesign = new Boat("AUS1-foo", "AUS1", "Foo", null, null, List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(noDesign);

        store.mergeDesigns("d1", List.of("d2"));

        assertTrue(store.boats().containsKey("AUS1-foo"), "boat with null designId must be untouched");
        assertNull(store.boats().get("AUS1-foo").designId());
        assertEquals(1, store.boats().size());
    }

    // --- Helpers ---

    private DataStore testDataStore() throws URISyntaxException {
        Path testData = Path.of(getClass().getClassLoader().getResource("testdata").toURI());
        return new DataStore(testData);
    }

    private Race buildRace() {
        return new Race(
                "myc.com.au-2020-09-13-0001",
                "myc.com.au",
                List.of("myc.com.au/club-championship"),
                LocalDate.of(2020, 9, 13),
                1,
                null,
                "PHS",
                false,
                List.of(
                        new Division("Division 1", List.of(
                                new Finisher("MYC100-shear_magic-adams10", Duration.ofMinutes(69).plusSeconds(42), false, null),
                                new Finisher("MYC7-tensixty-radford1060",  Duration.ofMinutes(67).plusSeconds(37), false, null),
                                new Finisher("MYC12-san_toy",              Duration.ofMinutes(67).plusSeconds(22), true,  null),
                                new Finisher("5656-mondo-sydney38",        Duration.ofMinutes(67).plusSeconds(19), false, null)
                        )),
                        new Division("Division 2", List.of(
                                new Finisher("1152-bokarra-santana22",     Duration.ofMinutes(80).plusSeconds(26), false, null),
                                new Finisher("1255-melody",               Duration.ofMinutes(77).plusSeconds(32), false, null),
                                new Finisher("6295-ratty_tooey",          Duration.ofMinutes(77).plusSeconds(59), false, null)
                        ))
                ),
                null, null, null
        );
    }
}
