package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the one-off migration of TopYacht races from {@code clubId-date-number} IDs to
 * event- and series-scoped ones.
 * <p>
 * The old scheme assumed (club, date, number) was unique. A regatta that publishes each
 * division as its own series breaks that — every division holds a "Race 1" on the same day —
 * so only one division's race survived per day and every sibling series claimed it.
 */
class TopYachtIdMigrationTest
{
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();

    private static final String CLUB = "tyc.com.au";
    private static final String INDEX = "https://tes.topyacht.net.au/results/2025/mirw/index.htm";

    private void writeClubsYaml(Path root, String body) throws IOException
    {
        Path file = root.resolve("config/clubs.yaml");
        Files.createDirectories(file.getParent());
        Files.writeString(file, body);
    }

    /**
     * Writes a race JSON directly, as an older build of the importer would have.
     */
    private void writeLegacyRace(Path root, String raceId, String seriesSlug, LocalDate date,
                                 int number, String sourceUrl) throws IOException
    {
        Race race = new Race(raceId, CLUB, List.of(CLUB + "/" + seriesSlug), date, number, null,
            List.of(new Division("PHS", List.of(new Finisher("b1-boat", java.time.Duration.ofHours(2), false, null)))),
            sourceUrl == null ? "TopYacht" : "TopYacht - " + sourceUrl, Instant.now(), null);
        Path file = root.resolve("imported/races").resolve(CLUB).resolve(seriesSlug)
            .resolve(raceId + ".json");
        Files.createDirectories(file.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), race);
    }

    private void writeClubJson(Path root, List<Series> series) throws IOException
    {
        Club club = new Club(CLUB, "TYC", null, null, false, null, List.of(), List.of(), series, null);
        Path file = root.resolve("imported/clubs").resolve(CLUB + ".json");
        Files.createDirectories(file.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), club);
    }

    /**
     * The realistic case: two divisions of one regatta both published "Race 1" on 29 Aug, so
     * only division 2's race was stored — and both division series listed it.
     */
    private void writeCollidedRegatta(Path root) throws IOException
    {
        writeClubsYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                topyacht:
                  mirw:
                    name: "Magnetic Island Race Week"
                    urls:
                      - "%s"
            """.formatted(INDEX));
        writeLegacyRace(root, CLUB + "-2025-08-29-0001", "spinnaker-division-2",
            LocalDate.of(2025, 8, 29), 1,
            "https://www.topyacht.net.au/results/2025/mirw/spin2/01RGrp1.htm");
        writeClubJson(root, List.of(
            new Series(CLUB + "/spinnaker-division-1", "Spinnaker Division 1",
                false, List.of(CLUB + "-2025-08-29-0001")),
            new Series(CLUB + "/spinnaker-division-2", "Spinnaker Division 2",
                false, List.of(CLUB + "-2025-08-29-0001"))));
    }

    @Test
    void migrationScopesRaceIdsByEventAndSeries(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);

        DataStore store = new DataStore(root);
        store.start();

        String expected = CLUB + "-mirw-spinnaker-division-2-2025-08-29-0001";
        assertEquals(1, store.races().size());
        Race race = store.races().get(expected);
        assertNotNull(race, () -> "expected " + expected + ", got " + store.races().keySet());
        assertEquals(List.of(CLUB + "/mirw-spinnaker-division-2"), race.seriesIds());
        assertEquals(LocalDate.of(2025, 8, 29), race.date());
        assertEquals(1, race.divisions().get(0).finishers().size(), "finishers must be preserved");
        store.stop();
    }

    @Test
    void migrationMovesTheRaceFileAndRemovesTheOldOne(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);
        Path oldFile = root.resolve("imported/races").resolve(CLUB)
            .resolve("spinnaker-division-2").resolve(CLUB + "-2025-08-29-0001.json");
        assertTrue(Files.exists(oldFile));

        DataStore store = new DataStore(root);
        store.start();

        assertFalse(Files.exists(oldFile), "stale race file should be deleted");
        assertTrue(Files.exists(root.resolve("imported/races").resolve(CLUB)
            .resolve("mirw-spinnaker-division-2")
            .resolve(CLUB + "-mirw-spinnaker-division-2-2025-08-29-0001.json")));
        store.stop();
    }

    /**
     * The series that merely claimed the winner's race must not keep it. Division 1 held a
     * race it never imported; after migration only the series the race actually belongs to
     * lists it, and division 1 is dropped so the next import can rebuild it honestly.
     */
    @Test
    void migrationRebuildsSeriesMembershipAndDropsPollutedSeries(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);

        DataStore store = new DataStore(root);
        store.start();

        Club club = store.clubs().get(CLUB);
        assertNotNull(club);
        List<String> seriesIds = club.series().stream().map(Series::id).toList();
        assertEquals(List.of(CLUB + "/mirw-spinnaker-division-2"), seriesIds);
        assertEquals("Spinnaker Division 2", club.series().getFirst().name(),
            "display name must survive the rename");
        assertEquals(List.of(CLUB + "-mirw-spinnaker-division-2-2025-08-29-0001"),
            club.series().getFirst().raceIds());
        store.stop();
    }

    @Test
    void migrationCarriesRaceExclusionsAcrossTheRename(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);
        Files.writeString(root.resolve("config/exclusions.yaml"), """
            races:
              - id: "%s-2025-08-29-0001"
                reason: "test exclusion"
            """.formatted(CLUB));

        DataStore store = new DataStore(root);
        store.start();

        String newId = CLUB + "-mirw-spinnaker-division-2-2025-08-29-0001";
        assertTrue(store.isRaceExcluded(newId), "exclusion must follow the race to its new ID");
        assertEquals("test exclusion", store.raceExclusionReason(newId));
        assertFalse(store.isRaceExcluded(CLUB + "-2025-08-29-0001"));
        store.stop();
    }

    /**
     * A second start() must be a no-op — in particular it must not double up the prefix.
     */
    @Test
    void migrationIsIdempotent(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);

        DataStore first = new DataStore(root);
        first.start();
        first.stop();

        DataStore second = new DataStore(root);
        second.start();
        assertEquals(List.of(CLUB + "-mirw-spinnaker-division-2-2025-08-29-0001"),
            List.copyOf(second.races().keySet()));
        second.stop();
    }

    /**
     * A race whose source predates URL-bearing sources borrows its sibling's event prefix.
     */
    @Test
    void migrationResolvesBareSourceViaASiblingInTheSameSeries(@TempDir Path root) throws IOException
    {
        writeClubsYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                topyacht:
                  mirw:
                    urls:
                      - "%s"
                  fos:
                    urls:
                      - "https://www.topyacht.net.au/results/2022/fos/index.htm"
            """.formatted(INDEX));
        writeLegacyRace(root, CLUB + "-2025-08-29-0001", "spinnaker-division-2",
            LocalDate.of(2025, 8, 29), 1,
            "https://www.topyacht.net.au/results/2025/mirw/spin2/01RGrp1.htm");
        writeLegacyRace(root, CLUB + "-2025-08-30-0002", "spinnaker-division-2",
            LocalDate.of(2025, 8, 30), 2, null);   // bare "TopYacht" source
        writeClubJson(root, List.of(new Series(CLUB + "/spinnaker-division-2",
            "Spinnaker Division 2", false,
            List.of(CLUB + "-2025-08-29-0001", CLUB + "-2025-08-30-0002"))));

        DataStore store = new DataStore(root);
        store.start();

        assertNotNull(store.races().get(CLUB + "-mirw-spinnaker-division-2-2025-08-30-0002"),
            () -> "bare-source race should have followed its sibling: " + store.races().keySet());
        store.stop();
    }

    /**
     * Nothing to attribute a race to means leaving it exactly as it was, not guessing.
     */
    @Test
    void migrationLeavesUnattributableRacesUntouched(@TempDir Path root) throws IOException
    {
        writeClubsYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                topyacht:
                  mirw:
                    urls:
                      - "%s"
                  fos:
                    urls:
                      - "https://www.topyacht.net.au/results/2022/fos/index.htm"
            """.formatted(INDEX));
        writeLegacyRace(root, CLUB + "-2019-01-01-0001", "some-series",
            LocalDate.of(2019, 1, 1), 1, null);   // bare source, two candidate groups
        writeClubJson(root, List.of(new Series(CLUB + "/some-series", "Some Series",
            false, List.of(CLUB + "-2019-01-01-0001"))));

        DataStore store = new DataStore(root);
        store.start();

        assertNotNull(store.races().get(CLUB + "-2019-01-01-0001"), "race must be left alone");
        store.stop();
    }

    /**
     * Races from other sources share the club but must not be touched by this migration.
     */
    @Test
    void migrationIgnoresNonTopYachtRaces(@TempDir Path root) throws IOException
    {
        writeCollidedRegatta(root);
        Race sailsys = new Race(CLUB + "-2025-10-01-0001", CLUB,
            List.of(CLUB + "/club-season"), LocalDate.of(2025, 10, 1), 1, null,
            List.of(new Division("PHS", List.of())), "SailSys", Instant.now(), null);
        Path file = root.resolve("imported/races").resolve(CLUB).resolve("club-season")
            .resolve(sailsys.id() + ".json");
        Files.createDirectories(file.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), sailsys);

        DataStore store = new DataStore(root);
        store.start();

        assertNotNull(store.races().get(CLUB + "-2025-10-01-0001"), "SailSys race must keep its ID");
        assertNull(store.races().get(CLUB + "-club-season-2025-10-01-0001"));
        store.stop();
    }
}
