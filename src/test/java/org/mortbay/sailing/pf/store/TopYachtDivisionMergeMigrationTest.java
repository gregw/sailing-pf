package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the in-place migration that folds a merged event's per-division races into one race
 * per sailing slot.
 * <p>
 * The fixtures reproduce the real Magnetic Island Race Week shape, including the detail that
 * makes the obvious approach wrong: Spinnaker Division 1 sails twice on the middle day, so
 * from then on its race numbers run one ahead of every other division. Merging on
 * {@code (date, number)} would mis-pair those; merging on {@code (date, ordinal-within-day)}
 * does not.
 */
class TopYachtDivisionMergeMigrationTest
{
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();

    private static final String CLUB = "tyc.com.au";

    private void writeClubsYaml(Path root, boolean merge) throws IOException
    {
        Path file = root.resolve("config/clubs.yaml");
        Files.createDirectories(file.getParent());
        String yaml = "clubs:\n"
            + "  tyc.com.au:\n"
            + "    shortName: TYC\n"
            + "    topyacht:\n"
            + "      mirw:\n"
            + "        name: \"Magnetic Island Race Week\"\n"
            + (merge ? "        merge: true\n" : "")
            + "        urls:\n"
            + "          - \"https://tes.topyacht.net.au/results/2025/mirw/index.htm\"\n";
        Files.writeString(file, yaml);
    }

    /**
     * Writes one per-division race in the post-ID-migration (event-scoped) form.
     */
    private void writeRace(Path root, String seriesSlug, LocalDate date, int number, String boatId)
        throws IOException
    {
        String seriesId = CLUB + "/" + seriesSlug;
        String raceId = CLUB + "-" + seriesSlug + "-" + date + String.format("-%04d", number);
        Race race = new Race(raceId, CLUB, List.of(seriesId), date, number, null,
            List.of(new Division(null,
                List.of(new Finisher(boatId, Duration.ofHours(2), false, null)))),
            "TopYacht - https://tes.topyacht.net.au/results/2025/mirw/" + seriesSlug + "/r.htm",
            Instant.now(), null);
        Path file = root.resolve("imported/races").resolve(CLUB).resolve(seriesSlug)
            .resolve(raceId + ".json");
        Files.createDirectories(file.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), race);
    }

    private void writeClubJson(Path root, List<Series> series) throws IOException
    {
        Club club = new Club(CLUB, "TYC", null, null, false, null, List.of(), List.of(), List.of(), series, null);
        Path file = root.resolve("imported/clubs").resolve(CLUB + ".json");
        Files.createDirectories(file.getParent());
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), club);
    }

    /**
     * The real MIRW 2025 shape: five divisions, with Division 1 sailing a second race on
     * 09-01 and therefore numbering one ahead of the rest for 09-02 and 09-03.
     */
    /**
     * Writes the same five-division shape for an arbitrary regatta week.
     */
    private void writeEdition(Path root, int year, int month, int startDay) throws IOException
    {
        LocalDate d0 = LocalDate.of(year, month, startDay);
        LocalDate[] days = {
            d0, d0.plusDays(1), d0.plusDays(3), d0.plusDays(3), d0.plusDays(4), d0.plusDays(5)
        };
        int n = 1;
        for (LocalDate d : days)
        {
            writeRace(root, "mirw-sealink-spinnaker-division-1", d, n++, "b1-one");
        }
        for (String slug : List.of("mirw-sealink-spinnaker-division-2",
            "mirw-sealink-spinnaker-division-3", "mirw-non-spinnaker"))
        {
            int m = 1;
            for (LocalDate d : new LocalDate[]{days[0], days[1], days[2], days[4], days[5]})
            {
                writeRace(root, slug, d, m++, "b-" + slug);
            }
        }
    }

    private void writeMirw2025(Path root, boolean merge) throws IOException
    {
        writeClubsYaml(root, merge);
        record Div(String slug, String name, List<int[]> races) {}
        // races as {day-of-month in September (0 => 29/30 Aug), number}
        LocalDate[] days = {
            LocalDate.of(2025, 8, 29), LocalDate.of(2025, 8, 30), LocalDate.of(2025, 9, 1),
            LocalDate.of(2025, 9, 1), LocalDate.of(2025, 9, 2), LocalDate.of(2025, 9, 3)
        };

        // Division 1: all six slots (two on 09-01)
        int n = 1;
        for (LocalDate d : days)
        {
            writeRace(root, "mirw-sealink-spinnaker-division-1", d, n++, "b1-one");
        }
        // The other divisions: five slots, skipping Division 1's extra 09-01 race
        for (String slug : List.of("mirw-sealink-spinnaker-division-2",
            "mirw-sealink-spinnaker-division-3", "mirw-non-spinnaker"))
        {
            int m = 1;
            for (LocalDate d : new LocalDate[]{days[0], days[1], days[2], days[4], days[5]})
            {
                writeRace(root, slug, d, m++, "b-" + slug);
            }
        }
        // Multihulls — excluded by name pattern, must be held out of the merge
        int k = 1;
        for (LocalDate d : new LocalDate[]{days[0], days[1], days[2], days[4], days[5]})
        {
            writeRace(root, "mirw-multihulls", d, k++, "b-multi");
        }

        List<Series> series = new ArrayList<>();
        series.add(seriesFor(root, "mirw-sealink-spinnaker-division-1", "SeaLink Spinnaker Division 1"));
        series.add(seriesFor(root, "mirw-sealink-spinnaker-division-2", "SeaLink Spinnaker Division 2"));
        series.add(seriesFor(root, "mirw-sealink-spinnaker-division-3", "SeaLink Spinnaker Division 3"));
        series.add(seriesFor(root, "mirw-non-spinnaker", "Non-Spinnaker"));
        series.add(seriesFor(root, "mirw-multihulls", "Multihulls"));
        writeClubJson(root, series);

        Files.writeString(root.resolve("config/exclusions.yaml"), """
            series:
              - pattern: "multihull"
                reason: "multihull"
            """);
    }

    /**
     * Series record listing every race file already written under that slug.
     */
    private Series seriesFor(Path root, String slug, String name) throws IOException
    {
        List<String> raceIds = new ArrayList<>();
        Path dir = root.resolve("imported/races").resolve(CLUB).resolve(slug);
        if (Files.exists(dir))
        {
            try (var s = Files.list(dir))
            {
                s.sorted().forEach(p -> raceIds.add(p.getFileName().toString().replace(".json", "")));
            }
        }
        return new Series(CLUB + "/" + slug, name, false, List.copyOf(raceIds));
    }

    @Test
    void mergesPerDivisionRacesIntoOneRacePerSlot(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore store = new DataStore(root);
        store.start();

        // 4 merged divisions x 5 slots + Division 1's extra race = 6 merged races,
        // plus the 5 held-out Multihulls races.
        List<String> merged = store.races().keySet().stream()
            .filter(id -> id.startsWith(CLUB + "-mirw-2025")).sorted().toList();
        assertEquals(6, merged.size(), () -> "merged races: " + merged);
        assertEquals(List.of(
            CLUB + "-mirw-2025-08-29-0001", CLUB + "-mirw-2025-08-30-0001",
            CLUB + "-mirw-2025-09-01-0001", CLUB + "-mirw-2025-09-01-0002",
            CLUB + "-mirw-2025-09-02-0001", CLUB + "-mirw-2025-09-03-0001"), merged);
        store.stop();
    }

    /**
     * The heart of it: on 09-02 Division 1 called it race 5 and the others race 4, but it is
     * everyone's first race that day, so they must land in the same merged race.
     */
    @Test
    void divisionsWithDivergentNumbersStillShareASlot(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore store = new DataStore(root);
        store.start();

        Race sept2 = store.races().get(CLUB + "-mirw-2025-09-02-0001");
        assertNotNull(sept2, () -> "got " + store.races().keySet());
        assertEquals(4, sept2.divisions().size(),
            () -> "divisions: " + sept2.divisions().stream().map(Division::name).toList());

        // Division 1's extra race on 09-01 correctly stands alone
        Race extra = store.races().get(CLUB + "-mirw-2025-09-01-0002");
        assertNotNull(extra);
        assertEquals(1, extra.divisions().size());
        assertEquals("SeaLink Spinnaker Division 1", extra.divisions().getFirst().name());
        store.stop();
    }

    @Test
    void mergedDivisionNamesAreDistinctAndNonNull(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore store = new DataStore(root);
        store.start();

        Race race = store.races().get(CLUB + "-mirw-2025-08-29-0001");
        List<String> names = race.divisions().stream().map(Division::name).toList();
        assertEquals(4, names.size());
        assertEquals(names.size(), new java.util.HashSet<>(names).size(), () -> "duplicates: " + names);
        assertFalse(names.contains(null), () -> "null name in " + names);
        assertTrue(names.contains("SeaLink Spinnaker Division 1"), () -> names.toString());
        assertTrue(names.contains("Non-Spinnaker"), () -> names.toString());
        store.stop();
    }

    @Test
    void excludedSeriesIsHeldOutOfTheMerge(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore store = new DataStore(root);
        store.start();

        List<String> multi = store.races().keySet().stream()
            .filter(id -> id.contains("mirw-multihulls")).sorted().toList();
        assertEquals(5, multi.size(), () -> "multihull races must be untouched: " + multi);
        assertTrue(store.isSeriesExcluded("Multihulls"));

        // and its series survives alongside the merged one
        Club club = store.clubs().get(CLUB);
        List<String> seriesIds = club.series().stream().map(Series::id).sorted().toList();
        assertTrue(seriesIds.contains(CLUB + "/mirw-2025"), () -> seriesIds.toString());
        assertTrue(seriesIds.contains(CLUB + "/mirw-multihulls"), () -> seriesIds.toString());
        store.stop();
    }

    @Test
    void mergedSeriesIsNamedForTheEventAndHoldsEveryMergedRace(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore store = new DataStore(root);
        store.start();

        Series merged = store.clubs().get(CLUB).series().stream()
            .filter(s -> (CLUB + "/mirw-2025").equals(s.id())).findFirst().orElseThrow();
        assertEquals("Magnetic Island Race Week 2025", merged.name());
        assertEquals(6, merged.raceIds().size(), () -> merged.raceIds().toString());
        store.stop();
    }

    @Test
    void oldPerDivisionRaceFilesAreRemoved(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);
        Path oldDir = root.resolve("imported/races").resolve(CLUB)
            .resolve("mirw-sealink-spinnaker-division-2");
        assertTrue(Files.exists(oldDir));

        DataStore store = new DataStore(root);
        store.start();

        try (var s = Files.list(oldDir))
        {
            assertEquals(0, s.count(), "superseded per-division race files should be gone");
        }
        store.stop();
    }

    @Test
    void migrationIsIdempotent(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, true);

        DataStore first = new DataStore(root);
        first.start();
        List<String> after1 = first.races().keySet().stream().sorted().toList();
        first.stop();

        DataStore second = new DataStore(root);
        second.start();
        assertEquals(after1, second.races().keySet().stream().sorted().toList());
        second.stop();
    }

    /**
     * The regression this rule exists for: three runnings of the same regatta must not
     * collapse into one series spanning 2022-2025.
     */
    @Test
    void successiveYearsBecomeSeparateSeries(@TempDir Path root) throws IOException
    {
        writeClubsYaml(root, true);
        writeEdition(root, 2022, 9, 2);
        writeEdition(root, 2023, 9, 1);
        writeEdition(root, 2025, 8, 29);
        writeClubJson(root, List.of(
            seriesFor(root, "mirw-sealink-spinnaker-division-1", "SeaLink Spinnaker Division 1"),
            seriesFor(root, "mirw-sealink-spinnaker-division-2", "SeaLink Spinnaker Division 2"),
            seriesFor(root, "mirw-sealink-spinnaker-division-3", "SeaLink Spinnaker Division 3"),
            seriesFor(root, "mirw-non-spinnaker", "Non-Spinnaker")));

        DataStore store = new DataStore(root);
        store.start();

        List<Series> editions = store.clubs().get(CLUB).series().stream()
            .filter(x -> x.id().matches(".*/mirw-\\d{4}$"))
            .sorted(java.util.Comparator.comparing(Series::id)).toList();
        assertEquals(3, editions.size(), () -> "one series per running, got "
            + store.clubs().get(CLUB).series().stream().map(Series::id).toList());
        assertEquals(List.of(CLUB + "/mirw-2022", CLUB + "/mirw-2023", CLUB + "/mirw-2025"),
            editions.stream().map(Series::id).toList());
        assertEquals(List.of("Magnetic Island Race Week 2022", "Magnetic Island Race Week 2023",
            "Magnetic Island Race Week 2025"), editions.stream().map(Series::name).toList());
        for (Series e : editions)
        {
            assertEquals(6, e.raceIds().size(), () -> e.id() + " -> " + e.raceIds());
        }
        store.stop();
    }

    /**
     * No series may span more than the edition gap.
     */
    @Test
    void noMergedSeriesSpansMoreThanSixMonths(@TempDir Path root) throws IOException
    {
        writeClubsYaml(root, true);
        writeEdition(root, 2022, 9, 2);
        writeEdition(root, 2023, 9, 1);
        writeEdition(root, 2025, 8, 29);
        writeClubJson(root, List.of(
            seriesFor(root, "mirw-sealink-spinnaker-division-1", "SeaLink Spinnaker Division 1"),
            seriesFor(root, "mirw-sealink-spinnaker-division-2", "SeaLink Spinnaker Division 2"),
            seriesFor(root, "mirw-sealink-spinnaker-division-3", "SeaLink Spinnaker Division 3"),
            seriesFor(root, "mirw-non-spinnaker", "Non-Spinnaker")));

        DataStore store = new DataStore(root);
        store.start();

        for (Series s : store.clubs().get(CLUB).series())
        {
            List<LocalDate> dates = s.raceIds().stream()
                .map(id -> store.races().get(id))
                .filter(java.util.Objects::nonNull).map(Race::date).sorted().toList();
            if (dates.size() < 2)
                continue;
            assertTrue(dates.getFirst().plusMonths(6).isAfter(dates.getLast().minusDays(1)),
                () -> "series " + s.id() + " spans " + dates.getFirst() + " to " + dates.getLast());
        }
        store.stop();
    }

    /**
     * Without the flag nothing is merged — the per-division races stay exactly as they were.
     */
    @Test
    void eventNotFlaggedForMergingIsLeftAlone(@TempDir Path root) throws IOException
    {
        writeMirw2025(root, false);

        DataStore store = new DataStore(root);
        store.start();

        assertEquals(0, store.races().keySet().stream()
            .filter(id -> id.startsWith(CLUB + "-mirw-2025")).count());
        assertEquals(26, store.races().size(), () -> store.races().keySet().toString());
        store.stop();
    }
}
