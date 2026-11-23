package org.mortbay.sailing.pf.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Finisher#twoHanded} is written only when set, so existing race files are
 * unchanged, and survives a save and reload.
 */
class FinisherTwoHandedPersistenceTest
{
    @TempDir Path tempDir;

    @Test
    void twoHandedFlagIsWrittenOnlyWhenSetAndReloads() throws Exception
    {
        String raceId = "cyca.com.au-2025-12-26-0001";
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putRace(new Race(raceId, "cyca.com.au", List.of("cyca.com.au/hobart"),
            LocalDate.of(2025, 12, 26), 1, "Hobart", List.of(new Division("Overall", List.of(
                new Finisher("crewed", Duration.ofHours(40), false, null),
                new Finisher("pair", Duration.ofHours(70), false, null, true)))),
            "RSHYR", null, null));
        store.save();
        store.stop();

        Path file;
        try (Stream<Path> s = Files.walk(tempDir))
        {
            file = s.filter(p -> p.getFileName().toString().equals(raceId + ".json")).findFirst().orElseThrow();
        }
        String json = Files.readString(file);
        assertEquals(1, json.split("\"twoHanded\"", -1).length - 1, "only the two-handed result has the field: " + json);

        DataStore reloaded = new DataStore(tempDir);
        reloaded.start();
        try
        {
            List<Finisher> fs = reloaded.races().get(raceId).divisions().getFirst().finishers();
            assertFalse(fs.get(0).twoHanded());
            assertTrue(fs.get(1).twoHanded());
        }
        finally
        {
            reloaded.stop();
        }
    }
}
