package org.mortbay.sailing.pf.analysis;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PfHistoryTest
{
    @TempDir Path tempDir;

    @Test
    void cutoffAfterIsTheNextFirstOfJuly()
    {
        assertEquals(LocalDate.of(2019, 7, 1), PfHistory.cutoffAfter(LocalDate.of(2019, 6, 30)));
        assertEquals(LocalDate.of(2020, 7, 1), PfHistory.cutoffAfter(LocalDate.of(2019, 7, 1)));
        assertEquals(LocalDate.of(2020, 7, 1), PfHistory.cutoffAfter(LocalDate.of(2019, 12, 31)));
    }

    @Test
    void seasonsStartWithTheFirstBusySeasonAndEndAtTheLatestCutoff()
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        races(store, LocalDate.of(2015, 1, 1), 5);                      // 2014-15: too few
        races(store, LocalDate.of(2017, 8, 1), PfHistory.MIN_SEASON_RACES); // 2017-18: busy
        races(store, LocalDate.of(2019, 8, 1), 3);                      // 2019-20: quiet, still kept

        List<LocalDate> cutoffs = PfHistory.seasonCutoffs(store, LocalDate.of(2021, 3, 1));
        assertEquals(List.of(LocalDate.of(2018, 7, 1), LocalDate.of(2019, 7, 1), LocalDate.of(2020, 7, 1)), cutoffs);
        assertEquals(LocalDate.of(2021, 7, 1),
            PfHistory.seasonCutoffs(store, LocalDate.of(2021, 7, 1)).getLast(), "a cutoff today is included");
    }

    @Test
    void keepsRacedVariantsAndReloads()
    {
        BoatPf raced = new BoatPf(new Factor(1.01, 0.8), new Factor(0.97, 0.3), null, 0, 0, 0, 12, 0, 0);
        BoatPf unraced = new BoatPf(new Factor(1.1, 0.9), null, null, 0, 0, 0, 0, 0, 0);
        LocalDate cutoff = LocalDate.of(2020, 7, 1);

        PfHistory history = PfHistory.load(tempDir);
        history.put(PfHistory.Season.of(cutoff, Map.of("raced", raced, "unraced", unraced)));
        history.save();

        PfHistory reloaded = PfHistory.load(tempDir);
        assertTrue(reloaded.has(cutoff));
        List<PfHistory.BoatPoint> points = reloaded.forBoat("raced");
        assertEquals(1, points.size());
        assertEquals(new PfHistory.Point(1.01, 0.8, 12), points.getFirst().pf().spin());
        assertNull(points.getFirst().pf().nonSpin(), "a variant with no races is only its RF");
        assertTrue(reloaded.forBoat("unraced").isEmpty());

        reloaded.retain(List.of());
        assertFalse(reloaded.has(cutoff));
    }

    private static void races(DataStore store, LocalDate from, int count)
    {
        for (int i = 0; i < count; i++)
        {
            LocalDate date = from.plusDays(i % 300);
            store.putRace(new Race("club-" + date + "-" + String.format("%04d", i + 1), "club", List.of("series"),
                date, i + 1, null,
                List.of(new Division("Div 1", List.of(new Finisher("b1", Duration.ofHours(1), false, null)))),
                "test", null, null));
        }
    }
}
