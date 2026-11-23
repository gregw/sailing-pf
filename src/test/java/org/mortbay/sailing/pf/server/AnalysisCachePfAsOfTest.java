package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.analysis.BoatPf;
import org.mortbay.sailing.pf.analysis.ConversionGraph;
import org.mortbay.sailing.pf.analysis.PfConfig;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AnalysisCache#pfAsOf}: the whole analysis run on the data as at a past date sees
 * only the races before it, and leaves the cache and data alone.
 */
class AnalysisCachePfAsOfTest
{
    @TempDir Path tempDir;

    private static final String[] STEADY = {"s1", "s2", "s3", "s4"};
    private static final double[] STEADY_TCF = {0.90, 0.95, 1.00, 1.05};
    private static final double OLD_TCF = 1.15;   // ten races in 2018
    private static final double NEW_TCF = 1.05;   // three races in 2024

    @Test
    void pfAsOfACutoffUsesOnlyTheRacesBeforeIt()
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        for (int i = 0; i < STEADY.length; i++)
            boat(store, STEADY[i], STEADY_TCF[i]);
        boat(store, "x1", (OLD_TCF + NEW_TCF) / 2);

        Random rng = new Random(7);
        double t0 = Duration.ofHours(2).toNanos();
        for (int r = 0; r < 13; r++)
        {
            boolean old = r < 10;
            LocalDate date = old ? LocalDate.of(2018, 1, 1 + 2 * r) : LocalDate.of(2024, 1, 1 + 2 * r);
            List<Finisher> fs = new ArrayList<>();
            for (int i = 0; i < STEADY.length; i++)
                fs.add(new Finisher(STEADY[i], Duration.ofNanos((long)(t0 / STEADY_TCF[i] * (1 + 0.02 * rng.nextGaussian()))), false, null));
            fs.add(new Finisher("x1", Duration.ofNanos((long)(t0 / (old ? OLD_TCF : NEW_TCF) * (1 + 0.02 * rng.nextGaussian()))), false, null));
            store.putRace(new Race("club-" + date + "-" + String.format("%04d", r + 1), "club", List.of("series"),
                date, r + 1, null, List.of(new Division("Div 1", fs)), "test", null, null));
        }

        AnalysisCache cache = new AnalysisCache(store);
        PfConfig config = new PfConfig(0.05, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.0, 0.2, 1.0e-3,
            false, 100.0, 200.0, 2.0);

        Map<String, BoatPf> then = cache.pfAsOf(LocalDate.of(2019, 7, 1), 2.5, 0.9,
            ConversionGraph.DEFAULT_MIN_R2, ConversionGraph.DEFAULT_MIN_PAIRS, config, () -> false);
        Map<String, BoatPf> now = cache.pfAsOf(LocalDate.of(2025, 7, 1), 2.5, 0.9,
            ConversionGraph.DEFAULT_MIN_R2, ConversionGraph.DEFAULT_MIN_PAIRS, config, () -> false);

        assertEquals(10, then.get("x1").spinRaceCount());
        assertEquals(13, now.get("x1").spinRaceCount());
        double pfThen = then.get("x1").spin().value();
        double pfNow = now.get("x1").spin().value();
        assertEquals(OLD_TCF, pfThen, 0.03, "as at 2019 the boat was at its old speed");
        assertTrue(pfNow < pfThen - 0.03, "it has since got faster: " + pfThen + " -> " + pfNow);

        assertTrue(cache.boatDerived().isEmpty(), "the cache is untouched");
        assertFalse(Files.exists(tempDir.resolve("log/duplicates.log")), "dated runs write no logs");
    }

    private static void boat(DataStore store, String id, double tcc)
    {
        store.putBoat(new Boat(id, id.toUpperCase(), id, "design", null,
            List.of(new Certificate("IRC", 2018, tcc, false, false, false, false, id + "-2018", null)),
            List.of(), null, null));
    }
}
