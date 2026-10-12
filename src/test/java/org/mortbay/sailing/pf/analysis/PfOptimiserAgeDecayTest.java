package org.mortbay.sailing.pf.analysis;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Age decay (PfConfig.ageHalfLifeYears): older results count less in a boat's PF, measured
 * back from the boat's own latest race.
 */
class PfOptimiserAgeDecayTest
{
    @TempDir
    Path tempDir;

    private static final String[] STEADY = {"s1-one-d", "s2-two-d", "s3-three-d", "s4-four-d"};
    private static final double[] STEADY_TCF = {0.90, 0.95, 1.00, 1.05};
    private static final String CHANGED = "x1-changed-d";
    private static final double OLD_TCF = 1.15;     // ten races in 2018
    private static final double NEW_TCF = 1.05;     // three races in 2024

    private static PfConfig config(double halfLife)
    {
        // Light regularisation so the races, not the RF, decide the PF.
        return new PfConfig(0.05, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.0, 0.2, 1.0e-3,
            false, 100.0, 200.0, halfLife);
    }

    @Test
    void ageDecayHalvesEachHalfLife()
    {
        PfConfig c = config(2.0);
        assertEquals(1.0, c.ageDecay(0), 1e-12);
        assertEquals(0.5, c.ageDecay(2), 1e-12);
        assertEquals(0.25, c.ageDecay(4), 1e-12);
        assertEquals(1.0, config(0).ageDecay(5), 1e-12, "0 = no decay");
    }

    @Test
    void recentResultsOutweighOldOnesWithDecay()
    {
        Map<String, BoatDerived> derived = buildScenario();
        double without = pf(new PfOptimiser().optimise(store, derived, config(0), () -> false), CHANGED);
        double with = pf(new PfOptimiser().optimise(store, derived, config(2.0), () -> false), CHANGED);

        // Without decay the ten old races dominate; with it the PF moves towards the recent era.
        assertTrue(Math.abs(with - NEW_TCF) < Math.abs(without - NEW_TCF),
            "with decay " + with + " should be nearer " + NEW_TCF + " than without " + without);
        assertTrue(with < (OLD_TCF + NEW_TCF) / 2, "recent era should dominate: " + with);
        assertTrue(without > (OLD_TCF + NEW_TCF) / 2, "old era dominates without decay: " + without);
    }

    @Test
    void steadyBoatsStayAtTheirSpeed()
    {
        Map<String, BoatDerived> derived = buildScenario();
        PfResult without = new PfOptimiser().optimise(store, derived, config(0), () -> false);
        PfResult with = new PfOptimiser().optimise(store, derived, config(2.0), () -> false);
        // A boat whose speed never changed stays at it either way — within the noise, which is a
        // little larger with decay because its old results now count less (fewer effective races).
        for (int i = 0; i < STEADY.length; i++)
        {
            assertEquals(STEADY_TCF[i], pf(without, STEADY[i]), 0.03, STEADY[i] + " without");
            assertEquals(STEADY_TCF[i], pf(with, STEADY[i]), 0.03, STEADY[i] + " with");
        }
    }

    private static double pf(PfResult result, String boatId)
    {
        return result.boatPfs().get(boatId).spin().value();
    }

    private DataStore store;

    /** Four steady boats in every race; the changed boat at OLD_TCF in 2018, NEW_TCF in 2024. */
    private Map<String, BoatDerived> buildScenario()
    {
        store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        Map<String, BoatDerived> derived = new LinkedHashMap<>();
        for (int i = 0; i < STEADY.length; i++)
            putBoat(derived, STEADY[i], "S" + i, STEADY_TCF[i]);
        putBoat(derived, CHANGED, "X1", (OLD_TCF + NEW_TCF) / 2);

        // ~2% noise per result, as real divisions have; without it every division's spread is
        // ~0 and any change of form looks like an extreme outlier.
        java.util.Random rng = new java.util.Random(7);
        double t0 = Duration.ofHours(2).toNanos();
        int n = 0;
        for (int r = 0; r < 13; r++)
        {
            boolean old = r < 10;
            LocalDate date = old ? LocalDate.of(2018, 1, 1 + 2 * r) : LocalDate.of(2024, 1, 1 + 2 * r);
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < STEADY.length; i++)
                finishers.add(new Finisher(STEADY[i],
                    Duration.ofNanos((long)(t0 / STEADY_TCF[i] * (1 + 0.02 * rng.nextGaussian()))), false, null));
            double x = old ? OLD_TCF : NEW_TCF;
            finishers.add(new Finisher(CHANGED,
                Duration.ofNanos((long)(t0 / x * (1 + 0.02 * rng.nextGaussian()))), false, null));
            store.putRace(new Race("club-" + date + "-" + String.format("%04d", ++n), "club", List.of("series"),
                date, n, null, List.of(new Division("Div 1", finishers)), "test", null, null));
        }
        return derived;
    }

    private void putBoat(Map<String, BoatDerived> derived, String id, String sail, double rf)
    {
        Boat boat = new Boat(id, sail, id, "design", null, List.of(), List.of(), null, null);
        store.putBoat(boat);
        ReferenceFactors refs = new ReferenceFactors(new Factor(rf, 0.9), null, null, 0, 0, 0);
        derived.put(id, new BoatDerived(boat, refs, Set.of(), Set.of(), null));
    }
}
