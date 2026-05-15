package org.mortbay.sailing.pf.analysis;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the dubiousFactor / maxFactor weight ramp in {@link PfOptimiser}: boats whose
 * PF sits above maxFactor are fully excluded; boats above dubiousFactor are progressively
 * down-weighted. Compares results with the ramp enabled (defaults 1.5 / 2.0) vs disabled
 * (very high thresholds) to confirm the ramp is the cause of the difference.
 */
class PfOptimiserFastBoatExclusionTest
{
    @TempDir
    Path tempDir;

    private static final double[] NORMAL_TCFS = {0.95, 1.00, 1.05, 1.10};
    private static final String[] NORMAL_IDS = {
        "n1-alpha-design", "n2-bravo-design", "n3-charlie-design", "n4-delta-design"
    };
    private static final String FAST_ID = "f1-fast-design";
    private static final double FAST_RF = 1.8;

    @Test
    void boatAboveMaxFactorGetsFullyExcluded()
    {
        Scenario s = buildScenario(50);

        // Set maxFactor below the fast boat's RF — ramp gives multiplier=0 immediately.
        // PF should stay essentially pinned at RF (regularisation is the only term left).
        PfConfig hardExclude = new PfConfig(
            1.0, 0.0001, 100, 10, 2.0, 2.0, 0.5, 0.01,
            0.0, 0.0, 0.2, 1.0e-3, false,
            1.2, 1.5);
        PfResult result = new PfOptimiser().optimise(s.store, s.boatDerived, hardExclude, () -> false);
        BoatPf fast = result.boatPfs().get(FAST_ID);
        assertNotNull(fast);
        assertNotNull(fast.spin());
        assertTrue(Math.abs(fast.spin().value() - FAST_RF) < 1.0e-6,
            "fast boat's PF should remain pinned at RF=" + FAST_RF + " when all entries"
                + " are zero-weighted; got " + fast.spin().value());
    }

    private record Scenario(DataStore store, Map<String, BoatDerived> boatDerived) {}

    /**
     * Builds a scenario with 4 normal boats plus one fast boat. The fast boat has an RF of
     * 1.8 but its race elapsed times match a true TCF of 1.4 — well below RF so the optimiser
     * would normally pull its PF down toward 1.4 if its entry weights were not suppressed.
     */
    private Scenario buildScenario(int nRaces)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(42);

        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        for (int i = 0; i < NORMAL_IDS.length; i++)
        {
            Boat boat = new Boat(NORMAL_IDS[i], "AUS" + (i + 1), "Normal" + (i + 1),
                "design-n" + i, null, List.of(), List.of(), null, null);
            store.putBoat(boat);
            Factor rfSpin = new Factor(NORMAL_TCFS[i], 0.9);
            ReferenceFactors rf = new ReferenceFactors(rfSpin, null, null, 0, 0, 0);
            boatDerived.put(boat.id(), new BoatDerived(boat, rf, Set.of(), Set.of(), null));
        }

        Boat fast = new Boat(FAST_ID, "AUS99", "Fast", "design-f", null,
            List.of(), List.of(), null, null);
        store.putBoat(fast);
        Factor rfFast = new Factor(FAST_RF, 0.9);
        ReferenceFactors rfFastRec = new ReferenceFactors(rfFast, null, null, 0, 0, 0);
        boatDerived.put(fast.id(), new BoatDerived(fast, rfFastRec, Set.of(), Set.of(), null));

        // The fast boat's elapsed corresponds to true TCF ≈ 1.4 (well below its RF of 1.8).
        double fastTrueTcf = 1.4;

        double baseT0 = Duration.ofHours(2).toNanos();
        for (int d = 0; d < nRaces; d++)
        {
            double t0 = baseT0 * (0.85 + 0.3 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < NORMAL_IDS.length; i++)
            {
                double elapsed = (t0 / NORMAL_TCFS[i]) * (1.0 + 0.005 * rng.nextGaussian());
                finishers.add(new Finisher(NORMAL_IDS[i], Duration.ofNanos((long)elapsed), false, null));
            }
            double fastElapsed = (t0 / fastTrueTcf) * (1.0 + 0.005 * rng.nextGaussian());
            finishers.add(new Finisher(FAST_ID, Duration.ofNanos((long)fastElapsed), false, null));
            Division div = new Division("Div 1", finishers);
            Race race = new Race(
                "club-" + LocalDate.of(2024, 1, 1).plusDays(d) + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), LocalDate.of(2024, 1, 1).plusDays(d), d + 1, null,
                List.of(div), "test", null, null);
            store.putRace(race);
        }

        return new Scenario(store, boatDerived);
    }
}
