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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one-sided variant-order pull ({@link PfConfig#variantOrderLambda()}): a boat whose results
 * put its non-spin PF above its spin PF is pulled back into order, toward its RF gap, while
 * boats already in order are left alone.
 */
class PfOptimiserVariantOrderTest
{
    /** Each boat's spin RF sits this far above its non-spin RF. */
    private static final double RF_GAP = 1.05;

    private record Scenario(DataStore store, Map<String, BoatDerived> boatDerived, List<String> ids) {}

    /**
     * Four boats race 6 spin and 6 non-spin races. Boats 1–3 sail exactly to their RFs; boat 0
     * sails 4% slower than its RF in spin and 4% faster in non-spin, so its results invert.
     */
    private static Scenario scenario(Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        Random rng = new Random(3);
        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        List<String> ids = new ArrayList<>();
        double[] nsTcf = {1.00, 0.92, 1.08, 1.15};
        for (int i = 0; i < 4; i++)
        {
            String id = "y" + i + "-name-design";
            ids.add(id);
            Boat b = new Boat(id, "Y" + i, "Name" + i, "design", null, List.of(), List.of(), null, null);
            store.putBoat(b);
            ReferenceFactors rf = new ReferenceFactors(
                new Factor(nsTcf[i] * RF_GAP, 0.3), new Factor(nsTcf[i], 0.3), null, 0, 0, 0);
            boatDerived.put(id, new BoatDerived(b, rf, Set.of(), Set.of(), null));
        }
        long base = Duration.ofHours(2).toNanos();
        for (int d = 0; d < 12; d++)
        {
            boolean ns = d >= 6;
            double t0 = base * (0.9 + 0.2 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
            {
                double tcf = ns ? nsTcf[i] : nsTcf[i] * RF_GAP;
                if (i == 0)
                    tcf *= ns ? 1.04 : 0.96;
                long el = (long)(t0 / tcf * (1.0 + 0.002 * rng.nextGaussian()));
                finishers.add(new Finisher(ids.get(i), Duration.ofNanos(el), ns, null));
            }
            LocalDate date = LocalDate.of(2026, 1, 1 + d);
            store.putRace(new Race("club-" + date + "-0001", "club", List.of("club/s"), date, d + 1, null,
                List.of(new Division(ns ? "NS" : "Spin", finishers)), null, null, null));
        }
        return new Scenario(store, boatDerived, ids);
    }

    /** Outlier weighting off (huge K), so boat 0's offset results count in full. */
    private static PfConfig config(double variantOrderLambda)
    {
        return new PfConfig(1.0, 0.0001, 100, 5, 1000.0, 1.0, 0.5, 0.01, 0.0, 0.0, 0.2, 1.0e-3,
            false, 100.0, 200.0, 0.0, variantOrderLambda);
    }

    @Test
    void invertedBoatIsPulledIntoOrderOthersUntouched(@TempDir Path tempDir)
    {
        Scenario s = scenario(tempDir);
        PfResult off = new PfOptimiser().optimise(s.store, s.boatDerived, config(0.0), () -> false);
        PfResult on = new PfOptimiser().optimise(s.store, s.boatDerived, config(50.0), () -> false);

        BoatPf offPf = off.boatPfs().get(s.ids.get(0));
        assertTrue(offPf.nonSpin().value() > offPf.spin().value(),
            "without the pull, boat 0's results invert its variants: " + offPf);

        BoatPf onPf = on.boatPfs().get(s.ids.get(0));
        assertTrue(onPf.spin().value() > onPf.nonSpin().value(),
            "with the pull, boat 0's spin PF is back above its non-spin PF: " + onPf);

        for (int i = 1; i < 4; i++)
        {
            BoatPf a = off.boatPfs().get(s.ids.get(i));
            BoatPf b = on.boatPfs().get(s.ids.get(i));
            assertEquals(a.spin().value(), b.spin().value(), a.spin().value() * 0.01,
                "boat " + i + " is in order, so its spin PF barely moves");
            assertEquals(a.nonSpin().value(), b.nonSpin().value(), a.nonSpin().value() * 0.01,
                "boat " + i + " is in order, so its non-spin PF barely moves");
        }
    }
}
