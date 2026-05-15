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
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the graph-driven cross-variant regularisation term in {@link PfOptimiser}.
 * Builds a minimal synthetic conversion graph with a known slope between IRC spin and
 * IRC non-spin nodes, then verifies the optimiser pulls boat PFs toward that fleet-wide
 * ratio when {@link PfConfig#graphCrossVariantLambda()} > 0.
 */
class PfOptimiserGraphCrossVariantTest
{
    private static final int TARGET_YEAR = 2026;

    /**
     * Builds a synthetic ConversionGraph with a known ALL/nonSpin→ALL/spin linear fit.
     * <p>
     * Slope/intercept passed in TCF space: a spin TCF is predicted from a nonSpin TCF as
     * {@code spin = slope × nonSpin + intercept}. {@code ConversionGraph.from} promotes
     * pooled ALL-system edges to per-system (IRC/ORC/AMS) edges and inverses, so a single
     * synthetic fit yields a complete cross-variant graph for the target year.
     */
    private static ConversionGraph synthGraph(double slope, double intercept, int n, double r2)
    {
        // Pack the fit into a ComparisonResult under the ALL/NS → ALL/Spin pooled key.
        LinearFit fit = new LinearFit(slope, intercept, r2, 0.01, n, 1.0, 1.0);
        ComparisonKey key = ComparisonKey.allNsVsSpin(TARGET_YEAR);
        // We need at least one pair to satisfy any downstream expectations, though the
        // graph itself only reads the LinearFit.
        List<DataPair> pairs = new ArrayList<>();
        for (int i = 0; i < n; i++)
        {
            pairs.add(new DataPair("b" + i, 1.0, 1.0));
        }
        ComparisonResult cr = new ComparisonResult(key, pairs, List.of(), fit);
        return ConversionGraph.from(List.of(cr), 0.0, n);
    }

    /**
     * With the graph term off, results match what we'd get from the existing overload
     * (no graph passed). Sanity-check that the new wiring doesn't silently activate.
     */
    @Test
    void termOffPreservesBaseline(@TempDir Path tempDir)
    {
        Scenario s = buildSpinFleetScenario(tempDir, 0L);
        // 3rd-from-last arg = crossVariantLambda (RF-ratio), last = graphCrossVariantLambda
        PfConfig off = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.0, 0.2, 1.0e-3, false);
        PfResult baseline = new PfOptimiser().optimise(s.store, s.boatDerived, off, () -> false);

        ConversionGraph graph = synthGraph(0.95, 0.05, 50, 0.99);
        PfResult withGraphButOff = new PfOptimiser().optimise(s.store, s.boatDerived, off,
            () -> false, graph, TARGET_YEAR);

        for (Map.Entry<String, BoatPf> e : baseline.boatPfs().entrySet())
        {
            BoatPf b = e.getValue();
            BoatPf g = withGraphButOff.boatPfs().get(e.getKey());
            assertNotNull(g);
            if (b.spin() != null && g.spin() != null)
                assertEquals(b.spin().value(), g.spin().value(), 1e-9,
                    "spin PF should be identical when graphCrossVariantLambda=0");
        }
    }

    /**
     * A null graph behaves like the no-graph overload — even if the config says the term
     * should be on. Guards the {@code graph == null} branch in {@code optimise}.
     */
    @Test
    void nullGraphIsSafe(@TempDir Path tempDir)
    {
        Scenario s = buildSpinFleetScenario(tempDir, 7L);
        PfConfig on = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 0.5, 0.2, 1.0e-3, false);
        PfResult result = new PfOptimiser().optimise(s.store, s.boatDerived, on,
            () -> false, null, TARGET_YEAR);
        assertTrue(!result.boatPfs().isEmpty(),
            "null graph should leave the optimiser running normally (term disabled)");
    }

    /**
     * With the graph term on and a strong constraint, a boat that has nonSpin races
     * should see its (race-derived) nonSpin PF pulled toward the graph's prediction
     * from its spin PF — and vice-versa. We construct a fleet where each boat races
     * in BOTH variants under the same conditions; the inferred PFs should respect
     * the synthetic graph's slope.
     */
    @Test
    void graphTermPullsTowardConvertedRatio(@TempDir Path tempDir)
    {
        Scenario s = buildBothVariantScenario(tempDir, 11L);

        // synth slope = 1.0, intercept = 0.0 → spin == nonSpin in graph land.
        // The target boat's race-derived spin and nonSpin PFs should converge.
        ConversionGraph graph = synthGraph(1.0, 0.0, 50, 0.99);
        PfConfig strongGraph = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 5.0, 0.2, 1.0e-3, false);
        PfResult result = new PfOptimiser().optimise(s.store, s.boatDerived, strongGraph,
            () -> false, graph, TARGET_YEAR);

        for (String boatId : s.boatIds)
        {
            BoatPf pf = result.boatPfs().get(boatId);
            assertNotNull(pf);
            if (pf.spin() != null && pf.nonSpin() != null)
            {
                double ratio = pf.spin().value() / pf.nonSpin().value();
                // With strong graph pull at slope=1.0 the ratio should be very close to 1.
                assertEquals(1.0, ratio, 0.03,
                    boatId + ": spin/nonSpin ratio with strong unit-slope graph pull"
                        + " (got spin=" + pf.spin().value() + ", nonSpin=" + pf.nonSpin().value() + ")");
            }
        }
    }

    /**
     * NoSpinnaker designs are exempt from spin↔nonSpin graph pull inside the optimiser
     * (the post-hoc collapse in AnalysisCache equalises the slots). Even with a strong
     * graph constraint that says spin ≠ nonSpin, the cat-rigged boat's spin and nonSpin
     * race-derived PFs are NOT pushed apart by the term.
     */
    @Test
    void noSpinnakerDesignExemptFromSpinNonSpinPull(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putDesign(new Design("catrig", "Cat Rig", List.of(), List.of(), null, false, null));
        store.setDesignNoSpinnaker("catrig", true);

        // Three peers race against the cat boat in BOTH a spin division and a non-spin
        // division, with the cat finishing at the same elapsed time both times.
        Map<String, BoatDerived> derived = new LinkedHashMap<>();
        for (int i = 1; i <= 3; i++)
        {
            String id = "p" + i + "-peer-design";
            Boat b = new Boat(id, "P" + i, "Peer" + i, "peerd", null,
                List.of(), List.of(), null, null);
            store.putBoat(b);
            // RF anchors peers at 1.0; we don't need a graph-derived RF for the test.
            derived.put(id, new BoatDerived(b,
                new ReferenceFactors(new Factor(1.0, 0.9), new Factor(1.0, 0.9), null, 0, 0, 0),
                Set.of(), Set.of(), null));
        }
        Boat cat = new Boat("CAT-tinsel-catrig", "CAT", "Tinsel", "catrig", null,
            List.of(), List.of(), null, null);
        store.putBoat(cat);
        derived.put(cat.id(), new BoatDerived(cat,
            new ReferenceFactors(new Factor(1.0, 0.5), new Factor(1.0, 0.5), null, 0, 0, 0),
            Set.of(), Set.of(), null));

        long elapsed = Duration.ofMinutes(60).toNanos();
        store.putRace(new Race("club-2026-03-01-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 3, 1), 1, null,
            List.of(new Division("Spin", List.of(
                new Finisher("p1-peer-design", Duration.ofNanos(elapsed), false, null),
                new Finisher("p2-peer-design", Duration.ofNanos(elapsed), false, null),
                new Finisher("p3-peer-design", Duration.ofNanos(elapsed), false, null),
                new Finisher("CAT-tinsel-catrig", Duration.ofNanos(elapsed), false, null)
            ))), null, null, null));
        store.putRace(new Race("club-2026-03-08-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 3, 8), 1, null,
            List.of(new Division("NS", List.of(
                new Finisher("p1-peer-design", Duration.ofNanos(elapsed), true, null),
                new Finisher("p2-peer-design", Duration.ofNanos(elapsed), true, null),
                new Finisher("p3-peer-design", Duration.ofNanos(elapsed), true, null),
                new Finisher("CAT-tinsel-catrig", Duration.ofNanos(elapsed), true, null)
            ))), null, null, null));

        // Synth graph that says spin = 0.9 × nonSpin (spinnaker boats faster) — would
        // normally push spin PF below nonSpin PF. Cat-rigged boat must NOT be split.
        ConversionGraph graph = synthGraph(0.9, 0.0, 50, 0.99);
        PfConfig strong = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 5.0, 0.2, 1.0e-3, false);
        PfResult result = new PfOptimiser().optimise(store, derived, strong,
            () -> false, graph, TARGET_YEAR);

        BoatPf catPf = result.boatPfs().get("CAT-tinsel-catrig");
        assertNotNull(catPf);
        assertNotNull(catPf.spin());
        assertNotNull(catPf.nonSpin());
        // The optimiser should NOT split the cat's spin and nonSpin via the graph pull.
        // They might differ very slightly from RF anchoring, but the ratio must stay near 1.0
        // (i.e. the boat hasn't been pulled toward 0.9× as the graph would say).
        double catRatio = catPf.spin().value() / catPf.nonSpin().value();
        assertEquals(1.0, catRatio, 0.02,
            "noSpinnaker design must not have spin/nonSpin split by graph term"
                + " (got spin=" + catPf.spin().value() + ", nonSpin=" + catPf.nonSpin().value() + ")");

        // Sanity: a non-flagged peer ought to be free to split — verify the graph term
        // actually fires by checking the ratio for a peer.
        BoatPf peerPf = result.boatPfs().get("p1-peer-design");
        assertNotNull(peerPf);
        if (peerPf.spin() != null && peerPf.nonSpin() != null)
        {
            double peerRatio = peerPf.spin().value() / peerPf.nonSpin().value();
            assertTrue(peerRatio < 0.99,
                "peer (non-flagged) should be pulled by the 0.9× graph slope: ratio=" + peerRatio);
        }
    }

    /**
     * With the graph term at strength 1.0 the optimiser must still converge — the new
     * linear term doesn't break the ALS contraction.
     */
    @Test
    void convergenceWithGraphTerm(@TempDir Path tempDir)
    {
        Scenario s = buildSpinFleetScenario(tempDir, 42L);
        ConversionGraph graph = synthGraph(0.95, 0.05, 50, 0.99);
        PfConfig strong = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0, 1.0, 0.2, 1.0e-3, false);
        PfResult result = new PfOptimiser().optimise(s.store, s.boatDerived, strong,
            () -> false, graph, TARGET_YEAR);
        assertNotNull(result.quality());
        assertTrue(result.quality().innerConverged(),
            "inner loop should converge with graphCrossVariantLambda=1.0");
    }

    // ---- Scenario builders ----

    private record Scenario(DataStore store, Map<String, BoatDerived> boatDerived, List<String> boatIds) {}

    /**
     * 4 boats racing 8 spin-only divisions, each anchored with an RF of 1.0 ± noise.
     */
    private static Scenario buildSpinFleetScenario(Path tempDir, long seed)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(seed);
        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        List<String> ids = new ArrayList<>();
        double[] trueTcf = {0.92, 1.00, 1.08, 1.15};
        for (int i = 0; i < 4; i++)
        {
            String id = "x" + i + "-name-design";
            ids.add(id);
            Boat b = new Boat(id, "X" + i, "Name" + i, "design", null,
                List.of(), List.of(), null, null);
            store.putBoat(b);
            ReferenceFactors rf = new ReferenceFactors(
                new Factor(trueTcf[i], 0.9), null, null, 0, 0, 0);
            boatDerived.put(id, new BoatDerived(b, rf, Set.of(), Set.of(), null));
        }
        long base = Duration.ofHours(2).toNanos();
        for (int d = 0; d < 8; d++)
        {
            double t0 = base * (0.9 + 0.2 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
            {
                long el = (long)(t0 / trueTcf[i] * (1.0 + 0.01 * rng.nextGaussian()));
                finishers.add(new Finisher(ids.get(i), Duration.ofNanos(el), false, null));
            }
            store.putRace(new Race("club-" + LocalDate.of(2026, 1, 1 + d) + "-0001", "club",
                List.of("club/s"), LocalDate.of(2026, 1, 1 + d), d + 1, null,
                List.of(new Division("Div", finishers)), null, null, null));
        }
        return new Scenario(store, boatDerived, ids);
    }

    /**
     * 4 boats each racing the SAME elapsed time in BOTH a spin and a non-spin division.
     * Cross-variant ratio for each boat ends up determined by the optimiser balance of
     * race-derived PF (which would say spin == nonSpin since elapsed times are equal)
     * and the graph constraint. RF anchors are neutral (1.0) so they don't dominate.
     */
    private static Scenario buildBothVariantScenario(Path tempDir, long seed)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(seed);
        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        List<String> ids = new ArrayList<>();
        double[] trueTcf = {0.92, 1.00, 1.08, 1.15};
        for (int i = 0; i < 4; i++)
        {
            String id = "y" + i + "-name-design";
            ids.add(id);
            Boat b = new Boat(id, "Y" + i, "Name" + i, "design", null,
                List.of(), List.of(), null, null);
            store.putBoat(b);
            ReferenceFactors rf = new ReferenceFactors(
                new Factor(trueTcf[i], 0.3), new Factor(trueTcf[i], 0.3), null, 0, 0, 0);
            boatDerived.put(id, new BoatDerived(b, rf, Set.of(), Set.of(), null));
        }
        long base = Duration.ofHours(2).toNanos();
        // 5 spin and 5 non-spin races, each with all 4 boats finishing.
        for (int d = 0; d < 10; d++)
        {
            boolean ns = (d >= 5);
            double t0 = base * (0.9 + 0.2 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
            {
                long el = (long)(t0 / trueTcf[i] * (1.0 + 0.01 * rng.nextGaussian()));
                finishers.add(new Finisher(ids.get(i), Duration.ofNanos(el), ns, null));
            }
            store.putRace(new Race("club-" + LocalDate.of(2026, 1, 1 + d) + "-0001", "club",
                List.of("club/s"), LocalDate.of(2026, 1, 1 + d), d + 1, null,
                List.of(new Division(ns ? "NS" : "Spin", finishers)), null, null, null));
        }
        return new Scenario(store, boatDerived, ids);
    }
}
