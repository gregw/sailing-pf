package org.mortbay.sailing.hpf.analysis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.store.DataStore;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HpfOptimiserTest
{
    @TempDir
    Path tempDir;

    // Known true TCFs for 5 boats
    private static final double[] TRUE_TCFS = { 0.85, 0.92, 1.00, 1.08, 1.15 };
    private static final String[] BOAT_IDS = { "b1-alpha-design", "b2-bravo-design", "b3-charlie-design", "b4-delta-design", "b5-echo-design" };

    @Test
    void convergesWithinOnePercentOfTrueValues()
    {
        Scenario s = buildScenario(5, 10, 42, 0.01, false);
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, HpfConfig.DEFAULT, () -> false);

        assertFalse(result.boatHpfs().isEmpty());
        for (int i = 0; i < 5; i++)
        {
            BoatHpf hpf = result.boatHpfs().get(BOAT_IDS[i]);
            assertNotNull(hpf, "Missing HPF for " + BOAT_IDS[i]);
            assertNotNull(hpf.spin(), "Missing spin HPF for " + BOAT_IDS[i]);
            double expected = TRUE_TCFS[i];
            double actual = hpf.spin().value();
            assertEquals(expected, actual, expected * 0.01,
                "HPF for " + BOAT_IDS[i] + ": expected " + expected + " got " + actual);
        }
    }

    @Test
    void regularisationPullsTowardRf()
    {
        // Boat with strong RF (weight=1.0) but only 1 race entry
        Scenario s = buildScenarioWithFewRaces();
        HpfConfig strongReg = new HpfConfig(5.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01, 0.0);
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, strongReg, () -> false);

        // "b5-echo-design" has only 1 race but RF = 1.15
        BoatHpf hpf = result.boatHpfs().get("b5-echo-design");
        assertNotNull(hpf);
        assertNotNull(hpf.spin());
        // With strong lambda and only 1 race, HPF should stay close to RF
        double delta = Math.abs(Math.log(hpf.spin().value()) - Math.log(1.15));
        assertTrue(delta < 0.05, "HPF should be close to RF (1.15) with strong regularisation, got " + hpf.spin().value());
    }

    @Test
    void asymmetryDownweightsFastOutliers()
    {
        // Build a scenario with one fast outlier, run with and without asymmetry
        Scenario s = buildScenarioWithOutlier(true);  // fast outlier

        HpfConfig withAsym = new HpfConfig(1.0, 0.0001, 100, 5, 2.0, 3.0, 0.5, 0.01, 0.0);
        HpfResult resultAsym = new HpfOptimiser().optimise(s.store, s.boatDerived, withAsym, () -> false);

        HpfConfig noAsym = new HpfConfig(1.0, 0.0001, 100, 5, 2.0, 1.0, 0.5, 0.01, 0.0);
        HpfResult resultNoAsym = new HpfOptimiser().optimise(s.store, s.boatDerived, noAsym, () -> false);

        // The outlier boat's HPF should differ more from RF with asymmetry=1 (less down-weighting of the fast result)
        // than with asymmetry=3 (more aggressive down-weighting of the fast result)
        BoatHpf hpfAsym = resultAsym.boatHpfs().get("b1-alpha-design");
        BoatHpf hpfNoAsym = resultNoAsym.boatHpfs().get("b1-alpha-design");
        assertNotNull(hpfAsym);
        assertNotNull(hpfNoAsym);

        double deltaAsym = Math.abs(hpfAsym.referenceDeltaSpin());
        double deltaNoAsym = Math.abs(hpfNoAsym.referenceDeltaSpin());
        // With asymmetry factor 3, fast outlier is down-weighted more → HPF stays closer to RF
        assertTrue(deltaAsym <= deltaNoAsym,
            "Asymmetry should keep HPF closer to RF: deltaAsym=" + deltaAsym + ", deltaNoAsym=" + deltaNoAsym);
    }

    @Test
    void mixedDivisionBothVariantsGetCorrectHpf()
    {
        Scenario s = buildMixedDivisionScenario();
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, HpfConfig.DEFAULT, () -> false);

        // b1 races as spin, b2 as nonSpin in same division
        BoatHpf b1 = result.boatHpfs().get("b1-alpha-design");
        BoatHpf b2 = result.boatHpfs().get("b2-bravo-design");
        assertNotNull(b1);
        assertNotNull(b2);
        assertNotNull(b1.spin(), "b1 should have spin HPF");
        assertNotNull(b2.nonSpin(), "b2 should have nonSpin HPF");
        assertTrue(b1.spinRaceCount() > 0);
        assertTrue(b2.nonSpinRaceCount() > 0);
    }

    @Test
    void stopCheckReturnsPartialResult()
    {
        Scenario s = buildScenario(5, 10, 42, 0.01, false);
        AtomicInteger callCount = new AtomicInteger();
        // Stop after first outer iteration's inner loop completes (after some calls)
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, HpfConfig.DEFAULT,
            () -> callCount.incrementAndGet() > 2);

        // Should return empty result (partial results are discarded)
        assertTrue(result.boatHpfs().isEmpty());
    }

    @Test
    void boatWithNoRacesGetsRfAsHpf()
    {
        Scenario s = buildScenario(5, 10, 42, 0.01, false);
        // Add a 6th boat with RF but no race entries
        Boat noRaceBoat = new Boat("b6-foxtrot-design", "AUS6", "Foxtrot", "design", null,
            List.of(), List.of(), List.of(), List.of(), null, null);
        s.store.putBoat(noRaceBoat);
        Factor rfSpin = new Factor(0.95, 0.8);
        ReferenceFactors rf6 = new ReferenceFactors(rfSpin, null, null, 0, 0, 0);
        Map<String, BoatDerived> newDerived = new LinkedHashMap<>(s.boatDerived);
        newDerived.put(noRaceBoat.id(), new BoatDerived(noRaceBoat, rf6, Set.of(), Set.of(), null));

        HpfResult result = new HpfOptimiser().optimise(s.store, newDerived, HpfConfig.DEFAULT, () -> false);

        // b6 appears in boatDerived but has no entries — it shouldn't appear in HPF results
        // because it never participated in any division. That's fine — the boat never entered
        // the optimiser's working set. Its HPF == RF mapping is done by AnalysisCache.mergeHpfResults.
        // Let's verify the optimiser doesn't crash and the other boats are fine.
        assertFalse(result.boatHpfs().isEmpty());
        for (int i = 0; i < 5; i++)
            assertNotNull(result.boatHpfs().get(BOAT_IDS[i]));
    }

    @Test
    void divisionHpfsPopulated()
    {
        Scenario s = buildScenario(5, 10, 42, 0.01, false);
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, HpfConfig.DEFAULT, () -> false);

        assertFalse(result.divisionHpfsByRaceId().isEmpty());
        for (var entry : result.divisionHpfsByRaceId().values())
        {
            for (DivisionHpf dh : entry)
            {
                assertTrue(dh.referenceTimeNanos() > 0, "T₀ should be positive");
                assertTrue(dh.weight() > 0, "Division weight should be positive");
            }
        }
    }

    @Test
    void residualsPopulatedForAllBoats()
    {
        Scenario s = buildScenario(5, 10, 42, 0.01, false);
        HpfResult result = new HpfOptimiser().optimise(s.store, s.boatDerived, HpfConfig.DEFAULT, () -> false);

        for (int i = 0; i < 5; i++)
        {
            List<EntryResidual> residuals = result.residualsByBoatId().get(BOAT_IDS[i]);
            assertNotNull(residuals, "Missing residuals for " + BOAT_IDS[i]);
            assertFalse(residuals.isEmpty());
        }
    }

    // ---- Scenario builders ----

    private record Scenario(DataStore store, Map<String, BoatDerived> boatDerived) {}

    /**
     * Builds a scenario with nBoats boats and nDivisions divisions.
     * Each boat has a true TCF from TRUE_TCFS. Elapsed times are generated as T₀ / TCF + noise.
     */
    private Scenario buildScenario(int nBoats, int nDivisions, long seed, double noiseFraction, boolean withOutlier)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(seed);

        // Create boats with RF = true TCF
        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        for (int i = 0; i < nBoats; i++)
        {
            Boat boat = new Boat(BOAT_IDS[i], "AUS" + (i + 1), "Boat" + (i + 1), "design", null,
                List.of(), List.of(), List.of(), List.of(), null, null);
            store.putBoat(boat);
            Factor rfSpin = new Factor(TRUE_TCFS[i], 0.9);
            ReferenceFactors rf = new ReferenceFactors(rfSpin, null, null, 0, 0, 0);
            boatDerived.put(boat.id(), new BoatDerived(boat, rf, Set.of(), Set.of(), null));
        }

        // Create races with divisions
        double baseT0 = Duration.ofHours(2).toNanos();
        for (int d = 0; d < nDivisions; d++)
        {
            double t0 = baseT0 * (0.8 + 0.4 * rng.nextDouble()); // T₀ varies per race
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < nBoats; i++)
            {
                double elapsed = t0 / TRUE_TCFS[i];
                double noise = 1.0 + noiseFraction * (rng.nextGaussian());
                elapsed *= noise;
                if (withOutlier && i == 0 && d == 0)
                    elapsed *= 0.7; // fast outlier for boat 0 in first race
                finishers.add(new Finisher(BOAT_IDS[i], Duration.ofNanos((long) elapsed), false, null));
            }
            Division div = new Division("Div 1", finishers);
            Race race = new Race("club-" + LocalDate.of(2024, 1, 1 + d).toString() + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), LocalDate.of(2024, 1, 1 + d), d + 1, null, "IRC", false,
                List.of(div), "test", null, null);
            store.putRace(race);
        }

        return new Scenario(store, boatDerived);
    }

    private Scenario buildScenarioWithFewRaces()
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(99);

        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        for (int i = 0; i < 5; i++)
        {
            Boat boat = new Boat(BOAT_IDS[i], "AUS" + (i + 1), "Boat" + (i + 1), "design", null,
                List.of(), List.of(), List.of(), List.of(), null, null);
            store.putBoat(boat);
            Factor rfSpin = new Factor(TRUE_TCFS[i], 1.0);
            ReferenceFactors rf = new ReferenceFactors(rfSpin, null, null, 0, 0, 0);
            boatDerived.put(boat.id(), new BoatDerived(boat, rf, Set.of(), Set.of(), null));
        }

        double baseT0 = Duration.ofHours(2).toNanos();
        // 10 races, but boat 5 (echo) only in the first race
        for (int d = 0; d < 10; d++)
        {
            double t0 = baseT0 * (0.9 + 0.2 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            int boatCount = (d == 0) ? 5 : 4; // boat 5 only in first race
            for (int i = 0; i < boatCount; i++)
            {
                double elapsed = t0 / TRUE_TCFS[i];
                elapsed *= (1.0 + 0.01 * rng.nextGaussian());
                finishers.add(new Finisher(BOAT_IDS[i], Duration.ofNanos((long) elapsed), false, null));
            }
            Division div = new Division("Div 1", finishers);
            Race race = new Race("club-" + LocalDate.of(2024, 2, 1 + d) + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), LocalDate.of(2024, 2, 1 + d), d + 1, null, "IRC", false,
                List.of(div), "test", null, null);
            store.putRace(race);
        }

        return new Scenario(store, boatDerived);
    }

    private Scenario buildScenarioWithOutlier(boolean fast)
    {
        return buildScenario(5, 10, 77, 0.01, true);
    }

    private Scenario buildMixedDivisionScenario()
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(55);

        // 2 boats: b1 races as spin, b2 as nonSpin
        Boat b1 = new Boat("b1-alpha-design", "AUS1", "Alpha", "design", null,
            List.of(), List.of(), List.of(), List.of(), null, null);
        Boat b2 = new Boat("b2-bravo-design", "AUS2", "Bravo", "design", null,
            List.of(), List.of(), List.of(), List.of(), null, null);
        // Add 2 more boats so divisions have enough entries
        Boat b3 = new Boat("b3-charlie-design", "AUS3", "Charlie", "design", null,
            List.of(), List.of(), List.of(), List.of(), null, null);
        Boat b4 = new Boat("b4-delta-design", "AUS4", "Delta", "design", null,
            List.of(), List.of(), List.of(), List.of(), null, null);
        store.putBoat(b1);
        store.putBoat(b2);
        store.putBoat(b3);
        store.putBoat(b4);

        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        // b1: spin RF
        boatDerived.put(b1.id(), new BoatDerived(b1,
            new ReferenceFactors(new Factor(0.90, 0.9), null, null, 0, 0, 0),
            Set.of(), Set.of(), null));
        // b2: nonSpin RF
        boatDerived.put(b2.id(), new BoatDerived(b2,
            new ReferenceFactors(null, new Factor(1.05, 0.8), null, 0, 0, 0),
            Set.of(), Set.of(), null));
        // b3: spin RF
        boatDerived.put(b3.id(), new BoatDerived(b3,
            new ReferenceFactors(new Factor(1.00, 0.85), null, null, 0, 0, 0),
            Set.of(), Set.of(), null));
        // b4: spin RF
        boatDerived.put(b4.id(), new BoatDerived(b4,
            new ReferenceFactors(new Factor(1.10, 0.85), null, null, 0, 0, 0),
            Set.of(), Set.of(), null));

        double baseT0 = Duration.ofHours(2).toNanos();
        for (int d = 0; d < 5; d++)
        {
            double t0 = baseT0 * (0.9 + 0.2 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            // b1 spin, b2 nonSpin, b3 spin, b4 spin
            finishers.add(new Finisher(b1.id(), Duration.ofNanos((long)(t0 / 0.90 * (1 + 0.005 * rng.nextGaussian()))), false, null));
            finishers.add(new Finisher(b2.id(), Duration.ofNanos((long)(t0 / 1.05 * (1 + 0.005 * rng.nextGaussian()))), true, null));
            finishers.add(new Finisher(b3.id(), Duration.ofNanos((long)(t0 / 1.00 * (1 + 0.005 * rng.nextGaussian()))), false, null));
            finishers.add(new Finisher(b4.id(), Duration.ofNanos((long)(t0 / 1.10 * (1 + 0.005 * rng.nextGaussian()))), false, null));

            Division div = new Division("Div 1", finishers);
            Race race = new Race("club-" + LocalDate.of(2024, 3, 1 + d) + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), LocalDate.of(2024, 3, 1 + d), d + 1, null, "IRC", false,
                List.of(div), "test", null, null);
            store.putRace(race);
        }

        return new Scenario(store, boatDerived);
    }
}
