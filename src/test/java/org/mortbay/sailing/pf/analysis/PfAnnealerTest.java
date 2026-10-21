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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PfAnnealerTest
{
    @TempDir
    Path tempDir;

    private static final double[] TRUE_PFS = { 0.82, 0.88, 0.93, 0.97, 1.00, 1.04, 1.09, 1.15 };
    private static final PfAnnealConfig FAST = new PfAnnealConfig(0.25, 0.02, 0.92, 400, 200, 1.0e-6, 7);

    private record Scenario(DataStore store, Map<String, BoatDerived> boatDerived) {}

    @Test
    void recoversTruePfsFromWrongRfs()
    {
        Scenario s = buildScenario(11);
        PfAnnealer.Result r = anneal(s, FAST);

        assertNotNull(r.stats());
        // RF errors average out, so check each PF relative to the fleet's geometric mean
        double meanLogTrue = 0, meanLogPf = 0;
        for (int i = 0; i < TRUE_PFS.length; i++)
        {
            meanLogTrue += Math.log(TRUE_PFS[i]) / TRUE_PFS.length;
            meanLogPf += Math.log(r.pfResult().boatPfs().get(boatId(i)).spin().value()) / TRUE_PFS.length;
        }
        for (int i = 0; i < TRUE_PFS.length; i++)
        {
            double rel = Math.log(r.pfResult().boatPfs().get(boatId(i)).spin().value()) - meanLogPf;
            double trueRel = Math.log(TRUE_PFS[i]) - meanLogTrue;
            assertEquals(trueRel, rel, 0.01, "relative log PF of " + boatId(i));
        }
    }

    @Test
    void scoresNoWorseThanAls()
    {
        Scenario s = buildScenario(12);
        PfResult als = new PfOptimiser().optimise(s.store, s.boatDerived, PfConfig.DEFAULT, () -> false);
        PfAnnealer.Result sa = anneal(s, FAST);

        PfScore alsScore = als.quality().score();
        PfScore saScore = sa.pfResult().quality().score();
        assertTrue(saScore.energy() <= alsScore.energy() * 1.01,
            "SA energy " + saScore.energy() + " vs ALS " + alsScore.energy());
        // and both beat the RFs they started from
        assertTrue(saScore.energy() < sa.stats().energyTrace().get(0) || sa.stats().bestStage() > 0);
    }

    @Test
    void sameSeedSameResult()
    {
        Scenario s = buildScenario(13);
        PfAnnealer.Result a = anneal(s, FAST);
        PfAnnealer.Result b = anneal(s, FAST);
        for (int i = 0; i < TRUE_PFS.length; i++)
            assertEquals(a.pfResult().boatPfs().get(boatId(i)).spin().value(),
                b.pfResult().boatPfs().get(boatId(i)).spin().value(), 0.0);
        assertEquals(a.stats().moves(), b.stats().moves());
    }

    @Test
    void stopCheckAbandonsRun()
    {
        Scenario s = buildScenario(14);
        PfAnnealer.Result r = new PfAnnealer().anneal(s.store, s.boatDerived, PfConfig.DEFAULT, FAST,
            () -> true, null, 0, DuplicateRaces.NONE);
        assertTrue(r.pfResult().boatPfs().isEmpty());
    }

    private static PfAnnealer.Result anneal(Scenario s, PfAnnealConfig cfg)
    {
        return new PfAnnealer().anneal(s.store, s.boatDerived, PfConfig.DEFAULT, cfg,
            () -> false, null, 0, DuplicateRaces.NONE);
    }

    private static String boatId(int i)
    {
        return "b" + i + "-design";
    }

    /**
     * 8 boats whose RFs are off by ~5% (weight 0.3), racing 30 divisions of 5–8 boats with 1%
     * noise and the odd breakdown (a slow outlier).
     */
    private Scenario buildScenario(long seed)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Random rng = new Random(seed);

        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        for (int i = 0; i < TRUE_PFS.length; i++)
        {
            Boat boat = new Boat(boatId(i), "AUS" + i, "Boat" + i, "design", null,
                List.of(), List.of(), null, null);
            store.putBoat(boat);
            Factor rf = new Factor(TRUE_PFS[i] * (1 + 0.05 * rng.nextGaussian()), 0.3);
            boatDerived.put(boat.id(), new BoatDerived(boat, new ReferenceFactors(rf, null, null, 0, 0, 0),
                Set.of(), Set.of(), null));
        }

        double baseT0 = Duration.ofHours(2).toNanos();
        for (int d = 0; d < 30; d++)
        {
            double t0 = baseT0 * (0.6 + 0.8 * rng.nextDouble());
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < TRUE_PFS.length; i++)
            {
                if (rng.nextDouble() < 0.25)
                    continue;
                double elapsed = t0 / TRUE_PFS[i] * (1 + 0.01 * rng.nextGaussian());
                if (rng.nextDouble() < 0.03)
                    elapsed *= 1.2; // breakdown
                finishers.add(new Finisher(boatId(i), Duration.ofNanos((long)elapsed), false, null));
            }
            LocalDate date = LocalDate.of(2024, 1, 1).plusDays(d);
            Race race = new Race("club-" + date + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), date, d + 1, null,
                List.of(new Division("Div 1", finishers)), "test", null, null);
            store.putRace(race);
        }
        return new Scenario(store, boatDerived);
    }
}
