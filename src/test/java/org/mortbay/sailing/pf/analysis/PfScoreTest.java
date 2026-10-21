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

class PfScoreTest
{
    @TempDir
    Path tempDir;

    /** No outlier or fast-boat down-weighting, no RF pull: the score is plain weighted variance. */
    private static final PfConfig PLAIN = new PfConfig(0.0, 0.0001, 100, 5, 1.0e9, 1.0, 0.5, 0.01,
        0.0, 0.0, 0.2, 1.0e-3, false, 100.0, 200.0);

    @Test
    void weightedVarianceOfLogCorrectedTimes()
    {
        // Two divisions, two boats each, RF 1.0 weight 1: corrected times are the elapsed times
        PfProblem p = problem(new double[][] { { 100, 110 }, { 200, 200 } }, new double[] { 1.0, 1.0 });
        PfScore score = PfScore.of(p, p.initialLogPf(), PLAIN);

        double half = (Math.log(110) - Math.log(100)) / 2;
        assertEquals(2 * half * half, score.varianceTerm(), 1e-12); // division 2 contributes 0
        assertEquals(4.0, score.totalWeight(), 1e-12);
        assertEquals(2 * half * half / 4, score.weightedVariance(), 1e-12);
        assertEquals(0.0, score.rfTerm(), 0.0);
    }

    @Test
    void scalingAllPfsChangesOnlyTheRfTerm()
    {
        PfProblem p = problem(new double[][] { { 100, 112, 95 }, { 200, 230, 190 } }, new double[] { 1.0, 0.9, 1.05 });
        PfConfig withRf = new PfConfig(1.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01,
            0.0, 0.0, 0.2, 1.0e-3, false, 100.0, 200.0);
        double[][] logPf = p.initialLogPf();
        PfScore before = PfScore.of(p, logPf, withRf);
        for (double[] row : logPf)
            for (int b = 0; b < row.length; b++)
                row[b] += Math.log(1.1);
        PfScore after = PfScore.of(p, logPf, withRf);

        assertEquals(before.varianceTerm(), after.varianceTerm(), 1e-12);
        assertTrue(after.rfTerm() > before.rfTerm());
    }

    @Test
    void outlierIsDownWeighted()
    {
        double[][] times = { { 100, 101, 99, 100, 150 } }; // boat 4 broke down
        double[] rfs = { 1, 1, 1, 1, 1 };
        PfProblem p = problem(times, rfs);
        PfConfig robust = new PfConfig(0.0, 0.0001, 100, 5, 2.0, 2.0, 0.5, 0.01,
            0.0, 0.0, 0.2, 1.0e-3, false, 100.0, 200.0);
        double[] w = PfScore.entryWeights(p, p.initialLogPf(), robust, PfScore.baseWeights(p));
        int outlier = -1;
        for (int i = 0; i < p.entries.size(); i++)
            if (p.entries.get(i).boatId().equals("b4-design"))
                outlier = i;
        for (int i = 0; i < w.length; i++)
            if (i != outlier)
                assertTrue(w[outlier] < 0.1 * w[i], "outlier weight " + w[outlier] + " vs " + w[i]);
        assertTrue(PfScore.of(p, p.initialLogPf(), robust).varianceTerm()
            < PfScore.of(p, p.initialLogPf(), PLAIN).varianceTerm());
    }

    /** One race per row of elapsed times (seconds), boat i in column i with RF {@code rfs[i]}, weight 1. */
    private PfProblem problem(double[][] times, double[] rfs)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        Map<String, BoatDerived> boatDerived = new LinkedHashMap<>();
        for (int i = 0; i < rfs.length; i++)
        {
            Boat boat = new Boat("b" + i + "-design", "AUS" + i, "Boat" + i, "design", null,
                List.of(), List.of(), null, null);
            store.putBoat(boat);
            boatDerived.put(boat.id(), new BoatDerived(boat,
                new ReferenceFactors(new Factor(rfs[i], 1.0), null, null, 0, 0, 0), Set.of(), Set.of(), null));
        }
        for (int d = 0; d < times.length; d++)
        {
            List<Finisher> finishers = new ArrayList<>();
            for (int i = 0; i < times[d].length; i++)
                finishers.add(new Finisher("b" + i + "-design", Duration.ofSeconds((long)times[d][i]), false, null));
            LocalDate date = LocalDate.of(2024, 1, 1).plusDays(d);
            store.putRace(new Race("club-" + date + "-" + String.format("%04d", d + 1),
                "club", List.of("series"), date, d + 1, null,
                List.of(new Division("Div 1", finishers)), "test", null, null));
        }
        return PfProblem.build(store, boatDerived, PLAIN, DuplicateRaces.NONE);
    }
}
