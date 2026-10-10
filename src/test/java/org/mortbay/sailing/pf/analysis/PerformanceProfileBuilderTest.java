package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Chaotic spoke's raw metric: the orderly-race share of a boat's error, independent of the
 * size of its errors (and so of Consistency).
 */
class PerformanceProfileBuilderTest
{
    private static final LocalDate DAY = LocalDate.now().minusDays(10);

    /**
     * Ten races: the even ones orderly (the other boats within ±0.01), the odd ones disorderly
     * (±0.2). The boat's own residual is {@code orderlyError} in orderly races and
     * {@code disorderlyError} in disorderly ones.
     */
    private static double ratio(double orderlyError, double disorderlyError)
    {
        Map<String, List<Double>> byRaceDiv = new HashMap<>();
        List<EntryResidual> boat = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            boolean orderly = i % 2 == 0;
            double s = orderly ? 0.01 : 0.2;
            double r = orderly ? orderlyError : disorderlyError;
            String race = "race-" + i;
            byRaceDiv.put(race + "|Div", new ArrayList<>(List.of(-s, -s / 2, 0.0, s / 2, s, r)));
            boat.add(new EntryResidual(race, "Div", DAY, false, false, r, 1.0));
        }
        return PerformanceProfileBuilder.computeChaotic(boat, byRaceDiv)[0];
    }

    @Test
    void orderlyRaceErrorsScoreWorseThanDisorderlyRaceErrors()
    {
        double inOrderly = ratio(0.05, 0.01);
        double inDisorderly = ratio(0.01, 0.05);
        assertTrue(inOrderly > inDisorderly, inOrderly + " vs " + inDisorderly);
    }

    @Test
    void ratioDoesNotDependOnTheSizeOfTheErrors()
    {
        // The same pattern at a tenth of the size — a far more consistent boat — scores the same.
        assertEquals(ratio(0.05, 0.01), ratio(0.005, 0.001), 1e-9);
    }

    @Test
    void aConsistentBoatsOneOrderlyRaceBlunderCountsHeavily()
    {
        // Tiny errors everywhere except one big one in an orderly race.
        Map<String, List<Double>> byRaceDiv = new HashMap<>();
        List<EntryResidual> boat = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            double s = i % 2 == 0 ? 0.01 : 0.2;
            double r = i == 0 ? 0.05 : 0.001;
            byRaceDiv.put("race-" + i + "|Div", new ArrayList<>(List.of(-s, -s / 2, 0.0, s / 2, s, r)));
            boat.add(new EntryResidual("race-" + i, "Div", DAY, false, false, r, 1.0));
        }
        double blunder = PerformanceProfileBuilder.computeChaotic(boat, byRaceDiv)[0];
        assertTrue(blunder > ratio(0.001, 0.001), "one orderly-race blunder: " + blunder);
    }

    @Test
    void fewRacesGiveNoScore()
    {
        Map<String, List<Double>> byRaceDiv = new HashMap<>();
        List<EntryResidual> boat = new ArrayList<>();
        for (int i = 0; i < 4; i++)
        {
            byRaceDiv.put("race-" + i + "|Div", new ArrayList<>(List.of(-0.1, 0.0, 0.1, 0.2, 0.05)));
            boat.add(new EntryResidual("race-" + i, "Div", DAY, false, false, 0.05, 1.0));
        }
        assertTrue(Double.isNaN(PerformanceProfileBuilder.computeChaotic(boat, byRaceDiv)[0]));
    }

    @Test
    void spreadLeavesTheBoatOut()
    {
        // Others are all 0; the boat's own 1.0 would otherwise widen the IQR.
        assertEquals(0.0, PerformanceProfileBuilder.iqrWithoutOne(List.of(0.0, 0.0, 0.0, 0.0, 1.0), 1.0), 1e-12);
        // IQR of {0, 0.5, 1, 1.5} (interpolated quartiles 0.375 and 1.125); the 9.0 is left out.
        assertEquals(0.75, PerformanceProfileBuilder.iqrWithoutOne(List.of(0.0, 0.5, 1.0, 1.5, 9.0), 9.0), 1e-12);
    }

    @Test
    void smallSamplesShrinkTowardsTheFleetMedian()
    {
        Map<String, double[]> raw = new LinkedHashMap<>();
        raw.put("a", new double[]{0, 0, 0, 0, 0.6, 100});
        raw.put("b", new double[]{0, 0, 0, 0, 0.8, 100});
        raw.put("c", new double[]{0, 0, 0, 0, 1.0, 100});
        raw.put("few", new double[]{0, 0, 0, 0, 2.0, 5});    // 5 races: half way to the median
        raw.put("none", new double[]{0, 0, 0, 0, Double.NaN, 2});
        PerformanceProfileBuilder.shrinkChaotic(raw);
        double median = 0.9;   // of 0.6, 0.8, 1.0, 2.0
        assertEquals((5 * 2.0 + 5 * median) / 10, raw.get("few")[4], 1e-12);
        assertEquals((100 * 0.6 + 5 * median) / 105, raw.get("a")[4], 1e-12);
        assertTrue(Double.isNaN(raw.get("none")[4]));
    }
}
