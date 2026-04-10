package org.mortbay.sailing.hpf.data;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FactorTest
{
    // --- apply ---

    @Test
    void applyScalesDuration()
    {
        Factor f = new Factor(1.1, 0.9);
        WeightedInterval t = new WeightedInterval(Duration.ofMinutes(60), 0.8);

        WeightedInterval result = Factor.apply(f, t);

        assertThat(result.duration(), equalTo(Duration.ofMinutes(66)));
    }

    @Test
    void applyMultipliesWeights()
    {
        Factor f = new Factor(1.0, 0.8);
        WeightedInterval t = new WeightedInterval(Duration.ofMinutes(60), 0.5);

        WeightedInterval result = Factor.apply(f, t);

        assertThat(result.weight(), closeTo(0.4, 1e-10));
    }

    @Test
    void applyWithUnitFactorIsIdentity()
    {
        Factor f = new Factor(1.0, 1.0);
        WeightedInterval t = new WeightedInterval(Duration.ofSeconds(4567), 0.9);

        WeightedInterval result = Factor.apply(f, t);

        assertThat(result.duration(), equalTo(t.duration()));
        assertThat(result.weight(), closeTo(t.weight(), 1e-10));
    }

    @Test
    void applyWithZeroWeightFactorProducesZeroWeight()
    {
        Factor f = new Factor(1.2, 0.0);
        WeightedInterval t = new WeightedInterval(Duration.ofMinutes(60), 0.9);

        WeightedInterval result = Factor.apply(f, t);

        assertThat(result.weight(), closeTo(0.0, 1e-10));
    }

    @Test
    void applyUnweightedDurationScalesDuration()
    {
        Factor f = new Factor(1.1, 0.9);

        WeightedInterval result = Factor.apply(f, Duration.ofMinutes(60));

        assertThat(result.duration(), equalTo(Duration.ofMinutes(66)));
    }

    @Test
    void applyUnweightedDurationCarriesFactorWeight()
    {
        Factor f = new Factor(1.1, 0.75);

        WeightedInterval result = Factor.apply(f, Duration.ofMinutes(60));

        assertThat(result.weight(), closeTo(0.75, 1e-10));
    }

    // --- compose ---

    @Test
    void composeMultipliesValues()
    {
        Factor a = new Factor(1.2, 0.9);
        Factor b = new Factor(0.9, 0.8);

        Factor result = Factor.compose(a, b);

        assertThat(result.value(), closeTo(1.08, 1e-10));
    }

    @Test
    void composeMultipliesWeights()
    {
        Factor a = new Factor(1.2, 0.9);
        Factor b = new Factor(0.9, 0.8);

        Factor result = Factor.compose(a, b);

        assertThat(result.weight(), closeTo(0.72, 1e-10));
    }

    @Test
    void composeIsCommutativeOnValues()
    {
        Factor a = new Factor(1.2, 0.9);
        Factor b = new Factor(0.8, 0.7);

        assertThat(Factor.compose(a, b).value(), closeTo(Factor.compose(b, a).value(), 1e-10));
    }

    @Test
    void composeIsAssociativeOnValues()
    {
        Factor a = new Factor(1.2, 0.9);
        Factor b = new Factor(0.8, 0.7);
        Factor c = new Factor(1.05, 0.6);

        double twoStep = Factor.compose(Factor.compose(a, b), c).value();
        double oneStep = Factor.compose(a, b, c).value();

        assertThat(twoStep, closeTo(oneStep, 1e-10));
    }

    @Test
    void composeSingleInputIsIdentity()
    {
        Factor f = new Factor(1.05, 0.8);

        Factor result = Factor.compose(f);

        assertThat(result.value(), closeTo(1.05, 1e-10));
        assertThat(result.weight(), closeTo(0.8, 1e-10));
    }

    @Test
    void composeThreeFactors()
    {
        Factor a = new Factor(1.2, 0.9);
        Factor b = new Factor(0.8, 0.8);
        Factor c = new Factor(1.1, 0.7);

        Factor result = Factor.compose(a, b, c);

        assertThat(result.value(), closeTo(1.2 * 0.8 * 1.1, 1e-10));
        assertThat(result.weight(), closeTo(0.9 * 0.8 * 0.7, 1e-10));
    }

    @Test
    void composeEmptyThrows()
    {
        assertThrows(IllegalArgumentException.class, () -> Factor.compose());
    }

    // --- aggregate ---

    @Test
    void aggregateSingleInputIsIdentity()
    {
        Factor f = new Factor(1.05, 0.8);

        Factor result = Factor.aggregate(f);

        assertThat(result.value(), closeTo(1.05, 1e-10));
        assertThat(result.weight(), closeTo(0.8, 1e-10));
    }

    @Test
    void aggregateIdenticalInputsReturnsSameValueNoVariancePenalty()
    {
        // All identical → weightedVariance = 0 → pooledWeight = 1 - (1-0.8)^3 = 0.992
        Factor f1 = new Factor(1.05, 0.8);
        Factor f2 = new Factor(1.05, 0.8);
        Factor f3 = new Factor(1.05, 0.8);

        Factor result = Factor.aggregate(f1, f2, f3);

        assertThat(result.value(), closeTo(1.05, 1e-10));
        assertThat(result.weight(), closeTo(1.0 - 0.2 * 0.2 * 0.2, 1e-10));
    }

    @Test
    void aggregateWeightedMeanOfValues()
    {
        // w=0.6 at v=1.0, w=0.4 at v=1.2 → weighted mean = (0.6×1.0 + 0.4×1.2) / 1.0 = 1.08
        Factor f1 = new Factor(1.0, 0.6);
        Factor f2 = new Factor(1.2, 0.4);

        Factor result = Factor.aggregate(f1, f2);

        assertThat(result.value(), closeTo(1.08, 1e-10));
    }

    @Test
    void aggregateRepeatIncreasesWeight()
    {
        Factor factor1 = new Factor(1.0, 0.8);
        Factor factor2 = new Factor(1.0, 0.8);
        Factor aggregate = Factor.aggregate(factor1, factor2);
        assertThat(aggregate.value(), closeTo(1.0, 1e-9));
        assertThat(aggregate.weight(), greaterThan(0.8));
        assertThat(aggregate.weight(), lessThan(1.0));
    }

    @Test
    void aggregateCloseIncreasesWeight()
    {
        Factor factor1 = new Factor(1.005, 0.8);
        Factor factor2 = new Factor(0.995, 0.8);
        Factor aggregate = Factor.aggregate(factor1, factor2);
        assertThat(aggregate.value(), closeTo(1.0, 1e-9));
        assertThat(aggregate.weight(), greaterThan(0.8));
        assertThat(aggregate.weight(), lessThan(Factor.aggregate(new Factor(1.0, 0.8), new Factor(1.0, 0.8)).weight()));
    }

    @Test
    void aggregateSpreadLargeWeightReducesWeight()
    {
        Factor factor1 = new Factor(0.9, 0.8);
        Factor factor2 = new Factor(1.1, 0.8);
        Factor aggregate = Factor.aggregate(factor1, factor2);
        assertThat(aggregate.value(), closeTo(1.0, 1e-9));
        assertThat(aggregate.weight(), lessThan(0.8));
        assertThat(aggregate.weight(), greaterThan(0.0));
    }

    @Test
    void aggregateSpreadSmallWeightIncreasesWeight()
    {
        Factor factor1 = new Factor(0.9, 0.1);
        Factor factor2 = new Factor(1.1, 0.1);
        Factor aggregate = Factor.aggregate(factor1, factor2);
        assertThat(aggregate.value(), closeTo(1.0, 1e-9));
        assertThat(aggregate.weight(), lessThan(0.5));
        assertThat(aggregate.weight(), greaterThan(0.1));
    }

    @Test
    void aggregateSpreadSplitWeightReducesWeight()
    {
        Factor factor1 = new Factor(0.9999, 0.9);
        Factor factor2 = new Factor(1.1000, 0.1);
        Factor aggregate = Factor.aggregate(factor1, factor2);
        assertThat(aggregate.value(), closeTo(1.0, 0.01));
        assertThat(aggregate.weight(), lessThan(0.9));
        assertThat(aggregate.weight(), greaterThan((0.9 + 0.1)/2.0));
    }



    @Test
    void aggregateSkipsZeroWeightInputs()
    {
        Factor active = new Factor(1.05, 0.8);
        Factor zero   = new Factor(2.00, 0.0);  // should be ignored

        Factor result = Factor.aggregate(active, zero);

        assertThat(result.value(), closeTo(1.05, 1e-10));
        assertThat(result.weight(), closeTo(0.8, 1e-10));
    }

    @Test
    void aggregateAllZeroWeightsThrows()
    {
        Factor f1 = new Factor(1.0, 0.0);
        Factor f2 = new Factor(1.1, 0.0);

        assertThrows(IllegalArgumentException.class, () -> Factor.aggregate(f1, f2));
    }

    @Test
    void aggregateEmptyThrows()
    {
        assertThrows(IllegalArgumentException.class, () -> Factor.aggregate());
    }

    @Test
    void aggregateHighSpreadAtSigma0HalvesWeight()
    {
        // When spread equals SIGMA_0, weightedVariance = SIGMA_0², denominator = 2,
        // so combinedWeight = pooledWeight / 2.
        // Two inputs at 1.0 ± σ₀ with equal weights:
        // pooledWeight = 1 - (1-0.5)² = 0.75
        // weightedVariance = σ₀² → combinedWeight = 0.75 / 2 = 0.375
        double sigma = Factor.SIGMA_0;
        Factor f1 = new Factor(1.0 - sigma, 0.5);
        Factor f2 = new Factor(1.0 + sigma, 0.5);

        Factor result = Factor.aggregate(f1, f2);

        double pooledWeight = 1.0 - 0.5 * 0.5;  // 0.75
        double expectedWeight = pooledWeight / 2.0;  // 0.375
        assertThat(result.weight(), closeTo(expectedWeight, 1e-10));
    }

    // --- constructor validation ---

    @Test
    void constructorRejectsNonPositiveValue()
    {
        assertThrows(IllegalArgumentException.class, () -> new Factor(0.0, 0.5));
        assertThrows(IllegalArgumentException.class, () -> new Factor(-1.0, 0.5));
    }

    @Test
    void constructorRejectsOutOfRangeWeight()
    {
        assertThrows(IllegalArgumentException.class, () -> new Factor(1.0, -0.1));
        assertThrows(IllegalArgumentException.class, () -> new Factor(1.0, 1.1));
    }
}
