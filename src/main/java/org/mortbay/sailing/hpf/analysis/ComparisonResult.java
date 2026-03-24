package org.mortbay.sailing.hpf.analysis;

import java.util.List;

/**
 * The result of a handicap comparison regression.
 * <p>
 * {@code pairs} contains the observations used for the fit (after outlier trimming).
 * {@code trimmedPairs} contains observations removed as outliers before the second-pass fit;
 * it is empty when no trimming occurred.
 * {@code fit} is the OLS result; it may be {@code null} if there are fewer than 3 pairs.
 */
public record ComparisonResult(
    ComparisonKey key,
    List<DataPair> pairs,         // kept pairs used for the fit
    List<DataPair> trimmedPairs,  // outliers removed before the second-pass fit; empty if none
    LinearFit fit                 // null when pairs.size() < 3
)
{
    /** Number of pairs used for the fit (after trimming). */
    public int n()
    {
        return pairs.size();
    }

    /** Total number of pairs before trimming. */
    public int nTotal()
    {
        return pairs.size() + trimmedPairs.size();
    }
}
