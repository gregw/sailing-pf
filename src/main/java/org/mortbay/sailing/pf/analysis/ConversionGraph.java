package org.mortbay.sailing.pf.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Directed graph of empirical handicap conversions between (system, year, variant) nodes.
 * <p>
 * Each node is a {@link ConversionNode} identifying a position in handicap space
 * (e.g. ORC spin 2023, IRC nonspin 2025). Each directed edge carries a {@link LinearFit}
 * that maps a certificate value at the source node to an equivalent value at the target node.
 * <p>
 * Only edges whose fit has R² ≥ the configured minimum (default {@link #DEFAULT_MIN_R2}) are included.
 * <p>
 * Year-transition edges are bidirectional: both the forward (yearN → yearN+1) and inverse
 * (yearN+1 → yearN) edges are stored.  This allows the Step-12 cross-variant fill to reach
 * the target year even when the cross-variant edge only exists for an adjacent year — e.g.
 * IRC-NS-2025 → IRC-spin-2025 → IRC-spin-2026 when currentYear=2026 but allNsVsSpin only
 * has data for 2025.
 * <p>
 * Use {@link #adjacencies} for all outgoing edges from a node, or
 * {@link #sameVariantAdjacencies} to restrict to edges that stay within the same
 * (nonSpinnaker, twoHanded) variant — used for the primary conversion pass.
 */
public class ConversionGraph
{
    /** Default minimum R² for a LinearFit to be included as a conversion edge. */
    public static final double DEFAULT_MIN_R2 = 0.50;

    /**
     * Default minimum number of post-trim paired observations for a fit to be included
     * as a conversion edge. With n &lt; 8 the OLS fit can be coincidentally high-R² yet
     * slope-wrong (e.g. a single n=3 ALL/twoHanded→ALL/spin edge produced systematic
     * twoHanded ≈ spin × 1.018 inversions across the fleet); requiring at least 8 paired
     * observations excludes these low-evidence fits.
     */
    public static final int DEFAULT_MIN_PAIRS = 8;

    private final Map<ConversionNode, List<ConversionEdge>> adjacency;

    private ConversionGraph(Map<ConversionNode, List<ConversionEdge>> adjacency)
    {
        this.adjacency = adjacency;
    }

    /**
     * Builds a ConversionGraph from a list of comparison results using {@link #DEFAULT_MIN_R2}
     * and {@link #DEFAULT_MIN_PAIRS}.
     */
    public static ConversionGraph from(List<ComparisonResult> results)
    {
        return from(results, DEFAULT_MIN_R2, DEFAULT_MIN_PAIRS);
    }

    /**
     * Builds a ConversionGraph from a list of comparison results, using
     * {@link #DEFAULT_MIN_PAIRS} as the minimum paired-observation threshold.
     */
    public static ConversionGraph from(List<ComparisonResult> results, double minR2)
    {
        return from(results, minR2, DEFAULT_MIN_PAIRS);
    }

    /**
     * Builds a ConversionGraph from a list of comparison results.
     * Results with no fit, R² below {@code minR2}, or fewer than {@code minPairs}
     * post-trim paired observations are ignored.
     * <p>
     * Year-transition edges (same system, same variant, consecutive years) have their
     * inverse also added, enabling backward-year traversal in the DFS.  This lets Step 12
     * derive a spin RF for currentYear even when the cross-variant (NS→spin) edge only exists
     * for a prior year: IRC-NS-currentYear ←(inv)← IRC-NS-priorYear →(cross)→ IRC-spin-priorYear
     * →(forward)→ IRC-spin-currentYear.
     */
    public static ConversionGraph from(List<ComparisonResult> results, double minR2, int minPairs)
    {
        Map<ConversionNode, List<ConversionEdge>> adj = new LinkedHashMap<>();
        for (ComparisonResult r : results)
        {
            LinearFit fit = r.fit();
            if (fit == null || fit.r2() < minR2 || r.n() < minPairs)
                continue;

            ComparisonKey k = r.key();
            ConversionNode from = new ConversionNode(k.systemA(), k.yearA(), k.nonSpinA(), k.twoHandedA());
            ConversionNode to   = new ConversionNode(k.systemB(), k.yearB(), k.nonSpinB(), k.twoHandedB());
            adj.computeIfAbsent(from, n -> new ArrayList<>()).add(new ConversionEdge(from, to, fit));

            // Year-transition edges: add the inverse so the DFS can also traverse backwards.
            boolean isYearTransition = k.yearA() != k.yearB()
                && k.systemA().equals(k.systemB())
                && k.nonSpinA() == k.nonSpinB()
                && k.twoHandedA() == k.twoHandedB();
            if (isYearTransition)
                adj.computeIfAbsent(to, n -> new ArrayList<>()).add(new ConversionEdge(to, from, fit.inverse()));

            // For pooled ALL-system variant comparisons (e.g. NS→spin), also add per-system
            // forward+inverse edges so the reference-factor DFS can reach cross-variant targets
            // from real system nodes (IRC, ORC, AMS) without needing a separate pair emission.
            if ("ALL".equals(k.systemA()) && "ALL".equals(k.systemB())
                && k.yearA() == k.yearB())
            {
                LinearFit inv = fit.inverse();
                for (String sys : new String[]{"IRC", "ORC", "AMS"})
                {
                    ConversionNode sFrom = new ConversionNode(sys, k.yearA(), k.nonSpinA(), k.twoHandedA());
                    ConversionNode sTo   = new ConversionNode(sys, k.yearB(), k.nonSpinB(), k.twoHandedB());
                    adj.computeIfAbsent(sFrom, n -> new ArrayList<>()).add(new ConversionEdge(sFrom, sTo, fit));
                    adj.computeIfAbsent(sTo,   n -> new ArrayList<>()).add(new ConversionEdge(sTo, sFrom, inv));
                }
            }
        }
        return new ConversionGraph(adj);
    }

    /**
     * All outgoing edges from {@code node}, including cross-variant edges
     * (e.g. NS → spin, 2H → spin).
     */
    public List<ConversionEdge> adjacencies(ConversionNode node)
    {
        return adjacency.getOrDefault(node, Collections.emptyList());
    }

    /**
     * Outgoing edges from {@code node} that stay within the same variant
     * (same nonSpinnaker and twoHanded flags). Cross-variant edges are excluded.
     * Used for the primary conversion pass to avoid conflating cert flavours.
     */
    public List<ConversionEdge> sameVariantAdjacencies(ConversionNode node)
    {
        List<ConversionEdge> all = adjacency.getOrDefault(node, Collections.emptyList());
        List<ConversionEdge> result = new ArrayList<>(all.size());
        for (ConversionEdge e : all)
        {
            if (e.to().nonSpinnaker() == node.nonSpinnaker()
                && e.to().twoHanded() == node.twoHanded())
                result.add(e);
        }
        return result;
    }
}