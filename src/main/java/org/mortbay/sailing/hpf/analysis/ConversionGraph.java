package org.mortbay.sailing.hpf.analysis;

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
 * Only edges whose fit has R² ≥ {@link #MIN_R2} are included.
 * <p>
 * Use {@link #adjacencies} for all outgoing edges from a node, or
 * {@link #sameVariantAdjacencies} to restrict to edges that stay within the same
 * (nonSpinnaker, twoHanded) variant — used for the primary conversion pass.
 */
public class ConversionGraph
{
    /** Minimum R² for a LinearFit to be included as a conversion edge. */
    public static final double MIN_R2 = 0.75;

    private final Map<ConversionNode, List<ConversionEdge>> adjacency;

    private ConversionGraph(Map<ConversionNode, List<ConversionEdge>> adjacency)
    {
        this.adjacency = adjacency;
    }

    /**
     * Builds a ConversionGraph from a list of comparison results.
     * Results with no fit or R² below {@link #MIN_R2} are ignored.
     */
    public static ConversionGraph from(List<ComparisonResult> results)
    {
        Map<ConversionNode, List<ConversionEdge>> adj = new LinkedHashMap<>();
        for (ComparisonResult r : results)
        {
            LinearFit fit = r.fit();
            if (fit == null || fit.r2() < MIN_R2)
                continue;

            ComparisonKey k = r.key();
            ConversionNode from = new ConversionNode(k.systemA(), k.yearA(), k.nonSpinA(), k.twoHandedA());
            ConversionNode to   = new ConversionNode(k.systemB(), k.yearB(), k.nonSpinB(), k.twoHandedB());
            adj.computeIfAbsent(from, n -> new ArrayList<>()).add(new ConversionEdge(from, to, fit));

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
