package org.mortbay.sailing.hpf.server;

import org.mortbay.sailing.hpf.analysis.BoatReferenceFactors;
import org.mortbay.sailing.hpf.analysis.ComparisonResult;
import org.mortbay.sailing.hpf.analysis.ConversionGraph;
import org.mortbay.sailing.hpf.analysis.HandicapAnalyser;
import org.mortbay.sailing.hpf.analysis.ReferenceNetworkBuilder;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Shared cache for analysis results. Holds the output of {@link HandicapAnalyser#analyseAll()}
 * and the reference factor map from {@link ReferenceNetworkBuilder#build(DataStore, int)}.
 * <p>
 * Both are recomputed together via {@link #refresh()} so they are always consistent.
 * {@link #refresh()} is called on startup and after each importer run completes.
 */
public class AnalysisCache
{
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisCache.class);

    private final DataStore store;

    private volatile List<ComparisonResult> comparisons = List.of();
    private volatile Map<String, BoatReferenceFactors> referenceFactors = Map.of();

    public AnalysisCache(DataStore store)
    {
        this.store = store;
    }

    /**
     * Recomputes comparisons and reference factors.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     * @param outlierSigma  outlier trimming threshold in units of SE, or null to use default (2.5)
     */
    public void refresh(Integer targetIrcYear, Double outlierSigma)
    {
        LOG.info("AnalysisCache: refreshing...");
        double sigma = outlierSigma != null ? outlierSigma : 2.5;
        List<ComparisonResult> newComparisons = new HandicapAnalyser(store, sigma).analyseAll();
        ConversionGraph graph = ConversionGraph.from(newComparisons);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        Map<String, BoatReferenceFactors> newFactors =
            new ReferenceNetworkBuilder().build(store, graph, year);

        comparisons = newComparisons;
        referenceFactors = newFactors;
        LOG.info("AnalysisCache: {} comparisons, {} reference factors (targetYear={})",
            newComparisons.size(), newFactors.size(), year);
    }

    /**
     * Recomputes reference factors only, using the existing comparisons and conversion graph.
     * Faster than {@link #refresh(Integer, Double)} when only the boat certificate data has changed.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     */
    public void refreshReferenceFactors(Integer targetIrcYear)
    {
        LOG.info("AnalysisCache: refreshing reference factors...");
        ConversionGraph graph = ConversionGraph.from(comparisons);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        Map<String, BoatReferenceFactors> newFactors =
            new ReferenceNetworkBuilder().build(store, graph, year);
        referenceFactors = newFactors;
        LOG.info("AnalysisCache: {} reference factors (targetYear={})", newFactors.size(), year);
    }

    /**
     * Returns the maximum year among real (issued) IRC certificates in the store,
     * falling back to the current calendar year if none exist.
     * <p>
     * Inferred IRC certificates (created by the race importer from race AHC values)
     * are excluded — they have a null expiry date and a cert number containing
     * "-inferred-". Using only issued certs ensures the DFS target year matches
     * the certs that boats actually hold, rather than race-scoring artefacts.
     * <p>
     * Australian IRC certs expiring in May of year Y are assigned year Y-1, so the
     * maximum issued cert year typically lags the calendar year in the first half
     * of the year.
     */
    private int maxIrcCertYear()
    {
        OptionalInt max = store.boats().values().stream()
            .flatMap(b -> b.certificates().stream())
            .filter(c -> "IRC".equals(c.system()) && c.expiryDate() != null)
            .mapToInt(c -> c.year())
            .max();
        int year = max.orElse(LocalDate.now().getYear());
        LOG.info("AnalysisCache: using currentYear={} for reference factor target", year);
        return year;
    }

    public List<ComparisonResult> comparisons()
    {
        return comparisons;
    }

    public Map<String, BoatReferenceFactors> referenceFactors()
    {
        return referenceFactors;
    }
}
