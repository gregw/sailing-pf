package org.mortbay.sailing.hpf.server;

import org.mortbay.sailing.hpf.analysis.BoatReferenceFactors;
import org.mortbay.sailing.hpf.analysis.ComparisonResult;
import org.mortbay.sailing.hpf.analysis.ConversionGraph;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.analysis.HandicapAnalyser;
import org.mortbay.sailing.hpf.analysis.ReferenceNetworkBuilder;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

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
    /** Design-level factors at the point race propagation converged (excludes design-fallback boats). */
    private volatile Map<String, Factor[]> designFactors = Map.of();
    private volatile int targetYear = LocalDate.now().getYear();

    /** designId → set of boatIds for that design. Empty until {@link #refreshIndexes()} is called. */
    private volatile Map<String, Set<String>> boatIdsByDesignId = Map.of();
    /** boatId → set of raceIds in which that boat finished. Empty until {@link #refreshIndexes()} is called. */
    private volatile Map<String, Set<String>> raceIdsByBoatId   = Map.of();
    /** boatId → set of seriesIds in which that boat finished. Empty until {@link #refreshIndexes()} is called. */
    private volatile Map<String, Set<String>> seriesIdsByBoatId = Map.of();

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
    public void refresh(Integer targetIrcYear, Double outlierSigma, double clubCertificateWeight)
    {
        LOG.info("AnalysisCache: refreshing...");
        double sigma = outlierSigma != null ? outlierSigma : 2.5;
        List<ComparisonResult> newComparisons = new HandicapAnalyser(store, sigma).analyseAll();
        ConversionGraph graph = ConversionGraph.from(newComparisons);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);

        comparisons     = newComparisons;
        referenceFactors = built.boatFactors();
        designFactors    = built.designFactors();
        targetYear       = year;
        LOG.info("AnalysisCache: {} comparisons, {} reference factors, {} design factors (targetYear={})",
            newComparisons.size(), referenceFactors.size(), designFactors.size(), year);
    }

    /**
     * Recomputes reference factors only, using the existing comparisons and conversion graph.
     * Faster than {@link #refresh(Integer, Double)} when only the boat certificate data has changed.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     */
    public void refreshReferenceFactors(Integer targetIrcYear, double clubCertificateWeight)
    {
        LOG.info("AnalysisCache: refreshing reference factors...");
        ConversionGraph graph = ConversionGraph.from(comparisons);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);
        referenceFactors = built.boatFactors();
        designFactors    = built.designFactors();
        targetYear       = year;
        LOG.info("AnalysisCache: {} reference factors, {} design factors (targetYear={})",
            referenceFactors.size(), designFactors.size(), year);
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

    /**
     * Builds the three navigation indexes from raw store data:
     * <ul>
     *   <li>boats by design (designId → Set of boatIds)</li>
     *   <li>races by boat  (boatId  → Set of raceIds)</li>
     *   <li>series by boat (boatId  → Set of seriesIds)</li>
     * </ul>
     * These are used by later optimisation steps and by the data browser filter links.
     */
    public void refreshIndexes()
    {
        LOG.info("AnalysisCache: building indexes...");
        Map<String, Set<String>> byDesign = new LinkedHashMap<>();
        Map<String, Set<String>> byBoatR  = new LinkedHashMap<>();
        Map<String, Set<String>> byBoatS  = new LinkedHashMap<>();

        for (var boat : store.boats().values())
        {
            if (boat.designId() != null)
                byDesign.computeIfAbsent(boat.designId(), k -> new LinkedHashSet<>()).add(boat.id());
        }

        for (Race race : store.races().values())
        {
            if (race.divisions() == null) continue;
            for (Division div : race.divisions())
            {
                for (Finisher f : div.finishers())
                {
                    byBoatR.computeIfAbsent(f.boatId(), k -> new LinkedHashSet<>()).add(race.id());
                    if (race.seriesIds() != null)
                        for (String sid : race.seriesIds())
                            byBoatS.computeIfAbsent(f.boatId(), k -> new LinkedHashSet<>()).add(sid);
                }
            }
        }

        boatIdsByDesignId = byDesign;
        raceIdsByBoatId   = byBoatR;
        seriesIdsByBoatId = byBoatS;
        LOG.info("AnalysisCache indexes: {} designs, {} boats with races, {} boats with series",
            byDesign.size(), byBoatR.size(), byBoatS.size());
    }

    public int targetYear()
    {
        return targetYear;
    }

    public List<ComparisonResult> comparisons()
    {
        return comparisons;
    }

    public Map<String, BoatReferenceFactors> referenceFactors()
    {
        return referenceFactors;
    }

    /** Design-level factors indexed as [0]=spin, [1]=nonSpin, [2]=twoHanded. */
    public Map<String, Factor[]> designFactors()
    {
        return designFactors;
    }

    public Map<String, Set<String>> boatIdsByDesignId() { return boatIdsByDesignId; }
    public Map<String, Set<String>> raceIdsByBoatId()   { return raceIdsByBoatId;   }
    public Map<String, Set<String>> seriesIdsByBoatId() { return seriesIdsByBoatId; }
}
