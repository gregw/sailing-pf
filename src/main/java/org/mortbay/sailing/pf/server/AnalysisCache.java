package org.mortbay.sailing.pf.server;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import org.mortbay.sailing.pf.analysis.BoatDerived;
import org.mortbay.sailing.pf.analysis.BoatPf;
import org.mortbay.sailing.pf.analysis.ComparisonResult;
import org.mortbay.sailing.pf.analysis.ConversionGraph;
import org.mortbay.sailing.pf.analysis.DesignDerived;
import org.mortbay.sailing.pf.analysis.DivisionPf;
import org.mortbay.sailing.pf.analysis.EntryResidual;
import org.mortbay.sailing.pf.analysis.HandicapAnalyser;
import org.mortbay.sailing.pf.analysis.PerformanceProfile;
import org.mortbay.sailing.pf.analysis.PerformanceProfileBuilder;
import org.mortbay.sailing.pf.analysis.PfConfig;
import org.mortbay.sailing.pf.analysis.PfOptimiser;
import org.mortbay.sailing.pf.analysis.PfQuality;
import org.mortbay.sailing.pf.analysis.PfResult;
import org.mortbay.sailing.pf.analysis.RaceDerived;
import org.mortbay.sailing.pf.analysis.ReferenceFactors;
import org.mortbay.sailing.pf.analysis.ReferenceNetworkBuilder;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared cache for analysis results. Holds the output of {@link HandicapAnalyser#analyseAll()}
 * and the reference factor map from {@link ReferenceNetworkBuilder#build(DataStore, int)}.
 * <p>
 * Both are recomputed together via {@link #refresh()} so they are always consistent.
 * {@link #refresh()} is called on startup and after each importer run completes.
 * <p>
 * Per-entity derived data is consolidated into three maps: {@link BoatDerived},
 * {@link DesignDerived}, and {@link RaceDerived}. Individual entries are invalidated
 * via the {@link DataStore.InvalidationListener} interface when raw entities change.
 */
public class AnalysisCache implements DataStore.InvalidationListener
{
    private static final Logger LOG = LoggerFactory.getLogger(AnalysisCache.class);

    private final DataStore store;

    private volatile List<ComparisonResult> comparisons = List.of();
    private volatile int targetYear = LocalDate.now().getYear();
    private volatile double minAnalysisR2 = ConversionGraph.DEFAULT_MIN_R2;
    private volatile int minAnalysisPairs = ConversionGraph.DEFAULT_MIN_PAIRS;

    // Consolidated per-entity derived data
    private volatile Map<String, BoatDerived> boatDerived = Map.of();
    private volatile Map<String, DesignDerived> designDerived = Map.of();
    private volatile Map<String, RaceDerived> raceDerived = Map.of();
    private volatile Map<String, List<EntryResidual>> residualsByBoatId = Map.of();
    private volatile Map<String, PerformanceProfile> profilesByBoatId = Map.of();
    private volatile PfQuality lastPfQuality;  // null until first run
    private volatile double diversityNonSpinWeight   = 0.8;
    private volatile double diversitySpinWeight      = 1.0;
    private volatile double diversityTwoHandedWeight = 1.2;
    private volatile int    consistencyDropInterval  = 11;

    public void setDiversityWeights(double nonSpin, double spin, double twoHanded)
    {
        this.diversityNonSpinWeight   = nonSpin;
        this.diversitySpinWeight      = spin;
        this.diversityTwoHandedWeight = twoHanded;
    }

    public void setConsistencyDropInterval(int interval)
    {
        this.consistencyDropInterval = interval;
    }

    public AnalysisCache(DataStore store)
    {
        this.store = store;
        store.setInvalidationListener(this);
    }

    /**
     * Recomputes comparisons and reference factors.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     * @param outlierSigma  outlier trimming threshold in units of SE, or null to use default (2.5)
     * @param minR2         minimum R² for a conversion edge to be included in the graph
     * @param minPairs      minimum post-trim paired observations for a conversion edge
     */
    public void refresh(Integer targetIrcYear, Double outlierSigma, double clubCertificateWeight,
                        double minR2, int minPairs)
    {
        LOG.info("AnalysisCache: refreshing...");
        double sigma = outlierSigma != null ? outlierSigma : 2.5;
        List<ComparisonResult> newComparisons = new HandicapAnalyser(store, sigma).analyseAll();
        ConversionGraph graph = ConversionGraph.from(newComparisons, minR2, minPairs);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);

        comparisons = newComparisons;
        targetYear  = year;
        minAnalysisR2 = minR2;
        minAnalysisPairs = minPairs;
        mergeReferenceFactors(built);
        LOG.info("AnalysisCache: {} comparisons, {} boat derived, {} design derived (targetYear={}, minR2={}, minPairs={})",
            newComparisons.size(), boatDerived.size(), designDerived.size(), year, minR2, minPairs);
    }

    /**
     * Recomputes reference factors only, using the existing comparisons and conversion graph.
     * Faster than {@link #refresh} when only the boat certificate data has changed.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     * @param minR2         minimum R² for a conversion edge to be included in the graph
     * @param minPairs      minimum post-trim paired observations for a conversion edge
     */
    public void refreshReferenceFactors(Integer targetIrcYear, double clubCertificateWeight,
                                        double minR2, int minPairs)
    {
        LOG.info("AnalysisCache: refreshing reference factors...");
        ConversionGraph graph = ConversionGraph.from(comparisons, minR2, minPairs);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);
        targetYear = year;
        minAnalysisR2 = minR2;
        minAnalysisPairs = minPairs;
        mergeReferenceFactors(built);
        LOG.info("AnalysisCache: {} boat derived, {} design derived (targetYear={}, minR2={}, minPairs={})",
            boatDerived.size(), designDerived.size(), year, minR2, minPairs);
    }

    /**
     * Merges reference factors from a BuildResult into the consolidated Derived maps,
     * preserving existing index data (raceIds, seriesIds, boatIds).
     */
    private void mergeReferenceFactors(ReferenceNetworkBuilder.BuildResult built)
    {
        // Merge boat reference factors with existing index data
        Map<String, BoatDerived> currentBoats = this.boatDerived;
        Map<String, BoatDerived> newBoats = new LinkedHashMap<>();
        for (Map.Entry<String, ReferenceFactors> e : built.boatFactors().entrySet())
        {
            String id = e.getKey();
            Boat boat = store.boats().get(id);
            if (boat == null) continue;
            BoatDerived existing = currentBoats.get(id);
            Set<String> raceIds = existing != null ? existing.raceIds() : Set.of();
            Set<String> seriesIds = existing != null ? existing.seriesIds() : Set.of();
            BoatPf existingPf = existing != null ? existing.pf() : null;
            newBoats.put(id, new BoatDerived(boat, e.getValue(), raceIds, seriesIds, existingPf));
        }
        // Keep entries that have index data but no reference factors (boats not in BuildResult)
        for (Map.Entry<String, BoatDerived> e : currentBoats.entrySet())
        {
            String id = e.getKey();
            if (newBoats.containsKey(id)) continue;
            if (store.isBoatExcluded(id)) continue;
            Boat b = e.getValue().boat();
            if (b.designId() != null && store.isDesignExcluded(b.designId())) continue;
            if (!e.getValue().raceIds().isEmpty() || !e.getValue().seriesIds().isEmpty())
                newBoats.put(id, new BoatDerived(b, null, e.getValue().raceIds(), e.getValue().seriesIds(), e.getValue().pf()));
        }
        this.boatDerived = Map.copyOf(newBoats);

        // Merge design reference factors with existing index data
        Map<String, DesignDerived> currentDesigns = this.designDerived;
        Map<String, DesignDerived> newDesigns = new LinkedHashMap<>();
        for (Map.Entry<String, ReferenceFactors> e : built.designFactors().entrySet())
        {
            String id = e.getKey();
            if (store.isDesignExcluded(id)) continue;
            Design design = store.designs().get(id);
            if (design == null) continue;
            DesignDerived existing = currentDesigns.get(id);
            Set<String> boatIds = existing != null ? existing.boatIds() : Set.of();
            newDesigns.put(id, new DesignDerived(design, e.getValue(), boatIds));
        }
        // Keep entries that have index data but no reference factors
        for (Map.Entry<String, DesignDerived> e : currentDesigns.entrySet())
        {
            if (!newDesigns.containsKey(e.getKey()) && !e.getValue().boatIds().isEmpty()
                    && !store.isDesignExcluded(e.getKey()))
                newDesigns.put(e.getKey(), new DesignDerived(e.getValue().design(), null, e.getValue().boatIds()));
        }
        this.designDerived = Map.copyOf(newDesigns);
    }

    /**
     * Minimum number of IRC certificates required in a year for it to be considered
     * as the target IRC year. Years with fewer certs lack the paired data needed to
     * build robust conversion-graph edges, so they are excluded in favour of the
     * most recent year that does meet the threshold.
     */
    private static final int MIN_IRC_CERT_COUNT = 100;

    /**
     * Returns the most recent IRC certificate year that has at least
     * {@link #MIN_IRC_CERT_COUNT} certificates in the store, falling back to the
     * absolute maximum IRC cert year, then to the current calendar year.
     *
     * <p>The threshold is needed because the early weeks of a new racing season
     * may already contain a handful of certs for the coming year (e.g. 59 certs
     * in 2026 vs 334 in 2025). Using that low-count year as the target would
     * require conversion-graph edges built from very few paired observations,
     * which typically fail the minimum-R² gate and break gen-0 RF computation
     * for the majority of boats that hold only the previous year's certs.
     */
    private int maxIrcCertYear()
    {
        Map<Integer, Long> yearCounts = store.boats().values().stream()
            .flatMap(b -> b.certificates().stream())
            .filter(c -> "IRC".equals(c.system()))
            .collect(Collectors.groupingBy(Certificate::year, Collectors.counting()));

        // Latest year with enough data to anchor the conversion graph
        OptionalInt max = yearCounts.entrySet().stream()
            .filter(e -> e.getValue() >= MIN_IRC_CERT_COUNT)
            .mapToInt(Map.Entry::getKey)
            .max();

        if (max.isEmpty())
            max = yearCounts.keySet().stream().mapToInt(Integer::intValue).max();

        int year = max.orElse(LocalDate.now().getYear());
        LOG.info("AnalysisCache: using currentYear={} for reference factor target", year);
        return year;
    }

    /**
     * Builds navigation indexes from raw store data and merges them into the
     * consolidated Derived maps. Also builds {@link RaceDerived} for all races.
     */
    public void refreshIndexes()
    {
        LOG.info("AnalysisCache: building indexes...");
        Map<String, Set<String>> byDesign = new LinkedHashMap<>();
        Map<String, Set<String>> byBoatR  = new LinkedHashMap<>();
        Map<String, Set<String>> byBoatS  = new LinkedHashMap<>();

        for (var boat : store.boats().values())
        {
            if (store.isBoatExcluded(boat.id())) continue;
            if (boat.designId() != null && store.isDesignExcluded(boat.designId())) continue;
            if (boat.designId() != null)
                byDesign.computeIfAbsent(boat.designId(), k -> new LinkedHashSet<>()).add(boat.id());
        }

        for (Race race : store.races().values())
        {
            if (store.isRaceExcluded(race.id())) continue;
            if (store.isClubExcluded(race.clubId())) continue;
            if (race.divisions() == null) continue;
            for (Division div : race.divisions())
            {
                for (Finisher f : div.finishers())
                {
                    if (store.isBoatExcluded(f.boatId())) continue;
                    Boat fBoat = store.boats().get(f.boatId());
                    if (fBoat != null && fBoat.designId() != null
                            && store.isDesignExcluded(fBoat.designId())) continue;
                    byBoatR.computeIfAbsent(f.boatId(), k -> new LinkedHashSet<>()).add(race.id());
                    if (race.seriesIds() != null)
                        for (String sid : race.seriesIds())
                            byBoatS.computeIfAbsent(f.boatId(), k -> new LinkedHashSet<>()).add(sid);
                }
            }
        }

        // Merge index data with existing reference factors into BoatDerived
        Map<String, BoatDerived> currentBoats = this.boatDerived;
        Map<String, BoatDerived> newBoats = new LinkedHashMap<>();
        // All boats that have index data
        Set<String> allBoatIds = new LinkedHashSet<>();
        allBoatIds.addAll(byBoatR.keySet());
        allBoatIds.addAll(byBoatS.keySet());
        allBoatIds.addAll(currentBoats.keySet());
        for (String id : allBoatIds)
        {
            Boat boat = store.boats().get(id);
            if (boat == null) continue;
            if (store.isBoatExcluded(id)) continue;
            if (boat.designId() != null && store.isDesignExcluded(boat.designId())) continue;
            BoatDerived existing = currentBoats.get(id);
            ReferenceFactors rf = existing != null ? existing.referenceFactors() : null;
            Set<String> raceIds = byBoatR.getOrDefault(id, Set.of());
            Set<String> seriesIds = byBoatS.getOrDefault(id, Set.of());
            BoatPf existingPf = existing != null ? existing.pf() : null;
            newBoats.put(id, new BoatDerived(boat, rf, raceIds, seriesIds, existingPf));
        }
        this.boatDerived = Map.copyOf(newBoats);

        // Merge index data with existing reference factors into DesignDerived
        Map<String, DesignDerived> currentDesigns = this.designDerived;
        Map<String, DesignDerived> newDesigns = new LinkedHashMap<>();
        Set<String> allDesignIds = new LinkedHashSet<>();
        allDesignIds.addAll(byDesign.keySet());
        allDesignIds.addAll(currentDesigns.keySet());
        for (String id : allDesignIds)
        {
            Design design = store.designs().get(id);
            if (design == null) continue;
            if (store.isDesignExcluded(id)) continue;
            DesignDerived existing = currentDesigns.get(id);
            ReferenceFactors rf = existing != null ? existing.referenceFactors() : null;
            Set<String> boatIds = byDesign.getOrDefault(id, Set.of());
            newDesigns.put(id, new DesignDerived(design, rf, boatIds));
        }
        this.designDerived = Map.copyOf(newDesigns);

        // Build RaceDerived for all races
        Map<String, RaceDerived> newRaces = new LinkedHashMap<>();
        for (Race race : store.races().values())
        {
            int finisherCount = 0;
            if (race.divisions() != null)
                for (Division div : race.divisions())
                    if (div.finishers() != null)
                        finisherCount += div.finishers().size();
            RaceDerived existingRd = this.raceDerived.get(race.id());
            List<DivisionPf> existingDivPfs = existingRd != null ? existingRd.divisionPfs() : null;
            newRaces.put(race.id(), new RaceDerived(race, finisherCount, existingDivPfs));
        }
        this.raceDerived = Map.copyOf(newRaces);

        LOG.info("AnalysisCache indexes: {} designs, {} boats with derived, {} races",
            newDesigns.size(), newBoats.size(), newRaces.size());
    }

    // --- InvalidationListener ---

    @Override
    public void onBoatChanged(String boatId)
    {
        Map<String, BoatDerived> current = this.boatDerived;
        if (current.containsKey(boatId))
        {
            var copy = new LinkedHashMap<>(current);
            copy.remove(boatId);
            this.boatDerived = Map.copyOf(copy);
        }
    }

    @Override
    public void onDesignChanged(String designId)
    {
        Map<String, DesignDerived> current = this.designDerived;
        if (current.containsKey(designId))
        {
            var copy = new LinkedHashMap<>(current);
            copy.remove(designId);
            this.designDerived = Map.copyOf(copy);
        }
    }

    @Override
    public void onRaceChanged(String raceId)
    {
        Map<String, RaceDerived> current = this.raceDerived;
        if (current.containsKey(raceId))
        {
            var copy = new LinkedHashMap<>(current);
            copy.remove(raceId);
            this.raceDerived = Map.copyOf(copy);
        }
    }

    @Override
    public void onClubChanged(String clubId)
    {
        // Club changes don't affect derived data currently
    }

    @Override
    public void onAllChanged()
    {
        this.boatDerived = Map.of();
        this.designDerived = Map.of();
        this.raceDerived = Map.of();
        this.residualsByBoatId = Map.of();
        this.profilesByBoatId = Map.of();
    }

    // --- Accessors ---

    public int targetYear()
    {
        return targetYear;
    }

    public double minAnalysisR2()
    {
        return minAnalysisR2;
    }

    public int minAnalysisPairs()
    {
        return minAnalysisPairs;
    }

    public List<ComparisonResult> comparisons()
    {
        return comparisons;
    }

    public Map<String, BoatDerived> boatDerived()
    {
        return boatDerived;
    }

    public Map<String, DesignDerived> designDerived()
    {
        return designDerived;
    }

    public Map<String, RaceDerived> raceDerived()
    {
        return raceDerived;
    }

    public Map<String, List<EntryResidual>> residualsByBoatId()
    {
        return residualsByBoatId;
    }

    public Map<String, PerformanceProfile> profilesByBoatId()
    {
        return profilesByBoatId;
    }

    public PfQuality pfQuality()
    {
        return lastPfQuality;
    }

    /**
     * Runs the PF optimiser and merges results into the Derived maps.
     */
    public void refreshPf(PfConfig config, java.util.function.Supplier<Boolean> stopCheck)
    {
        LOG.info("AnalysisCache: running PF optimiser...");
        // Build the conversion graph from the cached comparisons so the optimiser can
        // apply the fleet-wide cross-variant pull on top of per-boat RF anchoring.
        ConversionGraph graph = ConversionGraph.from(comparisons, minAnalysisR2, minAnalysisPairs);
        PfResult result = new PfOptimiser().optimise(store, boatDerived, config, stopCheck, graph, targetYear);
        if (result.boatPfs().isEmpty())
        {
            LOG.info("AnalysisCache: PF optimiser returned no results (stopped or no data)");
            return;
        }
        mergePfResults(result);
        LOG.info("AnalysisCache: PF merged — {} boats, {} races, {} inner iters, {} outer iters",
            result.boatPfs().size(), result.divisionPfsByRaceId().size(),
            result.innerIterations(), result.outerIterations());
    }

    /**
     * Merges PF results into the consolidated Derived maps using copy-on-write.
     */
    private void mergePfResults(PfResult result)
    {
        // Merge BoatPf into BoatDerived — skip excluded boats/designs
        Map<String, BoatDerived> currentBoats = this.boatDerived;
        Map<String, BoatDerived> newBoats = new LinkedHashMap<>();
        for (Map.Entry<String, BoatDerived> e : currentBoats.entrySet())
        {
            String id = e.getKey();
            if (store.isBoatExcluded(id)) continue;
            Boat b = e.getValue().boat();
            if (b.designId() != null && store.isDesignExcluded(b.designId())) continue;
            newBoats.put(id, e.getValue());
        }
        for (Map.Entry<String, BoatPf> e : result.boatPfs().entrySet())
        {
            BoatDerived existing = newBoats.get(e.getKey());
            if (existing == null)
                continue;
            BoatPf pf = e.getValue();
            // For boats whose design is flagged noSpinnaker, collapse spin and nonSpin into
            // one aggregated factor — the design physically cannot fly a kite, so any
            // spin/nonSpin split in the source data is a categorisation artifact.
            Boat boat = existing.boat();
            if (boat.designId() != null && store.isDesignNoSpinnaker(boat.designId())
                && (pf.spin() != null || pf.nonSpin() != null))
            {
                Factor combined = pf.spin() == null ? pf.nonSpin()
                    : pf.nonSpin() == null ? pf.spin()
                    : Factor.aggregate(pf.spin(), pf.nonSpin());
                // Pick the larger-magnitude (more recent / data-driven) delta and combine race counts.
                double delta = Math.abs(pf.referenceDeltaSpin()) > Math.abs(pf.referenceDeltaNonSpin())
                    ? pf.referenceDeltaSpin() : pf.referenceDeltaNonSpin();
                int totalRaces = pf.spinRaceCount() + pf.nonSpinRaceCount();
                pf = new BoatPf(combined, combined, pf.twoHanded(),
                    delta, delta, pf.referenceDeltaTwoHanded(),
                    totalRaces, totalRaces, pf.twoHandedRaceCount());
            }
            newBoats.put(e.getKey(), new BoatDerived(existing.boat(), existing.referenceFactors(),
                existing.raceIds(), existing.seriesIds(), pf));
        }
        this.boatDerived = Map.copyOf(newBoats);

        // Merge DivisionPf into RaceDerived
        Map<String, RaceDerived> currentRaces = this.raceDerived;
        Map<String, RaceDerived> newRaces = new LinkedHashMap<>(currentRaces);
        for (Map.Entry<String, List<DivisionPf>> e : result.divisionPfsByRaceId().entrySet())
        {
            RaceDerived existing = currentRaces.get(e.getKey());
            if (existing != null)
                newRaces.put(e.getKey(), new RaceDerived(existing.race(), existing.finisherCount(), e.getValue()));
        }
        this.raceDerived = Map.copyOf(newRaces);

        // Store residuals separately
        this.residualsByBoatId = result.residualsByBoatId();

        // Store quality summary
        if (result.quality() != null)
            this.lastPfQuality = result.quality();

        // Recompute fleet-relative performance profiles
        refreshProfiles();
    }

    /**
     * Computes fleet-relative performance profiles for all boats using the last 12 months
     * of residual data. Called automatically after each PF run.
     */
    private void refreshProfiles()
    {
        // Build raceId → divisionName → dispersion lookup from current raceDerived
        Map<String, Map<String, Double>> dispersionMap = new LinkedHashMap<>();
        for (RaceDerived rd : raceDerived.values())
        {
            if (rd.divisionPfs() == null) continue;
            Map<String, Double> divMap = new LinkedHashMap<>();
            for (DivisionPf dh : rd.divisionPfs())
                divMap.put(dh.divisionName(), dh.dispersion());
            if (!divMap.isEmpty())
                dispersionMap.put(rd.race().id(), divMap);
        }

        this.profilesByBoatId = new PerformanceProfileBuilder(
                diversityNonSpinWeight, diversitySpinWeight, diversityTwoHandedWeight,
                consistencyDropInterval)
            .buildAll(residualsByBoatId, dispersionMap, store.races());
        LOG.info("AnalysisCache: computed performance profiles for {} boats", profilesByBoatId.size());
    }
}
