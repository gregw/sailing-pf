package org.mortbay.sailing.hpf.server;

import org.mortbay.sailing.hpf.analysis.BoatDerived;
import org.mortbay.sailing.hpf.analysis.BoatHpf;
import org.mortbay.sailing.hpf.analysis.ComparisonResult;
import org.mortbay.sailing.hpf.analysis.ConversionGraph;
import org.mortbay.sailing.hpf.analysis.DesignDerived;
import org.mortbay.sailing.hpf.analysis.DivisionHpf;
import org.mortbay.sailing.hpf.analysis.EntryResidual;
import org.mortbay.sailing.hpf.analysis.HpfConfig;
import org.mortbay.sailing.hpf.analysis.HpfOptimiser;
import org.mortbay.sailing.hpf.analysis.HpfQuality;
import org.mortbay.sailing.hpf.analysis.HpfResult;
import org.mortbay.sailing.hpf.analysis.PerformanceProfile;
import org.mortbay.sailing.hpf.analysis.PerformanceProfileBuilder;
import org.mortbay.sailing.hpf.analysis.RaceDerived;
import org.mortbay.sailing.hpf.analysis.ReferenceFactors;
import org.mortbay.sailing.hpf.analysis.HandicapAnalyser;
import org.mortbay.sailing.hpf.analysis.ReferenceNetworkBuilder;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Design;
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

    // Consolidated per-entity derived data
    private volatile Map<String, BoatDerived> boatDerived = Map.of();
    private volatile Map<String, DesignDerived> designDerived = Map.of();
    private volatile Map<String, RaceDerived> raceDerived = Map.of();
    private volatile Map<String, List<EntryResidual>> residualsByBoatId = Map.of();
    private volatile Map<String, PerformanceProfile> profilesByBoatId = Map.of();
    private volatile HpfQuality lastHpfQuality;  // null until first run
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
     */
    public void refresh(Integer targetIrcYear, Double outlierSigma, double clubCertificateWeight, double minR2)
    {
        LOG.info("AnalysisCache: refreshing...");
        double sigma = outlierSigma != null ? outlierSigma : 2.5;
        List<ComparisonResult> newComparisons = new HandicapAnalyser(store, sigma).analyseAll();
        ConversionGraph graph = ConversionGraph.from(newComparisons, minR2);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);

        comparisons = newComparisons;
        targetYear  = year;
        minAnalysisR2 = minR2;
        mergeReferenceFactors(built);
        LOG.info("AnalysisCache: {} comparisons, {} boat derived, {} design derived (targetYear={}, minR2={})",
            newComparisons.size(), boatDerived.size(), designDerived.size(), year, minR2);
    }

    /**
     * Recomputes reference factors only, using the existing comparisons and conversion graph.
     * Faster than {@link #refresh(Integer, Double, double, double)} when only the boat certificate data has changed.
     *
     * @param targetIrcYear override target IRC year, or null to auto-detect from data
     * @param minR2         minimum R² for a conversion edge to be included in the graph
     */
    public void refreshReferenceFactors(Integer targetIrcYear, double clubCertificateWeight, double minR2)
    {
        LOG.info("AnalysisCache: refreshing reference factors...");
        ConversionGraph graph = ConversionGraph.from(comparisons, minR2);
        int year = targetIrcYear != null ? targetIrcYear : maxIrcCertYear();
        ReferenceNetworkBuilder.BuildResult built = new ReferenceNetworkBuilder(clubCertificateWeight).build(store, graph, year);
        targetYear = year;
        minAnalysisR2 = minR2;
        mergeReferenceFactors(built);
        LOG.info("AnalysisCache: {} boat derived, {} design derived (targetYear={}, minR2={})",
            boatDerived.size(), designDerived.size(), year, minR2);
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
            BoatHpf existingHpf = existing != null ? existing.hpf() : null;
            newBoats.put(id, new BoatDerived(boat, e.getValue(), raceIds, seriesIds, existingHpf));
        }
        // Keep entries that have index data but no reference factors (boats not in BuildResult)
        for (Map.Entry<String, BoatDerived> e : currentBoats.entrySet())
        {
            if (!newBoats.containsKey(e.getKey()) && (!e.getValue().raceIds().isEmpty() || !e.getValue().seriesIds().isEmpty()))
                newBoats.put(e.getKey(), new BoatDerived(e.getValue().boat(), null, e.getValue().raceIds(), e.getValue().seriesIds(), e.getValue().hpf()));
        }
        this.boatDerived = Map.copyOf(newBoats);

        // Merge design reference factors with existing index data
        Map<String, DesignDerived> currentDesigns = this.designDerived;
        Map<String, DesignDerived> newDesigns = new LinkedHashMap<>();
        for (Map.Entry<String, ReferenceFactors> e : built.designFactors().entrySet())
        {
            String id = e.getKey();
            Design design = store.designs().get(id);
            if (design == null) continue;
            DesignDerived existing = currentDesigns.get(id);
            Set<String> boatIds = existing != null ? existing.boatIds() : Set.of();
            newDesigns.put(id, new DesignDerived(design, e.getValue(), boatIds));
        }
        // Keep entries that have index data but no reference factors
        for (Map.Entry<String, DesignDerived> e : currentDesigns.entrySet())
        {
            if (!newDesigns.containsKey(e.getKey()) && !e.getValue().boatIds().isEmpty())
                newDesigns.put(e.getKey(), new DesignDerived(e.getValue().design(), null, e.getValue().boatIds()));
        }
        this.designDerived = Map.copyOf(newDesigns);
    }

    /**
     * Returns the maximum year among real (issued) IRC certificates in the store,
     * falling back to the current calendar year if none exist.
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
            BoatDerived existing = currentBoats.get(id);
            ReferenceFactors rf = existing != null ? existing.referenceFactors() : null;
            Set<String> raceIds = byBoatR.getOrDefault(id, Set.of());
            Set<String> seriesIds = byBoatS.getOrDefault(id, Set.of());
            BoatHpf existingHpf = existing != null ? existing.hpf() : null;
            newBoats.put(id, new BoatDerived(boat, rf, raceIds, seriesIds, existingHpf));
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
            List<DivisionHpf> existingDivHpfs = existingRd != null ? existingRd.divisionHpfs() : null;
            newRaces.put(race.id(), new RaceDerived(race, finisherCount, existingDivHpfs));
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

    public HpfQuality hpfQuality()
    {
        return lastHpfQuality;
    }

    /**
     * Runs the HPF optimiser and merges results into the Derived maps.
     */
    public void refreshHpf(HpfConfig config, java.util.function.Supplier<Boolean> stopCheck)
    {
        LOG.info("AnalysisCache: running HPF optimiser...");
        HpfResult result = new HpfOptimiser().optimise(store, boatDerived, config, stopCheck);
        if (result.boatHpfs().isEmpty())
        {
            LOG.info("AnalysisCache: HPF optimiser returned no results (stopped or no data)");
            return;
        }
        mergeHpfResults(result);
        LOG.info("AnalysisCache: HPF merged — {} boats, {} races, {} inner iters, {} outer iters",
            result.boatHpfs().size(), result.divisionHpfsByRaceId().size(),
            result.innerIterations(), result.outerIterations());
    }

    /**
     * Merges HPF results into the consolidated Derived maps using copy-on-write.
     */
    private void mergeHpfResults(HpfResult result)
    {
        // Merge BoatHpf into BoatDerived
        Map<String, BoatDerived> currentBoats = this.boatDerived;
        Map<String, BoatDerived> newBoats = new LinkedHashMap<>(currentBoats);
        for (Map.Entry<String, BoatHpf> e : result.boatHpfs().entrySet())
        {
            BoatDerived existing = currentBoats.get(e.getKey());
            if (existing != null)
                newBoats.put(e.getKey(), new BoatDerived(existing.boat(), existing.referenceFactors(),
                    existing.raceIds(), existing.seriesIds(), e.getValue()));
        }
        this.boatDerived = Map.copyOf(newBoats);

        // Merge DivisionHpf into RaceDerived
        Map<String, RaceDerived> currentRaces = this.raceDerived;
        Map<String, RaceDerived> newRaces = new LinkedHashMap<>(currentRaces);
        for (Map.Entry<String, List<DivisionHpf>> e : result.divisionHpfsByRaceId().entrySet())
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
            this.lastHpfQuality = result.quality();

        // Recompute fleet-relative performance profiles
        refreshProfiles();
    }

    /**
     * Computes fleet-relative performance profiles for all boats using the last 12 months
     * of residual data. Called automatically after each HPF run.
     */
    private void refreshProfiles()
    {
        // Build raceId → divisionName → dispersion lookup from current raceDerived
        Map<String, Map<String, Double>> dispersionMap = new LinkedHashMap<>();
        for (RaceDerived rd : raceDerived.values())
        {
            if (rd.divisionHpfs() == null) continue;
            Map<String, Double> divMap = new LinkedHashMap<>();
            for (DivisionHpf dh : rd.divisionHpfs())
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
