package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

/**
 * The data a PF optimiser works on: every qualifying result as an {@link Entry} in log space,
 * indexed by division and by boat-variant, with each boat's reference factors. Built once by
 * {@link #build} and shared by {@link PfOptimiser} (ALS) and {@link PfAnnealer} (simulated
 * annealing), so both optimise exactly the same problem and {@link PfScore} can compare them.
 */
final class PfProblem
{
    static final int SPIN = 0;
    static final int NON_SPIN = 1;
    static final int TWO_HANDED = 2;

    record DivisionKey(String raceId, int divisionIndex) {}

    record Entry(
        int boatOrdinal,        // index into logPf arrays
        int divOrdinal,         // index into logT array
        double logElapsed,      // log(elapsedTime.toNanos())
        int variant,            // 0=spin, 1=nonSpin, 2=twoHanded
        double refWeight,       // RF weight for the relevant variant
        String boatId,          // for output assembly
        String raceId,          // for output assembly
        String divisionName,    // for output assembly
        LocalDate raceDate,     // for output assembly
        boolean counted         // false: reference-only copy of a duplicate race (DuplicateRaces) —
                                // sets its division's T₀ but is not counted in the boat's PF
    ) {}

    final List<Entry> entries;
    final int nBoats;
    final int nDivs;
    final String[] ordinalToBoatId;
    final DivisionKey[] ordinalToDivKey;
    final Map<String, double[]> boatLogRf;      // boatId → [spin, nonSpin, twoHanded] log(RF), NaN if none
    final Map<String, double[]> boatRfWeight;   // boatId → [spin, nonSpin, twoHanded] RF weight
    final int[][] entryCount;                   // [variant][boat] counted entries
    final List<List<Integer>> divEntryIndexes;  // division → all its entries
    final List<Integer>[][] boatVariantEntries; // [variant][boat] → counted entries
    final double[] ageDecay;                    // per entry, see PfConfig.ageHalfLifeYears

    private PfProblem(List<Entry> entries, Map<String, Integer> boatOrdinals, Map<DivisionKey, Integer> divOrdinals,
                      Map<String, double[]> boatLogRf, Map<String, double[]> boatRfWeight, PfConfig config)
    {
        this.entries = entries;
        this.nBoats = boatOrdinals.size();
        this.nDivs = divOrdinals.size();
        this.boatLogRf = boatLogRf;
        this.boatRfWeight = boatRfWeight;

        ordinalToBoatId = new String[nBoats];
        for (var e : boatOrdinals.entrySet())
            ordinalToBoatId[e.getValue()] = e.getKey();

        ordinalToDivKey = new DivisionKey[nDivs];
        for (var e : divOrdinals.entrySet())
            ordinalToDivKey[e.getValue()] = e.getKey();

        // Per-boat per-variant counts of counted entries (reference-only ones are not the
        // boat's results — they count in another copy of the race)
        entryCount = new int[3][nBoats];
        for (Entry e : entries)
            if (e.counted())
                entryCount[e.variant()][e.boatOrdinal()]++;

        // Per-division entry index for fast iteration
        divEntryIndexes = new ArrayList<>();
        for (int d = 0; d < nDivs; d++)
            divEntryIndexes.add(new ArrayList<>());
        for (int i = 0; i < entries.size(); i++)
            divEntryIndexes.get(entries.get(i).divOrdinal()).add(i);

        // Per-boat-variant index of counted entries: Step B, confidence and residuals
        // read it, so a reference-only entry reaches only its division's T₀ (Step A).
        @SuppressWarnings("unchecked")
        List<Integer>[][] bve = new List[3][nBoats];
        for (int v = 0; v < 3; v++)
            for (int b = 0; b < nBoats; b++)
                bve[v][b] = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++)
        {
            Entry e = entries.get(i);
            if (e.counted())
                bve[e.variant()][e.boatOrdinal()].add(i);
        }
        boatVariantEntries = bve;

        // Age decay per entry: older results count less in the boat's own PF (Step B and its
        // confidence), measured back from the boat's latest race in that variant — see
        // PfConfig.ageHalfLifeYears.
        ageDecay = computeAgeDecay(entries, boatVariantEntries, config);
    }

    /**
     * Enumerates every non-excluded race division with at least two qualifying finishers.
     *
     * @param duplicates copies of the same race (see {@link DuplicateRaces}): a reference-only
     *                   result helps set its division's T₀ but is left out of its boat's PF,
     *                   confidence, race count and residuals; and a finisher's variant is the one
     *                   decided across the copies
     */
    static PfProblem build(DataStore store, Map<String, BoatDerived> boatDerivedMap, PfConfig config,
                           DuplicateRaces duplicates)
    {
        Map<String, Integer> boatOrdinals = new HashMap<>();
        Map<DivisionKey, Integer> divOrdinals = new HashMap<>();
        List<Entry> entries = new ArrayList<>();
        Map<String, double[]> boatLogRf = new HashMap<>();
        Map<String, double[]> boatRfWeight = new HashMap<>();

        // Enumerate all non-excluded races
        for (Race race : store.races().values())
        {
            if (store.isRaceExcluded(race.id())) continue;
            if (store.isClubExcluded(race.clubId())) continue;
            if (race.divisions() == null) continue;

            for (int di = 0; di < race.divisions().size(); di++)
            {
                Division div = race.divisions().get(di);
                if (div.finishers() == null || div.finishers().size() < 2) continue;

                List<Entry> divEntries = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    if (f.elapsedTime() == null) continue;
                    if (store.isBoatExcluded(f.boatId())) continue;

                    BoatDerived bd = boatDerivedMap.get(f.boatId());
                    if (bd != null && bd.boat().designId() != null
                            && store.isDesignExcluded(bd.boat().designId())) continue;
                    if (bd == null || bd.referenceFactors() == null) continue;

                    ReferenceFactors rf = bd.referenceFactors();
                    // The series type overrides per-entry flags (SailSys takes them from the
                    // boat's certificate, not the race rules) — see EntryVariant.of — and a
                    // duplicate race's copies agree on one variant (DuplicateRaces).
                    int variant = variantIndex(duplicates.variant(store, race, di, div, f));
                    Factor rfFactor = variantFactor(rf, variant);
                    // Allow rfFactor.weight() == 0: the Step-B formula degenerates cleanly to a
                    // pure race-derived PF (no regularisation toward RF) when rfW = 0, so these
                    // boats still contribute to division time calibration and receive an PF.
                    if (rfFactor == null) continue;

                    // Ensure boat has an ordinal
                    int boatOrd = boatOrdinals.computeIfAbsent(f.boatId(), k -> boatOrdinals.size());

                    // Store RF values per boat
                    boatLogRf.computeIfAbsent(f.boatId(), k -> new double[]{Double.NaN, Double.NaN, Double.NaN});
                    boatRfWeight.computeIfAbsent(f.boatId(), k -> new double[3]);
                    boatLogRf.get(f.boatId())[variant] = Math.log(rfFactor.value());
                    boatRfWeight.get(f.boatId())[variant] = rfFactor.weight();

                    divEntries.add(new Entry(
                        boatOrd, -1, // divOrdinal set below
                        Math.log(f.elapsedTime().toNanos()),
                        variant, rfFactor.weight(),
                        f.boatId(), race.id(), div.name(), race.date(),
                        !duplicates.isReferenceOnly(race.id(), di, f.boatId())));
                }

                // Skip divisions with <2 qualifying entries, or whose every result counts in
                // another copy of the race (it could not change any PF).
                if (divEntries.size() < 2) continue;
                if (divEntries.stream().noneMatch(Entry::counted)) continue;

                DivisionKey dk = new DivisionKey(race.id(), di);
                int divOrd = divOrdinals.computeIfAbsent(dk, k -> divOrdinals.size());

                // Fix up divOrdinal in entries
                for (Entry e : divEntries)
                    entries.add(new Entry(e.boatOrdinal(), divOrd, e.logElapsed(),
                        e.variant(), e.refWeight(), e.boatId(), e.raceId(), e.divisionName(), e.raceDate(),
                        e.counted()));
            }
        }

        return new PfProblem(entries, boatOrdinals, divOrdinals, boatLogRf, boatRfWeight, config);
    }

    boolean isEmpty()
    {
        return nBoats == 0 || nDivs == 0 || entries.isEmpty();
    }

    /** Starting point for an optimiser: each boat-variant's log(RF), or 0 where it has none. */
    double[][] initialLogPf()
    {
        double[][] logPf = new double[3][nBoats];
        for (int b = 0; b < nBoats; b++)
        {
            double[] lr = boatLogRf.get(ordinalToBoatId[b]);
            for (int v = 0; v < 3; v++)
                logPf[v][b] = Double.isNaN(lr[v]) ? 0.0 : lr[v];
        }
        return logPf;
    }

    /** {@code logElapsed + logPf − logT}: the entry's corrected time relative to its division's par. */
    double residual(int idx, double[][] logPf, double[] logT)
    {
        Entry e = entries.get(idx);
        return e.logElapsed() + logPf[e.variant()][e.boatOrdinal()] - logT[e.divOrdinal()];
    }

    static int variantIndex(org.mortbay.sailing.pf.data.EntryVariant variant)
    {
        return switch (variant)
        {
            case SPIN -> SPIN;
            case NON_SPIN -> NON_SPIN;
            case TWO_HANDED -> TWO_HANDED;
        };
    }

    static Factor variantFactor(ReferenceFactors rf, int variant)
    {
        return switch (variant)
        {
            case SPIN -> rf.spin();
            case NON_SPIN -> rf.nonSpin();
            case TWO_HANDED -> rf.twoHanded();
            default -> null;
        };
    }

    /**
     * The age-decay multiplier of each entry: {@link PfConfig#ageDecay} of the entry's age in
     * years before its boat's most recent race in the same variant (1 when decay is off or the
     * date is unknown).
     */
    static double[] computeAgeDecay(List<Entry> entries, List<Integer>[][] boatVariantEntries,
                                    PfConfig config)
    {
        double[] decay = new double[entries.size()];
        Arrays.fill(decay, 1.0);
        if (config.ageHalfLifeYears() <= 0)
            return decay;
        for (List<Integer>[] byBoat : boatVariantEntries)
        {
            for (List<Integer> idxs : byBoat)
            {
                LocalDate latest = null;
                for (int idx : idxs)
                {
                    LocalDate d = entries.get(idx).raceDate();
                    if (d != null && (latest == null || d.isAfter(latest)))
                        latest = d;
                }
                if (latest == null)
                    continue;
                for (int idx : idxs)
                {
                    LocalDate d = entries.get(idx).raceDate();
                    if (d != null)
                        decay[idx] = config.ageDecay(ChronoUnit.DAYS.between(d, latest) / 365.25);
                }
            }
        }
        return decay;
    }
}
