package org.mortbay.sailing.hpf.analysis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a map of boatId → {@link ReferenceFactors} implementing pipeline steps 8–12.
 *
 * <h2>Step 8 — Certificate-based factors (generation 0)</h2>
 * For each boat and each target variant (spin, non-spin, two-handed):
 * <ol>
 *   <li>Primary pass: enumerate all conversion paths using <em>same-variant</em> edges only
 *       (no cross-variant conversions like NS→spin).</li>
 *   <li>Fallback pass: if the primary pass found no factors, allow cross-variant edges so
 *       a boat with only spin certs can still produce an NS estimate (and vice-versa).</li>
 *   <li>All factors from all paths across all certs are combined with
 *       {@link Factor#aggregate}. Paths that agree reinforce confidence; paths that
 *       disagree are penalised by the variance term.</li>
 * </ol>
 *
 * <h2>Steps 9–12 — Iterative propagation</h2>
 * After step 8, the algorithm iterates until convergence (or {@link #MAX_ITERATIONS}):
 * <ol>
 *   <li><b>Step 9</b>: Aggregate all certificated boats of the same design into a
 *       design-level reference factor using a weighted mean in log space.
 *       Weight capped at {@link #DESIGN_FACTOR_WEIGHT}.</li>
 *   <li><b>Step 10</b>: For boats that still lack a factor, propagate from race
 *       co-participants: for each race in which the boat competed alongside reference
 *       boats, estimate its implied factor from elapsed-time ratios, then aggregate
 *       across all qualifying races. Weight capped at {@link #PROPAGATION_FACTOR_WEIGHT}.</li>
 *   <li><b>Step 11</b>: For boats that still lack a factor, fall back to the design-level
 *       factor (if their design has one).</li>
 * </ol>
 * Each iteration's generation number is recorded on every {@link ReferenceFactors}
 * record it modifies; generation 0 = step 8 only.
 *
 * <h2>Certificate base weights</h2>
 * <ul>
 *   <li>International certs: 1.0</li>
 *   <li>Club certs ({@link Certificate#club()} = true): configurable, default 0.9×</li>
 *   <li>ORC windward/leeward ({@link Certificate#windwardLeeward()} = true): 0.8×</li>
 * </ul>
 * Multiplicative: an ORC club WL cert gets 0.9 × 0.8 = 0.72 base weight (at default settings).
 *
 * <h2>Age cap</h2>
 * Certificates older than {@link #MAX_CERT_AGE_YEARS} years relative to currentYear are ignored.
 */
public class ReferenceNetworkBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(ReferenceNetworkBuilder.class);

    /** Certificates older than this many years relative to currentYear are ignored. */
    public static final int MAX_CERT_AGE_YEARS = 5;

    /** Default base weight multiplier for club-level certificates (vs 1.0 for international). */
    public static final double DEFAULT_CLUB_BASE_WEIGHT = 0.9;

    /** Base weight multiplier for ORC windward/leeward course-specific certificates. */
    public static final double ORC_WL_BASE_WEIGHT = 0.8;

    /** DFS paths with accumulated weight below this threshold are pruned. */
    private static final double MIN_PATH_WEIGHT = 0.001;

    /** Maximum DFS depth (number of conversion hops) to prevent runaway traversal. */
    private static final int MAX_DEPTH = 8;

    /** Maximum weight for design-level reference factors (step 9). */
    public static final double DESIGN_FACTOR_WEIGHT = 0.85;

    /** Maximum weight for race co-participation propagated factors (step 10). */
    public static final double PROPAGATION_FACTOR_WEIGHT = 0.7;

    /** Maximum propagation iterations before declaring non-convergence (step 12). */
    public static final int MAX_ITERATIONS = 20;

    /**
     * Age discount step for race co-participation propagation: weight is reduced by this
     * amount for each year the race predates {@code currentYear}.
     * Races from {@code currentYear} → 1.0, one year old → 0.8, two → 0.6,
     * three → 0.4, four → 0.2, five or more → 0.0 (excluded).
     */
    public static final double RACE_AGE_WEIGHT_STEP = 0.2;

    /** Actual club certificate weight multiplier used by this instance. */
    private final double clubBaseWeight;

    public ReferenceNetworkBuilder()
    {
        this(DEFAULT_CLUB_BASE_WEIGHT);
    }

    public ReferenceNetworkBuilder(double clubBaseWeight)
    {
        this.clubBaseWeight = clubBaseWeight;
    }

    // Per-boat race participation entry: one per (race, division) containing this boat.
    private record DivisionEntry(Duration elapsed, boolean nonSpinnaker, List<Finisher> peers, double ageWeight) {}

    // ==========================================================================
    // Public API
    // ==========================================================================

    /**
     * Result of a full {@link #build} run: boat-level reference factors plus the design-level
     * factors that were in place at the point race propagation converged (i.e. derived only from
     * cert-based and race-propagated boats, not from design-fallback boats).
     *
     * <p>Design factor arrays are indexed as {@code [0]=spin, [1]=nonSpin, [2]=twoHanded}.
     * A null factor within a {@link ReferenceFactors} means no factor was available for that variant.
     */
    public record BuildResult(
        Map<String, ReferenceFactors> boatFactors,
        Map<String, ReferenceFactors> designFactors
    ) {}

    /**
     * Computes reference factors for a single boat using a pre-built conversion graph.
     * Only step 8 (certificate-based) is applied; propagation (steps 9–12) is skipped.
     */
    public ReferenceFactors buildForBoat(Boat boat, ConversionGraph graph, int currentYear)
    {
        List<Certificate> validCerts = validCerts(boat, currentYear);

        Factor spin    = computeVariantFactor(validCerts, graph, currentYear, false, false, false);
        Factor nonSpin = computeVariantFactor(validCerts, graph, currentYear, true,  false, false);
        Factor twoH    = computeVariantFactor(validCerts, graph, currentYear, false, true,  false);

        if (spin    == null) spin    = computeVariantFactor(validCerts, graph, currentYear, false, false, true);
        if (nonSpin == null) nonSpin = computeVariantFactor(validCerts, graph, currentYear, true,  false, true);
        if (twoH    == null) twoH    = computeVariantFactor(validCerts, graph, currentYear, false, true,  true);

        return new ReferenceFactors(spin, nonSpin, twoH, 0, 0, 0);
    }

    /**
     * Builds a ConversionGraph from the store's current analysis results.
     */
    public ConversionGraph buildGraph(DataStore store)
    {
        return ConversionGraph.from(new HandicapAnalyser(store).analyseAll());
    }

    /**
     * Builds the reference factor map for all boats in the store (steps 8–12).
     */
    public BuildResult build(DataStore store, int currentYear)
    {
        ConversionGraph graph = buildGraph(store);
        return build(store, graph, currentYear);
    }

    /**
     * Builds the reference factor map using a pre-built conversion graph (steps 8–12).
     * Use this when the graph has already been built (e.g. from a shared cache)
     * to avoid re-running {@link HandicapAnalyser#analyseAll()}.
     */
    public BuildResult build(DataStore store, ConversionGraph graph, int currentYear)
    {
        // Step 8: certificate-based factors (generation 0)
        Map<String, ReferenceFactors> result = new LinkedHashMap<>();
        for (Boat boat : store.boats().values())
        {
            List<Certificate> validCerts = validCerts(boat, currentYear);

            Factor spin    = computeVariantFactor(validCerts, graph, currentYear, false, false, false);
            Factor nonSpin = computeVariantFactor(validCerts, graph, currentYear, true,  false, false);
            Factor twoH    = computeVariantFactor(validCerts, graph, currentYear, false, true,  false);

            if (spin    == null) spin    = computeVariantFactor(validCerts, graph, currentYear, false, false, true);
            if (nonSpin == null) nonSpin = computeVariantFactor(validCerts, graph, currentYear, true,  false, true);
            if (twoH    == null) twoH    = computeVariantFactor(validCerts, graph, currentYear, false, true,  true);

            result.put(boat.id(), new ReferenceFactors(spin, nonSpin, twoH, 0, 0, 0));

            if (spin != null)
                LOG.debug("boat={} spin=({}, w={})", boat.id(),
                    String.format("%.4f", spin.value()), String.format("%.3f", spin.weight()));
        }

        long withSpin = result.values().stream().filter(r -> r.spin() != null).count();
        LOG.info("ReferenceNetworkBuilder step 8: {} boats, {} with spin factor", result.size(), withSpin);

        // Steps 9–12: iterative design and race propagation
        Map<String, List<DivisionEntry>> boatDivIndex = buildBoatDivisionIndex(store, currentYear);
        Map<String, Boat> boats = store.boats();

        // Steps 9–10: race co-participation propagation only (no design fallback during the loop).
        // Run for at least 2 generations before considering convergence, so that boats added in
        // generation 1 can themselves serve as references for others in generation 2.
        int lastGen = 1;
        Map<String, ReferenceFactors> convergenceDesignFactors = Map.of();
        for (int gen = 1; gen <= MAX_ITERATIONS; gen++)
        {
            lastGen = gen;
            // Step 9: aggregate to design level (used only as reference for race propagation)
            convergenceDesignFactors = computeDesignFactors(result, boats);

            // Step 10: propagate via race co-participation (spin and nonSpin variants only)
            int fromRaces = propagateViaRaces(result, boatDivIndex, gen);
            LOG.info("ReferenceNetworkBuilder iteration {}: {} from races", gen, fromRaces);

            // Converged — but only exit after at least 2 generations so that race propagation
            // has had a second chance to use boats that were newly added in generation 1.
            if (fromRaces == 0 && gen >= 2)
                break;

            if (gen == MAX_ITERATIONS)
                throw new IllegalStateException(
                    "Reference factor propagation did not converge after " + MAX_ITERATIONS
                    + " iterations — a subgraph may be entirely disconnected from the"
                    + " measurement certificate network");
        }

        // Step 11: design fallback — applied once, only for boats that race propagation could not
        // reach.  Uses the design factors computed at convergence (cert-based + race-propagated
        // boats only), so the design factors returned in BuildResult are not polluted by
        // circular fallback-derived values.
        int fromDesign = applyDesignFallback(result, convergenceDesignFactors, boats, lastGen + 1);
        LOG.info("ReferenceNetworkBuilder design fallback: {} boats", fromDesign);

        // Step 12: cross-variant fill — for any boat that has one variant but is missing another,
        // derive the missing variant via the graph's cross-variant edges (e.g. NS → spin).
        // This covers boats whose RF came entirely from race propagation or design fallback and
        // therefore never had the cert-based cross-variant pass applied to them.
        int fromCrossVariant = fillMissingVariantsFromExisting(result, graph, currentYear, lastGen + 2);
        LOG.info("ReferenceNetworkBuilder cross-variant fill: {} boats", fromCrossVariant);

        long withSpinFinal = result.values().stream().filter(r -> r.spin() != null).count();
        LOG.info("ReferenceNetworkBuilder complete: {} boats, {} with spin factor",
            result.size(), withSpinFinal);
        return new BuildResult(result, convergenceDesignFactors);
    }

    // ==========================================================================
    // Step 9: design-level aggregation
    // ==========================================================================

    /**
     * Aggregates per-boat reference factors up to design level.
     * Returns a map from designId to {@link ReferenceFactors},
     * where each non-null factor is the log-space weighted mean of all contributing boats,
     * capped at {@link #DESIGN_FACTOR_WEIGHT}. Generations are the ceiling of the
     * average generation of contributing boats for each variant.
     */
    private static Map<String, ReferenceFactors> computeDesignFactors(
        Map<String, ReferenceFactors> brf, Map<String, Boat> boats)
    {
        Map<String, List<Factor>> spinByDesign    = new LinkedHashMap<>();
        Map<String, List<Factor>> nonSpinByDesign = new LinkedHashMap<>();
        Map<String, List<Factor>> twoHByDesign    = new LinkedHashMap<>();
        Map<String, List<Integer>> spinGenByDesign    = new LinkedHashMap<>();
        Map<String, List<Integer>> nonSpinGenByDesign = new LinkedHashMap<>();
        Map<String, List<Integer>> twoHGenByDesign    = new LinkedHashMap<>();

        for (Map.Entry<String, ReferenceFactors> e : brf.entrySet())
        {
            Boat boat = boats.get(e.getKey());
            if (boat == null || boat.designId() == null) continue;
            String designId = boat.designId();
            ReferenceFactors f = e.getValue();

            if (f.spin() != null)
            {
                spinByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.spin());
                spinGenByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.spinGeneration());
            }
            if (f.nonSpin() != null)
            {
                nonSpinByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.nonSpin());
                nonSpinGenByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.nonSpinGeneration());
            }
            if (f.twoHanded() != null)
            {
                twoHByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.twoHanded());
                twoHGenByDesign.computeIfAbsent(designId, k -> new ArrayList<>()).add(f.twoHandedGeneration());
            }
        }

        Set<String> allDesigns = new HashSet<>();
        allDesigns.addAll(spinByDesign.keySet());
        allDesigns.addAll(nonSpinByDesign.keySet());
        allDesigns.addAll(twoHByDesign.keySet());

        Map<String, ReferenceFactors> result = new LinkedHashMap<>();
        for (String designId : allDesigns)
        {
            List<Factor> s  = spinByDesign.getOrDefault(designId, List.of());
            List<Factor> ns = nonSpinByDesign.getOrDefault(designId, List.of());
            List<Factor> th = twoHByDesign.getOrDefault(designId, List.of());
            Factor spinF = !s.isEmpty()  ? logAggregate(s,  DESIGN_FACTOR_WEIGHT) : null;
            Factor nsF   = !ns.isEmpty() ? logAggregate(ns, DESIGN_FACTOR_WEIGHT) : null;
            Factor thF   = !th.isEmpty() ? logAggregate(th, DESIGN_FACTOR_WEIGHT) : null;
            int spinGen = ceilAvgGen(spinGenByDesign.getOrDefault(designId, List.of()));
            int nsGen   = ceilAvgGen(nonSpinGenByDesign.getOrDefault(designId, List.of()));
            int thGen   = ceilAvgGen(twoHGenByDesign.getOrDefault(designId, List.of()));
            result.put(designId, new ReferenceFactors(spinF, nsF, thF, spinGen, nsGen, thGen));
        }
        return result;
    }

    /** Returns the ceiling of the average of a list of generation ints, or 0 if empty. */
    private static int ceilAvgGen(List<Integer> gens)
    {
        if (gens.isEmpty()) return 0;
        return (int) Math.ceil(gens.stream().mapToInt(Integer::intValue).average().orElse(0));
    }

    // ==========================================================================
    // Step 10: race co-participation propagation
    // ==========================================================================

    /**
     * Builds an index from boatId to all (elapsed, nonSpinnaker, peers, ageWeight) entries —
     * one per (race, division) in which the boat appeared.
     *
     * <p>The {@code ageWeight} is {@code max(0, 1 - age × RACE_AGE_WEIGHT_STEP)} where
     * {@code age = currentYear - race.date().getYear()}.  Entries with ageWeight == 0
     * (i.e. five or more years old) are omitted entirely.
     */
    private static Map<String, List<DivisionEntry>> buildBoatDivisionIndex(
        DataStore store, int currentYear)
    {
        Map<String, List<DivisionEntry>> index = new LinkedHashMap<>();
        for (Race race : store.races().values())
        {
            if (race.date() == null) continue;
            int age = Math.abs(currentYear - race.date().getYear());
            double ageWeight = Math.max(0.0, 1.0 - age * RACE_AGE_WEIGHT_STEP);
            if (ageWeight == 0.0) continue;

            for (Division div : race.divisions())
            {
                List<Finisher> finishers = div.finishers();
                for (Finisher f : finishers)
                {
                    if (f.elapsedTime() == null) continue;
                    List<Finisher> peers = new ArrayList<>(finishers.size() - 1);
                    for (Finisher peer : finishers)
                    {
                        if (!peer.boatId().equals(f.boatId()) && peer.elapsedTime() != null)
                            peers.add(peer);
                    }
                    index.computeIfAbsent(f.boatId(), k -> new ArrayList<>())
                         .add(new DivisionEntry(f.elapsedTime(), f.nonSpinnaker(), peers, ageWeight));
                }
            }
        }
        return index;
    }

    /**
     * Propagates spin and nonSpin reference factors to boats that lack them,
     * using elapsed-time ratios relative to reference boats present in the same
     * race divisions. Returns the count of boats updated.
     *
     * <p>For each qualifying (race, division) the implied factor is computed as:
     * <pre>implied_j = factor_i × (elapsed_j / elapsed_i)</pre>
     * across all reference co-finishers {@code i} of the same variant, then aggregated
     * with a log-space weighted mean. The per-division estimates are further aggregated
     * across all races, with the result capped at {@link #PROPAGATION_FACTOR_WEIGHT}.
     *
     * <p>Modifying existing map entries while iterating is safe because all keys are
     * present (step 8 pre-populates every boat), so no structural changes occur.
     */
    private static int propagateViaRaces(
        Map<String, ReferenceFactors> brf,
        Map<String, List<DivisionEntry>> boatDivIndex,
        int generation)
    {
        int newCount = 0;

        for (Map.Entry<String, ReferenceFactors> e : brf.entrySet())
        {
            String boatId = e.getKey();
            ReferenceFactors current = e.getValue();

            boolean needSpin    = current.spin()    == null;
            boolean needNonSpin = current.nonSpin() == null;
            if (!needSpin && !needNonSpin) continue;

            List<DivisionEntry> entries = boatDivIndex.getOrDefault(boatId, List.of());
            if (entries.isEmpty()) continue;

            // Accumulate per-division implied factors across all relevant race divisions
            List<Factor> impliedSpin    = needSpin    ? new ArrayList<>() : null;
            List<Factor> impliedNonSpin = needNonSpin ? new ArrayList<>() : null;

            for (DivisionEntry entry : entries)
            {
                boolean isNS = entry.nonSpinnaker();
                if ( isNS && !needNonSpin) continue;
                if (!isNS && !needSpin)    continue;

                long myNanos = entry.elapsed().toNanos();
                if (myNanos <= 0) continue;

                // Implied factor per reference co-finisher of the same variant
                List<Factor> divImplied = new ArrayList<>();
                for (Finisher peer : entry.peers())
                {
                    if (peer.nonSpinnaker() != isNS) continue;
                    ReferenceFactors peerFactors = brf.get(peer.boatId());
                    if (peerFactors == null) continue;

                    Factor peerFactor = isNS ? peerFactors.nonSpin() : peerFactors.spin();
                    if (peerFactor == null || peerFactor.weight() <= 0) continue;

                    long peerNanos = peer.elapsedTime().toNanos();
                    if (peerNanos <= 0) continue;

                    double implied = peerFactor.value() * peerNanos / (double) myNanos;
                    if (implied <= 0 || implied > 3.0) continue;

                    divImplied.add(new Factor(implied, peerFactor.weight()));
                }

                if (divImplied.isEmpty()) continue;
                // Per pipeline spec: per-race weight = total reference weight of co-finishers.
                // Use a simple linear weighted mean here — no variance penalty within one race.
                // The cross-race logAggregate below applies the variance penalty where it belongs.
                // Scale the division weight by the race age weight before cross-race aggregation.
                Factor divFactor = linearMeanAggregate(divImplied);
                if (divFactor != null)
                {
                    Factor aged = new Factor(divFactor.value(), divFactor.weight() * entry.ageWeight());
                    if (isNS) impliedNonSpin.add(aged);
                    else      impliedSpin.add(aged);
                }
            }

            Factor newSpin = null, newNonSpin = null;
            if (needSpin    && !impliedSpin.isEmpty())
                newSpin    = logAggregate(impliedSpin,    PROPAGATION_FACTOR_WEIGHT);
            if (needNonSpin && !impliedNonSpin.isEmpty())
                newNonSpin = logAggregate(impliedNonSpin, PROPAGATION_FACTOR_WEIGHT);

            if (newSpin != null || newNonSpin != null)
            {
                brf.put(boatId, new ReferenceFactors(
                    newSpin    != null ? newSpin    : current.spin(),
                    newNonSpin != null ? newNonSpin : current.nonSpin(),
                    current.twoHanded(),
                    newSpin    != null ? generation : current.spinGeneration(),
                    newNonSpin != null ? generation : current.nonSpinGeneration(),
                    current.twoHandedGeneration()
                ));
                newCount++;
            }
        }
        return newCount;
    }

    // ==========================================================================
    // Step 11: design fallback
    // ==========================================================================

    /**
     * Assigns the design-level reference factor to any boat that still has a null
     * variant and whose design has a computed factor. Returns the count updated.
     */
    private static int applyDesignFallback(
        Map<String, ReferenceFactors> brf,
        Map<String, ReferenceFactors> designFactors,
        Map<String, Boat> boats,
        int generation)
    {
        int newCount = 0;
        for (Map.Entry<String, ReferenceFactors> e : brf.entrySet())
        {
            String boatId = e.getKey();
            ReferenceFactors current = e.getValue();
            if (current.spin() != null && current.nonSpin() != null && current.twoHanded() != null)
                continue;

            Boat boat = boats.get(boatId);
            if (boat == null || boat.designId() == null) continue;

            ReferenceFactors df = designFactors.get(boat.designId());
            if (df == null) continue;

            Factor newSpin    = current.spin()      != null ? current.spin()      : df.spin();
            Factor newNonSpin = current.nonSpin()   != null ? current.nonSpin()   : df.nonSpin();
            Factor newTwoH    = current.twoHanded() != null ? current.twoHanded() : df.twoHanded();

            boolean changed = newSpin != current.spin()
                || newNonSpin != current.nonSpin()
                || newTwoH != current.twoHanded();

            if (changed)
            {
                brf.put(boatId, new ReferenceFactors(
                    newSpin, newNonSpin, newTwoH,
                    newSpin    != current.spin()      ? generation : current.spinGeneration(),
                    newNonSpin != current.nonSpin()   ? generation : current.nonSpinGeneration(),
                    newTwoH    != current.twoHanded() ? generation : current.twoHandedGeneration()
                ));
                newCount++;
            }
        }
        return newCount;
    }

    // ==========================================================================
    // Step 12: cross-variant fill
    // ==========================================================================

    /**
     * For any boat that holds one variant's IRC-equivalent RF but is missing another,
     * derives the missing variant by traversing the cross-variant edges of the conversion
     * graph starting from the boat's existing IRC node.  The conversion uses the boat's
     * existing factor value as the starting point and its weight as the path seed weight.
     *
     * <p>Priority: nonSpin → spin first (if spin is missing), then spin → nonSpin (if
     * nonSpin is missing), then spin → twoHanded (using the spin factor that may have
     * just been derived).  Any weight above {@link #MIN_PATH_WEIGHT} is accepted so that
     * boats are not left without a spin RF merely because the conversion path has low
     * but non-zero confidence.
     *
     * @return number of boats updated
     */
    private static int fillMissingVariantsFromExisting(
        Map<String, ReferenceFactors> brf,
        ConversionGraph graph,
        int currentYear,
        int generation)
    {
        ConversionNode ircSpin  = new ConversionNode("IRC", currentYear, false, false);
        ConversionNode ircNS    = new ConversionNode("IRC", currentYear, true,  false);
        ConversionNode ircTwoH  = new ConversionNode("IRC", currentYear, false, true);

        int newCount = 0;
        for (Map.Entry<String, ReferenceFactors> e : brf.entrySet())
        {
            ReferenceFactors cur = e.getValue();
            Factor spin    = cur.spin();
            Factor nonSpin = cur.nonSpin();
            Factor twoH    = cur.twoHanded();
            boolean changed = false;

            // nonSpin → spin
            if (spin == null && nonSpin != null)
            {
                Factor derived = convertViaGraph(nonSpin, ircNS, ircSpin, graph);
                if (derived != null) { spin = derived; changed = true; }
            }
            // spin → nonSpin
            if (nonSpin == null && spin != null)
            {
                Factor derived = convertViaGraph(spin, ircSpin, ircNS, graph);
                if (derived != null) { nonSpin = derived; changed = true; }
            }
            // spin → twoHanded (spin may have just been derived above)
            if (twoH == null && spin != null)
            {
                Factor derived = convertViaGraph(spin, ircSpin, ircTwoH, graph);
                if (derived != null) { twoH = derived; changed = true; }
            }

            if (changed)
            {
                brf.put(e.getKey(), new ReferenceFactors(
                    spin, nonSpin, twoH,
                    spin    != cur.spin()      ? generation : cur.spinGeneration(),
                    nonSpin != cur.nonSpin()   ? generation : cur.nonSpinGeneration(),
                    twoH    != cur.twoHanded() ? generation : cur.twoHandedGeneration()
                ));
                newCount++;
            }
        }
        return newCount;
    }

    /**
     * Converts {@code source} factor to the {@code target} variant node using DFS over
     * cross-variant edges.
     *
     * <p>The DFS is seeded with {@code max(source.weight(), MIN_PATH_WEIGHT)} so that graph
     * edges are explored even when the source confidence is zero.  The final aggregated
     * factor's weight is then scaled back by {@code source.weight()}, preserving the
     * semantics that a zero-confidence source produces a zero-confidence derived factor.
     *
     * @return the best aggregated factor, or null if no path was found
     */
    private static Factor convertViaGraph(
        Factor source, ConversionNode from, ConversionNode target, ConversionGraph graph)
    {
        List<Factor> out = new ArrayList<>();
        // Use at least MIN_PATH_WEIGHT as the seed so the DFS explores conversion edges
        // even when source.weight() == 0; the result is scaled back by source.weight() below.
        double seedWeight = Math.max(source.weight(), MIN_PATH_WEIGHT);
        dfsAllPaths(from, source.value(), seedWeight, target,
            graph, true, new HashSet<>(), 0, out);
        if (out.isEmpty())
            return null;
        Factor pathFactor = Factor.aggregate(out.toArray(new Factor[0]));
        // Scale by source confidence: a zero-weight source produces a zero-weight derived factor.
        return new Factor(pathFactor.value(), pathFactor.weight() * source.weight());
    }

    // ==========================================================================
    // Log-space aggregation
    // ==========================================================================

    /**
     * Weighted mean in log space with a variance-based confidence penalty.
     * Equivalent to the pipeline specification:
     * <pre>log(result.value) = Σ(wᵢ × log(vᵢ)) / Σwᵢ</pre>
     * with combined weight penalised by spread:
     * <pre>combinedWeight = meanInputWeight / (1 + logVariance / σ₀²)</pre>
     * where σ₀ = log(1 + {@link Factor#SIGMA_0}) ≈ 0.0488.
     *
     * @param factors   one or more factor estimates; zero-weight entries are skipped
     * @param maxWeight ceiling applied to the returned weight
     * @return aggregated factor, or null if all inputs have zero weight
     */
    private static Factor logAggregate(List<Factor> factors, double maxWeight)
    {
        double sumW = 0, sumWLogV = 0;
        int n = 0;
        for (Factor f : factors)
        {
            if (f.weight() > 0)
            {
                sumW     += f.weight();
                sumWLogV += f.weight() * Math.log(f.value());
                n++;
            }
        }
        if (n == 0) return null;

        double logMeanV  = sumWLogV / sumW;
        double meanValue = Math.exp(logMeanV);

        double sumWLogVar = 0;
        for (Factor f : factors)
        {
            if (f.weight() > 0)
            {
                double d = Math.log(f.value()) - logMeanV;
                sumWLogVar += f.weight() * d * d;
            }
        }
        double logVariance     = sumWLogVar / sumW;
        double sigma0Log       = Math.log(1.0 + Factor.SIGMA_0);  // ≈ 0.0488 for SIGMA_0=0.05
        double meanInputWeight = sumW / n;
        double combinedWeight  = meanInputWeight / (1.0 + logVariance / (sigma0Log * sigma0Log));
        combinedWeight = Math.min(Math.min(combinedWeight, maxWeight), 1.0);

        return new Factor(meanValue, combinedWeight);
    }

    /**
     * Simple weighted mean (linear space) of multiple implied-factor estimates from one
     * race division.  The returned weight is {@code min(Σwᵢ, 1.0)} — proportional to the
     * total reference factor weight of all contributing boats, capped at 1.
     *
     * <p>No variance penalty is applied here; that belongs only in the cross-race aggregation
     * step (see {@link #logAggregate}).
     *
     * @param factors  per-boat implied factors; zero-weight entries are skipped
     * @return aggregated factor, or null if all inputs have zero weight
     */
    private static Factor linearMeanAggregate(List<Factor> factors)
    {
        double sumW = 0, sumWV = 0;
        for (Factor f : factors)
        {
            if (f.weight() > 0)
            {
                sumW  += f.weight();
                sumWV += f.weight() * f.value();
            }
        }
        if (sumW == 0) return null;

        double meanValue   = sumWV / sumW;
        double totalWeight = Math.min(sumW, 1.0);
        return new Factor(meanValue, totalWeight);
    }

    // ==========================================================================
    // Step 8 internals
    // ==========================================================================

    /**
     * Returns the certificates to use for this boat's reference factor computation.
     *
     * <p>Prefers the most directly applicable certificates by scanning years in this order:
     * currentYear, currentYear+1, currentYear-1, currentYear-2, … currentYear-{@link #MAX_CERT_AGE_YEARS}.
     * The first year that has at least one plausible certificate is used exclusively —
     * all other years are ignored.  This ensures that a boat with, say, a 2024 IRC cert
     * is not diluted by its older 2022 or 2023 certs; the conversion graph already
     * back-adjusts year-offset certs to the current-year IRC equivalent via year-transition edges.
     *
     * @return certificates from the single best year, or an empty list if none found
     */
    private static List<Certificate> validCerts(Boat boat, int currentYear)
    {
        // Preference order: current year, one year ahead, then going back year by year
        int[] yearOrder = new int[2 + MAX_CERT_AGE_YEARS];
        yearOrder[0] = currentYear;
        yearOrder[1] = currentYear + 1;
        for (int i = 0; i < MAX_CERT_AGE_YEARS; i++)
            yearOrder[2 + i] = currentYear - 1 - i;

        for (int year : yearOrder)
        {
            List<Certificate> valid = new ArrayList<>();
            for (Certificate c : boat.certificates())
            {
                if (c.year() != year) continue;
                if (c.value() < 0.3 || c.value() > 3.0) continue;
                valid.add(c);
            }
            if (!valid.isEmpty())
                return valid;
        }
        return List.of();
    }

    /**
     * Computes the aggregated reference factor for one variant by finding all conversion
     * paths from the boat's certificates to the target IRC node for the given variant.
     *
     * @param nonSpinTarget     target is IRC non-spinnaker
     * @param twoHandedTarget   target is IRC two-handed
     * @param allowCrossVariant if true, cross-variant edges (e.g. NS→spin) are included
     * @return aggregated Factor, or null if no valid paths were found
     */
    private Factor computeVariantFactor(List<Certificate> certs,
                                               ConversionGraph graph,
                                               int currentYear,
                                               boolean nonSpinTarget,
                                               boolean twoHandedTarget,
                                               boolean allowCrossVariant)
    {
        ConversionNode target = new ConversionNode("IRC", currentYear, nonSpinTarget, twoHandedTarget);

        List<Factor> allFactors = new ArrayList<>();
        for (Certificate cert : certs)
        {
            ConversionNode start = new ConversionNode(
                cert.system(), cert.year(), cert.nonSpinnaker(), cert.twoHanded());
            double baseWeight = 1.0;
            if (cert.club()) baseWeight *= clubBaseWeight;
            if (cert.windwardLeeward()) baseWeight *= ORC_WL_BASE_WEIGHT;

            dfsAllPaths(start, cert.value(), baseWeight, target,
                graph, allowCrossVariant, new HashSet<>(), 0, allFactors);
        }

        if (allFactors.isEmpty())
            return null;
        return Factor.aggregate(allFactors.toArray(new Factor[0]));
    }

    /**
     * Depth-first search enumerating all conversion paths from {@code node} to {@code target}.
     * Each complete path produces one {@link Factor} added to {@code results}.
     *
     * @param node              current graph node
     * @param value             current predicted TCF value at this node
     * @param weight            accumulated path weight so far
     * @param target            the destination node
     * @param graph             the conversion graph
     * @param allowCrossVariant include cross-variant edges in traversal
     * @param visited           nodes visited on the current path (for cycle prevention)
     * @param depth             current recursion depth
     * @param results           accumulator for completed path factors
     */
    private static void dfsAllPaths(ConversionNode node,
                                    double value,
                                    double weight,
                                    ConversionNode target,
                                    ConversionGraph graph,
                                    boolean allowCrossVariant,
                                    Set<ConversionNode> visited,
                                    int depth,
                                    List<Factor> results)
    {
        if (node.equals(target))
        {
            results.add(new Factor(value, Math.min(weight, 1.0)));
            return;
        }

        if (depth >= MAX_DEPTH || weight < MIN_PATH_WEIGHT)
            return;

        visited.add(node);

        List<ConversionEdge> edges = allowCrossVariant
            ? graph.adjacencies(node)
            : graph.sameVariantAdjacencies(node);

        for (ConversionEdge edge : edges)
        {
            if (visited.contains(edge.to()))
                continue;

            double nextValue = edge.fit().predict(value);
            if (nextValue <= 0)
                continue;
            double nextWeight = weight * edge.fit().weight(value);

            dfsAllPaths(edge.to(), nextValue, nextWeight, target,
                graph, allowCrossVariant, visited, depth + 1, results);
        }

        visited.remove(node);
    }
}
