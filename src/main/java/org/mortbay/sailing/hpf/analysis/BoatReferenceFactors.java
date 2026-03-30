package org.mortbay.sailing.hpf.analysis;

import org.mortbay.sailing.hpf.data.Factor;

/**
 * First-order reference factors for a single boat, one per racing variant.
 * Each factor is the aggregated IRC-equivalent TCF for the current year,
 * derived from all available certificates via empirical conversion tables.
 * <p>
 * A null factor means no valid conversion path was found for that variant.
 * <p>
 * Each variant carries its own {@code *Generation} field tracking which iteration
 * of the reference propagation algorithm (pipeline steps 9–12) produced it:
 * <ul>
 *   <li>0 — derived directly from measurement certificates (step 8)</li>
 *   <li>1 — assigned in the first propagation iteration</li>
 *   <li>n — the propagation iteration that last assigned this variant</li>
 * </ul>
 * Variants may therefore be at different generations on the same record.
 */
public record BoatReferenceFactors(
    Factor spin,              // IRC spin equivalent for currentYear; null if no path found
    Factor nonSpin,           // IRC non-spinnaker equivalent; null if no path found
    Factor twoHanded,         // IRC two-handed equivalent; null if no path found
    int spinGeneration,       // propagation generation for spin
    int nonSpinGeneration,    // propagation generation for nonSpin
    int twoHandedGeneration   // propagation generation for twoHanded
) {}
