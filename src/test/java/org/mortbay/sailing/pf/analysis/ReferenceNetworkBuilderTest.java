package org.mortbay.sailing.pf.analysis;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration-style tests that drive {@link ReferenceNetworkBuilder#build} end-to-end
 * against a {@link DataStore} seeded with hand-crafted certs/races, exercising the
 * post-step-14 blend, clamp, and noSpinnaker collapse passes.
 */
class ReferenceNetworkBuilderTest
{
    /**
     * A boat whose design is flagged noSpinnaker must have spin == nonSpin in its
     * RF output, regardless of which divisions it raced in. The combined factor's
     * value falls between the two inputs (weighted aggregate).
     */
    @Test
    void noSpinnakerDesignCollapsesSpinAndNonSpin(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();

        store.putDesign(new Design("catrig", "Cat Rig", List.of(), List.of(), null, false, null));
        store.setDesignNoSpinnaker("catrig", true);

        // A reference fleet of three IRC-certificated peers (spin) — gives the analyser
        // a conversion graph to anchor against. Their values cluster around 1.0.
        for (int i = 1; i <= 4; i++)
        {
            store.putDesign(new Design("ref" + i, "Ref " + i, List.of(), List.of(), null, false, null));
            store.putBoat(new Boat("REF" + i + "-ref-ref" + i, "REF" + i, "Ref", "ref" + i, null,
                List.of(new Certificate("IRC", 2026, 1.0 + 0.02 * (i - 2), false, false, false, false, null, null)),
                List.of(), null, null));
        }

        // The cat-rigged boat itself races against the fleet in two divisions — one
        // labelled "spin" (because the org bundles all boats together), one labelled
        // non-spin. Same elapsed time → same implied RF either way, so the collapse
        // is a no-op on values; we test that the SLOTS end up equal.
        store.putBoat(new Boat("CAT-tinsel-catrig", "CAT", "Tinsel", "catrig", null,
            List.of(), List.of(), null, null));

        Race spinRace = new Race("club-2026-01-01-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 1, 1), 1, null,
            List.of(new Division("Spin", List.of(
                new Finisher("CAT-tinsel-catrig", Duration.ofMinutes(60), false, null),
                new Finisher("REF1-ref-ref1", Duration.ofMinutes(58), false, null),
                new Finisher("REF2-ref-ref2", Duration.ofMinutes(60), false, null),
                new Finisher("REF3-ref-ref3", Duration.ofMinutes(62), false, null),
                new Finisher("REF4-ref-ref4", Duration.ofMinutes(64), false, null)
            ))), null, null, null);
        Race nsRace = new Race("club-2026-01-08-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 1, 8), 1, null,
            List.of(new Division("NonSpin", List.of(
                new Finisher("CAT-tinsel-catrig", Duration.ofMinutes(60), true, null),
                new Finisher("REF1-ref-ref1", Duration.ofMinutes(58), true, null),
                new Finisher("REF2-ref-ref2", Duration.ofMinutes(60), true, null),
                new Finisher("REF3-ref-ref3", Duration.ofMinutes(62), true, null),
                new Finisher("REF4-ref-ref4", Duration.ofMinutes(64), true, null)
            ))), null, null, null);
        store.putRace(spinRace);
        store.putRace(nsRace);

        ReferenceNetworkBuilder.BuildResult built =
            new ReferenceNetworkBuilder().build(store, 2026);

        ReferenceFactors cat = built.boatFactors().get("CAT-tinsel-catrig");
        assertNotNull(cat, "cat-rigged boat should receive a reference factor");
        assertNotNull(cat.spin());
        assertNotNull(cat.nonSpin());
        assertEquals(cat.spin().value(), cat.nonSpin().value(), 1e-9,
            "noSpinnaker collapse: spin.value should equal nonSpin.value");
        assertEquals(cat.spin().weight(), cat.nonSpin().weight(), 1e-9,
            "noSpinnaker collapse: spin.weight should equal nonSpin.weight");
    }

    /**
     * Monotonicity clamp: after all blending, the spin factor must be at least as large as
     * the nonSpin and twoH factors. We synthesise an inverted boat by hand and run the clamp
     * helper through the public {@code build} pathway with a boat whose nonSpin RF would
     * otherwise exceed spin via race propagation. Verifies the strict invariant.
     */
    @Test
    void monotonicityClampEnforcesSpinAtLeastNonSpinAndTwoH(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();

        // Three reference boats with strong IRC certs anchor the conversion graph.
        for (int i = 1; i <= 4; i++)
        {
            store.putDesign(new Design("ref" + i, "Ref " + i, List.of(), List.of(), null, false, null));
            store.putBoat(new Boat("REF" + i + "-ref-ref" + i, "REF" + i, "Ref", "ref" + i, null,
                List.of(new Certificate("IRC", 2026, 1.0 + 0.05 * (i - 2), false, false, false, false, null, null)),
                List.of(), null, null));
        }

        // A target boat with no certs sails ONE non-spin race against the slowest peer
        // (REF1 with TCF 0.95) — implied RF will be ~0.95×(58/60)=0.918. It also sails ONE
        // spin race against the FASTEST peer (REF4 with TCF 1.10) where it finishes very
        // slowly — implied spin RF will be much lower than 0.918. This creates an inversion
        // candidate.
        store.putDesign(new Design("oddhull", "Odd Hull", List.of(), List.of(), null, false, null));
        store.putBoat(new Boat("ODD-odd-oddhull", "ODD", "Odd", "oddhull", null,
            List.of(), List.of(), null, null));

        Race spinRace = new Race("club-2026-02-01-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 2, 1), 1, null,
            List.of(new Division("Spin", List.of(
                new Finisher("ODD-odd-oddhull", Duration.ofMinutes(100), false, null),
                new Finisher("REF4-ref-ref4", Duration.ofMinutes(60), false, null)
            ))), null, null, null);
        Race nsRace = new Race("club-2026-02-08-0001", "club", List.of("club/s"),
            LocalDate.of(2026, 2, 8), 1, null,
            List.of(new Division("NonSpin", List.of(
                new Finisher("ODD-odd-oddhull", Duration.ofMinutes(58), true, null),
                new Finisher("REF1-ref-ref1", Duration.ofMinutes(60), true, null)
            ))), null, null, null);
        store.putRace(spinRace);
        store.putRace(nsRace);

        ReferenceNetworkBuilder.BuildResult built =
            new ReferenceNetworkBuilder().build(store, 2026);

        ReferenceFactors odd = built.boatFactors().get("ODD-odd-oddhull");
        assertNotNull(odd);
        if (odd.spin() != null && odd.nonSpin() != null)
            assertTrue(odd.spin().value() >= odd.nonSpin().value() - 1e-9,
                "post-clamp invariant: spin >= nonSpin (got spin=" + odd.spin().value()
                    + ", nonSpin=" + odd.nonSpin().value() + ")");
        if (odd.spin() != null && odd.twoHanded() != null)
            assertTrue(odd.spin().value() >= odd.twoHanded().value() - 1e-9,
                "post-clamp invariant: spin >= twoHanded (got spin=" + odd.spin().value()
                    + ", twoH=" + odd.twoHanded().value() + ")");
    }

    /**
     * The combined ({@code Factor.aggregate}) operation underlying both the noSpinnaker
     * collapse and the cross-variant blend has the property we rely on: combining two factors
     * with the same value yields the same value with higher confidence (weight). Sanity-check
     * via {@link Factor#aggregate} directly so a regression in that primitive can't silently
     * break the pipeline.
     */
    @Test
    void aggregateOfEqualValuesPreservesValueAndIncreasesWeight()
    {
        Factor f = Factor.aggregate(new Factor(1.05, 0.5), new Factor(1.05, 0.5));
        assertEquals(1.05, f.value(), 1e-9, "equal-value aggregate preserves value");
        assertTrue(f.weight() > 0.5, "equal-value aggregate increases pooled weight");
    }
}
