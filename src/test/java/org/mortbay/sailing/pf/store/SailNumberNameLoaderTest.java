package org.mortbay.sailing.pf.store;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SailNumberNameLoaderTest
{
    @Test
    void loadsSeedFromClasspath()
    {
        // aliases.yaml is on the classpath under src/main/resources
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        assertNotNull(seed);
    }

    @Test
    void designAliasResolvesToCanonicalId()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        // "Sydney 36 OD" normalises to "sydney36od" → should resolve to "sydney36cr"
        assertEquals("sydney36cr", seed.resolveDesignAlias("sydney36od"));
    }

    @Test
    void unknownDesignAliasReturnsInput()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        // Unknown aliases pass through unchanged
        assertEquals("unknowndesign99", seed.resolveDesignAlias("unknowndesign99"));
    }

    @Test
    void designCanonicalNameReturned()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        assertEquals("Sydney 36 CR", seed.designCanonicalName("sydney36cr"));
    }

    @Test
    void unknownDesignCanonicalNameReturnsNull()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        assertNull(seed.designCanonicalName("notinseednever"));
    }

    @Test
    void lookupBoatByAlternateName()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        // MYC7-daydreaming has aliases "1060" and "TenSixty"
        // lookupBoat returns the canonical sail/name pair plus display name
        Optional<Aliases.BoatMatch> result = seed.lookupBoat("MYC7", "1060");
        assertTrue(result.isPresent());
        assertEquals("MYC7", result.get().normSailNumber());
        assertEquals("daydreaming", result.get().normName());
        assertEquals("Day Dreaming", result.get().canonicalDisplayName());
    }

    @Test
    void lookupBoatByTenSixtyAlias()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        Optional<Aliases.BoatMatch> result = seed.lookupBoat("MYC7", "tensixty");
        assertTrue(result.isPresent());
        assertEquals("MYC7", result.get().normSailNumber());
        assertEquals("daydreaming", result.get().normName());
    }

    @Test
    void unknownBoatReturnsEmpty()
    {
        Aliases.Loaded seed = Aliases.load(Path.of("nonexistent"));
        assertTrue(seed.lookupBoat("NOSUCHSAIL999", "noname").isEmpty());
    }

    @Test
    void missingFileReturnsEmptySeedWithoutThrowing()
    {
        // The EMPTY sentinel should return nulls/empty lists without throwing
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        // EMPTY resolveDesignAlias passes through the input
        assertEquals("anything", empty.resolveDesignAlias("anything"));
        assertNull(empty.designCanonicalName("anything"));
        assertTrue(empty.lookupBoat("anything", "anything").isEmpty());
    }

    @Test
    void appendDesignMergeAliasesSetsCanonicalName(@TempDir Path tempDir) throws Exception
    {
        Aliases.appendDesignMergeAliases(tempDir, "mydesign", "My Design Name", List.of("Alt Name"));

        Aliases.Loaded seed = Aliases.load(tempDir);
        assertEquals("My Design Name", seed.designCanonicalName("mydesign"));
        assertEquals("mydesign", seed.resolveDesignAlias("altname"));
    }

    @Test
    void appendDesignMergeAliasesDoesNotOverwriteExistingCanonicalName(@TempDir Path tempDir) throws Exception
    {
        Aliases.appendDesignMergeAliases(tempDir, "mydesign", "First Name", List.of());
        Aliases.appendDesignMergeAliases(tempDir, "mydesign", "Second Name", List.of());

        Aliases.Loaded seed = Aliases.load(tempDir);
        assertEquals("First Name", seed.designCanonicalName("mydesign"));
    }

    // --- Implicit AUS prefix alias tests ---

    @Test
    void lookupBoatStripsAusPrefixWithNoYamlEntry()
    {
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        Optional<Aliases.BoatMatch> result = empty.lookupBoat("AUS1234", "someboat");
        assertTrue(result.isPresent());
        assertEquals("1234", result.get().normSailNumber());
        assertEquals("someboat", result.get().normName());
        assertNull(result.get().canonicalDisplayName());
    }

    @Test
    void lookupBoatStripsJausPrefixWithNoYamlEntry()
    {
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        Optional<Aliases.BoatMatch> result = empty.lookupBoat("JAUS103", "myyacht");
        assertTrue(result.isPresent());
        assertEquals("103", result.get().normSailNumber());
        assertEquals("myyacht", result.get().normName());

        result = empty.lookupBoat("0103", "myyacht");
        assertTrue(result.isPresent());
        assertEquals("103", result.get().normSailNumber());
        assertEquals("myyacht", result.get().normName());

        result = empty.lookupBoat("AUS00103", "myyacht");
        assertTrue(result.isPresent());
        assertEquals("103", result.get().normSailNumber());
        assertEquals("myyacht", result.get().normName());
    }

    @Test
    void lookupBoatNoPrefixAndNoYamlEntryReturnsEmpty()
    {
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        assertTrue(empty.lookupBoat("1234", "someboat").isEmpty());
    }

    /**
     * Regression: when the alias entry shares its canonical sail number with the alias's
     * own sail number (e.g. canonical (10001, wildoatsxi) with alias (10001, hamiltonislandwildoats)),
     * the sail-index skips that entry — so the only lookup path is the name branch. The
     * name branch must still honour AUS-prefix equivalence on the sail number, otherwise
     * an input of "AUS10001" falls through to the implicit prefix-strip with the raw name
     * and a fresh duplicate boat record gets created.
     */
    @Test
    void lookupBoatMatchesNameAliasAcrossAusPrefix(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "10001", "Wild Oats XI",
            List.of(new Aliases.SailNumberName("10001", "hamiltonislandwildoats")));

        Aliases.Loaded seed = Aliases.load(tempDir);

        // Without AUS prefix — this worked before too.
        Optional<Aliases.BoatMatch> plain = seed.lookupBoat("10001", "hamiltonislandwildoats");
        assertTrue(plain.isPresent());
        assertEquals("10001", plain.get().normSailNumber());
        assertEquals("wildoatsxi", plain.get().normName());
        assertEquals("Wild Oats XI", plain.get().canonicalDisplayName());

        // With AUS prefix — this was the broken case.
        Optional<Aliases.BoatMatch> prefixed = seed.lookupBoat("AUS10001", "hamiltonislandwildoats");
        assertTrue(prefixed.isPresent(), "AUS-prefixed sail should still resolve via the alias");
        assertEquals("10001", prefixed.get().normSailNumber());
        assertEquals("wildoatsxi", prefixed.get().normName(),
            "name must be canonicalised, not left as hamiltonislandwildoats");
        assertEquals("Wild Oats XI", prefixed.get().canonicalDisplayName());
    }

    /**
     * Symmetric case: alias stored with AUS prefix, input without it. Fix should handle
     * both directions since stripPrefix is applied to both sides of the comparison.
     */
    @Test
    void lookupBoatMatchesNameAliasAcrossAusPrefixInverted(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "10001", "Wild Oats XI",
            List.of(new Aliases.SailNumberName("AUS10001", "hamiltonislandwildoats")));

        Aliases.Loaded seed = Aliases.load(tempDir);

        Optional<Aliases.BoatMatch> plain = seed.lookupBoat("10001", "hamiltonislandwildoats");
        assertTrue(plain.isPresent());
        assertEquals("wildoatsxi", plain.get().normName());
    }

    @Test
    void lookupBoatPrefixOnlyNoDigitsDoesNotStrip()
    {
        // "AUS" alone (no trailing digit) should not be stripped
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        assertTrue(empty.lookupBoat("AUS", "someboat").isEmpty());
    }

    // --- Combinatorial expansion of partial aliases ---

    /**
     * A sail-only alias and a name-only alias under the same boat must produce three
     * derived match keys: (sailAlias, canonicalName), (canonicalSail, nameAlias), AND the
     * cross (sailAlias, nameAlias). The cross is the new behaviour.
     */
    @Test
    void sailOnlyAndNameOnlyGenerateCrossPair(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "AUS1234", "Raging Bull",
            List.of(
                new Aliases.SailNumberName("5656", null),    // sail-only
                new Aliases.SailNumberName(null, "rbull")    // name-only
            ));
        Aliases.Loaded seed = Aliases.load(tempDir);

        // (sailAlias, canonicalName)
        Optional<Aliases.BoatMatch> a = seed.lookupBoat("5656", "ragingbull");
        assertTrue(a.isPresent(), "(5656, ragingbull) should resolve");
        assertEquals("AUS1234", a.get().normSailNumber());
        assertEquals("ragingbull", a.get().normName());

        // (canonicalSail, nameAlias)
        Optional<Aliases.BoatMatch> b = seed.lookupBoat("AUS1234", "rbull");
        assertTrue(b.isPresent(), "(AUS1234, rbull) should resolve");
        assertEquals("AUS1234", b.get().normSailNumber());
        assertEquals("ragingbull", b.get().normName());

        // (sailAlias, nameAlias) — the cross, missing before the rule was added
        Optional<Aliases.BoatMatch> cross = seed.lookupBoat("5656", "rbull");
        assertTrue(cross.isPresent(), "(5656, rbull) cross should resolve");
        assertEquals("AUS1234", cross.get().normSailNumber());
        assertEquals("ragingbull", cross.get().normName());
    }

    /**
     * Two sail-only aliases (no name-only) must NOT produce sail×sail crosses — only
     * each paired with the canonical name. Asymmetric rule prevents quadratic blow-up.
     */
    @Test
    void twoSailOnlyAliasesDoNotCross(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "AUS1234", "Raging Bull",
            List.of(
                new Aliases.SailNumberName("5656", null),
                new Aliases.SailNumberName("7777", null)
            ));
        Aliases.Loaded seed = Aliases.load(tempDir);

        assertTrue(seed.lookupBoat("5656", "ragingbull").isPresent());
        assertTrue(seed.lookupBoat("7777", "ragingbull").isPresent());
        // No name-only alias was declared, so no other name should resolve via the sail aliases.
        assertTrue(seed.lookupBoat("5656", "7777").isEmpty(),
            "(5656, 7777) must not resolve — sail×sail is forbidden");
        assertTrue(seed.lookupBoat("7777", "5656").isEmpty(),
            "(7777, 5656) must not resolve — sail×sail is forbidden");
    }

    /**
     * Two name-only aliases (no sail-only) must NOT produce name×name crosses — only
     * each paired with the canonical sail.
     */
    @Test
    void twoNameOnlyAliasesDoNotCross(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "AUS1234", "Raging Bull",
            List.of(
                new Aliases.SailNumberName(null, "rbull"),
                new Aliases.SailNumberName(null, "thebull")
            ));
        Aliases.Loaded seed = Aliases.load(tempDir);

        assertTrue(seed.lookupBoat("AUS1234", "rbull").isPresent());
        assertTrue(seed.lookupBoat("AUS1234", "thebull").isPresent());
        // No sail-only alias was declared, so the names should not cross-pair with each other.
        assertTrue(seed.lookupBoat("rbull", "thebull").isEmpty(),
            "(rbull, thebull) must not resolve — name×name is forbidden");
    }

    /**
     * One sail-only + two name-only. The sail-only alias must cross with BOTH name-only
     * aliases (linear in the number of name-onlys, not quadratic in the total).
     */
    @Test
    void oneSailOnlyCrossesAllNameOnly(@TempDir Path tempDir) throws Exception
    {
        Aliases.addAliases(tempDir, "AUS1234", "Raging Bull",
            List.of(
                new Aliases.SailNumberName("5656", null),
                new Aliases.SailNumberName(null, "rbull"),
                new Aliases.SailNumberName(null, "thebull")
            ));
        Aliases.Loaded seed = Aliases.load(tempDir);

        // sail-only × canonical name
        assertTrue(seed.lookupBoat("5656", "ragingbull").isPresent());
        // sail-only × each name-only
        assertTrue(seed.lookupBoat("5656", "rbull").isPresent(), "5656 × rbull cross");
        assertTrue(seed.lookupBoat("5656", "thebull").isPresent(), "5656 × thebull cross");
        // canonical sail × each name-only
        assertTrue(seed.lookupBoat("AUS1234", "rbull").isPresent());
        assertTrue(seed.lookupBoat("AUS1234", "thebull").isPresent());
    }
}
