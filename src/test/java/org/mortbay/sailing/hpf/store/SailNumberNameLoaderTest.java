package org.mortbay.sailing.hpf.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

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
    }

    @Test
    void lookupBoatNoPrefixAndNoYamlEntryReturnsEmpty()
    {
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        assertTrue(empty.lookupBoat("1234", "someboat").isEmpty());
    }

    @Test
    void lookupBoatPrefixOnlyNoDigitsDoesNotStrip()
    {
        // "AUS" alone (no trailing digit) should not be stripped
        Aliases.Loaded empty = Aliases.Loaded.EMPTY;
        assertTrue(empty.lookupBoat("AUS", "someboat").isEmpty());
    }
}
