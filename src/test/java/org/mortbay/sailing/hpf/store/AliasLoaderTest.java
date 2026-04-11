package org.mortbay.sailing.hpf.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AliasLoaderTest
{
    @Test
    void loadsSeedFromClasspath()
    {
        // aliases.yaml is on the classpath under src/main/resources
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertNotNull(seed);
    }

    @Test
    void designAliasResolvesToCanonicalId()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        // "Sydney 36 OD" normalises to "sydney36od" → should resolve to "sydney36cr"
        assertEquals("sydney36cr", seed.resolveDesignAlias("sydney36od"));
    }

    @Test
    void unknownDesignAliasReturnsNull()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertNull(seed.resolveDesignAlias("unknowndesign99"));
    }

    @Test
    void designCanonicalNameReturned()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertEquals("Sydney 36 CR", seed.designCanonicalName("sydney36cr"));
    }

    @Test
    void unknownDesignCanonicalNameReturnsNull()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertNull(seed.designCanonicalName("notinseednever"));
    }

    @Test
    void boatAliasesReturnedForKnownSailNumber()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        // MYC7 is in aliases.yaml with aliases "1060" and "TenSixty"
        List<String> aliases = seed.boatAliases("MYC7");
        assertFalse(aliases.isEmpty());
        assertTrue(aliases.contains("1060"));
        assertTrue(aliases.contains("TenSixty"));
    }

    @Test
    void unknownSailNumberReturnsEmptyList()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertTrue(seed.boatAliases("NOSUCHSAIL999").isEmpty());
    }

    @Test
    void boatCanonicalNameReturned()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertEquals("Day Dreaming", seed.boatCanonicalName("MYC7"));
    }

    @Test
    void unknownBoatCanonicalNameReturnsNull()
    {
        AliasLoader.Aliases seed = AliasLoader.load(Path.of("nonexistent"));
        assertNull(seed.boatCanonicalName("NOSUCHSAIL999"));
    }

    @Test
    void missingFileReturnsEmptySeedWithoutThrowing()
    {
        // The EMPTY sentinel should return nulls/empty lists without throwing
        AliasLoader.Aliases empty = AliasLoader.Aliases.EMPTY;
        assertNull(empty.resolveDesignAlias("anything"));
        assertNull(empty.designCanonicalName("anything"));
        assertTrue(empty.boatAliases("anything").isEmpty());
        assertNull(empty.boatCanonicalName("anything"));
    }

    @Test
    void appendDesignMergeAliasesSetsCanonicalName(@TempDir Path tempDir) throws Exception
    {
        AliasLoader.appendDesignMergeAliases(tempDir, "mydesign", "My Design Name", List.of("Alt Name"));

        AliasLoader.Aliases seed = AliasLoader.load(tempDir);
        assertEquals("My Design Name", seed.designCanonicalName("mydesign"));
        assertEquals("mydesign", seed.resolveDesignAlias("altname"));
    }

    @Test
    void appendDesignMergeAliasesDoesNotOverwriteExistingCanonicalName(@TempDir Path tempDir) throws Exception
    {
        AliasLoader.appendDesignMergeAliases(tempDir, "mydesign", "First Name", List.of());
        AliasLoader.appendDesignMergeAliases(tempDir, "mydesign", "Second Name", List.of());

        AliasLoader.Aliases seed = AliasLoader.load(tempDir);
        assertEquals("First Name", seed.designCanonicalName("mydesign"));
    }
}
