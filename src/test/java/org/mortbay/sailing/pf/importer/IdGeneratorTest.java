package org.mortbay.sailing.pf.importer;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IdGeneratorTest
{
    // --- stripStandardSuffixes ---

    @Test
    void stripStandardSuffixesRemovesGmFamily()
    {
        assertEquals("Foobar", IdGenerator.stripStandardSuffixes("Foobar - GM"));
        assertEquals("Foobar", IdGenerator.stripStandardSuffixes("Foobar-gm"));
        assertEquals("Pompus", IdGenerator.stripStandardSuffixes("Pompus - GGM"));
        assertEquals("Pompus", IdGenerator.stripStandardSuffixes("Pompus-GGGM"));
        assertEquals("Foobar", IdGenerator.stripStandardSuffixes("Foobar -M"));
        assertEquals("Foobar", IdGenerator.stripStandardSuffixes("Foobar - L"));
    }

    @Test
    void stripStandardSuffixesRemovesYouthSuffixes()
    {
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat -U16"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - U17"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - U18"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - Under16"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - under18"));
    }

    @Test
    void stripStandardSuffixesAllowsInternalWhitespaceInYouthSuffix()
    {
        // The actual source data carries the suffix with an internal space:
        // "Kilifi - Under 17", "Sesto Elemento (W) - Under 16" etc.
        assertEquals("Kilifi", IdGenerator.stripStandardSuffixes("Kilifi - Under 17"));
        assertEquals("Sesto Elemento (W)",
            IdGenerator.stripStandardSuffixes("Sesto Elemento (W) - Under 16"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - U 17"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - UNDER  18"));
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat-Under 16"));
    }

    @Test
    void stripStandardSuffixesIterates()
    {
        // "Boat - GM - U18" → strip "- U18" → strip "- GM" → "Boat"
        assertEquals("Boat", IdGenerator.stripStandardSuffixes("Boat - GM - U18"));
    }

    @Test
    void stripStandardSuffixesLeavesUnknownSuffix()
    {
        assertEquals("Boat - X", IdGenerator.stripStandardSuffixes("Boat - X"));
        assertEquals("Boat - 2014", IdGenerator.stripStandardSuffixes("Boat - 2014"));
        // "-ll" not in the list
        assertEquals("Boat -LL", IdGenerator.stripStandardSuffixes("Boat -LL"));
    }

    @Test
    void stripStandardSuffixesLeavesEmbeddedDashContent()
    {
        // Only the LAST dash-clause is considered; embedded dashes are kept.
        assertEquals("Wing-It", IdGenerator.stripStandardSuffixes("Wing-It"));
        assertEquals("Wing-It", IdGenerator.stripStandardSuffixes("Wing-It - GM"));
    }

    @Test
    void stripStandardSuffixesHandlesNullAndBlank()
    {
        assertEquals(null, IdGenerator.stripStandardSuffixes(null));
        assertEquals("", IdGenerator.stripStandardSuffixes(""));
        assertEquals("   ", IdGenerator.stripStandardSuffixes("   "));
    }

    // --- normaliseName (now drops standard suffixes too) ---

    @Test
    void normaliseNameDropsStandardSuffixes()
    {
        assertEquals("foobar", IdGenerator.normaliseName("Foobar - GM"));
        assertEquals("pompus", IdGenerator.normaliseName("Pompus - GGM"));
        assertEquals("boat", IdGenerator.normaliseName("Boat - U17"));
        assertEquals("foobar", IdGenerator.normaliseName("Foobar - L"));
    }

    @Test
    void normaliseNameUnchangedForRegularNames()
    {
        assertEquals("ragingbull", IdGenerator.normaliseName("Raging Bull"));
        assertEquals("thegoat", IdGenerator.normaliseName("The Goat"));
        assertEquals("stickyii", IdGenerator.normaliseName("Sticky II"));
        assertEquals("sticky2", IdGenerator.normaliseName("Sticky 2"));
    }

    // --- nameMatchKey ---

    @Test
    void nameMatchKeyCollapsesThePrefix()
    {
        assertEquals("goat", IdGenerator.nameMatchKey("Goat"));
        assertEquals("goat", IdGenerator.nameMatchKey("The Goat"));
        assertEquals("goat", IdGenerator.nameMatchKey("THE GOAT"));
    }

    @Test
    void nameMatchKeyDoesNotEatThePrefixWithoutSpace()
    {
        assertEquals("thelma", IdGenerator.nameMatchKey("Thelma"));
        assertEquals("thereby", IdGenerator.nameMatchKey("Thereby"));
    }

    @Test
    void nameMatchKeyStripsTrailingNumerals()
    {
        assertEquals("sticky", IdGenerator.nameMatchKey("Sticky"));
        assertEquals("sticky", IdGenerator.nameMatchKey("Sticky 2"));
        assertEquals("sticky", IdGenerator.nameMatchKey("Sticky II"));
        assertEquals("sticky", IdGenerator.nameMatchKey("Sticky ii"));
        assertEquals("sticky", IdGenerator.nameMatchKey("Sticky II 2"));   // iterative
        assertEquals("anna", IdGenerator.nameMatchKey("Anna 11"));
        assertEquals("anna", IdGenerator.nameMatchKey("Anna II"));
    }

    @Test
    void nameMatchKeyLeavesEmbeddedNumeralLikeRuns()
    {
        // Trailing token needs whitespace before it; embedded letters that happen to
        // look like roman digits must not be stripped.
        assertEquals("tivoli", IdGenerator.nameMatchKey("Tivoli"));
        assertEquals("pinta", IdGenerator.nameMatchKey("Pinta"));
        assertEquals("anna11", IdGenerator.nameMatchKey("Anna11"));
    }

    @Test
    void nameMatchKeyCombinesAllRules()
    {
        // strip "- GM", then strip "The ", then strip trailing "2" → "sticky"
        assertEquals("sticky", IdGenerator.nameMatchKey("The Sticky 2 - GM"));
        // strip "- L", then strip "The ", then no numeral → "goat"
        assertEquals("goat", IdGenerator.nameMatchKey("The Goat - L"));
    }

    @Test
    void nameMatchKeyEmptyForBlankOrJustArticle()
    {
        assertEquals("", IdGenerator.nameMatchKey(null));
        assertEquals("", IdGenerator.nameMatchKey(""));
        assertEquals("", IdGenerator.nameMatchKey("   "));
        // "The " by itself: strip leading "the " → "" → match key empty
        assertEquals("", IdGenerator.nameMatchKey("The "));
    }

    // --- preferredDisplayName ---

    @Test
    void preferredDisplayNameLongestWins()
    {
        assertEquals("The Goat", IdGenerator.preferredDisplayName(List.of("Goat", "The Goat")));
        assertEquals("The Goat", IdGenerator.preferredDisplayName(List.of("The Goat", "Goat")));
    }

    @Test
    void preferredDisplayNameRomanWhenNoArabic()
    {
        assertEquals("Sticky II", IdGenerator.preferredDisplayName(List.of("Sticky", "Sticky II")));
    }

    @Test
    void preferredDisplayNameArabicBeatsRoman()
    {
        // Body tie ("Sticky" vs "Sticky"); Arabic numeral wins.
        assertEquals("Sticky 2", IdGenerator.preferredDisplayName(List.of("Sticky 2", "Sticky II")));
        assertEquals("Sticky 2", IdGenerator.preferredDisplayName(List.of("Sticky II", "Sticky 2")));
    }

    @Test
    void preferredDisplayNameStripsStandardSuffixes()
    {
        assertEquals("Foobar", IdGenerator.preferredDisplayName(List.of("Foobar - GM", "Foobar")));
        assertEquals("Foobar", IdGenerator.preferredDisplayName(List.of("Foobar - GM")));
        assertEquals("Pompus", IdGenerator.preferredDisplayName(List.of("Pompus - GGM")));
    }

    @Test
    void preferredDisplayNameComplexCase()
    {
        // ["The Sticky 2 - GM", "Sticky"]: body candidates "The Sticky" (10) vs "Sticky" (6)
        // → "The Sticky"; numeral "2" observed → "The Sticky 2".
        assertEquals("The Sticky 2",
            IdGenerator.preferredDisplayName(List.of("The Sticky 2 - GM", "Sticky")));
        // Same input, normaliseName of canonical → "thesticky2".
        assertEquals("thesticky2", IdGenerator.normaliseName(
            IdGenerator.preferredDisplayName(List.of("The Sticky 2 - GM", "Sticky"))));
    }

    @Test
    void preferredDisplayNameTieBreaksInInputOrder()
    {
        // Two equal-length bodies "Goat" / "Goat" — first input wins (stability for callers
        // that put the incoming name first to prefer fresh data over stored data).
        assertEquals("Goat A", IdGenerator.preferredDisplayName(List.of("Goat A", "Goat B")));
        assertEquals("Goat B", IdGenerator.preferredDisplayName(List.of("Goat B", "Goat A")));
    }

    @Test
    void preferredDisplayNameIgnoresNullAndBlankEntries()
    {
        assertEquals("Foobar", IdGenerator.preferredDisplayName(java.util.Arrays.asList(null, "Foobar", "")));
    }
}
