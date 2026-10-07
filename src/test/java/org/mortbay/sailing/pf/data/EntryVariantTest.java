package org.mortbay.sailing.pf.data;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mortbay.sailing.pf.data.EntryVariant.NON_SPIN;
import static org.mortbay.sailing.pf.data.EntryVariant.SPIN;
import static org.mortbay.sailing.pf.data.EntryVariant.TWO_HANDED;

/**
 * The series type × entry table that {@link EntryVariant#of} implements. An entry is
 * spin or NS by its own flag, or 2H by its division name.
 */
class EntryVariantTest
{
    private static final Division OPEN = new Division("Open", List.of());
    private static final Division TWO_HANDED_DIV = new Division("Two Handed", List.of());
    private static final Division NS_DIV = new Division("Non Spinnaker", List.of());

    private static Finisher finisher(boolean nonSpinnaker)
    {
        return new Finisher("boat", Duration.ofMinutes(60), nonSpinnaker, null);
    }

    private static final Finisher SPIN_ENTRY = finisher(false);
    private static final Finisher NS_ENTRY = finisher(true);

    @Test
    void unknownOrMixedSeriesKeepsEntryAndDivision()
    {
        assertEquals(SPIN, EntryVariant.of(null, OPEN, SPIN_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(null, OPEN, NS_ENTRY, false));
        assertEquals(TWO_HANDED, EntryVariant.of(null, TWO_HANDED_DIV, SPIN_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(null, NS_DIV, SPIN_ENTRY, false));
    }

    @Test
    void spinSeriesMakesSpinButKeepsTwoHanded()
    {
        assertEquals(SPIN, EntryVariant.of(SeriesType.SPIN, OPEN, SPIN_ENTRY, false));
        assertEquals(SPIN, EntryVariant.of(SeriesType.SPIN, OPEN, NS_ENTRY, false));
        assertEquals(SPIN, EntryVariant.of(SeriesType.SPIN, NS_DIV, NS_ENTRY, false));
        assertEquals(TWO_HANDED, EntryVariant.of(SeriesType.SPIN, TWO_HANDED_DIV, SPIN_ENTRY, false));
    }

    @Test
    void nsSeriesMakesEveryoneNs()
    {
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.NON_SPIN, OPEN, SPIN_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.NON_SPIN, OPEN, NS_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.NON_SPIN, TWO_HANDED_DIV, SPIN_ENTRY, false));
    }

    @Test
    void twoHandedSeriesUpgradesSpinButKeepsNs()
    {
        assertEquals(TWO_HANDED, EntryVariant.of(SeriesType.TWO_HANDED, OPEN, SPIN_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.TWO_HANDED, OPEN, NS_ENTRY, false));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.TWO_HANDED, NS_DIV, SPIN_ENTRY, false));
        assertEquals(TWO_HANDED, EntryVariant.of(SeriesType.TWO_HANDED, TWO_HANDED_DIV, SPIN_ENTRY, false));
    }

    @Test
    void noSpinDesignIsNotUpgradedBySpinOrTwoHandedSeries()
    {
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.SPIN, OPEN, NS_ENTRY, true));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.TWO_HANDED, OPEN, NS_ENTRY, true));
        assertEquals(SPIN, EntryVariant.of(SeriesType.TWO_HANDED, OPEN, SPIN_ENTRY, true));
        assertEquals(NON_SPIN, EntryVariant.of(SeriesType.NON_SPIN, OPEN, SPIN_ENTRY, true));
    }

    @Test
    void seriesTypeDefaultsFromName()
    {
        assertEquals(SeriesType.NON_SPIN, SeriesType.fromName("Wednesday Non-Spinnaker 2025/26"));
        assertEquals(SeriesType.TWO_HANDED, SeriesType.fromName("Short-handed Offshore Series"));
        assertEquals(SeriesType.TWO_HANDED, SeriesType.fromName("2HD Winter Series"));
        assertEquals(SeriesType.NON_SPIN, SeriesType.fromName("Two Handed Non Spinnaker"));
        assertEquals(SeriesType.UNKNOWN, SeriesType.fromName("Twilight Series"));
        assertEquals(SeriesType.TWO_HANDED, SeriesType.parse("2h"));
    }
}
