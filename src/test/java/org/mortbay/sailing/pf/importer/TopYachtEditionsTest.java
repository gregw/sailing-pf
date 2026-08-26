package org.mortbay.sailing.pf.importer;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A merged event is normally an annual regatta whose index URLs span years. Its races must
 * split into one series per running — races more than six months apart are different events,
 * not one series spanning years.
 */
class TopYachtEditionsTest
{
    @Test
    void racesWithinARegattaWeekAreOneEdition()
    {
        List<List<LocalDate>> clusters = TopYachtEditions.cluster(List.of(
            LocalDate.of(2025, 8, 29), LocalDate.of(2025, 8, 30),
            LocalDate.of(2025, 9, 1), LocalDate.of(2025, 9, 3)));

        assertEquals(1, clusters.size());
        assertEquals(4, clusters.getFirst().size());
    }

    @Test
    void successiveYearsOfARegattaAreSeparateEditions()
    {
        List<List<LocalDate>> clusters = TopYachtEditions.cluster(List.of(
            LocalDate.of(2022, 9, 2), LocalDate.of(2022, 9, 7),
            LocalDate.of(2023, 9, 1), LocalDate.of(2023, 9, 6),
            LocalDate.of(2025, 8, 29), LocalDate.of(2025, 9, 3)));

        assertEquals(3, clusters.size(), () -> clusters.toString());
        assertEquals(List.of("2022", "2023", "2025"),
            clusters.stream().map(c -> TopYachtEditions.label(c.getFirst())).toList());
    }

    @Test
    void aRegattaSpanningNewYearStaysOneEdition()
    {
        List<List<LocalDate>> clusters = TopYachtEditions.cluster(List.of(
            LocalDate.of(2024, 12, 28), LocalDate.of(2025, 1, 3)));

        assertEquals(1, clusters.size());
        assertEquals("2024", TopYachtEditions.label(clusters.getFirst().getFirst()));
    }

    @Test
    void sixMonthsIsTheBoundary()
    {
        LocalDate base = LocalDate.of(2025, 1, 1);
        assertTrue(TopYachtEditions.sameEdition(base, base.plusMonths(6)), "exactly six months joins");
        assertFalse(TopYachtEditions.sameEdition(base, base.plusMonths(6).plusDays(1)),
            "a day beyond six months splits");
        assertTrue(TopYachtEditions.sameEdition(base.plusMonths(6), base), "order does not matter");
    }

    @Test
    void severalDivisionsRacingOnOneDayStayInOneEdition()
    {
        LocalDate day = LocalDate.of(2025, 8, 29);
        List<List<LocalDate>> clusters = TopYachtEditions.cluster(List.of(day, day, day, day, day));
        assertEquals(1, clusters.size());
        assertEquals(5, clusters.getFirst().size(), "duplicate dates are preserved");
    }

    @Test
    void editionSeriesIdsAreRecognisedAndDivisionSeriesAreNot()
    {
        assertTrue(TopYachtEditions.isEditionSeriesId("tyc.com.au/mirw-2025", "tyc.com.au", "mirw"));
        assertFalse(TopYachtEditions.isEditionSeriesId(
                "tyc.com.au/mirw-multihulls", "tyc.com.au", "mirw"),
            "a division series must not be mistaken for an edition");
        assertFalse(TopYachtEditions.isEditionSeriesId("tyc.com.au/mirw", "tyc.com.au", "mirw"));
        assertFalse(TopYachtEditions.isEditionSeriesId(
            "other.com.au/mirw-2025", "tyc.com.au", "mirw"), "club must match");
    }

    @Test
    void editionNameCarriesTheYear()
    {
        assertEquals("Magnetic Island Race Week 2025", TopYachtEditions.editionName(
            "Magnetic Island Race Week", "tyc.com.au/mirw-2025"));
    }

    @Test
    void clusterOfNothingIsEmpty()
    {
        assertTrue(TopYachtEditions.cluster(List.of()).isEmpty());
        assertTrue(TopYachtEditions.cluster(java.util.Arrays.asList((LocalDate)null)).isEmpty());
    }
}
