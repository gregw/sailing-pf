package org.mortbay.sailing.pf.data;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Club#hasCurrentFeed}: a club is live in a year with a SailSys club entry, a TopYacht
 * URL naming the year, or a race that year from a feed the club still has configured; otherwise
 * its data is historic only.
 */
class ClubCurrentFeedTest
{
    private static Club club(List<TopYachtGroup> topyacht, List<SailSysEvent> sailsys)
    {
        return new Club("example.com.au", "EX", null, null, false, null, List.of(),
            topyacht, sailsys, List.of(), null);
    }

    private static List<TopYachtGroup> topyacht(String... urls)
    {
        return List.of(new TopYachtGroup("ex", null, List.of(urls)));
    }

    @Test
    void noFeedsIsHistoric()
    {
        assertFalse(club(null, null).hasCurrentFeed(2026, false, false));
        assertFalse(club(List.of(), List.of()).hasCurrentFeed(2026, false, false));
    }

    @Test
    void sailSysClubEntryIsCurrent()
    {
        assertTrue(club(null, List.of(SailSysEvent.ofClub(41, "Example"))).hasCurrentFeed(2026, false, false));
    }

    @Test
    void sailSysSeriesEntryAloneIsHistoric()
    {
        SailSysEvent series = new SailSysEvent("series-3386", null, null, 3386);
        assertFalse(club(null, List.of(series)).hasCurrentFeed(2026, false, false));
    }

    @Test
    void topYachtUrlNamingYearIsCurrent()
    {
        assertTrue(club(topyacht("https://topyacht.net.au/results/ex/2026/club/index.htm"), null)
            .hasCurrentFeed(2026, false, false));
        assertTrue(club(topyacht("https://topyacht.net.au/results/ex/2025-2026_keel_boat_club_series/"), null)
            .hasCurrentFeed(2026, false, false));
        assertTrue(club(topyacht("https://www.orcv.org.au/results/2025-26/index.htm"), null)
            .hasCurrentFeed(2026, false, false));
    }

    @Test
    void topYachtUrlForPastYearsIsHistoric()
    {
        assertFalse(club(topyacht("https://topyacht.net.au/results/ex/2024/club/index.htm",
                "https://topyacht.net.au/results/ex/2025/club/index.htm"), null)
            .hasCurrentFeed(2026, false, false));
    }

    @Test
    void yearInsideLongerNumberDoesNotCount()
    {
        assertFalse(club(topyacht("https://topyacht.net.au/results/ex/120265/index.htm"), null)
            .hasCurrentFeed(2026, false, false));
        assertFalse(club(topyacht("https://www.orcv.org.au/results/2024-25/index.htm"), null)
            .hasCurrentFeed(2026, false, false));
    }

    @Test
    void feedRaceThisYearIsCurrent()
    {
        assertTrue(club(topyacht("https://topyacht.net.au/results/ex/2025/club/index.htm"), null)
            .hasCurrentFeed(2026, false, true));
        SailSysEvent series = new SailSysEvent("series-3386", null, null, 3386);
        assertTrue(club(null, List.of(series)).hasCurrentFeed(2026, true, false));
    }

    @Test
    void feedRaceThisYearWithoutConfiguredFeedIsHistoric()
    {
        assertFalse(club(null, null).hasCurrentFeed(2026, true, true));
        assertFalse(club(List.of(), List.of()).hasCurrentFeed(2026, true, true));
    }

    @Test
    void raceFromOtherFeedDoesNotVouchForConfiguredFeed()
    {
        // e.g. SASC: an old TopYacht URL, with this year's races from the old SailSys platform scan
        assertFalse(club(topyacht("https://www.topyacht.net.au/results/sasc/2023/club/index.htm"), null)
            .hasCurrentFeed(2026, true, false));
        SailSysEvent series = new SailSysEvent("series-3386", null, null, 3386);
        assertFalse(club(null, List.of(series)).hasCurrentFeed(2026, false, true));
    }
}
