package org.mortbay.sailing.pf.importer;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.data.TopYachtGroup;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The prefix must identify the <em>event</em> and stay the same from season to season —
 * a prefix that drifts with the year would stop a regatta's races from grouping together
 * and would churn their IDs on every new season's import. Every case below is a real URL
 * shape taken from {@code clubs.yaml}.
 */
class TopYachtPrefixTest
{
    @Test
    void deriveTakesTheEventCodeAfterTheYear()
    {
        assertEquals("mirw", TopYachtPrefix.derive(
            "https://tes.topyacht.net.au/results/2025/mirw/index.htm", "TYC"));
        assertEquals("hirw", TopYachtPrefix.derive(
            "https://www.topyacht.net.au/results/2025/hirw/index.htm", "HIYC"));
    }

    @Test
    void deriveJoinsClubAndEventSegments()
    {
        assertEquals("cycsa-club", TopYachtPrefix.derive(
            "https://topyacht.net.au/results/cycsa/2020/club/index.htm", "CYCSA"));
        assertEquals("plyc-lincolnweek", TopYachtPrefix.derive(
            "https://topyacht.net.au/results/plyc/lincolnweek/2024/", "PLYC"));
    }

    @Test
    void derivedPrefixIsStableAcrossSeasons()
    {
        // Trailing two-digit season inside the code
        assertEquals(TopYachtPrefix.derive("https://www.topyacht.net.au/results/fsc/2023/kb23/", "FSC"),
            TopYachtPrefix.derive("https://www.topyacht.net.au/results/fsc/2025/kb25/", "FSC"));
        assertEquals("fsc-kb",
            TopYachtPrefix.derive("https://www.topyacht.net.au/results/fsc/2026/kb26/", "FSC"));

        // Trailing four-digit year inside the code
        assertEquals("gryc-club-winter", TopYachtPrefix.derive(
            "https://www.topyacht.net.au/results/gryc/2020/club/Winter2020/series.htm", "GRYC"));
        assertEquals("gryc-club-winter", TopYachtPrefix.derive(
            "https://www.topyacht.net.au/results/gryc/2025/club/Winter2025/series.htm", "GRYC"));

        // Season range inside the code, and a leading year
        assertEquals("bwps", TopYachtPrefix.derive(
            "https://cyca.com.au/results/2019/bwps_1920/index.htm", "CYCA"));
        assertEquals("club-series", TopYachtPrefix.derive(
            "https://gfs.org.au/handicapper/results/2020/2020_club_series/index.htm", "GFS"));
        assertEquals("rgyc-keel-boat-club-series", TopYachtPrefix.derive(
            "https://topyacht.net.au/results/rgyc/2020-2021_keel_boat_club_series/", "RGYC"));

        // Event code carrying only a season suffix
        assertEquals("bruny", TopYachtPrefix.derive("http://results.ryct.org.au/bruny20/", "RYCT"));
        assertEquals("bruny", TopYachtPrefix.derive("http://results.ryct.org.au/bruny23/", "RYCT"));
    }

    @Test
    void classNumbersSurviveWhenTheyAreNotSeasons()
    {
        // 24 as a season would be stripped; as a class number it must not be
        assertEquals("j24-nationals", TopYachtPrefix.derive(
            "https://www.topyacht.net.au/results/2024/j24-nationals/index.htm", "XYZ"));
    }

    @Test
    void deriveFallsBackToShortNameWhenThePathHasOnlyASeason()
    {
        assertEquals("orcv", TopYachtPrefix.derive("https://www.orcv.org.au/results/2024-25/", "ORCV"));
        assertEquals("rmyct", TopYachtPrefix.derive(
            "https://www.raceresults.rmyctoronto.com.au/results/2020_2021/hcwindex.htm", "RMYCT"));
        assertEquals("cycsa", TopYachtPrefix.derive("http://cycsa.com.au/results/2017/index.htm", "CYCSA"));
    }

    @Test
    void deriveFallsBackToClubWhenThereIsNoShortNameEither()
    {
        assertEquals("club", TopYachtPrefix.derive("https://example.org/results/2024-25/", null));
        assertEquals("club", TopYachtPrefix.derive("https://example.org/results/2024-25/", "  "));
    }

    @Test
    void groupMergesSeasonsOfOneEventAndSeparatesDistinctEvents()
    {
        List<TopYachtGroup> groups = TopYachtPrefix.group(List.of(
            "https://tes.topyacht.net.au/results/2022/mirw/index.htm",
            "https://tes.topyacht.net.au/results/2025/mirw/index.htm",
            "https://www.topyacht.net.au/results/2022/fos/index.htm"), "TYC");

        assertEquals(2, groups.size());
        assertEquals("mirw", groups.get(0).prefix());
        assertEquals(2, groups.get(0).urls().size());
        assertEquals("fos", groups.get(1).prefix());
        assertEquals(1, groups.get(1).urls().size());
    }

    @Test
    void groupIgnoresBlankUrlsAndPreservesOrder()
    {
        List<TopYachtGroup> groups = TopYachtPrefix.group(
            java.util.Arrays.asList("  ", null, "https://x.au/results/2025/abrw/index.htm"), "WSC");
        assertEquals(1, groups.size());
        assertEquals("abrw", groups.getFirst().prefix());
    }
}
