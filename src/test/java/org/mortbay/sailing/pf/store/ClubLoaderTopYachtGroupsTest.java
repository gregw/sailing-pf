package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.TopYachtGroup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code topyacht:} entry in clubs.yaml has two accepted shapes: the legacy plain list
 * of URLs, which is grouped automatically, and the map of prefix → {name, urls} the admin
 * editor writes. Both must load; only the map form is ever written back.
 */
class ClubLoaderTopYachtGroupsTest
{
    private Path writeYaml(Path root, String body) throws IOException
    {
        Path file = root.resolve("clubs.yaml");
        Files.writeString(file, body);
        return root;
    }

    @Test
    void legacyUrlListIsGroupedByDerivedPrefix(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  - "https://tes.topyacht.net.au/results/2022/mirw/index.htm"
                  - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
                  - "https://www.topyacht.net.au/results/2022/fos/index.htm"
            """);

        Map<String, Club> clubs = ClubLoader.load(root);
        List<TopYachtGroup> groups = clubs.get("tyc.com.au").topyachtGroups();

        assertEquals(2, groups.size());
        assertEquals("mirw", groups.get(0).prefix());
        assertEquals(2, groups.get(0).urls().size());
        assertNull(groups.get(0).name(), "a derived group has no long name until one is set");
        assertEquals("fos", groups.get(1).prefix());
    }

    @Test
    void mapFormIsReadWithNamesAndUrls(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  mirw:
                    name: "Magnetic Island Race Week"
                    urls:
                      - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
                  fos:
                    urls:
                      - "https://www.topyacht.net.au/results/2022/fos/index.htm"
            """);

        List<TopYachtGroup> groups = ClubLoader.load(root).get("tyc.com.au").topyachtGroups();

        assertEquals(2, groups.size());
        assertEquals("mirw", groups.get(0).prefix());
        assertEquals("Magnetic Island Race Week", groups.get(0).name());
        assertEquals("fos", groups.get(1).prefix());
        assertNull(groups.get(1).name());
        assertEquals("fos", groups.get(1).displayName(), "displayName falls back to the prefix");
    }

    @Test
    void shorthandMapOfPrefixToUrlListIsAccepted(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  mirw:
                    - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
            """);

        List<TopYachtGroup> groups = ClubLoader.load(root).get("tyc.com.au").topyachtGroups();
        assertEquals(1, groups.size());
        assertEquals("mirw", groups.getFirst().prefix());
        assertEquals(1, groups.getFirst().urls().size());
    }

    @Test
    void updateWritesTheMapFormAndReloads(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
            """);

        boolean changed = ClubLoader.updateClubTopyachtGroups(root, "tyc.com.au", "TYC",
            List.of(new TopYachtGroup("mirw", "Magnetic Island Race Week",
                List.of("https://tes.topyacht.net.au/results/2025/mirw/index.htm"))));
        assertTrue(changed);

        String yaml = Files.readString(root.resolve("clubs.yaml"));
        assertTrue(yaml.contains("mirw:"), yaml);
        assertTrue(yaml.contains("Magnetic Island Race Week"), yaml);

        List<TopYachtGroup> groups = ClubLoader.load(root).get("tyc.com.au").topyachtGroups();
        assertEquals(1, groups.size());
        assertEquals("Magnetic Island Race Week", groups.getFirst().name());
    }

    @Test
    void updateWithNoChangeIsANoop(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  mirw:
                    name: "Magnetic Island Race Week"
                    urls:
                      - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
            """);

        assertFalse(ClubLoader.updateClubTopyachtGroups(root, "tyc.com.au", "TYC",
            List.of(new TopYachtGroup("mirw", "Magnetic Island Race Week",
                List.of("https://tes.topyacht.net.au/results/2025/mirw/index.htm")))));
    }

    /**
     * The merge setting belongs to one event, not to the club: a regatta at a club can be
     * merged while that club's own season, configured alongside it, is not.
     */
    @Test
    void mergeSettingIsPerEventNotPerClub(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  mirw:
                    name: "Magnetic Island Race Week"
                    merge: true
                    urls:
                      - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
                  club-season:
                    urls:
                      - "https://www.topyacht.net.au/results/tyc/2025/club/index.htm"
            """);

        List<TopYachtGroup> groups = ClubLoader.load(root).get("tyc.com.au").topyachtGroups();

        assertEquals(2, groups.size());
        assertTrue(groups.get(0).mergeDivisions(), "the regatta merges its divisions");
        assertFalse(groups.get(1).mergeDivisions(), "the club season does not");
    }

    /**
     * A per-event merge flag survives a write/read round trip independently of its siblings.
     */
    @Test
    void mergeSettingRoundTripsPerEvent(@TempDir Path root) throws IOException
    {
        writeYaml(root, "clubs:\n  tyc.com.au:\n    shortName: TYC\n    state: QLD\n");

        ClubLoader.updateClubTopyachtGroups(root, "tyc.com.au", "TYC", List.of(
            new TopYachtGroup("mirw", "Magnetic Island Race Week",
                List.of("https://x/results/2025/mirw/index.htm"), true),
            new TopYachtGroup("club-season", null,
                List.of("https://x/results/tyc/2025/club/index.htm"), false)));

        String yaml = Files.readString(root.resolve("clubs.yaml"));
        assertTrue(yaml.contains("merge: true"), yaml);
        assertEquals(1, yaml.split("merge: true", -1).length - 1,
            "only the event that asked for it is flagged");

        List<TopYachtGroup> groups = ClubLoader.load(root).get("tyc.com.au").topyachtGroups();
        assertTrue(groups.get(0).mergeDivisions());
        assertFalse(groups.get(1).mergeDivisions());
    }

    @Test
    void cleanGroupsNormalisesPrefixesMergesDuplicatesAndDropsEmpties()
    {
        List<TopYachtGroup> cleaned = ClubLoader.cleanGroups(List.of(
            new TopYachtGroup("  MIRW ", "Magnetic Island Race Week", List.of("http://a/1", "http://a/1")),
            new TopYachtGroup("mirw", "ignored second name", List.of("http://a/2")),
            new TopYachtGroup("", null, List.of("http://b/1")),
            new TopYachtGroup("empty", null, List.of())));

        assertEquals(1, cleaned.size());
        assertEquals("mirw", cleaned.getFirst().prefix());
        assertEquals("Magnetic Island Race Week", cleaned.getFirst().name());
        assertEquals(List.of("http://a/1", "http://a/2"), cleaned.getFirst().urls());
    }

    // --- SailSys events ---

    @Test
    void sailsysEventsAreReadFromTheMapForm(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                sailsys:
                  club-41:
                    name: "Townsville Yacht Club"
                    club: 41
                  series-3386:
                    name: "2024 Racing Division"
                    series: 3386
            """);

        List<org.mortbay.sailing.pf.data.SailSysEvent> events =
            ClubLoader.load(root).get("tyc.com.au").sailsysEvents();

        assertEquals(2, events.size());
        assertTrue(events.get(0).isClub());
        assertEquals(41, events.get(0).clubId());
        assertEquals("Townsville Yacht Club", events.get(0).name());
        assertTrue(events.get(1).isSeries());
        assertEquals(3386, events.get(1).seriesId());
    }

    /** An entry naming neither a club nor a series would fetch nothing, so it is dropped. */
    @Test
    void sailsysEventNamingNothingIsIgnored(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                sailsys:
                  broken:
                    name: "no ids here"
            """);

        assertEquals(List.of(), ClubLoader.load(root).get("tyc.com.au").sailsysEvents());
    }

    @Test
    void sailsysEventsRoundTripThroughTheWriter(@TempDir Path root) throws IOException
    {
        writeYaml(root, "clubs:\n  tyc.com.au:\n    shortName: TYC\n    state: QLD\n");

        assertTrue(ClubLoader.updateClubSailsysEvents(root, "tyc.com.au", "TYC", List.of(
            org.mortbay.sailing.pf.data.SailSysEvent.ofClub(41, "Townsville Yacht Club"),
            org.mortbay.sailing.pf.data.SailSysEvent.ofSeries(3386, null))));

        String yaml = Files.readString(root.resolve("clubs.yaml"));
        assertTrue(yaml.contains("club-41:"), yaml);
        assertTrue(yaml.contains("series-3386:"), yaml);

        List<org.mortbay.sailing.pf.data.SailSysEvent> events =
            ClubLoader.load(root).get("tyc.com.au").sailsysEvents();
        assertEquals(2, events.size());
        assertEquals(41, events.get(0).clubId());
        assertEquals(3386, events.get(1).seriesId());
        assertNull(events.get(1).name());

        // and writing the same thing again changes nothing
        assertFalse(ClubLoader.updateClubSailsysEvents(root, "tyc.com.au", "TYC", List.of(
            org.mortbay.sailing.pf.data.SailSysEvent.ofClub(41, "Townsville Yacht Club"),
            org.mortbay.sailing.pf.data.SailSysEvent.ofSeries(3386, null))));
    }

    /** TopYacht and SailSys config coexist on one club without disturbing each other. */
    @Test
    void topyachtAndSailsysCoexistOnOneClub(@TempDir Path root) throws IOException
    {
        writeYaml(root, """
            clubs:
              tyc.com.au:
                shortName: TYC
                state: QLD
                topyacht:
                  mirw:
                    merge: true
                    urls:
                      - "https://tes.topyacht.net.au/results/2025/mirw/index.htm"
                sailsys:
                  club-41:
                    club: 41
            """);

        Club club = ClubLoader.load(root).get("tyc.com.au");
        assertEquals(1, club.topyachtGroups().size());
        assertTrue(club.topyachtGroups().getFirst().mergeDivisions());
        assertEquals(1, club.sailsysEvents().size());
        assertEquals(41, club.sailsysEvents().getFirst().clubId());
    }

    @Test
    void everyConfiguredClubYieldsAUsablePrefix()
    {
        // The real clubs.yaml on the classpath: no club may end up with a blank prefix,
        // and no two events at one club may collapse onto the same prefix.
        for (Club club : ClubLoader.load(Path.of("nonexistent")).values())
        {
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (TopYachtGroup g : club.topyachtGroups())
            {
                assertFalse(g.prefix() == null || g.prefix().isBlank(),
                    "Blank TopYacht prefix for club " + club.id());
                assertTrue(seen.add(g.prefix()),
                    "Duplicate TopYacht prefix '" + g.prefix() + "' for club " + club.id());
            }
        }
    }
}
