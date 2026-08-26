package org.mortbay.sailing.pf.importer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Discovery — turning an opted-in club or series into the race IDs worth fetching — tested
 * against real captured responses rather than the network.
 * <p>
 * Fixtures were taken from a browser session on app.sailsys.com.au for Townsville Yacht Club
 * (SailSys club 41) and its 2024 Racing Division (series 3386). The series fixture has its
 * pointscore trimmed: discovery never reads it, and it accounted for most of the 500 KB.
 */
class SailSysDiscoveryTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String fixture(String name) throws IOException
    {
        try (InputStream in = getClass().getResourceAsStream("/sailsys/" + name))
        {
            assertNotNull(in, () -> "missing fixture /sailsys/" + name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private SailSysImporter importer()
    {
        return new SailSysImporter(null, null);
    }

    @Test
    void clubProfileYieldsItsCurrentSeriesIds() throws IOException
    {
        List<Integer> ids = importer().parseClubSeriesIds(fixture("club-41-profile.json"));

        // Five upcoming, plus the Knobel Cup (5480) which has results but no next race and
        // so appears only in eventsWithEntrantsOrResults. First-seen order across both lists.
        assertEquals(List.of(5478, 5479, 5481, 5483, 5456, 5480), ids);
    }

    /**
     * The profile carries the same series in both {@code nextEvents} and
     * {@code eventsWithEntrantsOrResults}; each must appear once.
     */
    @Test
    void clubProfileSeriesIdsAreDeduplicatedAcrossBothLists() throws IOException
    {
        List<Integer> ids = importer().parseClubSeriesIds(fixture("club-41-profile.json"));

        assertEquals(ids.size(), new java.util.HashSet<>(ids).size(), () -> "duplicates in " + ids);
        assertTrue(ids.contains(5480),
            () -> "a series with results but no next race must still be found: " + ids);
    }

    @Test
    void clubProfileWithNoSeriesYieldsNothing() throws IOException
    {
        assertEquals(List.of(), importer().parseClubSeriesIds(
            "{\"data\":{\"nextEvents\":[],\"eventsWithEntrantsOrResults\":null}}"));
        assertEquals(List.of(), importer().parseClubSeriesIds("{\"data\":null}"));
    }

    @Test
    void seriesListingYieldsItsRaces() throws IOException
    {
        SailSysImporter.SeriesRacesResponse response = MAPPER.readValue(
            fixture("series-3386-display-races.json"), SailSysImporter.SeriesRacesResponse.class);

        assertEquals("2024 Racing Division", response.data.name);
        assertEquals("TYC", response.data.club.shortName);
        assertEquals(21, response.data.races.size());

        SailSysImporter.SeriesRace first = response.data.races.get(response.data.races.size() - 1);
        assertEquals(23617, first.id);
        assertEquals(1, first.number);
        assertEquals(LocalDate.of(2024, 3, 2), SailSysImporter.peekDate(first.dateTime));
    }

    /**
     * The series listing is metadata only — every race has an empty competitor list, which is
     * why elapsed times still have to come from the per-race endpoint.
     */
    @Test
    void seriesListingCarriesNoResults() throws IOException
    {
        String json = fixture("series-3386-display-races.json");
        assertTrue(json.contains("\"competitors\""), "the field exists…");
        SailSysImporter.SeriesRacesResponse response =
            MAPPER.readValue(json, SailSysImporter.SeriesRacesResponse.class);
        // …but nothing in the parsed race carries a time, so discovery cannot short-circuit
        for (SailSysImporter.SeriesRace race : response.data.races)
            assertNotNull(race.id, "every race must expose an ID to fetch results with");
    }

    @Test
    void peekDateReadsTheLeadingDate()
    {
        assertEquals(LocalDate.of(2024, 12, 3), SailSysImporter.peekDate("2024-12-03T00:00:00.000"));
        assertNull(SailSysImporter.peekDate(null));
        assertNull(SailSysImporter.peekDate("garbage"));
        assertNull(SailSysImporter.peekDate("2024-13-45T00:00:00.000"));
    }
}
