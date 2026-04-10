package org.mortbay.sailing.hpf.store;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.hpf.data.Club;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ClubLoaderTest
{
    @Test
    void loadsWithoutError()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        assertFalse(clubs.isEmpty(), "clubs.yaml should produce at least one entry");
    }

    @Test
    void placeholderDomainsAreExcluded()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        clubs.keySet().forEach(domain ->
            assertFalse(domain.startsWith("unknown.domain."),
                "Placeholder domain should not appear in loaded clubs: " + domain));
    }

    @Test
    void allLoadedClubsHaveNonBlankShortNameAndState()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        clubs.forEach((domain, club) ->
        {
            assertFalse(club.shortName() == null || club.shortName().isBlank(),
                "Club " + domain + " has blank shortName");
            assertFalse(club.state() == null || club.state().isBlank(),
                "Club " + domain + " has blank state");
        });
    }

    @Test
    void noTwoLoadedClubsShareShortNameAndState()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        // Group by (shortName, state) — any group with >1 entry is a duplicate
        Map<String, Long> counts = clubs.values().stream()
            .collect(Collectors.groupingBy(
                c -> c.shortName().toUpperCase() + "/" + c.state().toUpperCase(),
                Collectors.counting()));
        counts.forEach((key, count) ->
            assertEquals(1L, count,
                "Duplicate (shortName/state) in clubs.yaml: " + key));
    }

    @Test
    void domainIsUsedAsClubId()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        clubs.forEach((domain, club) ->
            assertEquals(domain, club.id(),
                "Club id should equal its domain key"));
    }

    @Test
    void spotCheckKnownClubs()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));

        Club cyca = clubs.get("cyca.com.au");
        assertNotNull(cyca, "cyca.com.au should be in seed");
        assertEquals("CYCA", cyca.shortName());
        assertEquals("NSW", cyca.state());
        assertEquals("Cruising Yacht Club of Australia", cyca.longName());

        Club rbyc = clubs.get("rbyc.org.au");
        assertNotNull(rbyc, "rbyc.org.au (Royal Brighton YC, VIC) should be in seed");
        assertEquals("RBYC", rbyc.shortName());
        assertEquals("VIC", rbyc.state());

        Club myc = clubs.get("myc.org.au");
        assertNotNull(myc, "myc.org.au (Manly YC, NSW) should be in seed");
        assertEquals("MYC", myc.shortName());
        assertEquals("NSW", myc.state());

        // BYC disambiguated: TAS = Bellerive, VIC = Ballarat
        Club bycTas = clubs.get("byc.org.au");
        assertNotNull(bycTas, "byc.org.au (Bellerive, TAS) should be in seed");
        assertEquals("BYC", bycTas.shortName());
        assertEquals("TAS", bycTas.state());

        Club bycVic = clubs.get("ballaratyachtclub.com.au");
        assertNotNull(bycVic, "ballaratyachtclub.com.au (Ballarat, VIC) should be in seed");
        assertEquals("BYC", bycVic.shortName());
        assertEquals("VIC", bycVic.state());
    }

    @Test
    void spotCheckMissingPlaceholders()
    {
        Map<String, Club> clubs = ClubLoader.load(Path.of("nonexistent"));
        // These three remain as unknowns in the file — they must NOT appear
        assertNull(clubs.get("unknown.domain.14"), "RBYC/TAS placeholder should be absent");
        assertNull(clubs.get("unknown.domain.22"), "RYCV/SA placeholder should be absent");
        assertNull(clubs.get("unknown.domain.25"), "SBYC/VIC placeholder should be absent");
        // And they must not have leaked in under any other key
        assertFalse(clubs.values().stream()
            .anyMatch(c -> "RBYC".equals(c.shortName()) && "TAS".equals(c.state())),
            "No RBYC/TAS entry should be present");
    }
}
