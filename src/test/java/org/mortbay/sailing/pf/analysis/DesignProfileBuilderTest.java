package org.mortbay.sailing.pf.analysis;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Factor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The design profile's raw metrics, threshold, modified-boat exclusion and spoke order.
 */
class DesignProfileBuilderTest
{
    private static final LocalDate DAY = LocalDate.now().minusDays(10);
    private static final int YEAR = LocalDate.now().getYear();

    private final Map<String, Boat> boats = new LinkedHashMap<>();
    private final Map<String, List<EntryResidual>> residuals = new LinkedHashMap<>();
    private final Map<String, BoatPf> pfs = new HashMap<>();

    /** A boat of {@code design} at {@code club} racing {@code nRaces} races, with log(PF / design RF) = {@code delta}. */
    private void boat(String id, String design, String club, int nRaces, double delta, boolean certified)
    {
        boat(id, design, club, nRaces, delta, certified, "race-", 0.0);
    }

    /** As above, racing in races named {@code racePrefix + i} with residual {@code residual} in each. */
    private void boat(String id, String design, String club, int nRaces, double delta, boolean certified,
                      String racePrefix, double residual)
    {
        List<Certificate> certs = certified
            ? List.of(new Certificate("IRC", YEAR, 1.0, false, false, false, false, "irc-" + id, null))
            : List.of();
        boats.put(id, new Boat(id, id, id, design, club == null ? List.of() : List.of(club), certs,
            List.of("test"), null, null));
        List<EntryResidual> rs = new ArrayList<>();
        for (int i = 0; i < nRaces; i++)
        {
            rs.add(new EntryResidual(racePrefix + i, "Div", DAY.minusDays(i), false, false, residual, 1.0));
        }
        residuals.put(id, rs);
        pfs.put(id, new BoatPf(new Factor(Math.exp(delta), 1.0), null, null, 0, 0, 0, nRaces, 0, 0));
    }

    private final Map<String, DesignProfileBuilder.DesignYear> designYears = new HashMap<>();

    private Map<String, DesignProfile> build()
    {
        Map<String, ReferenceFactors> designRfs = new HashMap<>();
        for (Boat b : boats.values())
            designRfs.put(b.designId(), new ReferenceFactors(new Factor(1.0, 1.0), null, null, 0, 0, 0));
        return new DesignProfileBuilder().buildAll(residuals, boats, pfs, designRfs, designYears);
    }

    @Test
    void effectiveClubsIsTheInverseSimpsonIndex()
    {
        assertEquals(1.0, DesignProfileBuilder.effectiveClubs(Map.of("a", 10)), 1e-12);
        assertEquals(5.0, DesignProfileBuilder.effectiveClubs(Map.of("a", 2, "b", 2, "c", 2, "d", 2, "e", 2)), 1e-12);
        assertEquals(0.0, DesignProfileBuilder.effectiveClubs(Map.of()), 1e-12);
    }

    @Test
    void designsNeedThreeActiveBoats()
    {
        for (int i = 0; i < 3; i++)
            boat("a" + i, "alpha", "c1", 5, 0.0, false);
        boat("b0", "beta", "c1", 5, 0.0, false);
        boat("b1", "beta", "c1", 5, 0.0, false);
        boat("b2", "beta", "c1", 2, 0.0, false);   // only 2 races: not active
        Map<String, DesignProfile> profiles = build();
        assertEquals(3, profiles.get("alpha").activeBoats());
        assertNull(profiles.get("beta"), "two active boats");
    }

    @Test
    void spokesRankDesignsAndTheScoreIsTheHexagonArea()
    {
        // alpha: 4 boats at 4 clubs, 6 races each, PFs matching RF, a 2015 design, and placing
        // ahead of beta on PF-corrected time in the races they share
        for (int i = 0; i < 4; i++)
            boat("a" + i, "alpha", "c" + i, 6, 0.0, true, "race-", -0.01);
        // beta: 3 boats at 1 club, 3 races each, PF/RF spread, a 1985 design
        boat("b0", "beta", "c0", 3, -0.05, false, "race-", 0.01);
        boat("b1", "beta", "c0", 3, 0.0, false, "race-", 0.01);
        boat("b2", "beta", "c0", 3, 0.05, false, "race-", 0.01);
        designYears.put("alpha", new DesignProfileBuilder.DesignYear(2015, "ORC", 4));
        designYears.put("beta", new DesignProfileBuilder.DesignYear(1985, "manual", 0));
        Map<String, DesignProfile> profiles = build();
        DesignProfile a = profiles.get("alpha");
        DesignProfile b = profiles.get("beta");
        assertEquals(1.0, a.quantity());
        assertEquals(1.0, a.ubiquity());
        assertEquals(1.0, a.frequency());
        assertEquals(1.0, a.results());
        assertEquals(1.0, a.consistency());
        assertEquals(1.0, a.modern());
        assertTrue(b.consistency() < a.consistency());
        assertEquals(0.0, b.modern(), 1e-12);
        assertEquals(0.0, b.results(), 1e-12);

        for (DesignProfile p : profiles.values())
        {
            double[] r = {p.quantity(), p.ubiquity(), p.frequency(), p.results(), p.consistency(), p.modern()};
            double sum = 0;
            for (int i = 0; i < r.length; i++)
                sum += r[i] * r[(i + 1) % r.length];
            assertEquals(sum / r.length, p.overallScore(), 1e-12);
        }
        assertTrue(a.overallScore() > b.overallScore());
    }

    @Test
    void aDesignWithNoYearScoresNeutralOnModern()
    {
        for (int i = 0; i < 3; i++)
            boat("a" + i, "alpha", "c1", 5, 0.0, false);
        for (int i = 0; i < 3; i++)
            boat("b" + i, "beta", "c1", 5, 0.0, false);
        designYears.put("alpha", new DesignProfileBuilder.DesignYear(2015, "ORC", 3));
        assertEquals(0.5, build().get("beta").modern(), 1e-12);
    }

    /** A boat of {@code design} with ORC certificates of the given (certificate year, age year) pairs. */
    private void orcBoat(String id, String design, int... yearAge)
    {
        List<Certificate> certs = new ArrayList<>();
        for (int i = 0; i < yearAge.length; i += 2)
            certs.add(new Certificate("ORC", yearAge[i], 1.0, false, false, false, false, id + i, null, yearAge[i + 1]));
        boats.put(id, new Boat(id, id, id, design, List.of(), certs, List.of("test"), null, null));
    }

    @Test
    void designYearIsTheMedianOrcAgeOfTheBoatsLatestCertificates()
    {
        orcBoat("a0", "alpha", 2025, 1997);
        orcBoat("a1", "alpha", 2024, 1985, 2026, 1996);   // latest certificate's age counts
        orcBoat("a2", "alpha", 2026, 1990);
        orcBoat("a3", "alpha", 2026, 1971);                // modified: left out
        orcBoat("b0", "beta", 2026, 2001);
        boats.put("c0", new Boat("c0", "c0", "c0", "gamma", List.of(), List.of(), List.of("test"), null, null));

        Map<String, DesignProfileBuilder.DesignYear> years = DesignProfileBuilder.designYears(
            boats, "a3"::equals, id -> "beta".equals(id) ? 2005 : null);
        assertEquals(new DesignProfileBuilder.DesignYear(1996, "ORC", 3), years.get("alpha"));
        assertEquals(new DesignProfileBuilder.DesignYear(2005, "manual", 0), years.get("beta"), "manual wins");
        assertNull(years.get("gamma"), "no ORC age and no manual year");
    }
}
