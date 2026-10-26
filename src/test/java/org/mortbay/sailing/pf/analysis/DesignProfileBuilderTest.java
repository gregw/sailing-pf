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

    private Map<String, DesignProfile> build(String... modified)
    {
        List<String> mod = List.of(modified);
        Map<String, ReferenceFactors> designRfs = new HashMap<>();
        for (Boat b : boats.values())
            designRfs.put(b.designId(), new ReferenceFactors(new Factor(1.0, 1.0), null, null, 0, 0, 0));
        return new DesignProfileBuilder().buildAll(residuals, boats, pfs, designRfs, mod::contains);
    }

    @Test
    void effectiveClubsIsTheInverseSimpsonIndex()
    {
        assertEquals(1.0, DesignProfileBuilder.effectiveClubs(Map.of("a", 10)), 1e-12);
        assertEquals(5.0, DesignProfileBuilder.effectiveClubs(Map.of("a", 2, "b", 2, "c", 2, "d", 2, "e", 2)), 1e-12);
        assertEquals(0.0, DesignProfileBuilder.effectiveClubs(Map.of()), 1e-12);
    }

    @Test
    void designsNeedThreeActiveBoatsAndModifiedBoatsDoNotCount()
    {
        for (int i = 0; i < 3; i++)
            boat("a" + i, "alpha", "c1", 5, 0.0, false);
        boat("b0", "beta", "c1", 5, 0.0, false);
        boat("b1", "beta", "c1", 5, 0.0, false);
        boat("b2", "beta", "c1", 2, 0.0, false);   // only 2 races: not active
        Map<String, DesignProfile> profiles = build();
        assertEquals(3, profiles.get("alpha").activeBoats());
        assertNull(profiles.get("beta"), "two active boats");

        assertNull(build("a0").get("alpha"), "a modified boat does not count");
    }

    @Test
    void spokesRankDesignsAndTheScoreIsTheHexagonArea()
    {
        // alpha: 4 boats at 4 clubs, 6 races each, PFs matching RF, all certified, and placing
        // ahead of beta on PF-corrected time in the races they share
        for (int i = 0; i < 4; i++)
            boat("a" + i, "alpha", "c" + i, 6, 0.0, true, "race-", -0.01);
        // beta: 3 boats at 1 club, 3 races each, PF/RF spread, none certified
        boat("b0", "beta", "c0", 3, -0.05, false, "race-", 0.01);
        boat("b1", "beta", "c0", 3, 0.0, false, "race-", 0.01);
        boat("b2", "beta", "c0", 3, 0.05, false, "race-", 0.01);
        Map<String, DesignProfile> profiles = build();
        DesignProfile a = profiles.get("alpha");
        DesignProfile b = profiles.get("beta");
        assertEquals(1.0, a.quantity());
        assertEquals(1.0, a.ubiquity());
        assertEquals(1.0, a.frequency());
        assertEquals(1.0, a.results());
        assertEquals(1.0, a.consistency());
        assertEquals(1.0, a.certified());
        assertTrue(b.consistency() < a.consistency());
        assertEquals(0.0, b.certified(), 1e-12);
        assertEquals(0.0, b.results(), 1e-12);

        for (DesignProfile p : profiles.values())
        {
            double[] r = {p.quantity(), p.ubiquity(), p.frequency(), p.results(), p.consistency(), p.certified()};
            double sum = 0;
            for (int i = 0; i < r.length; i++)
                sum += r[i] * r[(i + 1) % r.length];
            assertEquals(sum / r.length, p.overallScore(), 1e-12);
        }
        assertTrue(a.overallScore() > b.overallScore());
    }
}
