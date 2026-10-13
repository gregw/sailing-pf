package org.mortbay.sailing.pf.analysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.EntryVariant;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Copies of the same race (DuplicateRaces): found by identical elapsed times, each boat's
 * result counted once, and the copies' variants reconciled.
 */
class DuplicateRacesTest
{
    private static final LocalDate DAY = LocalDate.of(2025, 10, 12);

    @TempDir
    Path tempDir;

    private DataStore store;

    @AfterEach
    void stop()
    {
        if (store != null)
            store.stop();
    }

    private void start(String seriesTypes) throws IOException
    {
        Files.createDirectories(tempDir.resolve("config"));
        Files.writeString(tempDir.resolve("config/clubs.yaml"),
            "clubs:\n  c.au:\n    shortName: C\n" + (seriesTypes == null ? "" : "    seriesTypes:\n" + seriesTypes));
        store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
    }

    /** A finisher: boat id, elapsed minutes, non-spinnaker flag. */
    private static Finisher fin(String boatId, int minutes, boolean ns)
    {
        return new Finisher(boatId, Duration.ofMinutes(minutes), ns, null);
    }

    private static Finisher fin(String boatId, int minutes)
    {
        return fin(boatId, minutes, false);
    }

    private Race race(String id, String series, Division... divisions)
    {
        Race race = new Race(id, "c.au", List.of("c.au/" + series), DAY, 1, null, List.of(divisions),
            "test", null, null);
        store.putRace(race);
        return race;
    }

    private static Division div(String name, Finisher... finishers)
    {
        return new Division(name, List.of(finishers));
    }

    @Test
    void subsetCopyCountsInTheLargerDivision() throws IOException
    {
        start(null);
        race("open", "twilight", div("Open", fin("a", 60), fin("b", 61), fin("c", 62), fin("d", 63), fin("e", 64)));
        race("helm", "female-helm", div("Female Helm", fin("a", 60), fin("c", 62), fin("e", 64)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(1, dups.groups());
        for (String boat : List.of("a", "c", "e"))
        {
            assertTrue(dups.isReferenceOnly("helm", 0, boat), boat);
            assertEquals("open", dups.countedIn("helm", 0, boat));
            assertFalse(dups.isReferenceOnly("open", 0, boat), boat);
        }
        assertFalse(dups.isReferenceOnly("open", 0, "b"));
        assertEquals(List.of("helm"), dups.sameRaceAs("open"));
        assertEquals(List.of("open"), dups.sameRaceAs("helm"));
    }

    @Test
    void boatOnlyInTheSmallerCopyStillCounts() throws IOException
    {
        start(null);
        race("a", "champs", div("Div 1", fin("p", 60), fin("q", 61), fin("c", 62), fin("d", 63)));
        race("b", "pointscore", div("Div 1", fin("c", 62), fin("d", 63), fin("e", 64)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertTrue(dups.isReferenceOnly("b", 0, "c"));
        assertTrue(dups.isReferenceOnly("b", 0, "d"));
        assertFalse(dups.isReferenceOnly("b", 0, "e"), "only in the smaller copy -- counts there");
        assertNull(dups.countedIn("b", 0, "e"));
    }

    @Test
    void copiesOfCopiesAreOneGroup() throws IOException
    {
        start(null);
        race("r1", "s1", div("D", fin("a", 60), fin("b", 61)));
        race("r2", "s2", div("D", fin("a", 60), fin("b", 61), fin("c", 62)));
        race("r3", "s3", div("D", fin("b", 61), fin("c", 62)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(1, dups.groups());
        assertEquals(List.of("r2", "r3"), dups.sameRaceAs("r1"));
        assertFalse(dups.isReferenceOnly("r2", 0, "b"), "r2's division is largest");
        assertTrue(dups.isReferenceOnly("r1", 0, "b"));
        assertTrue(dups.isReferenceOnly("r3", 0, "b"));
    }

    @Test
    void differentRacesOnTheSameDayAreNotCopies() throws IOException
    {
        start(null);
        race("r1", "s", div("D", fin("a", 60), fin("b", 61), fin("c", 62)));
        race("r2", "s", div("D", fin("a", 55), fin("b", 58), fin("c", 62)));   // one shared time only
        race("r3", "s", div("D", fin("a", 60), fin("x", 61)));                  // one shared boat only

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(0, dups.groups());
        assertEquals(0, dups.referenceOnlyCount());
    }

    @Test
    void aCorrectedTimeStillMatchesButTwoDoNot() throws IOException
    {
        start(null);
        List<Finisher> base = new ArrayList<>(), oneOff = new ArrayList<>(), twoOff = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            base.add(fin("b" + i, 60 + i));
            oneOff.add(fin("b" + i, 60 + i + (i == 0 ? 1 : 0)));
            twoOff.add(fin("b" + i, 60 + i + (i < 2 ? 1 : 0)));
        }
        race("base", "s", new Division("D", base));
        race("one", "t", new Division("D", oneOff));
        assertEquals(1, DuplicateRaces.find(store).groups(), "9 of 10 identical");

        store.putRace(new Race("one", "c.au", List.of("c.au/t"), DAY.plusDays(1), 1, null,
            List.of(new Division("D", oneOff)), "test", null, null));   // move it off the day
        race("two", "u", new Division("D", twoOff));
        assertEquals(0, DuplicateRaces.find(store).groups(), "8 of 10 identical");
    }

    // ---- Variants ----

    @Test
    void explicitVariantsThatDisagreeAreAnErrorAndCountSeparately() throws IOException
    {
        start("      c.au/ns-series: ns\n      c.au/spin-series: spin\n");
        race("ns", "ns-series", div("D", fin("a", 60), fin("b", 61), fin("c", 62)));
        race("spin", "spin-series", div("D", fin("a", 60), fin("b", 61)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(2, dups.errors(), "a and b");
        assertEquals(0, dups.referenceOnlyCount(), "both copies count");
        String log = Files.readString(tempDir.resolve("log/duplicates.log"));
        assertTrue(log.contains("ERROR boat a is"), log);
        assertTrue(log.contains("ERROR boat b is"), log);
    }

    @Test
    void anUnknownCopyBelievesTheExplicitOne() throws IOException
    {
        start("      c.au/ns-series: ns\n");
        Race ns = race("ns", "ns-series", div("D", fin("a", 60), fin("b", 61)));
        Race other = race("other", "pointscore", div("D", fin("a", 60), fin("b", 61), fin("c", 62)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(0, dups.errors());
        assertEquals(0, dups.warnings());
        Race stored = store.races().get("other");
        Division d = stored.divisions().getFirst();
        assertEquals(EntryVariant.NON_SPIN, dups.variant(store, stored, 0, d, d.finishers().get(0)), "a");
        assertEquals(EntryVariant.SPIN, dups.variant(store, stored, 0, d, d.finishers().get(2)),
            "c is only in this copy -- keeps its own variant");
        assertTrue(dups.isReferenceOnly("ns", 0, "a"), "counts in the larger copy, as non-spinnaker");
    }

    @Test
    void allUnknownTakesTheCopyWithMostBoatsOfTheSameVariant() throws IOException
    {
        start(null);
        // a is spin among three spin boats in "big", but non-spinnaker alone in "odd".
        race("big", "s", div("D", fin("a", 60), fin("b", 61), fin("c", 62)));
        race("odd", "t", div("D", fin("a", 60, true), fin("b", 61), fin("c", 62), fin("d", 63)));

        DuplicateRaces dups = DuplicateRaces.find(store);
        assertEquals(1, dups.warnings());
        assertTrue(Files.readString(tempDir.resolve("log/duplicates.log")).contains("WARN boat a is"));
        Race odd = store.races().get("odd");
        Division d = odd.divisions().getFirst();
        assertEquals(EntryVariant.SPIN, dups.variant(store, odd, 0, d, d.finishers().getFirst()));
    }

    // ---- In the optimiser ----

    @Test
    void anExactCopyChangesNoPf() throws IOException
    {
        start(null);
        Map<String, BoatDerived> derived = new LinkedHashMap<>();
        String[] boats = {"a", "b", "c", "d"};
        double[] tcf = {0.95, 1.0, 1.05, 1.10};
        for (int i = 0; i < boats.length; i++)
        {
            Boat boat = new Boat(boats[i], "S" + i, boats[i], "design", null, List.of(), List.of(), null, null);
            store.putBoat(boat);
            derived.put(boats[i], new BoatDerived(boat,
                new ReferenceFactors(new Factor(1.0, 0.5), null, null, 0, 0, 0), Set.of(), Set.of(), null));
        }
        java.util.Random rng = new java.util.Random(3);
        List<Race> originals = new ArrayList<>();
        for (int r = 0; r < 6; r++)
        {
            List<Finisher> fs = new ArrayList<>();
            for (int i = 0; i < boats.length; i++)
                fs.add(new Finisher(boats[i],
                    Duration.ofSeconds((long)(7200 / tcf[i] * (1 + 0.02 * rng.nextGaussian()))), false, null));
            Race race = new Race("c.au-2025-0" + (r + 1), "c.au", List.of("c.au/s"), DAY.plusDays(7L * r), r + 1,
                null, List.of(new Division("D", fs)), "test", null, null);
            store.putRace(race);
            originals.add(race);
        }
        PfResult single = new PfOptimiser().optimise(store, derived, PfConfig.DEFAULT, () -> false);

        for (Race race : originals)
            store.putRace(new Race(race.id() + "-copy", "c.au", List.of("c.au/other"), race.date(), race.number(),
                null, race.divisions(), "test", null, null));
        PfResult counted = new PfOptimiser().optimise(store, derived, PfConfig.DEFAULT, () -> false,
            null, 0, DuplicateRaces.find(store));
        PfResult doubled = new PfOptimiser().optimise(store, derived, PfConfig.DEFAULT, () -> false);

        for (String boat : boats)
        {
            BoatPf s = single.boatPfs().get(boat), c = counted.boatPfs().get(boat);
            assertEquals(s.spin().value(), c.spin().value(), 1e-9, boat + " PF");
            assertEquals(s.spin().weight(), c.spin().weight(), 1e-9, boat + " confidence");
            assertEquals(6, c.spinRaceCount(), boat + " race count");
            assertEquals(12, doubled.boatPfs().get(boat).spinRaceCount(), boat + " without duplicate handling");
            assertEquals(6, counted.residualsByBoatId().get(boat).size(), boat + " chart points");
        }
    }
}
