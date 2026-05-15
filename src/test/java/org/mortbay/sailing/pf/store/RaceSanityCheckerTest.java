package org.mortbay.sailing.pf.store;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaceSanityCheckerTest
{
    @Test
    void healthyRacePassesAllChecks()
    {
        // 10 boats with realistic, varied finish times
        Race r = race(
            time(2, 14, 31),
            time(2, 15, 47),
            time(2, 16, 8),
            time(2, 18, 51),
            time(2, 20, 0),       // one round time — not enough to trip
            time(2, 22, 13),
            time(2, 24, 5),
            time(2, 27, 19),
            time(2, 30, 0),       // another round time
            time(2, 35, 42)
        );
        Optional<RaceSanityChecker.Issue> result = RaceSanityChecker.check(r);
        assertTrue(result.isEmpty(), "Healthy race should not be flagged; got " + result);
    }

    @Test
    void flagsRaceWithMostlySameElapsedTime()
    {
        // 9 of 10 share exactly PT2H
        Race r = race(
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0),
            time(3, 17, 31)
        );
        Optional<RaceSanityChecker.Issue> result = RaceSanityChecker.check(r);
        assertTrue(result.isPresent(), "Should flag 90%-shared elapsed time");
        assertEquals("same-elapsed-time", result.get().checkName());
    }

    @Test
    void flagsRaceWithMostlyRoundFiveMinuteTimes()
    {
        // All elapsed times are exact 5-minute multiples, but distinct values
        // (does not trip same-time, must trip round-time)
        Race r = race(
            time(1, 30, 0),
            time(2, 0, 0),
            time(2, 5, 0),
            time(2, 10, 0),
            time(2, 15, 0),
            time(2, 20, 0),
            time(2, 25, 0),
            time(2, 30, 0),
            time(2, 35, 0),
            time(2, 40, 0)
        );
        Optional<RaceSanityChecker.Issue> result = RaceSanityChecker.check(r);
        assertTrue(result.isPresent(), "Should flag all-round 5-minute times");
        assertEquals("round-elapsed-time", result.get().checkName());
    }

    @Test
    void doesNotFlagRaceWithModestNumberOfRoundTimes()
    {
        // 3 of 10 round (30%) — below 85% threshold
        Race r = race(
            time(2, 14, 31),
            time(2, 15, 0),       // round
            time(2, 16, 8),
            time(2, 18, 51),
            time(2, 20, 0),       // round
            time(2, 22, 13),
            time(2, 24, 5),
            time(2, 27, 19),
            time(2, 30, 0),       // round
            time(2, 35, 42)
        );
        assertFalse(RaceSanityChecker.check(r).isPresent());
    }

    @Test
    void skipsTinyRaces()
    {
        // 3 boats all with the same time — would trip the 85% rule, but too small to judge
        Race r = race(
            time(2, 0, 0),
            time(2, 0, 0),
            time(2, 0, 0)
        );
        assertFalse(RaceSanityChecker.check(r).isPresent(),
            "Races below MIN_FINISHERS should never be flagged");
    }

    @Test
    void flagsHugeTimeSpread()
    {
        // The Etchells-championship pattern: short-course racers (10 min) and a
        // long-passage race (5-7 hours) accidentally merged into one division.
        Race r = race(
            time(0, 9, 40),
            time(0, 9, 44),
            time(0, 9, 50),
            time(0, 9, 54),
            time(0, 10, 25),
            time(5, 25, 54),
            time(6, 16, 7),
            time(6, 36, 44),
            time(7, 12, 29),
            time(7, 28, 3)
        );
        Optional<RaceSanityChecker.Issue> result = RaceSanityChecker.check(r);
        assertTrue(result.isPresent(), "Should flag huge max/min ratio");
        assertEquals("huge-time-spread", result.get().checkName());
    }

    @Test
    void doesNotFlagModestSpreadWithinNormalRange()
    {
        // ~2x spread — wide but plausible in a heterogeneous fleet on one course
        Race r = race(
            time(2, 0, 0),
            time(2, 14, 31),
            time(2, 30, 0),
            time(2, 45, 47),
            time(3, 10, 13),
            time(3, 45, 5)
        );
        assertFalse(RaceSanityChecker.check(r).isPresent());
    }

    @Test
    void flagsTimingErrorWithVeryFastFinisher()
    {
        // One boat with 1-second elapsed (timing error) plus normal fleet
        Race r = race(
            Duration.ofSeconds(1),
            time(0, 15, 31),
            time(0, 16, 8),
            time(0, 18, 51),
            time(0, 20, 0)
        );
        Optional<RaceSanityChecker.Issue> result = RaceSanityChecker.check(r);
        assertTrue(result.isPresent(), "Timing error producing huge ratio should be flagged");
        assertEquals("huge-time-spread", result.get().checkName());
    }

    @Test
    void ignoresNullElapsedTime()
    {
        // A finisher with null elapsed time should not be counted as a "round" match
        List<Finisher> fs = new ArrayList<>();
        fs.add(new Finisher("b1", null, false, null));
        fs.add(new Finisher("b2", time(2, 14, 31), false, null));
        fs.add(new Finisher("b3", time(2, 15, 47), false, null));
        fs.add(new Finisher("b4", time(2, 16, 8), false, null));
        fs.add(new Finisher("b5", time(2, 18, 51), false, null));
        Race r = raceFromFinishers(fs);
        assertFalse(RaceSanityChecker.check(r).isPresent());
    }

    // --- helpers ---

    private static Duration time(int h, int m, int s)
    {
        return Duration.ofHours(h).plusMinutes(m).plusSeconds(s);
    }

    private static Race race(Duration... times)
    {
        List<Finisher> fs = new ArrayList<>();
        for (int i = 0; i < times.length; i++)
        {
            fs.add(new Finisher("b" + i, times[i], false, null));
        }
        return raceFromFinishers(fs);
    }

    private static Race raceFromFinishers(List<Finisher> fs)
    {
        Division d = new Division("Test", fs);
        return new Race("test-race-id", "club", List.of("series"),
            LocalDate.of(2024, 1, 1), 1, null, List.of(d), "test", null, null);
    }
}
