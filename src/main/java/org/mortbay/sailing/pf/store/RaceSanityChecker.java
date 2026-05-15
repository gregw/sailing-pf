package org.mortbay.sailing.pf.store;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

/**
 * Detects data-quality issues in imported races. Each check inspects a single
 * division's finishers for a signature of synthetic / placeholder / merged
 * data and returns an {@link Issue} if the division is suspicious.
 *
 * <p>{@link #check(Race)} runs every check against every division and returns
 * the first issue found, or empty if the race looks fine. The issue description
 * names the offending division so it's clear which one tripped the rule.
 *
 * <p>Invoked from {@link DataStore#putRace(Race)} for newly imported races; if
 * an issue is found, the race is auto-added to the race exclusion list but
 * kept in the store. An operator can review and un-exclude via the admin UI if
 * the race turns out to be real. Re-imports of an existing race do not re-run
 * the check, so a manual un-exclude is not silently reverted.
 *
 * <p>To add a new check: write a private static method following the
 * {@code checkXxx} pattern and add a call to it in {@link #checkDivision}.
 */
public final class RaceSanityChecker
{
    private RaceSanityChecker()
    {
    }

    /**
     * Issue reported by a check. {@code checkName} is a short identifier for logs / UI.
     */
    public record Issue(String checkName, String description) {}

    /**
     * Fraction of finishers needed for fraction-based signatures to fire.
     */
    private static final double SUSPICION_THRESHOLD = 0.85;

    /**
     * Skip checks when there are too few finishers to draw a conclusion.
     */
    private static final int MIN_FINISHERS = 4;

    /**
     * Threshold for the slowest/fastest elapsed-time ratio within a division.
     * Real-world divisions sit overwhelmingly under 2.5× even in heterogeneous
     * fleets; anything ≥ 3.0× is almost always two different courses merged
     * into one division by the importer, a timing-data error (e.g. fastest
     * recorded as 1 second), or a tiny dinghy fleet where the back markers
     * never really finished.
     */
    private static final double MAX_TIME_SPREAD_RATIO = 3.0;

    /**
     * Run all sanity checks against the race. Returns the first {@link Issue}
     * found, or empty if no check flagged it. The returned description names
     * the offending division.
     */
    public static Optional<Issue> check(Race race)
    {
        if (race == null || race.divisions() == null)
            return Optional.empty();
        for (Division d : race.divisions())
        {
            Optional<Issue> i = checkDivision(d);
            if (i.isPresent())
                return i;
        }
        return Optional.empty();
    }

    private static Optional<Issue> checkDivision(Division d)
    {
        if (d == null || d.finishers() == null)
            return Optional.empty();
        List<Finisher> finishers = d.finishers();
        if (finishers.size() < MIN_FINISHERS)
            return Optional.empty();

        Optional<Issue> i;
        if ((i = checkSameElapsedTime(d.name(), finishers)).isPresent())
            return i;
        if ((i = checkRoundElapsedTime(d.name(), finishers)).isPresent())
            return i;
        if ((i = checkHugeTimeSpread(d.name(), finishers)).isPresent())
            return i;
        return Optional.empty();
    }

    /**
     * Flag when ≥85% of finishers in the division share a single elapsed time.
     * Real races don't produce identical times across many boats — this is the
     * signature of placeholder / synthetic data (e.g. a series row pre-populated
     * with a default duration before actual results were entered).
     */
    private static Optional<Issue> checkSameElapsedTime(String divName, List<Finisher> finishers)
    {
        Map<Duration, Integer> counts = new HashMap<>();
        int total = 0;
        for (Finisher f : finishers)
        {
            if (f.elapsedTime() == null)
                continue;
            counts.merge(f.elapsedTime(), 1, Integer::sum);
            total++;
        }
        if (total < MIN_FINISHERS)
            return Optional.empty();
        int max = 0;
        Duration maxKey = null;
        for (var e : counts.entrySet())
        {
            if (e.getValue() > max)
            {
                max = e.getValue();
                maxKey = e.getKey();
            }
        }
        double fraction = (double)max / total;
        if (fraction >= SUSPICION_THRESHOLD)
            return Optional.of(new Issue("same-elapsed-time",
                String.format("division '%s': %d of %d finishers (%.0f%%) share elapsed time %s",
                    divName, max, total, fraction * 100, maxKey)));
        return Optional.empty();
    }

    /**
     * Flag when ≥85% of finishers in the division have elapsed times that are an
     * exact multiple of 5 minutes. Real boats produce times with seconds; finishing
     * on a 5-minute boundary is a signature of a coarse-grained placeholder.
     */
    private static Optional<Issue> checkRoundElapsedTime(String divName, List<Finisher> finishers)
    {
        long fiveMinMs = Duration.ofMinutes(5).toMillis();
        int round = 0;
        int total = 0;
        for (Finisher f : finishers)
        {
            if (f.elapsedTime() == null)
                continue;
            total++;
            if (f.elapsedTime().toMillis() % fiveMinMs == 0)
                round++;
        }
        if (total < MIN_FINISHERS)
            return Optional.empty();
        double fraction = (double)round / total;
        if (fraction >= SUSPICION_THRESHOLD)
            return Optional.of(new Issue("round-elapsed-time",
                String.format("division '%s': %d of %d finishers (%.0f%%) have elapsed time on a 5-minute boundary",
                    divName, round, total, fraction * 100)));
        return Optional.empty();
    }

    /**
     * Flag when the slowest finisher takes ≥ 3× as long as the fastest. Real
     * single-course divisions sit well below this — only ~0.3% of real divisions
     * exceed it. Common causes:
     * <ul>
     *   <li>Two different races (e.g. an Etchells one-design and a passage race)
     *       have been merged into a single "Overall" or "All Boats" division.</li>
     *   <li>Timing data error — fastest recorded as a few seconds, or slowest as
     *       hours when the boat retired.</li>
     *   <li>Junior dinghy fleets where the back markers never finished.</li>
     * </ul>
     */
    private static Optional<Issue> checkHugeTimeSpread(String divName, List<Finisher> finishers)
    {
        long minMs = Long.MAX_VALUE;
        long maxMs = 0;
        int total = 0;
        for (Finisher f : finishers)
        {
            if (f.elapsedTime() == null)
                continue;
            long ms = f.elapsedTime().toMillis();
            if (ms <= 0)
                continue;
            if (ms < minMs)
                minMs = ms;
            if (ms > maxMs)
                maxMs = ms;
            total++;
        }
        if (total < MIN_FINISHERS || minMs == Long.MAX_VALUE)
            return Optional.empty();
        double ratio = (double)maxMs / minMs;
        if (ratio >= MAX_TIME_SPREAD_RATIO)
            return Optional.of(new Issue("huge-time-spread",
                String.format("division '%s': slowest/fastest elapsed-time ratio %.1fx (fastest %ds, slowest %ds) — likely merged courses or bad timing data",
                    divName, ratio, minMs / 1000, maxMs / 1000)));
        return Optional.empty();
    }
}
