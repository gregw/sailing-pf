package org.mortbay.sailing.pf.store;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.mortbay.sailing.pf.data.Race;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-shot utility: walks every race already in the {@link DataStore} and runs
 * {@link RaceSanityChecker}. By default reports findings; pass {@code --apply}
 * to also add the offending race IDs to the persisted exclusion list.
 *
 * <p>Use this after introducing a new sanity check, or to retrospectively scan
 * a previously imported corpus. Already-excluded races are skipped (no-op for
 * them, since exclusion is the action the check would have taken anyway).
 *
 * <p>Usage:
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=org.mortbay.sailing.pf.store.RaceSanityCheckerMain \
 *       -Dexec.args="--apply"
 * </pre>
 * or with explicit data root:
 * <pre>
 *   ... -Dexec.args="--data-root=/path/to/pf-data --apply"
 * </pre>
 */
public final class RaceSanityCheckerMain
{
    private static final Logger LOG = LoggerFactory.getLogger(RaceSanityCheckerMain.class);

    private RaceSanityCheckerMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        boolean apply = false;
        List<String> passthrough = new ArrayList<>();
        for (String a : args)
        {
            if ("--apply".equals(a) || "-a".equals(a))
                apply = true;
            else
                passthrough.add(a);
        }

        Path dataRoot = DataStore.resolveDataRoot(passthrough.toArray(String[]::new));
        DataStore store = new DataStore(dataRoot);
        store.start();
        // We are *manually* applying the check; the per-putRace hook is irrelevant here
        // (no putRace calls happen during this run) but turn it off defensively.
        store.setAutoSanityCheck(false);
        try
        {
            int total = 0;
            int alreadyExcluded = 0;
            int flagged = 0;
            int newlyExcluded = 0;
            Map<String, Integer> byCheck = new LinkedHashMap<>();
            List<String> flaggedIds = new ArrayList<>();

            for (Race race : store.races().values())
            {
                total++;
                if (store.isRaceExcluded(race.id()))
                {
                    alreadyExcluded++;
                    continue;
                }
                Optional<RaceSanityChecker.Issue> issue = RaceSanityChecker.check(race);
                if (issue.isEmpty())
                    continue;

                flagged++;
                byCheck.merge(issue.get().checkName(), 1, Integer::sum);
                flaggedIds.add(race.id());
                LOG.info("FLAG [{}] {} — {}", issue.get().checkName(), race.id(), issue.get().description());

                if (apply)
                {
                    String reason = "sanity-check: " + issue.get().checkName() + " — " + issue.get().description();
                    store.setRaceExcluded(race.id(), true, reason);
                    newlyExcluded++;
                }
            }

            LOG.info("--- Sanity scan complete ---");
            LOG.info("Total races inspected:   {}", total);
            LOG.info("Already excluded:        {}", alreadyExcluded);
            LOG.info("Newly flagged:           {}", flagged);
            for (var e : byCheck.entrySet())
            {
                LOG.info("  via {}: {}", e.getKey(), e.getValue());
            }
            if (apply)
            {
                LOG.info("Applied: {} race(s) added to exclusions.yaml", newlyExcluded);
            }
            else
            {
                LOG.info("Dry run — no exclusions written. Re-run with --apply to persist.");
                if (!flaggedIds.isEmpty())
                {
                    for (String flaggedId : flaggedIds)
                    {
                        LOG.info("  {}", flaggedId);
                    }
                }
            }
        }
        finally
        {
            store.stop();
        }
    }
}
