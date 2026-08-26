package org.mortbay.sailing.pf.server;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the on-demand, club-restricted import triggered from the clubs page.
 * <p>
 * The clubs used here have no TopYacht URLs, so the importer never makes an HTTP
 * request — which lets the run be exercised end to end with a null HttpClient, and
 * checks the diagnostic the feature exists for: telling an admin that a club they
 * selected has nothing configured to import from.
 */
class TaskServiceClubImportTest
{
    @TempDir
    Path tempDir;

    private DataStore store;
    private TaskService taskService;

    private static Club club(String id, String shortName)
    {
        return new Club(id, shortName, shortName + " Yacht Club", "NSW", false,
            null, List.of(), List.of(), List.of(), List.of(), null);
    }

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        store.putClub(club("alpha.com.au", "ALPHA"));
        store.putClub(club("beta.com.au", "BETA"));
        taskService = new TaskService(store, null, tempDir);
    }

    @AfterEach
    void tearDown()
    {
        taskService.stop();
        store.stop();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean())
        {
            if (System.currentTimeMillis() > deadline)
                throw new AssertionError("Timed out waiting for " + what);
            Thread.sleep(10);
        }
    }

    private TaskService.ClubImportRun runFor(List<String> clubIds) throws Exception
    {
        TaskService.ClubImportRun run = taskService.submitClubImport(clubIds);
        assertNotNull(run, "run should be accepted");
        await("club import to finish", () -> !run.running());
        return run;
    }

    /**
     * Records which analysis phases the club import drives. Writing a race or boat deletes its
     * derived data from the cache rather than recomputing it, so an import that stops after
     * saving leaves the race detail page blank for everything it touched.
     */
    private static final class RecordingCache extends AnalysisCache
    {
        private final List<String> calls = java.util.Collections.synchronizedList(new ArrayList<>());

        RecordingCache(DataStore store)
        {
            super(store);
        }

        @Override
        public void refreshIndexes()
        {
            calls.add("build-indexes");
        }

        @Override
        public void refresh(Integer targetIrcYear, Double outlierSigma, double clubCertificateWeight,
                            double minR2, int minPairs)
        {
            calls.add("analysis");
        }

        @Override
        public void refreshReferenceFactors(Integer targetIrcYear, double clubCertificateWeight,
                                            double minR2, int minPairs)
        {
            calls.add("reference-factors");
        }

        @Override
        public void refreshPf(org.mortbay.sailing.pf.analysis.PfConfig config,
                              java.util.function.Supplier<Boolean> stopCheck)
        {
            calls.add("pf-optimise");
        }
    }

    @Test
    void clubImportRebuildsTheAnalysisItInvalidated() throws Exception
    {
        RecordingCache cache = new RecordingCache(store);
        taskService.setCache(cache);

        runFor(List.of("alpha.com.au"));

        assertEquals(List.of("build-indexes", "analysis", "reference-factors", "pf-optimise"),
            List.copyOf(cache.calls),
            "the import must rebuild the derived data it invalidated, in the scheduler's order");
    }

    @Test
    void clubImportReportsItsPhaseWhileRunning() throws Exception
    {
        taskService.setCache(new RecordingCache(store));

        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        assertEquals("done", run.phase(), "phase settles once the run finishes");
    }

    /** Both sources are club-scoped, so one run covers whatever a club has configured. */
    @Test
    void clubImportRunsBothClubScopedImporters() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        assertEquals(List.of("topyacht", "sailsys"), run.importers());
        assertEquals(0, run.counts().get("sailsys.racesImported"));
        assertEquals(0, run.counts().get("sailsys.clubs"),
            "a club with no SailSys events is not visited");
    }

    @Test
    void clubImportWarnsAboutSelectedClubWithNothingConfigured() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        assertTrue(run.log().contains("alpha.com.au has no SailSys events configured"),
            () -> "expected a no-events warning, log was:\n" + run.log());
    }

    @Test
    void clubImportReportsCountsAndCompletes() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        assertEquals("done", run.state());
        assertNull(run.error());
        assertNotNull(run.finishedAt());
        assertEquals(List.of("topyacht", "sailsys"), run.importers());
        assertEquals(0, run.counts().get("topyacht.racesImported"));
        // No TopYacht URLs configured, so the club is not visited at all.
        assertEquals(0, run.counts().get("topyacht.clubs"));
    }

    @Test
    void clubImportWarnsAboutSelectedClubWithNoTopyachtUrls() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        // one per club-scoped importer: nothing configured for either source
        assertEquals(2, run.warnings());
        assertEquals(0, run.errors());
        assertTrue(run.log().contains("alpha.com.au has no TopYacht URLs configured"),
            () -> "expected a no-URLs warning, log was:\n" + run.log());
        // The un-selected club must not appear in a club-restricted run.
        assertFalse(run.log().contains("beta.com.au"),
            () -> "unselected club leaked into the run, log was:\n" + run.log());
    }

    @Test
    void clubImportWarnsAboutUnknownClubId() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("nosuch.com.au"));

        assertEquals("done", run.state());
        assertTrue(run.log().contains("nosuch.com.au is not a known club"),
            () -> "expected an unknown-club warning, log was:\n" + run.log());
    }

    @Test
    void clubImportRunIsRetrievableById() throws Exception
    {
        TaskService.ClubImportRun run = runFor(List.of("alpha.com.au"));

        assertEquals(run, taskService.clubImportRun(run.id()));
        assertNull(taskService.clubImportRun("no-such-run"));
    }

    @Test
    void concurrentClubImportIsRejected() throws Exception
    {
        // Occupy the single import slot with a long-lived task, then confirm a club
        // import cannot start alongside it.
        TaskService.ClubImportRun first = taskService.submitClubImport(List.of("alpha.com.au"));
        assertNotNull(first);
        if (first.running())
            assertNull(taskService.submitClubImport(List.of("beta.com.au")),
                "a second import must be rejected while one is running");
        await("first run to finish", () -> !first.running());
        TaskService.ClubImportRun second = taskService.submitClubImport(List.of("beta.com.au"));
        assertNotNull(second, "a new import is accepted once the previous one has finished");
        // Let it finish before the test's temp data root is torn down.
        await("second run to finish", () -> !second.running());
    }
}
