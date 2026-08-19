package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@code clear-cache-orc} and {@code clear-cache-sailsys} tasks:
 * each must delete the contents of its own HTTP cache directory under
 * {@code <dataRoot>/cache} and leave the other importer's cache untouched.
 */
class TaskServiceClearCacheTest
{
    @TempDir
    Path tempDir;

    private DataStore store;
    private TaskService taskService;

    @BeforeEach
    void setUp() throws Exception
    {
        store = new DataStore(tempDir);
        store.start();
        // httpClient is unused by the clear-cache tasks.
        taskService = new TaskService(store, null, tempDir);
    }

    @AfterEach
    void tearDown() throws Exception
    {
        taskService.stop();
        store.stop();
    }

    private Path createCacheFile(String relative) throws Exception
    {
        Path file = tempDir.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{}");
        return file;
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

    /**
     * Runs the named task, then waits for {@code sideEffect} — something the task
     * writes or deletes — and finally for the task to finish. The task sets
     * currentStatus before producing any side effect, so once the side effect is
     * visible, a null currentStatus can only mean the task has completed.
     */
    private void runTask(String name, BooleanSupplier sideEffect) throws Exception
    {
        assertTrue(taskService.submit(name, "run", 0), "task should be accepted");
        await("side effect of " + name, sideEffect);
        await(name + " to finish", () -> taskService.currentStatus() == null);
    }

    @Test
    void clearCacheOrcDeletesOnlyOrcCache() throws Exception
    {
        Path orcFile = createCacheFile("cache/orc/certs/2026.json");
        Path sailsysFile = createCacheFile("cache/sailsys/races/1.json");

        runTask("clear-cache-orc", () -> !Files.exists(orcFile));

        assertFalse(Files.exists(orcFile), "ORC cache file should be deleted");
        assertTrue(Files.exists(tempDir.resolve("cache/orc")), "cache/orc directory itself is kept");
        assertTrue(Files.exists(sailsysFile), "SailSys cache must be untouched");
    }

    @Test
    void clearCacheSailsysDeletesOnlySailsysCache() throws Exception
    {
        Path orcFile = createCacheFile("cache/orc/certs/2026.json");
        Path sailsysFile = createCacheFile("cache/sailsys/races/1.json");

        runTask("clear-cache-sailsys", () -> !Files.exists(sailsysFile));

        assertFalse(Files.exists(sailsysFile), "SailSys cache file should be deleted");
        assertTrue(Files.exists(tempDir.resolve("cache/sailsys")), "cache/sailsys directory itself is kept");
        assertTrue(Files.exists(orcFile), "ORC cache must be untouched");
    }

    @Test
    void clearCacheOnMissingDirectoryIsANoOp() throws Exception
    {
        // The task persists config/admin.yaml before clearing, so use that as the
        // signal that the task ran even though there is no cache to observe.
        Path config = tempDir.resolve("config/admin.yaml");
        assertFalse(Files.exists(config));

        runTask("clear-cache-orc", () -> Files.exists(config));

        assertFalse(Files.exists(tempDir.resolve("cache/orc")), "no directory should be created");
    }
}
