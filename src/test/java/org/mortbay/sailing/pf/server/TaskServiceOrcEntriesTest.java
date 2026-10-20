package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.importer.OrcApiImporter;
import org.mortbay.sailing.pf.server.TaskService.ImporterEntry;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the task entry rules: {@code orc-api} is unavailable without an API key, and at
 * most one of the {@code orc} / {@code orc-api} importers may be in the schedule or run at startup.
 */
class TaskServiceOrcEntriesTest
{
    @TempDir
    Path tempDir;

    private DataStore store;
    private TaskService taskService;

    @BeforeEach
    void setUp()
    {
        assumeTrue(System.getenv("ORC_API_KEY") == null, "ORC_API_KEY is set");
        store = new DataStore(tempDir);
        store.start();
        taskService = new TaskService(store, null, tempDir);
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if (taskService != null)
            taskService.stop();
        if (store != null)
            store.stop();
    }

    private void writeKey() throws Exception
    {
        Files.createDirectories(tempDir.resolve("config"));
        Files.writeString(tempDir.resolve("config").resolve(OrcApiImporter.API_KEY_FILE), "orc_test\n");
    }

    private static ImporterEntry entry(List<ImporterEntry> entries, String name)
    {
        return entries.stream().filter(e -> e.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void orcApiUnavailableWithoutKey()
    {
        assertNotNull(taskService.unavailableReason("orc-api"));
        assertNull(taskService.unavailableReason("orc"));
    }

    @Test
    void orcApiAvailableWithKeyFile() throws Exception
    {
        writeKey();
        assertNull(taskService.unavailableReason("orc-api"));
    }

    @Test
    void unavailableTaskLosesScheduleAndStartupFlags()
    {
        List<ImporterEntry> checked = taskService.checkEntries(List.of(
            new ImporterEntry("orc-api", "api", true, true),
            new ImporterEntry("ams", "api", true, true)), true);
        assertFalse(entry(checked, "orc-api").includeInSchedule());
        assertFalse(entry(checked, "orc-api").runAtStartup());
        assertTrue(entry(checked, "ams").includeInSchedule());
        assertTrue(entry(checked, "ams").runAtStartup());
    }

    @Test
    void bothOrcImportersInScheduleRejected() throws Exception
    {
        writeKey();
        List<ImporterEntry> entries = List.of(
            new ImporterEntry("orc", "api", true, false),
            new ImporterEntry("orc-api", "api", true, false));
        assertThrows(IllegalArgumentException.class, () -> taskService.checkEntries(entries, true));
    }

    @Test
    void bothOrcImportersAtStartupRejected() throws Exception
    {
        writeKey();
        List<ImporterEntry> entries = List.of(
            new ImporterEntry("orc", "api", false, true),
            new ImporterEntry("orc-api", "api", false, true));
        assertThrows(IllegalArgumentException.class, () -> taskService.checkEntries(entries, true));
    }

    @Test
    void orcInScheduleAndOrcApiAtStartupRejected() throws Exception
    {
        writeKey();
        List<ImporterEntry> entries = List.of(
            new ImporterEntry("orc", "api", true, false),
            new ImporterEntry("orc-api", "api", false, true));
        assertThrows(IllegalArgumentException.class, () -> taskService.checkEntries(entries, true));
    }

    @Test
    void oneOrcImporterInBothColumnsAllowed() throws Exception
    {
        writeKey();
        List<ImporterEntry> checked = taskService.checkEntries(List.of(
            new ImporterEntry("orc", "api", false, false),
            new ImporterEntry("orc-api", "api", true, true)), true);
        assertTrue(entry(checked, "orc-api").includeInSchedule());
        assertTrue(entry(checked, "orc-api").runAtStartup());
    }

    @Test
    void clashFromConfigFileKeepsFirst() throws Exception
    {
        writeKey();
        List<ImporterEntry> checked = taskService.checkEntries(List.of(
            new ImporterEntry("orc-api", "api", false, true),
            new ImporterEntry("orc", "api", true, false)), false);
        assertEquals(List.of(false, true),
            List.of(entry(checked, "orc-api").includeInSchedule(), entry(checked, "orc-api").runAtStartup()));
        assertEquals(List.of(false, false),
            List.of(entry(checked, "orc").includeInSchedule(), entry(checked, "orc").runAtStartup()));
    }

    @Test
    void startDropsFlagsOfUnavailableTaskFromAdminYaml() throws Exception
    {
        Files.createDirectories(tempDir.resolve("config"));
        Files.writeString(tempDir.resolve("config/admin.yaml"), """
            importers:
              - name: orc-api
                mode: api
                includeInSchedule: true
                runAtStartup: true
            """);
        taskService.start();
        ImporterEntry orcApi = entry(taskService.importerEntries(), "orc-api");
        assertFalse(orcApi.includeInSchedule());
        assertFalse(orcApi.runAtStartup());
    }
}
