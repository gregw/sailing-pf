package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.importer.AmsImporter;
import org.mortbay.sailing.hpf.importer.BwpsImporter;
import org.mortbay.sailing.hpf.importer.OrcImporter;
import org.mortbay.sailing.hpf.importer.SailSysBoatImporter;
import org.mortbay.sailing.hpf.importer.SailSysRaceImporter;
import org.mortbay.sailing.hpf.importer.TopYachtImporter;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImporterService
{
    private static final Logger LOG = LoggerFactory.getLogger(ImporterService.class);
    private static final YAMLMapper MAPPER = YAMLMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
        .build();

    private final DataStore store;
    private final HttpClient httpClient;
    private final Path dataRoot;
    private final Path configFile;
    private volatile AnalysisCache cache;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final ExecutorService importExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile ImportStatus currentStatus;
    private volatile int currentSailSysId;

    public record ImportStatus(String importerName, String mode, Instant startedAt) {}

    public record ImporterEntry(String name, String mode, boolean includeInSchedule) {}

    public record GlobalSchedule(List<DayOfWeek> days, LocalTime time) {}

    private record AdminConfig(List<ImporterEntry> importers, GlobalSchedule schedule,
                               Map<String, Integer> lastSailSysIds, Integer targetIrcYear,
                               Double outlierSigma) {}

    private static final List<ImporterEntry> DEFAULT_ENTRIES = List.of(
        new ImporterEntry("sailsys-boats", "directory", false),
        new ImporterEntry("sailsys-boats", "api",       false),
        new ImporterEntry("sailsys-races", "directory", false),
        new ImporterEntry("sailsys-races", "api",       false),
        new ImporterEntry("orc",           "api",       false),
        new ImporterEntry("ams",           "api",       false),
        new ImporterEntry("topyacht",            "api",       false),
        new ImporterEntry("bwps",               "api",       false),
        new ImporterEntry("analysis",           "run",       false),
        new ImporterEntry("reference-factors",  "run",       false)
    );

    private List<ImporterEntry> importerEntries = new ArrayList<>(DEFAULT_ENTRIES);
    private GlobalSchedule globalSchedule = new GlobalSchedule(List.of(), LocalTime.of(3, 0));
    private ScheduledFuture<?> scheduledFuture;
    private volatile Map<String, Integer> lastSailSysIds = Map.of();
    private volatile Integer targetIrcYear = null;   // null = auto-detect from data
    private volatile Double outlierSigma = null;     // null = use default (2.5)

    public ImporterService(DataStore store, HttpClient httpClient, Path dataRoot)
    {
        this.store = store;
        this.httpClient = httpClient;
        this.dataRoot = dataRoot;
        this.configFile = dataRoot.resolve("config/admin.yaml");
    }

    public void start()
    {
        if (!Files.exists(configFile))
            return;

        try
        {
            AdminConfig config = MAPPER.readValue(configFile.toFile(), AdminConfig.class);
            if (config.importers() != null)
            {
                importerEntries = new ArrayList<>(config.importers());
                // Append any default entries not present in the saved config
                for (ImporterEntry def : DEFAULT_ENTRIES)
                {
                    boolean present = importerEntries.stream()
                        .anyMatch(e -> e.name().equals(def.name()) && e.mode().equals(def.mode()));
                    if (!present)
                        importerEntries.add(def);
                }
            }
            if (config.schedule() != null)
                globalSchedule = config.schedule();
            if (config.lastSailSysIds() != null)
                lastSailSysIds = Map.copyOf(config.lastSailSysIds());
            targetIrcYear = config.targetIrcYear();   // null is valid (auto-detect)
            outlierSigma = config.outlierSigma();    // null is valid (use default 2.5)
            if (globalSchedule != null && !globalSchedule.days().isEmpty())
                armSchedule();
            LOG.info("Loaded admin config from {}", configFile);
        }
        catch (IOException e)
        {
            LOG.warn("Failed to load admin.yaml: {}", e.getMessage());
        }
    }

public void stop()
    {
        scheduler.shutdown();
        importExecutor.shutdown();
    }

    /**
     * Submit an import job. Returns false (caller should send 409) if one is already running.
     * startId is only used for SailSys api-mode importers; ignored otherwise.
     */
    public boolean submit(String name, String mode, int startId)
    {
        if (!running.compareAndSet(false, true))
            return false;

        stopRequested.set(false);
        currentSailSysId = 0;

        try
        {
            importExecutor.submit(() ->
            {
                try
                {
                    currentStatus = new ImportStatus(name, mode, Instant.now());
                    LOG.info("Starting importer={} mode={} startId={}", name, mode, startId);
                    runImporter(name, mode, startId);
                    persistLastSailSysId(name, mode);
                    store.save();
                    LOG.info("Finished importer={}", name);
                }
                catch (Exception e)
                {
                    LOG.error("Importer {} failed", name, e);
                }
                finally
                {
                    currentStatus = null;
                    running.set(false);
                }
            });
            return true;
        }
        catch (Exception e)
        {
            running.set(false);
            return false;
        }
    }

    public synchronized void setConfig(List<ImporterEntry> entries, GlobalSchedule schedule,
                                       Integer targetIrcYear, Double outlierSigma)
    {
        importerEntries = new ArrayList<>(entries);
        globalSchedule = schedule;
        this.targetIrcYear = targetIrcYear;
        this.outlierSigma = outlierSigma;
        if (scheduledFuture != null)
        {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
        }
        if (schedule != null && !schedule.days().isEmpty())
            armSchedule();
        persistConfig();
    }

    public void setCache(AnalysisCache cache)
    {
        this.cache = cache;
    }

    public ImportStatus currentStatus()
    {
        return currentStatus;
    }

    public int currentSailSysId()
    {
        return currentSailSysId;
    }

    public void requestStop()
    {
        stopRequested.set(true);
    }

    public Map<String, Integer> lastSailSysIds()
    {
        return lastSailSysIds;
    }

    public List<ImporterEntry> importerEntries()
    {
        return List.copyOf(importerEntries);
    }

    public GlobalSchedule globalSchedule()
    {
        return globalSchedule;
    }

    public Integer targetIrcYear()
    {
        return targetIrcYear;
    }

    public Double outlierSigma()
    {
        return outlierSigma;
    }

    public void submitScheduledRun()
    {
        List<ImporterEntry> toRun = importerEntries.stream()
            .filter(ImporterEntry::includeInSchedule).toList();
        if (toRun.isEmpty())
            return;
        if (!running.compareAndSet(false, true))
        {
            LOG.warn("Scheduled run skipped — import already running");
            return;
        }
        importExecutor.submit(() ->
        {
            try
            {
                for (ImporterEntry entry : toRun)
                {
                    currentSailSysId = 0;
                    currentStatus = new ImportStatus(entry.name(), entry.mode(), Instant.now());
                    LOG.info("Scheduled: importer={} mode={}", entry.name(), entry.mode());
                    runImporter(entry.name(), entry.mode(), 1);
                    persistLastSailSysId(entry.name(), entry.mode());
                    store.save();
                }
                LOG.info("Scheduled run complete");
            }
            catch (Exception e)
            {
                LOG.error("Scheduled run failed", e);
            }
            finally
            {
                currentStatus = null;
                running.set(false);
            }
        });
    }

    private void armSchedule()
    {
        Duration delay = delayUntilNextOccurrence(globalSchedule.days(), globalSchedule.time());
        LOG.info("Scheduling next run at {} on one of {} (delay={})", globalSchedule.time(), globalSchedule.days(), delay);
        scheduledFuture = scheduler.schedule(() ->
        {
            submitScheduledRun();
            GlobalSchedule current = globalSchedule;
            if (current != null && !current.days().isEmpty())
                armSchedule();
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Duration delayUntilNextOccurrence(List<DayOfWeek> days, LocalTime time)
    {
        LocalDateTime now = LocalDateTime.now();
        return days.stream()
            .map(day ->
            {
                LocalDateTime next = now.with(TemporalAdjusters.nextOrSame(day)).with(time);
                if (!next.isAfter(now))
                    next = now.with(TemporalAdjusters.next(day)).with(time);
                return Duration.between(now, next);
            })
            .min(Comparator.naturalOrder())
            .orElseThrow();
    }

    private void runImporter(String name, String mode, int startId) throws Exception
    {
        switch (name)
        {
            case "sailsys-boats" ->
            {
                Path boatsDir = dataRoot.resolve("sailsys/boats");
                SailSysBoatImporter importer = new SailSysBoatImporter(store, httpClient);
                if ("api".equals(mode))
                    importer.runFromApi(startId, id -> currentSailSysId = id, stopRequested::get, boatsDir);
                else
                    importer.runFromDirectory(boatsDir);
            }
            case "sailsys-races" ->
            {
                Path racesDir = dataRoot.resolve("sailsys/races");
                SailSysRaceImporter importer = new SailSysRaceImporter(store, httpClient);
                if ("api".equals(mode))
                    importer.runFromApi(startId, id -> currentSailSysId = id, stopRequested::get, racesDir);
                else
                    importer.runFromDirectory(racesDir);
            }
            case "orc" -> new OrcImporter(store, httpClient).run();
            case "ams" -> new AmsImporter(store, httpClient).run();
            case "topyacht" -> new TopYachtImporter(store, httpClient).run();
            case "bwps"     -> new BwpsImporter(store, httpClient).run();
            case "analysis" ->
            {
                if (cache != null)
                    cache.refresh(targetIrcYear, outlierSigma);
                else
                    LOG.warn("Analysis requested but cache is not configured");
            }
            case "reference-factors" ->
            {
                if (cache != null)
                    cache.refreshReferenceFactors(targetIrcYear);
                else
                    LOG.warn("Reference factors requested but cache is not configured");
            }
            default -> throw new IllegalArgumentException("Unknown importer: " + name);
        }
    }

    private void persistLastSailSysId(String name, String mode)
    {
        if (currentSailSysId > 0 && "api".equals(mode)
                && ("sailsys-boats".equals(name) || "sailsys-races".equals(name)))
        {
            Map<String, Integer> updated = new LinkedHashMap<>(lastSailSysIds);
            updated.put(name + "-api", currentSailSysId);
            lastSailSysIds = Map.copyOf(updated);
            persistConfig();
        }
    }

    private void persistConfig()
    {
        try
        {
            Files.createDirectories(configFile.getParent());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                configFile.toFile(),
                new AdminConfig(importerEntries, globalSchedule, new LinkedHashMap<>(lastSailSysIds),
                    targetIrcYear, outlierSigma));
        }
        catch (IOException e)
        {
            LOG.warn("Failed to persist admin.yaml: {}", e.getMessage());
        }
    }
}
