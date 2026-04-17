package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.hpf.analysis.ConversionGraph;
import org.mortbay.sailing.hpf.analysis.HpfConfig;
import org.mortbay.sailing.hpf.importer.AmsImporter;
import org.mortbay.sailing.hpf.importer.ImporterLog;
import org.mortbay.sailing.hpf.importer.BwpsImporter;
import org.mortbay.sailing.hpf.importer.OrcImporter;
import org.mortbay.sailing.hpf.importer.SailSysImporter;
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
import java.util.List;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskService
{
    private static final Logger LOG = LoggerFactory.getLogger(TaskService.class);
    private static final YAMLMapper MAPPER = YAMLMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
        .disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    private static final String CONFIG_FILE_HEADER =
        """
            # IMPORTANT: This file is managed by the server.
            # It is overwritten whenever settings are saved via the admin UI.
            # Only edit manually when the server is NOT running.
            
            # --- Server Import and Analysis tasks ---
            """;

    /** Comments inserted above specific keys in the serialized YAML. */
    private static final Map<String, String> CONFIG_COMMENTS = new LinkedHashMap<>();
    static
    {
        CONFIG_COMMENTS.put("sailsysNextRaceId:",           "# --- SailSys importer ---");
        CONFIG_COMMENTS.put("bwpsMinYear:",                 "# --- BWPS importer ---");
        CONFIG_COMMENTS.put("orcListMaxAgeDays:",           "# --- ORC importer ---");
        CONFIG_COMMENTS.put("minAnalysisR2:",               "# --- Analysis ---");
        CONFIG_COMMENTS.put("hpfLambda:",                   "# --- HPF optimiser ---");
        CONFIG_COMMENTS.put("slidingAverageCount:",         "# --- Sliding average / consistency ---");
        CONFIG_COMMENTS.put("diversityNonSpinWeight:",      "# --- Diversity weights (multi-variant HPF) ---");
        CONFIG_COMMENTS.put("googleClientId:",              "# --- Authentication ---");
        CONFIG_COMMENTS.put("adminPort:",                   "# --- Server ports ---");
    }

    /** Post-processes a serialized YAML string to insert section comments. */
    private static String addConfigComments(String yaml)
    {
        StringBuilder sb = new StringBuilder(CONFIG_FILE_HEADER);
        for (String line : yaml.split("\n", -1))
        {
            String trimmed = line.stripLeading();
            for (Map.Entry<String, String> e : CONFIG_COMMENTS.entrySet())
            {
                if (trimmed.startsWith(e.getKey()))
                {
                    sb.append('\n').append(e.getValue()).append('\n');
                    break;
                }
            }
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

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
    private volatile boolean scheduledRunActive = false;

    public record ImportStatus(String importerName, String mode, Instant startedAt) {}

    public record ImporterEntry(String name, String mode, boolean includeInSchedule, boolean runAtStartup) {}

    public record GlobalSchedule(List<DayOfWeek> days, LocalTime time) {}

    private record AdminConfig(List<ImporterEntry> importers, GlobalSchedule schedule,
                               Integer targetIrcYear,
                               Double outlierSigma,
                               Integer recentRaceReimportDays,      // null → default 90
                               Integer sailsysNextRaceId, Integer sailsysEndRaceId,
                               Integer sailsysYoungCacheMaxAgeDays, // null → default 7
                               Integer sailsysOldCacheMaxAgeDays,   // null → default 352
                               Integer sailsysYoungRaceMaxAgeDays,  // null → default 365
                               Integer sailsysHttpDelayMs,          // null → default 200
                               Integer sailsysRecentRaceDays,       // null → default 14
                               Integer bwpsMinYear,                 // null → default 2020
                               Integer orcListMaxAgeDays,           // null → default 1
                               Double minAnalysisR2,             // null → default 0.50
                               Double clubCertificateWeight,     // null → default 0.9
                               Double hpfLambda,                 // null → default 1.0
                               Double hpfOutlierK,               // null → default 2.0
                               Double hpfAsymmetryFactor,        // null → default 2.0
                               Double hpfOuterDampingFactor,           // null → default 0.5
                               Double hpfOuterConvergenceThreshold,    // null → default 0.01
                               Double hpfConvergenceThreshold,         // null → default 0.0001
                               Integer hpfMaxInnerIterations,    // null → default 100
                               Integer hpfMaxOuterIterations,    // null → default 5
                               Double hpfCrossVariantLambda,     // null → default 0.0
                               Integer slidingAverageCount,       // null → default 8
                               Integer slidingAverageDrops,       // null → default 0
                               Double diversityNonSpinWeight,     // null → default 0.8
                               Double diversitySpinWeight,        // null → default 1.0
                               Double diversityTwoHandedWeight,   // null → default 1.2
                               Integer consistencyDropInterval,   // null → default 11
                               String googleClientId,            // null → fall back to env/devMode
                               String googleClientSecret,        // null → fall back to env
                               String authBaseUrl,               // null → fall back to env, then localhost
                               String authAllowedDomain,         // null → no domain restriction
                               Integer adminPort,                // null → default 8888
                               Integer userPort,                 // null → default 8080
                               String natGatewayIp)              // null → no gateway protection
    {}

    private static final List<ImporterEntry> DEFAULT_ENTRIES = List.of(
        new ImporterEntry("sailsys-races",      "run",  false, false),
        new ImporterEntry("orc",                "api",  false, false),
        new ImporterEntry("ams",                "api",  false, false),
        new ImporterEntry("topyacht",           "api",  false, false),
        new ImporterEntry("bwps",               "api",  false, false),
        new ImporterEntry("analysis",           "run",  false, false),
        new ImporterEntry("reference-factors",  "run",  false, false),
        new ImporterEntry("build-indexes",      "run",  false, false),
        new ImporterEntry("hpf-optimise",       "run",  false, false)
    );

    private List<ImporterEntry> importerEntries = new ArrayList<>(DEFAULT_ENTRIES);
    private GlobalSchedule globalSchedule = new GlobalSchedule(List.of(), LocalTime.of(3, 0));
    private ScheduledFuture<?> scheduledFuture;
    private volatile Integer sailsysNextRaceId = null;    // null = start from 1
    private volatile Integer sailsysEndRaceId  = null;    // null = use large default
    private volatile Integer targetIrcYear = null;          // null = auto-detect from data
    private volatile Double outlierSigma = null;            // null = use default (2.5)
    private volatile int recentRaceReimportDays = 30;
    private volatile int sailsysYoungCacheMaxAgeDays = 7;
    private volatile int sailsysOldCacheMaxAgeDays = 352;
    private volatile int sailsysYoungRaceMaxAgeDays = 365;
    private volatile int sailsysHttpDelayMs = 200;
    private volatile int sailsysRecentRaceDays = 14;
    private volatile int bwpsMinYear = BwpsImporter.DEFAULT_MIN_YEAR;
    private volatile int orcListMaxAgeDays = 1;
    private volatile double minAnalysisR2 = ConversionGraph.DEFAULT_MIN_R2;
    private volatile double clubCertificateWeight = 0.9;
    private volatile double hpfLambda = 1.0;
    private volatile double hpfOutlierK = 2.0;
    private volatile double hpfAsymmetryFactor = 2.0;
    private volatile double hpfOuterDampingFactor = 0.5;
    private volatile double hpfOuterConvergenceThreshold = 0.01;
    private volatile double hpfConvergenceThreshold = 0.0001;
    private volatile int hpfMaxInnerIterations = 100;
    private volatile int hpfMaxOuterIterations = 5;
    private volatile double hpfCrossVariantLambda = 0.0;
    private volatile int slidingAverageCount = 8;
    private volatile int slidingAverageDrops = 0;
    private volatile double diversityNonSpinWeight   = 0.8;
    private volatile double diversitySpinWeight      = 1.0;
    private volatile double diversityTwoHandedWeight = 1.2;
    private volatile int    consistencyDropInterval  = 11;
    private volatile String googleClientId = null;
    private volatile String googleClientSecret = null;
    private volatile String authBaseUrl = null;
    private volatile String authAllowedDomain = null;
    private volatile int adminPort = 8888;
    private volatile int userPort = 8080;
    private volatile String natGatewayIp = null;

    public TaskService(DataStore store, HttpClient httpClient, Path dataRoot)
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
            if (config.sailsysNextRaceId() != null)
                sailsysNextRaceId = config.sailsysNextRaceId();
            if (config.sailsysEndRaceId() != null)
                sailsysEndRaceId = config.sailsysEndRaceId();
            targetIrcYear = config.targetIrcYear();   // null is valid (auto-detect)
            outlierSigma = config.outlierSigma();    // null is valid (use default 2.5)
            if (config.recentRaceReimportDays() != null) recentRaceReimportDays = config.recentRaceReimportDays();
            if (config.sailsysYoungCacheMaxAgeDays() != null) sailsysYoungCacheMaxAgeDays = config.sailsysYoungCacheMaxAgeDays();
            if (config.sailsysOldCacheMaxAgeDays() != null) sailsysOldCacheMaxAgeDays = config.sailsysOldCacheMaxAgeDays();
            if (config.sailsysYoungRaceMaxAgeDays() != null) sailsysYoungRaceMaxAgeDays = config.sailsysYoungRaceMaxAgeDays();
            if (config.sailsysHttpDelayMs() != null) sailsysHttpDelayMs = config.sailsysHttpDelayMs();
            if (config.sailsysRecentRaceDays() != null) sailsysRecentRaceDays = config.sailsysRecentRaceDays();
            if (config.bwpsMinYear() != null) bwpsMinYear = config.bwpsMinYear();
            if (config.orcListMaxAgeDays() != null) orcListMaxAgeDays = config.orcListMaxAgeDays();
            if (config.minAnalysisR2() != null) minAnalysisR2 = config.minAnalysisR2();
            if (config.clubCertificateWeight() != null) clubCertificateWeight = config.clubCertificateWeight();
            if (config.hpfLambda() != null) hpfLambda = config.hpfLambda();
            if (config.hpfOutlierK() != null) hpfOutlierK = config.hpfOutlierK();
            if (config.hpfAsymmetryFactor() != null) hpfAsymmetryFactor = config.hpfAsymmetryFactor();
            if (config.hpfOuterDampingFactor() != null) hpfOuterDampingFactor = config.hpfOuterDampingFactor();
            if (config.hpfOuterConvergenceThreshold() != null) hpfOuterConvergenceThreshold = config.hpfOuterConvergenceThreshold();
            if (config.hpfConvergenceThreshold() != null) hpfConvergenceThreshold = config.hpfConvergenceThreshold();
            if (config.hpfMaxInnerIterations() != null) hpfMaxInnerIterations = config.hpfMaxInnerIterations();
            if (config.hpfMaxOuterIterations() != null) hpfMaxOuterIterations = config.hpfMaxOuterIterations();
            if (config.hpfCrossVariantLambda() != null) hpfCrossVariantLambda = config.hpfCrossVariantLambda();
            if (config.slidingAverageCount() != null) slidingAverageCount = config.slidingAverageCount();
            if (config.slidingAverageDrops() != null) slidingAverageDrops = config.slidingAverageDrops();
            if (config.diversityNonSpinWeight()   != null) diversityNonSpinWeight   = config.diversityNonSpinWeight();
            if (config.diversitySpinWeight()      != null) diversitySpinWeight      = config.diversitySpinWeight();
            if (config.diversityTwoHandedWeight() != null) diversityTwoHandedWeight = config.diversityTwoHandedWeight();
            if (config.consistencyDropInterval()  != null) consistencyDropInterval  = config.consistencyDropInterval();
            googleClientId     = config.googleClientId();
            googleClientSecret = config.googleClientSecret();
            authBaseUrl        = config.authBaseUrl();
            authAllowedDomain  = config.authAllowedDomain();
            if (config.adminPort() != null) adminPort = config.adminPort();
            if (config.userPort() != null) userPort = config.userPort();
            natGatewayIp = config.natGatewayIp();
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
                    persistsailsysNextRaceId(name);
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
                                       Integer sailsysStartRaceId, Integer sailsysEndRaceId,
                                       Integer targetIrcYear, Double outlierSigma,
                                       Double hpfLambda, Double hpfConvergenceThreshold,
                                       Integer hpfMaxInnerIterations, Integer hpfMaxOuterIterations,
                                       Double hpfOutlierK, Double hpfAsymmetryFactor,
                                       Double hpfOuterDampingFactor, Double hpfOuterConvergenceThreshold,
                                       Double hpfCrossVariantLambda)
    {
        importerEntries = new ArrayList<>(entries);
        globalSchedule = schedule;
        if (sailsysStartRaceId != null) sailsysNextRaceId = sailsysStartRaceId;
        this.sailsysEndRaceId = sailsysEndRaceId;
        this.targetIrcYear = targetIrcYear;
        this.outlierSigma = outlierSigma;
        if (hpfLambda != null) this.hpfLambda = hpfLambda;
        if (hpfConvergenceThreshold != null) this.hpfConvergenceThreshold = hpfConvergenceThreshold;
        if (hpfMaxInnerIterations != null) this.hpfMaxInnerIterations = hpfMaxInnerIterations;
        if (hpfMaxOuterIterations != null) this.hpfMaxOuterIterations = hpfMaxOuterIterations;
        if (hpfOutlierK != null) this.hpfOutlierK = hpfOutlierK;
        if (hpfAsymmetryFactor != null) this.hpfAsymmetryFactor = hpfAsymmetryFactor;
        if (hpfOuterDampingFactor != null) this.hpfOuterDampingFactor = hpfOuterDampingFactor;
        if (hpfOuterConvergenceThreshold != null) this.hpfOuterConvergenceThreshold = hpfOuterConvergenceThreshold;
        if (hpfCrossVariantLambda != null) this.hpfCrossVariantLambda = hpfCrossVariantLambda;
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
        cache.setDiversityWeights(diversityNonSpinWeight, diversitySpinWeight, diversityTwoHandedWeight);
        cache.setConsistencyDropInterval(consistencyDropInterval);
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

    public boolean isScheduledRunActive()
    {
        return scheduledRunActive;
    }

    public Integer sailsysNextRaceId()
    {
        return sailsysNextRaceId;
    }

    public Integer sailsysEndRaceId()
    {
        return sailsysEndRaceId;
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

    public double minAnalysisR2()
    {
        return minAnalysisR2;
    }

    public double clubCertificateWeight()
    {
        return clubCertificateWeight;
    }

    public double hpfLambda() { return hpfLambda; }
    public double hpfOutlierK() { return hpfOutlierK; }
    public double hpfAsymmetryFactor() { return hpfAsymmetryFactor; }
    public double hpfOuterDampingFactor() { return hpfOuterDampingFactor; }
    public double hpfOuterConvergenceThreshold() { return hpfOuterConvergenceThreshold; }
    public int slidingAverageCount() { return slidingAverageCount; }
    public int slidingAverageDrops() { return slidingAverageDrops; }
    public double hpfConvergenceThreshold() { return hpfConvergenceThreshold; }
    public int hpfMaxInnerIterations() { return hpfMaxInnerIterations; }
    public int hpfMaxOuterIterations() { return hpfMaxOuterIterations; }

    public AuthConfig authConfig()
    {
        String id     = firstNonBlank(System.getenv("GOOGLE_CLIENT_ID"),     googleClientId);
        String secret = firstNonBlank(System.getenv("GOOGLE_CLIENT_SECRET"), googleClientSecret);
        String base   = firstNonBlank(System.getenv("AUTH_BASE_URL"),        authBaseUrl,
                                      "http://localhost:" + userPort);
        String domain = firstNonBlank(System.getenv("AUTH_ALLOWED_DOMAIN"),  authAllowedDomain);
        return new AuthConfig(id, secret, base, domain, adminPort, userPort, natGatewayIp);
    }

    private static String firstNonBlank(String... candidates)
    {
        for (String s : candidates)
            if (s != null && !s.isBlank()) return s;
        return null;
    }

    public boolean submitScheduledRun()
    {
        List<ImporterEntry> toRun = importerEntries.stream()
            .filter(ImporterEntry::includeInSchedule).toList();
        if (toRun.isEmpty())
            return false;
        if (!running.compareAndSet(false, true))
        {
            LOG.warn("Scheduled run skipped — import already running");
            return false;
        }
        scheduledRunActive = true;
        stopRequested.set(false);
        importExecutor.submit(() ->
        {
            try
            {
                for (ImporterEntry entry : toRun)
                {
                    if (stopRequested.get())

                    {
                        LOG.info("Scheduled run stopped by request before {}", entry.name());
                        break;
                    }
                    currentSailSysId = 0;
                    currentStatus = new ImportStatus(entry.name(), entry.mode(), Instant.now());
                    LOG.info("Scheduled: importer={} mode={}", entry.name(), entry.mode());
                    int startId = "sailsys-races".equals(entry.name()) && sailsysNextRaceId != null
                        ? sailsysNextRaceId
                        : 1;
                    runImporter(entry.name(), entry.mode(), startId);
                    persistsailsysNextRaceId(entry.name());
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
                scheduledRunActive = false;
                running.set(false);
            }
        });
        return true;
    }

    /**
     * Runs tasks marked {@code runAtStartup=true}, in order, asynchronously.
     * Called once from HpfServer after the cache and all services are initialised.
     */
    public void runStartupTasks()
    {
        List<ImporterEntry> toRun = importerEntries.stream()
            .filter(ImporterEntry::runAtStartup).toList();
        if (toRun.isEmpty())
            return;
        if (!running.compareAndSet(false, true))
        {
            LOG.warn("Startup run skipped — import already running");
            return;
        }
        stopRequested.set(false);
        importExecutor.submit(() ->
        {
            try
            {
                for (ImporterEntry entry : toRun)
                {
                    if (stopRequested.get())
                    {
                        LOG.info("Startup run stopped by request before {}", entry.name());
                        break;
                    }
                    currentSailSysId = 0;
                    currentStatus = new ImportStatus(entry.name(), entry.mode(), Instant.now());
                    LOG.info("Startup: importer={} mode={}", entry.name(), entry.mode());
                    int startId = "sailsys-races".equals(entry.name()) && sailsysNextRaceId != null
                        ? sailsysNextRaceId : 1;
                    runImporter(entry.name(), entry.mode(), startId);
                    persistsailsysNextRaceId(entry.name());
                    store.save();
                }
                LOG.info("Startup run complete");
            }
            catch (Exception e)
            {
                LOG.error("Startup run failed", e);
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

    private static final java.util.Set<String> IMPORTER_NAMES = java.util.Set.of(
        "sailsys-races", "orc", "ams", "topyacht", "bwps");

    private void runImporter(String name, String mode, int startId) throws Exception
    {
        if (IMPORTER_NAMES.contains(name))
            ImporterLog.open(dataRoot.resolve("log"), name);
        try
        {
            runImporterSwitch(name, mode, startId);
        }
        finally
        {
            if (IMPORTER_NAMES.contains(name))
                ImporterLog.close();
        }
    }

    private void runImporterSwitch(String name, String mode, int startId) throws Exception
    {
        switch (name)
        {
            case "sailsys-races" ->
            {
                Path racesDir = dataRoot.resolve("cache/sailsys/races");
                int endId = sailsysEndRaceId != null ? sailsysEndRaceId : 99999;
                SailSysImporter.RunResult result = new SailSysImporter(store, httpClient).run(
                    startId, endId, id -> currentSailSysId = id, stopRequested::get,
                    racesDir, sailsysYoungCacheMaxAgeDays, sailsysOldCacheMaxAgeDays,
                    sailsysYoungRaceMaxAgeDays, sailsysHttpDelayMs,
                    sailsysRecentRaceDays);
                if (result.minRecentId() > 0)
                    currentSailSysId = result.minRecentId() - 1;
                if (result.maxFoundId() > 0)
                    sailsysEndRaceId = result.maxFoundId() + 100;
            }
            case "orc" -> new OrcImporter(store, httpClient).run(dataRoot.resolve("cache/orc"), orcListMaxAgeDays);
            case "ams" -> new AmsImporter(store, httpClient).run();
            case "topyacht" -> new TopYachtImporter(store, httpClient).run(recentRaceReimportDays);
            case "bwps"     -> new BwpsImporter(store, httpClient).run(recentRaceReimportDays, bwpsMinYear);
            case "analysis" ->
            {
                if (cache != null)
                    cache.refresh(targetIrcYear, outlierSigma, clubCertificateWeight, minAnalysisR2);
                else
                    LOG.warn("Analysis requested but cache is not configured");
            }
            case "reference-factors" ->
            {
                if (cache != null)
                    cache.refreshReferenceFactors(targetIrcYear, clubCertificateWeight, minAnalysisR2);
                else
                    LOG.warn("Reference factors requested but cache is not configured");
            }
            case "build-indexes" ->
            {
                if (cache != null)
                    cache.refreshIndexes();
                else
                    LOG.warn("Build indexes requested but cache is not configured");
            }
            case "hpf-optimise" ->
            {
                if (cache != null)
                    cache.refreshHpf(hpfConfig(), stopRequested::get);
                else
                    LOG.warn("HPF optimise requested but cache is not configured");
            }
            default -> throw new IllegalArgumentException("Unknown importer: " + name);
        }
    }

    private void persistsailsysNextRaceId(String name)
    {
        if (currentSailSysId > 0 && "sailsys-races".equals(name))
        {
            sailsysNextRaceId = currentSailSysId + 1;
            persistConfig();
        }
    }

    private HpfConfig hpfConfig()
    {
        return new HpfConfig(hpfLambda, hpfConvergenceThreshold,
            hpfMaxInnerIterations, hpfMaxOuterIterations,
            hpfOutlierK, hpfAsymmetryFactor, hpfOuterDampingFactor, hpfOuterConvergenceThreshold,
            hpfCrossVariantLambda);
    }

    private void persistConfig()
    {
        try
        {
            Files.createDirectories(configFile.getParent());
            String yaml = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(
                new AdminConfig(importerEntries, globalSchedule,
                    targetIrcYear, outlierSigma,
                    recentRaceReimportDays,
                    sailsysNextRaceId, sailsysEndRaceId,
                    sailsysYoungCacheMaxAgeDays, sailsysOldCacheMaxAgeDays, sailsysYoungRaceMaxAgeDays,
                    sailsysHttpDelayMs, sailsysRecentRaceDays, bwpsMinYear,
                    orcListMaxAgeDays,
                    minAnalysisR2, clubCertificateWeight, hpfLambda, hpfOutlierK, hpfAsymmetryFactor,
                    hpfOuterDampingFactor, hpfOuterConvergenceThreshold, hpfConvergenceThreshold, hpfMaxInnerIterations, hpfMaxOuterIterations,
                    hpfCrossVariantLambda,
                    slidingAverageCount, slidingAverageDrops,
                    diversityNonSpinWeight, diversitySpinWeight, diversityTwoHandedWeight,
                    consistencyDropInterval,
                    googleClientId, googleClientSecret, authBaseUrl, authAllowedDomain,
                    adminPort, userPort, natGatewayIp));
            Files.writeString(configFile, addConfigComments(yaml));
        }
        catch (IOException e)
        {
            LOG.warn("Failed to persist admin.yaml: {}", e.getMessage());
        }
    }
}
