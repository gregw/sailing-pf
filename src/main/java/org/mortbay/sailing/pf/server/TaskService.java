package org.mortbay.sailing.pf.server;

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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.pf.analysis.ConversionGraph;
import org.mortbay.sailing.pf.analysis.PfConfig;
import org.mortbay.sailing.pf.importer.AmsImporter;
import org.mortbay.sailing.pf.importer.BwpsImporter;
import org.mortbay.sailing.pf.importer.ImporterLog;
import org.mortbay.sailing.pf.importer.OrcImporter;
import org.mortbay.sailing.pf.importer.SailSysImporter;
import org.mortbay.sailing.pf.importer.TopYachtImporter;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CONFIG_COMMENTS.put("sailsysYoungCacheMaxAgeDays:", "# --- SailSys importer ---");
        CONFIG_COMMENTS.put("bwpsMinYear:",                 "# --- BWPS importer ---");
        CONFIG_COMMENTS.put("orcListMaxAgeDays:",           "# --- ORC importer ---");
        CONFIG_COMMENTS.put("minAnalysisR2:",               "# --- Analysis ---");
        CONFIG_COMMENTS.put("pfLambda:",                   "# --- PF optimiser ---");
        CONFIG_COMMENTS.put("slidingAverageCount:",         "# --- Sliding average / consistency ---");
        CONFIG_COMMENTS.put("diversityNonSpinWeight:",      "# --- Diversity weights (multi-variant PF) ---");
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
    private volatile boolean scheduledRunActive = false;

    public record ImportStatus(String importerName, String mode, Instant startedAt) {}

    public record ImporterEntry(String name, String mode, boolean includeInSchedule, boolean runAtStartup) {}

    /**
     * Importers that can be restricted to a subset of clubs, in the order a club-scoped
     * run executes them. Only TopYacht is club-scoped today; SailSys and others can be
     * added here (plus a case in {@link #runClubScopedImporter}) when they gain the
     * ability to filter by club.
     */
    private static final List<String> CLUB_SCOPED_IMPORTERS = List.of("topyacht", "sailsys");

    /**
     * Analysis rebuilt after a club-scoped import, in order.
     * <p>
     * Writing a race or boat <em>deletes</em> its derived data from {@link AnalysisCache}
     * (see its {@code onRaceChanged}/{@code onBoatChanged}) rather than recomputing it, so an
     * import leaves holes wherever it touched — and the race detail page renders nothing for
     * a race with no {@code RaceDerived}. The scheduled run never shows this because it always
     * follows its importers with the same chain; an on-demand club import has to do the same
     * or it leaves the UI looking broken.
     */
    private static final List<String> POST_IMPORT_ANALYSIS = List.of(
        "build-indexes", "analysis", "reference-factors", "pf-optimise");

    /**
     * One on-demand, club-restricted import run, as triggered by the "Import races"
     * button on the clubs page. Holds live state while the run is in progress and the
     * full captured log afterwards so the UI can offer it as a download.
     */
    public static final class ClubImportRun
    {
        private final String id;
        private final List<String> clubIds;
        private final List<String> importers;
        private final Instant startedAt;
        private volatile Instant finishedAt;
        private volatile String state = "running";   // running | done | failed
        private volatile String phase = "importing"; // importing | <analysis task> | done
        private volatile String error;
        private final Map<String, Object> counts = new LinkedHashMap<>();
        private volatile ImporterLog.Capture capture;

        ClubImportRun(String id, List<String> clubIds, List<String> importers)
        {
            this.id = id;
            this.clubIds = List.copyOf(clubIds);
            this.importers = List.copyOf(importers);
            this.startedAt = Instant.now();
        }

        public String id()
        {
            return id;
        }

        public List<String> clubIds()
        {
            return clubIds;
        }

        public List<String> importers()
        {
            return importers;
        }

        public Instant startedAt()
        {
            return startedAt;
        }

        public Instant finishedAt()
        {
            return finishedAt;
        }

        public String state()
        {
            return state;
        }

        public String phase()
        {
            return phase;
        }

        public String error()
        {
            return error;
        }

        public boolean running()
        {
            return "running".equals(state);
        }

        public synchronized Map<String, Object> counts()
        {
            return new LinkedHashMap<>(counts);
        }

        synchronized void putCount(String key, Object value)
        {
            counts.put(key, value);
        }

        public int warnings()
        {
            return capture != null ? capture.warnings() : 0;
        }

        public int errors()
        {
            return capture != null ? capture.errors() : 0;
        }

        public String log()
        {
            return capture != null ? capture.text() : "";
        }
    }

    /**
     * Most recent club-scoped import runs, newest last, capped at {@link #MAX_CLUB_RUNS}.
     */
    private static final int MAX_CLUB_RUNS = 10;
    private final java.util.concurrent.atomic.AtomicInteger clubRunSequence =
        new java.util.concurrent.atomic.AtomicInteger();
    private final Map<String, ClubImportRun> clubImportRuns =
        java.util.Collections.synchronizedMap(new LinkedHashMap<>()
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ClubImportRun> eldest)
            {
                return size() > MAX_CLUB_RUNS;
            }
        });

    public record GlobalSchedule(List<DayOfWeek> days, LocalTime time) {}

    private record AdminConfig(List<ImporterEntry> importers, GlobalSchedule schedule,
                               Integer targetIrcYear,
                               Double outlierSigma,
                               Integer recentRaceReimportDays,      // null → default 90
                               Integer sailsysYoungCacheMaxAgeDays, // null → default 7
                               Integer sailsysOldCacheMaxAgeDays,   // null → default 352
                               Integer sailsysYoungRaceMaxAgeDays,  // null → default 365
                               Integer sailsysHttpDelayMs,          // null → default 200
                               Integer sailsysRecentRaceDays,       // null → default 14
                               Integer bwpsMinYear,                 // null → default 2020
                               Integer orcListMaxAgeDays,           // null → default 1
                               Double minAnalysisR2,             // null → default 0.50
                               Integer minAnalysisPairs,         // null → default 8
                               Double clubCertificateWeight,     // null → default 0.9
                               Double pfLambda,                 // null → default 1.0
                               Double pfOutlierK,               // null → default 2.0
                               Double pfAsymmetryFactor,        // null → default 2.0
                               Double pfOuterDampingFactor,           // null → default 0.5
                               Double pfOuterConvergenceThreshold,    // null → default 0.01
                               Double pfConvergenceThreshold,         // null → default 0.0001
                               Integer pfMaxInnerIterations,    // null → default 100
                               Integer pfMaxOuterIterations,    // null → default 5
                               Double pfCrossVariantLambda,     // null → default 0.0
                               Double pfGraphCrossVariantLambda, // null → default 0.25
                               Double pfNoRaceFallbackWeight,   // null → default 0.2
                               Double pfOuterPfConvergenceThreshold, // null → default 1.0e-3
                               Boolean pfLogOuterDiagnostics,        // null → false
                               Double pfDubiousFactor,               // null → default 1.5
                               Double pfMaxFactor,                   // null → default 2.0
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
                               String natGatewayIp,              // null → no gateway protection
                               Map<String, Instant> lastRunTimes) // null → empty
    {}

    private static final List<ImporterEntry> DEFAULT_ENTRIES = List.of(
        new ImporterEntry("sailsys",            "api",  false, false),
        new ImporterEntry("orc",                "api",  false, false),
        new ImporterEntry("ams",                "api",  false, false),
        new ImporterEntry("topyacht",           "api",  false, false),
        new ImporterEntry("bwps",               "api",  false, false),
        new ImporterEntry("analysis",           "run",  false, false),
        new ImporterEntry("reference-factors",  "run",  false, false),
        new ImporterEntry("build-indexes",      "run",  false, false),
        new ImporterEntry("pf-optimise",       "run",  false, false),
        new ImporterEntry("save-database", "run", false, false),
        new ImporterEntry("clear-cache-orc", "run", false, false),
        new ImporterEntry("clear-cache-sailsys", "run", false, false)
    );

    private List<ImporterEntry> importerEntries = new ArrayList<>(DEFAULT_ENTRIES);
    private GlobalSchedule globalSchedule = new GlobalSchedule(List.of(), LocalTime.of(3, 0));
    private ScheduledFuture<?> scheduledFuture;
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
    private volatile int minAnalysisPairs = ConversionGraph.DEFAULT_MIN_PAIRS;
    private volatile double clubCertificateWeight = 0.9;
    private volatile double pfLambda = 1.0;
    private volatile double pfOutlierK = 2.0;
    private volatile double pfAsymmetryFactor = 2.0;
    private volatile double pfOuterDampingFactor = 0.5;
    private volatile double pfOuterConvergenceThreshold = 0.01;
    private volatile double pfConvergenceThreshold = 0.0001;
    private volatile int pfMaxInnerIterations = 100;
    private volatile int pfMaxOuterIterations = 5;
    private volatile double pfCrossVariantLambda = 0.0;
    private volatile double pfGraphCrossVariantLambda = 0.25;
    private volatile double pfNoRaceFallbackWeight = 0.2;
    private volatile double pfOuterPfConvergenceThreshold = 1.0e-3;
    private volatile boolean pfLogOuterDiagnostics = false;
    private volatile double pfDubiousFactor = 1.5;
    private volatile double pfMaxFactor = 2.0;
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
    private final Map<String, Instant> lastRunTimes = new ConcurrentHashMap<>();

    /**
     * Lines from {@code pf-data/log/user-requests.log} marked by an admin for removal.
     * Cleared when {@link #pruneUserRequestsLog()} rewrites the log file.
     */
    private final java.util.Set<String> tickedUserRequests =
        java.util.concurrent.ConcurrentHashMap.newKeySet();

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
                // Drop entries for tasks that no longer exist, so a retired importer (e.g. the
                // old "sailsys-races" scanner) does not linger in the UI offering a Run button
                // that can only throw.
                Set<String> known = DEFAULT_ENTRIES.stream()
                    .map(ImporterEntry::name).collect(java.util.stream.Collectors.toSet());
                importerEntries = new ArrayList<>();
                for (ImporterEntry e : config.importers())
                {
                    if (known.contains(e.name()))
                        importerEntries.add(e);
                    else
                        LOG.info("Dropping retired task '{}' from admin.yaml", e.name());
                }
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
            if (config.minAnalysisPairs() != null)
                minAnalysisPairs = config.minAnalysisPairs();
            if (config.clubCertificateWeight() != null) clubCertificateWeight = config.clubCertificateWeight();
            if (config.pfLambda() != null) pfLambda = config.pfLambda();
            if (config.pfOutlierK() != null) pfOutlierK = config.pfOutlierK();
            if (config.pfAsymmetryFactor() != null) pfAsymmetryFactor = config.pfAsymmetryFactor();
            if (config.pfOuterDampingFactor() != null) pfOuterDampingFactor = config.pfOuterDampingFactor();
            if (config.pfOuterConvergenceThreshold() != null) pfOuterConvergenceThreshold = config.pfOuterConvergenceThreshold();
            if (config.pfConvergenceThreshold() != null) pfConvergenceThreshold = config.pfConvergenceThreshold();
            if (config.pfMaxInnerIterations() != null) pfMaxInnerIterations = config.pfMaxInnerIterations();
            if (config.pfMaxOuterIterations() != null) pfMaxOuterIterations = config.pfMaxOuterIterations();
            if (config.pfCrossVariantLambda() != null) pfCrossVariantLambda = config.pfCrossVariantLambda();
            if (config.pfGraphCrossVariantLambda() != null)
                pfGraphCrossVariantLambda = config.pfGraphCrossVariantLambda();
            if (config.pfNoRaceFallbackWeight() != null)
                pfNoRaceFallbackWeight = config.pfNoRaceFallbackWeight();
            if (config.pfOuterPfConvergenceThreshold() != null)
                pfOuterPfConvergenceThreshold = config.pfOuterPfConvergenceThreshold();
            if (config.pfLogOuterDiagnostics() != null)
                pfLogOuterDiagnostics = config.pfLogOuterDiagnostics();
            if (config.pfDubiousFactor() != null)
                pfDubiousFactor = config.pfDubiousFactor();
            if (config.pfMaxFactor() != null)
                pfMaxFactor = config.pfMaxFactor();
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
            if (config.lastRunTimes() != null)
                lastRunTimes.putAll(config.lastRunTimes());
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

        try
        {
            importExecutor.submit(() ->
            {
                try
                {
                    currentStatus = new ImportStatus(name, mode, Instant.now());
                    LOG.info("Starting importer={} mode={} startId={}", name, mode, startId);
                    runImporter(name, mode, startId);
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

    /**
     * Submits an on-demand import restricted to the given clubs, running every
     * {@link #CLUB_SCOPED_IMPORTERS} importer in turn. Returns the new run (whose state
     * is {@code running}), or null if another import is already in progress — the caller
     * should then send 409.
     */
    public ClubImportRun submitClubImport(List<String> clubIds)
    {
        if (clubIds == null || clubIds.isEmpty())
            throw new IllegalArgumentException("clubIds must not be empty");
        if (!running.compareAndSet(false, true))
            return null;

        ClubImportRun run = new ClubImportRun(
            Long.toString(System.currentTimeMillis(), 36) + "-" + clubRunSequence.incrementAndGet(),
            clubIds, CLUB_SCOPED_IMPORTERS);
        clubImportRuns.put(run.id(), run);
        stopRequested.set(false);

        try
        {
            importExecutor.submit(() ->
            {
                run.capture = ImporterLog.openCapture();
                String finalState = "done";
                try
                {
                    Set<String> ids = new java.util.LinkedHashSet<>(clubIds);
                    ImporterLog.info(LOG, "Club import started for {} club(s): {}", ids.size(), ids);
                    for (String name : CLUB_SCOPED_IMPORTERS)
                    {
                        if (stopRequested.get())
                        {
                            ImporterLog.warn(LOG, "Club import stopped by request before {}", name);
                            break;
                        }
                        currentStatus = new ImportStatus(name, "club", Instant.now());
                        lastRunTimes.put(name + "/club", Instant.now());
                        ImporterLog.open(dataRoot.resolve("log"), name);
                        try
                        {
                            runClubScopedImporter(name, ids, run);
                        }
                        finally
                        {
                            ImporterLog.close();
                        }
                    }
                    store.save();

                    // Rebuild what the import invalidated, or the UI is left with holes
                    for (String task : POST_IMPORT_ANALYSIS)
                    {
                        if (stopRequested.get())
                        {
                            ImporterLog.warn(LOG, "Club import stopped by request before {}", task);
                            break;
                        }
                        run.phase = task;
                        currentStatus = new ImportStatus(task, "club", Instant.now());
                        ImporterLog.info(LOG, "Club import: {}", task);
                        runImporterSwitch(task, "run", 1);
                    }
                    store.save();
                    ImporterLog.info(LOG, "Club import complete: {}", run.counts());
                }
                catch (Exception e)
                {
                    finalState = "failed";
                    run.error = e.toString();
                    ImporterLog.error(LOG, "Club import failed: {}", e.toString());
                    LOG.error("Club import failed", e);
                }
                finally
                {
                    run.finishedAt = Instant.now();
                    ImporterLog.closeCapture();
                    currentStatus = null;
                    persistConfig();
                    running.set(false);
                    run.phase = "done";
                    // Published last: a poller (or test) that sees the run finished can
                    // rely on the import slot already having been released.
                    run.state = finalState;
                }
            });
            return run;
        }
        catch (Exception e)
        {
            run.state = "failed";
            run.error = e.toString();
            run.finishedAt = Instant.now();
            running.set(false);
            return run;
        }
    }

    /**
     * Runs one club-scoped importer, recording its tallies on the run.
     */
    private void runClubScopedImporter(String name, Set<String> clubIds, ClubImportRun run)
        throws Exception
    {
        switch (name)
        {
            case "topyacht" ->
            {
                TopYachtImporter.RunResult result =
                    new TopYachtImporter(store, httpClient).run(recentRaceReimportDays, clubIds);
                run.putCount("topyacht.clubs", result.clubs());
                run.putCount("topyacht.series", result.series());
                run.putCount("topyacht.racesImported", result.racesImported());
                run.putCount("topyacht.racesAlreadyPresent", result.racesAlreadyPresent());
                run.putCount("topyacht.finishers", result.finishers());
            }
            case "sailsys" ->
            {
                SailSysImporter.RunResult result = new SailSysImporter(store, httpClient)
                    .withCache(dataRoot.resolve("cache/sailsys/races"),
                        sailsysYoungCacheMaxAgeDays, sailsysOldCacheMaxAgeDays,
                        sailsysYoungRaceMaxAgeDays, sailsysHttpDelayMs, sailsysRecentRaceDays)
                    .run(recentRaceReimportDays, clubIds);
                run.putCount("sailsys.clubs", result.clubs());
                run.putCount("sailsys.series", result.series());
                run.putCount("sailsys.racesImported", result.racesImported());
                run.putCount("sailsys.racesAlreadyPresent", result.racesAlreadyPresent());
                run.putCount("sailsys.finishers", result.finishers());
                if (result.seriesCollisions() > 0)
                    run.putCount("sailsys.seriesCollisions", result.seriesCollisions());
                if (result.racesSkipped() > 0)
                    run.putCount("sailsys.racesSkipped", result.racesSkipped());
            }
            default -> throw new IllegalArgumentException("Importer is not club-scoped: " + name);
        }
    }

    public ClubImportRun clubImportRun(String id)
    {
        return clubImportRuns.get(id);
    }

    public synchronized void setConfig(List<ImporterEntry> entries, GlobalSchedule schedule,
                                       Integer targetIrcYear, Double outlierSigma,
                                       Double pfLambda, Double pfConvergenceThreshold,
                                       Integer pfMaxInnerIterations, Integer pfMaxOuterIterations,
                                       Double pfOutlierK, Double pfAsymmetryFactor,
                                       Double pfOuterDampingFactor, Double pfOuterConvergenceThreshold,
                                       Double pfCrossVariantLambda,
                                       Double pfGraphCrossVariantLambda,
                                       Double pfNoRaceFallbackWeight,
                                       Double pfOuterPfConvergenceThreshold,
                                       Boolean pfLogOuterDiagnostics,
                                       Double pfDubiousFactor,
                                       Double pfMaxFactor)
    {
        importerEntries = new ArrayList<>(entries);
        globalSchedule = schedule;
        this.targetIrcYear = targetIrcYear;
        this.outlierSigma = outlierSigma;
        if (pfLambda != null) this.pfLambda = pfLambda;
        if (pfConvergenceThreshold != null) this.pfConvergenceThreshold = pfConvergenceThreshold;
        if (pfMaxInnerIterations != null) this.pfMaxInnerIterations = pfMaxInnerIterations;
        if (pfMaxOuterIterations != null) this.pfMaxOuterIterations = pfMaxOuterIterations;
        if (pfOutlierK != null) this.pfOutlierK = pfOutlierK;
        if (pfAsymmetryFactor != null) this.pfAsymmetryFactor = pfAsymmetryFactor;
        if (pfOuterDampingFactor != null) this.pfOuterDampingFactor = pfOuterDampingFactor;
        if (pfOuterConvergenceThreshold != null) this.pfOuterConvergenceThreshold = pfOuterConvergenceThreshold;
        if (pfCrossVariantLambda != null) this.pfCrossVariantLambda = pfCrossVariantLambda;
        if (pfGraphCrossVariantLambda != null)
            this.pfGraphCrossVariantLambda = pfGraphCrossVariantLambda;
        if (pfNoRaceFallbackWeight != null)
            this.pfNoRaceFallbackWeight = pfNoRaceFallbackWeight;
        if (pfOuterPfConvergenceThreshold != null)
            this.pfOuterPfConvergenceThreshold = pfOuterPfConvergenceThreshold;
        if (pfLogOuterDiagnostics != null)
            this.pfLogOuterDiagnostics = pfLogOuterDiagnostics;
        if (pfDubiousFactor != null)
            this.pfDubiousFactor = pfDubiousFactor;
        if (pfMaxFactor != null)
            this.pfMaxFactor = pfMaxFactor;
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

    public void requestStop()
    {
        stopRequested.set(true);
    }

    public boolean isScheduledRunActive()
    {
        return scheduledRunActive;
    }

    public List<ImporterEntry> importerEntries()
    {
        return List.copyOf(importerEntries);
    }

    public Map<String, Instant> lastRunTimes()
    {
        return Map.copyOf(lastRunTimes);
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

    public int minAnalysisPairs()
    {
        return minAnalysisPairs;
    }

    public double clubCertificateWeight()
    {
        return clubCertificateWeight;
    }

    public double pfLambda() { return pfLambda; }
    public double pfOutlierK() { return pfOutlierK; }
    public double pfAsymmetryFactor() { return pfAsymmetryFactor; }
    public double pfOuterDampingFactor() { return pfOuterDampingFactor; }
    public double pfOuterConvergenceThreshold() { return pfOuterConvergenceThreshold; }

    public double pfOuterPfConvergenceThreshold()
    {
        return pfOuterPfConvergenceThreshold;
    }

    public boolean pfLogOuterDiagnostics()
    {
        return pfLogOuterDiagnostics;
    }
    public int slidingAverageCount() { return slidingAverageCount; }
    public int slidingAverageDrops() { return slidingAverageDrops; }
    public double pfConvergenceThreshold() { return pfConvergenceThreshold; }
    public int pfMaxInnerIterations() { return pfMaxInnerIterations; }
    public int pfMaxOuterIterations() { return pfMaxOuterIterations; }

    public double pfCrossVariantLambda()
    {
        return pfCrossVariantLambda;
    }

    public double pfGraphCrossVariantLambda()
    {
        return pfGraphCrossVariantLambda;
    }

    public double pfNoRaceFallbackWeight()
    {
        return pfNoRaceFallbackWeight;
    }

    public double pfDubiousFactor()
    {
        return pfDubiousFactor;
    }

    public double pfMaxFactor()
    {
        return pfMaxFactor;
    }

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
                                currentStatus = new ImportStatus(entry.name(), entry.mode(), Instant.now());
                    LOG.info("Scheduled: importer={} mode={}", entry.name(), entry.mode());
                    runImporter(entry.name(), entry.mode(), 1);
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
     * Called once from PfServer after the cache and all services are initialised,
     * and on demand from the admin "Run Now" button under the On Start column.
     *
     * @return true if a run was submitted, false if no tasks are flagged or another
     *         run is already in progress.
     */
    public boolean runStartupTasks()
    {
        List<ImporterEntry> toRun = importerEntries.stream()
            .filter(ImporterEntry::runAtStartup).toList();
        if (toRun.isEmpty())
            return false;
        if (!running.compareAndSet(false, true))
        {
            LOG.warn("Startup run skipped — import already running");
            return false;
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
                                currentStatus = new ImportStatus(entry.name(), entry.mode(), Instant.now());
                    LOG.info("Startup: importer={} mode={}", entry.name(), entry.mode());
                    runImporter(entry.name(), entry.mode(), 1);
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
        return true;
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
        "sailsys", "orc", "ams", "topyacht", "bwps");

    private void runImporter(String name, String mode, int startId) throws Exception
    {
        lastRunTimes.put(name + "/" + mode, Instant.now());
        persistConfig();
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
            case "orc" -> new OrcImporter(store, httpClient).run(dataRoot.resolve("cache/orc"), orcListMaxAgeDays);
            case "ams" -> new AmsImporter(store, httpClient).run();
            case "topyacht" -> new TopYachtImporter(store, httpClient).run(recentRaceReimportDays);
            case "bwps"     -> new BwpsImporter(store, httpClient).run(recentRaceReimportDays, bwpsMinYear);
            case "analysis" ->
            {
                if (cache != null)
                    cache.refresh(targetIrcYear, outlierSigma, clubCertificateWeight, minAnalysisR2, minAnalysisPairs);
                else
                    LOG.warn("Analysis requested but cache is not configured");
            }
            case "reference-factors" ->
            {
                if (cache != null)
                    cache.refreshReferenceFactors(targetIrcYear, clubCertificateWeight, minAnalysisR2, minAnalysisPairs);
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
            case "pf-optimise" ->
            {
                if (cache != null)
                    cache.refreshPf(pfConfig(), stopRequested::get);
                else
                    LOG.warn("PF optimise requested but cache is not configured");
            }
            case "save-database" ->
            {
                store.save();
                persistConfig();
                pruneUserRequestsLog();
            }
            case "clear-cache-orc" -> clearCacheDir(dataRoot.resolve("cache/orc"));
            case "clear-cache-sailsys" -> clearCacheDir(dataRoot.resolve("cache/sailsys"));
            default -> throw new IllegalArgumentException("Unknown importer: " + name);
        }
    }

    /**
     * Recursively deletes the contents of a cache directory, keeping the directory itself.
     */
    private static void clearCacheDir(Path dir) throws IOException
    {
        if (!Files.exists(dir))
        {
            LOG.info("Cache directory {} absent, nothing to clear", dir);
            return;
        }
        try (var stream = Files.walk(dir))
        {
            for (Path p : stream.sorted(Comparator.reverseOrder()).toList())
            {
                if (!p.equals(dir))
                    Files.delete(p);
            }
        }
        LOG.info("Cleared cache directory {}", dir);
    }

    private PfConfig pfConfig()
    {
        return new PfConfig(pfLambda, pfConvergenceThreshold,
            pfMaxInnerIterations, pfMaxOuterIterations,
            pfOutlierK, pfAsymmetryFactor, pfOuterDampingFactor, pfOuterConvergenceThreshold,
            pfCrossVariantLambda, pfGraphCrossVariantLambda,
            pfNoRaceFallbackWeight,
            pfOuterPfConvergenceThreshold, pfLogOuterDiagnostics,
            pfDubiousFactor, pfMaxFactor);
    }

    /**
     * Reads the user-requests log file (one request per line) and returns its lines.
     * Returns an empty list if the file does not yet exist.
     */
    public List<String> readUserRequestsLog()
    {
        Path logFile = dataRoot.resolve("log").resolve("user-requests.log");
        if (!Files.exists(logFile))
            return List.of();
        try
        {
            return Files.readAllLines(logFile);
        }
        catch (IOException e)
        {
            LOG.warn("Failed to read user-requests.log: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Returns the set of log lines currently marked for removal on the next save.
     */
    public java.util.Set<String> getTickedUserRequests()
    {
        return java.util.Set.copyOf(tickedUserRequests);
    }

    /**
     * Marks or unmarks a user-request log line for removal on the next save.
     */
    public void setUserRequestTicked(String line, boolean ticked)
    {
        if (line == null)
            return;
        if (ticked)
            tickedUserRequests.add(line);
        else
            tickedUserRequests.remove(line);
    }

    /**
     * Rewrites the user-requests log file, dropping any lines currently ticked, then clears the
     * tick set. Invoked from the save-database task.
     */
    public void pruneUserRequestsLog()
    {
        if (tickedUserRequests.isEmpty())
            return;
        Path logFile = dataRoot.resolve("log").resolve("user-requests.log");
        if (!Files.exists(logFile))
        {
            tickedUserRequests.clear();
            return;
        }
        try
        {
            List<String> kept = new ArrayList<>();
            for (String line : Files.readAllLines(logFile))
            {
                if (!tickedUserRequests.contains(line))
                    kept.add(line);
            }
            StringBuilder out = new StringBuilder();
            for (String line : kept)
            {
                out.append(line).append('\n');
            }
            Files.writeString(logFile, out.toString());
            tickedUserRequests.clear();
        }
        catch (IOException e)
        {
            LOG.warn("Failed to prune user-requests.log: {}", e.getMessage());
        }
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
                    sailsysYoungCacheMaxAgeDays, sailsysOldCacheMaxAgeDays, sailsysYoungRaceMaxAgeDays,
                    sailsysHttpDelayMs, sailsysRecentRaceDays, bwpsMinYear,
                    orcListMaxAgeDays,
                    minAnalysisR2, minAnalysisPairs, clubCertificateWeight, pfLambda, pfOutlierK, pfAsymmetryFactor,
                    pfOuterDampingFactor, pfOuterConvergenceThreshold, pfConvergenceThreshold, pfMaxInnerIterations, pfMaxOuterIterations,
                    pfCrossVariantLambda, pfGraphCrossVariantLambda,
                    pfNoRaceFallbackWeight,
                    pfOuterPfConvergenceThreshold, pfLogOuterDiagnostics,
                    pfDubiousFactor, pfMaxFactor,
                    slidingAverageCount, slidingAverageDrops,
                    diversityNonSpinWeight, diversitySpinWeight, diversityTwoHandedWeight,
                    consistencyDropInterval,
                    googleClientId, googleClientSecret, authBaseUrl, authAllowedDomain,
                    adminPort, userPort, natGatewayIp,
                    new LinkedHashMap<>(lastRunTimes)));
            Files.writeString(configFile, addConfigComments(yaml));
        }
        catch (IOException e)
        {
            LOG.warn("Failed to persist admin.yaml: {}", e.getMessage());
        }
    }
}
