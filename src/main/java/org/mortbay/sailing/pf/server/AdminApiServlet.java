package org.mortbay.sailing.pf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mortbay.sailing.pf.analysis.BoatDerived;
import org.mortbay.sailing.pf.analysis.BoatPf;
import org.mortbay.sailing.pf.analysis.DesignDerived;
import org.mortbay.sailing.pf.analysis.EntryResidual;
import org.mortbay.sailing.pf.analysis.PfQuality;
import org.mortbay.sailing.pf.analysis.PerformanceProfile;
import org.mortbay.sailing.pf.analysis.ReferenceFactors;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.mortbay.sailing.pf.store.Aliases;
import org.mortbay.sailing.pf.store.DataStore;

import java.io.IOException;
import java.nio.file.Path;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AdminApiServlet extends HttpServlet
{
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    private static final int DEFAULT_PAGE_SIZE = 50;

    private final DataStore store;
    private final TaskService _taskService;
    private final AnalysisCache cache;

    public AdminApiServlet(DataStore store, TaskService taskService, AnalysisCache cache)
    {
        this.store = store;
        this._taskService = taskService;
        this.cache = cache;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String path = req.getPathInfo();
        if (path == null)
            path = "/";

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        if ("/stats".equals(path))
            handleStats(resp);
        else if (path.matches("/boats/[^/]+/pf"))
            handleBoatPf(path.replaceAll("^/boats/|/pf$", ""), resp);
        else if (path.matches("/boats/[^/]+/reference"))
            handleBoatReference(path.replaceAll("^/boats/|/reference$", ""), resp);
        else if (path.matches("/boats/[^/]+/aliases"))
            handleBoatAliases(path.replaceAll("^/boats/|/aliases$", ""), resp);
        else if (path.startsWith("/boats"))
            handleBoats(path.substring("/boats".length()), req, resp);
        else if (path.startsWith("/designs"))
            handleDesigns(path.substring("/designs".length()), req, resp);
        else if (path.startsWith("/clubs"))
            handleClubs(path.substring("/clubs".length()), req, resp);
        else if (path.startsWith("/races"))
            handleRaces(path.substring("/races".length()), req, resp);
        else if ("/series/chart".equals(path))
            handleSeriesChart(req, resp);
        else if (path.startsWith("/series"))
            handleSeries(path.substring("/series".length()), req, resp);
        else if ("/pf/quality".equals(path))
            handlePfQuality(resp);
        else if ("/comparison/candidates".equals(path))
            handleComparisonCandidates(req, resp);
        else if ("/comparison/chart".equals(path))
            handleComparisonChart(req, resp);
        else if ("/comparison/division".equals(path))
            handleComparisonDivision(req, resp);
        else if ("/comparison/elapsed-chart".equals(path))
            handleElapsedComparisonChart(req, resp);
        else if ("/design-comparison/candidates".equals(path))
            handleDesignComparisonCandidates(req, resp);
        else if ("/design-comparison/chart".equals(path))
            handleDesignComparisonChart(req, resp);
        else if ("/importers/status".equals(path))
            handleImporterStatus(resp);
        else if ("/importers".equals(path))
            handleImporters(resp);
        else
            resp.sendError(404);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String path = req.getPathInfo();
        if (path == null)
            path = "/";

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        if ("/boats/exclude".equals(path))
            handleSetExcluded("boats", req, resp);
        else if ("/designs/exclude".equals(path))
            handleSetExcluded("designs", req, resp);
        else if ("/clubs/exclude".equals(path))
            handleSetClubExcluded(req, resp);
        else if ("/races/exclude".equals(path))
            handleSetExcluded("races", req, resp);
        else if ("/series/exclude".equals(path))
            handleSetSeriesExcluded(req, resp);
        else if (path.startsWith("/importers/") && path.endsWith("/run"))
        {
            String name = path.substring("/importers/".length(), path.length() - "/run".length());
            String mode = req.getParameter("mode");
            if (mode == null)
                mode = "api";
            handleImporterRun(name, mode, req, resp);
        }
        else if ("/importers/stop".equals(path))
        {
            _taskService.requestStop();
            resp.setStatus(200);
            writeJson(resp, Map.of("ok", true));
        }
        else if ("/importers/run-schedule".equals(path))
        {
            if (_taskService.submitScheduledRun())
            {
                resp.setStatus(202);
                writeJson(resp, Map.of("accepted", true));
            }
            else
            {
                resp.setStatus(409);
                writeJson(resp, Map.of("error", "An import is already running or no tasks are scheduled"));
            }
        }
        else if ("/importers/run-startup".equals(path))
        {
            if (_taskService.runStartupTasks())
            {
                resp.setStatus(202);
                writeJson(resp, Map.of("accepted", true));
            }
            else
            {
                resp.setStatus(409);
                writeJson(resp, Map.of("error", "An import is already running or no tasks are flagged for on-start"));
            }
        }
        else if ("/schedule".equals(path))
        {
            handleSetSchedule(req, resp);
        }
        else if ("/boats/merge".equals(path))
        {
            handleMergeBoats(req, resp);
        }
        else if ("/designs/merge".equals(path))
        {
            handleMergeDesigns(req, resp);
        }
        else if ("/boats/edit".equals(path))
        {
            handleEditBoat(req, resp);
        }
        else if ("/boats/merge-request".equals(path) || "/designs/merge-request".equals(path)
            || "/boats/edit-request".equals(path)
            || "/boats/exclude-request".equals(path) || "/designs/exclude-request".equals(path)
            || "/clubs/exclude-request".equals(path) || "/races/exclude-request".equals(path)
            || "/series/exclude-request".equals(path))
        {
            handleUserRequest(path, req, resp);
        }
        else
        {
            resp.sendError(404);
        }
    }

    /**
     * POST /api/{boats|designs|races}/exclude — sets the excluded flag for one or more entities.
     * <p>
     * Reads a JSON body with either {@code id} (String, single) or {@code ids} (String array)
     * plus {@code excluded} (boolean). Each id is delegated to the matching store setter
     * ({@code setBoatExcluded}, {@code setDesignExcluded}, {@code setRaceExcluded}), which
     * persists the change to {@code exclusions.yaml}. Returns {@code {ok, excluded, count}}
     * on success; 400 if no ids were supplied or the entity is unrecognised, 500 on store error.
     */
    @SuppressWarnings("unchecked")
    private void handleSetExcluded(String entity, HttpServletRequest req,
                                   HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            List<String> ids = readIds(body, "id", "ids");
            if (ids.isEmpty())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "id or ids is required"));
                return;
            }
            boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
            for (String id : ids)
            {
                switch (entity)
                {
                    case "boats"   -> store.setBoatExcluded(id, excluded);
                    case "designs" -> store.setDesignExcluded(id, excluded);
                    case "races"   -> store.setRaceExcluded(id, excluded);
                    default        -> { resp.sendError(400); return; }
                }
            }
            writeJson(resp, Map.of("ok", true, "excluded", excluded, "count", ids.size()));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/series/exclude — sets the excluded flag for one or more series, keyed by name.
     * Accepts either {@code name} (single) or {@code names} (array) plus {@code excluded}.
     */
    @SuppressWarnings("unchecked")
    private void handleSetSeriesExcluded(HttpServletRequest req,
                                       HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            List<String> names = readIds(body, "name", "names");
            if (names.isEmpty())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "name or names is required"));
                return;
            }
            boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
            for (String name : names)
                store.setSeriesExcluded(name, excluded);
            writeJson(resp, Map.of("ok", true, "excluded", excluded, "count", names.size()));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /** Read either {@code singleKey} (String) or {@code listKey} (List<String>) from the body. */
    @SuppressWarnings("unchecked")
    private static List<String> readIds(Map<String, Object> body, String singleKey, String listKey)
    {
        Object list = body.get(listKey);
        if (list instanceof List<?> l)
            return l.stream().filter(Objects::nonNull).map(Object::toString).toList();
        Object single = body.get(singleKey);
        if (single instanceof String s && !s.isBlank())
            return List.of(s);
        return List.of();
    }

    /**
     * GET /api/stats — returns aggregate entity counts for the dashboard header.
     * <p>
     * Merges the club seed with persisted club records to obtain the total club count,
     * then returns JSON with keys {@code races}, {@code boats}, {@code designs}, {@code clubs},
     * and {@code series} (non-catch-all, non-excluded series only). Each count has a matching
     * {@code …Excluded} key giving the number of entities of that type that are currently
     * excluded from analysis.
     */
    private void handleStats(HttpServletResponse resp) throws IOException
    {
        // Merge seed + persisted to get total club count
        Map<String, Club> allClubs = new LinkedHashMap<>(store.clubSeed());
        allClubs.putAll(store.clubs());
        long excludedClubs = allClubs.keySet().stream()
            .filter(store::isClubExcluded)
            .count();
        // Series: count non-catch-all series, splitting by excluded vs not.
        long seriesCount = 0, excludedSeries = 0;
        for (Club c : store.clubs().values())
        {
            if (c.series() == null) continue;
            for (Series s : c.series())
            {
                if (s.isCatchAll()) continue;
                if (store.isSeriesExcluded(s.name())) excludedSeries++;
                else seriesCount++;
            }
        }
        long excludedBoats = store.boats().values().stream()
            .filter(b -> store.isBoatExcluded(b.id())
                      || (b.designId() != null && store.isDesignExcluded(b.designId())))
            .count();
        long excludedDesigns = store.designs().keySet().stream()
            .filter(store::isDesignExcluded)
            .count();
        long excludedRaces = store.races().keySet().stream()
            .filter(store::isRaceExcluded)
            .count();
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("boats",          store.boats().size()   - excludedBoats);
        stats.put("boatsExcluded",  excludedBoats);
        stats.put("designs",        store.designs().size() - excludedDesigns);
        stats.put("designsExcluded", excludedDesigns);
        stats.put("clubs",          allClubs.size() - excludedClubs);
        stats.put("clubsExcluded",  excludedClubs);
        stats.put("series",         seriesCount);
        stats.put("seriesExcluded", excludedSeries);
        stats.put("races",          store.races().size()   - excludedRaces);
        stats.put("racesExcluded",  excludedRaces);
        writeJson(resp, stats);
    }

    /**
     * GET /api/boats[/{id}] — boat listing or single-boat detail.
     * <p>
     * <b>List</b> (sub is empty or "/"): supports pagination ({@code page}, {@code size}),
     * free-text search ({@code q} matches id or name), optional filters by {@code designId} or
     * {@code clubId}, and sort by {@code sailNumber}, {@code name}, {@code designId},
     * {@code clubId}, {@code spinRef}, {@code pf}, {@code finishes}, or {@code profile}.
     * The {@code dupeSails} flag restricts results to boats sharing a sail number with at least
     * one other boat. The {@code showExcluded} flag (default false) hides boats that are
     * individually excluded or whose design is excluded. The {@code excludeNulls} flag removes
     * rows where the sort column is null.
     * Each row includes the boat's spin RF, PF, finish count, performance profile score, and
     * excluded status from the analysis cache.
     * <p>
     * <b>Detail</b> (sub is "/{id}"): returns the raw {@link org.mortbay.sailing.pf.data.Boat}
     * record as JSON; 404 if not found.
     */
    private void handleBoats(String sub, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        if (sub.isEmpty() || "/".equals(sub))
        {
            int page = parseIntParam(req, "page", 0);
            int size = parseIntParam(req, "size", DEFAULT_PAGE_SIZE);
            String q = req.getParameter("q");
            String sort = req.getParameter("sort");
            boolean asc = !"desc".equals(req.getParameter("dir"));
            String lower = q != null && !q.isBlank() ? q.toLowerCase() : null;
            boolean dupeSails = "true".equals(req.getParameter("dupeSails"));
            boolean hideEmpty = "true".equals(req.getParameter("hideEmpty"));

            // Duplicate-sail filter operates on the full dataset, text search is applied on top.
            Set<String> candidateIds = null;
            if (dupeSails)
            {
                Set<String> dupeSailNums = store.boats().values().stream()
                    .collect(Collectors.groupingBy(Boat::sailNumber, Collectors.counting()))
                    .entrySet().stream()
                    .filter(e -> e.getValue() > 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
                candidateIds = store.boats().values().stream()
                    .filter(b -> dupeSailNums.contains(b.sailNumber()))
                    .map(Boat::id)
                    .collect(Collectors.toCollection(HashSet::new));
            }

            String filterDesignId = req.getParameter("designId");
            String filterClubId   = req.getParameter("clubId");
            boolean showExcluded = "true".equals(req.getParameter("showExcluded"));
            final Set<String> finalCandidateIds = candidateIds;
            List<Boat> all = store.boats().values().stream()
                .filter(b -> finalCandidateIds == null || finalCandidateIds.contains(b.id()))
                .filter(b -> filterDesignId == null || filterDesignId.equals(b.designId()))
                .filter(b -> filterClubId   == null || filterClubId.equals(b.clubId()))
                .filter(b -> showExcluded
                    || (!store.isBoatExcluded(b.id())
                        && (b.designId() == null || !store.isDesignExcluded(b.designId()))))
                .filter(b -> lower == null
                    || b.id().toLowerCase().contains(lower)
                    || b.name().toLowerCase().contains(lower))
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
            {
                Comparator<Boat> cmp = switch (sort)
                {
                    case "sailNumber" -> Comparator.comparing(Boat::sailNumber, Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                    case "name"       -> Comparator.comparing(Boat::name,       Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                    case "designId"   -> Comparator.comparing(Boat::designId,   Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                    case "clubId"     -> Comparator.comparing(Boat::clubId,     Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                    case "spinRef"      -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.referenceFactors() != null && bd2.referenceFactors().spin() != null) ? bd2.referenceFactors().spin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "nonSpinRef"   -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.referenceFactors() != null && bd2.referenceFactors().nonSpin() != null) ? bd2.referenceFactors().nonSpin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "twoHandedRef" -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.referenceFactors() != null && bd2.referenceFactors().twoHanded() != null) ? bd2.referenceFactors().twoHanded().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "pf"          -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.pf() != null && bd2.pf().spin() != null) ? bd2.pf().spin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "pfNonSpin"   -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.pf() != null && bd2.pf().nonSpin() != null) ? bd2.pf().nonSpin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "pfTwoHanded" -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.pf() != null && bd2.pf().twoHanded() != null) ? bd2.pf().twoHanded().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "finishes"   -> Comparator.comparingInt(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return bd2 != null ? bd2.raceIds().size() : 0; });
                    case "profile"    -> Comparator.comparingDouble(
                                            (Boat b2) -> { PerformanceProfile p2 = cache.profilesByBoatId().get(b2.id()); return p2 != null ? p2.overallScore() : 0.0; });
                    default           -> Comparator.comparing(Boat::id,         Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                };
                all.sort(asc ? cmp : cmp.reversed());
            }

            List<Map<String, Object>> rows = all.stream().map(b ->
            {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id",         b.id());
                row.put("sailNumber", b.sailNumber());
                row.put("name",       b.name());
                row.put("designId",   b.designId());
                row.put("designExcluded", b.designId() != null && store.isDesignExcluded(b.designId()));
                row.put("clubId",     b.clubId());
                putClubNaming(row, b.clubId());
                row.put("clubExcluded", b.clubId() != null && store.isClubExcluded(b.clubId()));
                BoatDerived bd = cache.boatDerived().get(b.id());
                ReferenceFactors rf = (bd != null) ? bd.referenceFactors() : null;
                row.put("spinRef",      rf != null && rf.spin()      != null ? factorMap(rf.spin())      : null);
                row.put("nonSpinRef",   rf != null && rf.nonSpin()   != null ? factorMap(rf.nonSpin())   : null);
                row.put("twoHandedRef", rf != null && rf.twoHanded() != null ? factorMap(rf.twoHanded()) : null);
                BoatPf pf = (bd != null) ? bd.pf() : null;
                row.put("pf",          pf != null && pf.spin()      != null ? factorMap(pf.spin())      : null);
                row.put("pfNonSpin",   pf != null && pf.nonSpin()   != null ? factorMap(pf.nonSpin())   : null);
                row.put("pfTwoHanded", pf != null && pf.twoHanded() != null ? factorMap(pf.twoHanded()) : null);
                row.put("finishes", bd != null ? bd.raceIds().size() : 0);
                PerformanceProfile prof = cache.profilesByBoatId().get(b.id());
                row.put("profile", prof != null ? prof.overallScore() : null);
                row.put("excluded", store.isBoatExcluded(b.id()));
                return row;
            }).collect(Collectors.toList());

            if (hideEmpty && sort != null && !sort.isBlank())
                rows.removeIf(r -> isEmptyValue(r.get(sort)));

            writeJson(resp, paginate(rows, page, size));
        }
        else
        {
            String id = sub.startsWith("/") ? sub.substring(1) : sub;
            Boat boat = store.boats().get(id);
            if (boat == null)
            {
                resp.sendError(404);
                return;
            }
            writeJson(resp, boat);
        }
    }

    /**
     * GET /api/boats/{id}/reference — returns the three-variant reference factors for a boat.
     * <p>
     * Reads the {@link org.mortbay.sailing.pf.analysis.ReferenceFactors} from the analysis
     * cache and serialises spin, nonSpin, and twoHanded factors (value, weight, generation) along
     * with the current target IRC year. Returns 404 if the boat ID is not in the store.
     * If no RF has been computed yet all factor fields are null.
     */
    private void handleBoatReference(String id, HttpServletResponse resp) throws IOException
    {
        if (store.boats().get(id) == null)
        {
            resp.sendError(404);
            return;
        }
        BoatDerived bd = cache.boatDerived().get(id);
        ReferenceFactors factors = bd != null ? bd.referenceFactors() : null;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatId", id);
        result.put("currentYear", cache.targetYear());
        result.put("spin",      factors != null ? factorMap(factors.spin(),      factors.spinGeneration())      : null);
        result.put("nonSpin",   factors != null ? factorMap(factors.nonSpin(),   factors.nonSpinGeneration())   : null);
        result.put("twoHanded", factors != null ? factorMap(factors.twoHanded(), factors.twoHandedGeneration()) : null);
        writeJson(resp, result);
    }

    /**
     * GET /api/boats/{id}/pf — returns the full PF detail panel for a boat.
     * <p>
     * Assembles a response containing:
     * <ul>
     *   <li>Spin, nonSpin, and twoHanded PF factors (value, weight, referenceDelta, raceCount)</li>
     *   <li>The corresponding RF factors for side-by-side comparison in the UI</li>
     *   <li>All per-race residuals from the last PF run (raceId, division, date, variant,
     *       residual, weight) for the boat's performance history chart</li>
     *   <li>The fleet-relative performance profile (diversity, consistency, overall score)
     *       if one has been computed</li>
     * </ul>
     * Returns 404 if the boat ID is unknown. All factor and profile fields are null if the
     * PF optimiser has not yet run.
     */
    private void handleBoatPf(String id, HttpServletResponse resp) throws IOException
    {
        Boat boat = store.boats().get(id);
        if (boat == null)
        {
            resp.sendError(404);
            return;
        }
        BoatDerived bd = cache.boatDerived().get(id);
        BoatPf pf = bd != null ? bd.pf() : null;
        ReferenceFactors rf = bd != null ? bd.referenceFactors() : null;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatId", id);
        result.put("boatName", boat.name());
        result.put("currentYear", cache.targetYear());

        result.put("spin", pfVariantMap(pf != null ? pf.spin() : null,
            pf != null ? pf.referenceDeltaSpin() : 0.0,
            pf != null ? pf.spinRaceCount() : 0));
        result.put("nonSpin", pfVariantMap(pf != null ? pf.nonSpin() : null,
            pf != null ? pf.referenceDeltaNonSpin() : 0.0,
            pf != null ? pf.nonSpinRaceCount() : 0));
        result.put("twoHanded", pfVariantMap(pf != null ? pf.twoHanded() : null,
            pf != null ? pf.referenceDeltaTwoHanded() : 0.0,
            pf != null ? pf.twoHandedRaceCount() : 0));

        // Reference factors for comparison (include generation for display)
        result.put("rfSpin",      rf != null ? factorMap(rf.spin(),      rf.spinGeneration())      : null);
        result.put("rfNonSpin",   rf != null ? factorMap(rf.nonSpin(),   rf.nonSpinGeneration())   : null);
        result.put("rfTwoHanded", rf != null ? factorMap(rf.twoHanded(), rf.twoHandedGeneration()) : null);

        // Residuals
        List<EntryResidual> residuals = cache.residualsByBoatId().get(id);
        if (residuals != null)
        {
            List<Map<String, Object>> resList = new ArrayList<>();
            for (EntryResidual r : residuals)
            {
                Race race = store.races().get(r.raceId());
                String rName = raceName(race);
                String sName = null;
                if (race != null && race.seriesIds() != null && !race.seriesIds().isEmpty())
                {
                    String sid = race.seriesIds().getFirst();
                    var club = store.clubs().get(race.clubId());
                    if (club != null && club.series() != null)
                        for (var s : club.series())
                            if (sid.equals(s.id())) { sName = s.name(); break; }
                    if (sName == null) sName = sid;
                }
                Map<String, Object> rm = new LinkedHashMap<>();
                rm.put("raceId",       r.raceId());
                rm.put("raceName",     rName);
                rm.put("seriesName",   sName);
                rm.put("division",     r.divisionName());
                rm.put("date",         r.raceDate().toString());
                rm.put("nonSpinnaker", r.nonSpinnaker());
                rm.put("twoHanded",    r.twoHanded());
                rm.put("residual",     r.residual());
                rm.put("weight",       r.weight());
                resList.add(rm);
            }
            result.put("residuals", resList);
        }
        else
        {
            result.put("residuals", List.of());
        }

        // Performance profile — computed fleet-wide after PF run, read from cache
        PerformanceProfile profile = cache.profilesByBoatId().get(id);
        if (profile != null)
            result.put("profile", profileMap(profile));

        writeJson(resp, result);
    }

    /**
     * GET /api/boats/{id}/aliases — returns the alias list for a boat from the alias seed.
     * <p>
     * Looks up the boat's normalised sail number and name in {@code aliases.yaml} via
     * {@link org.mortbay.sailing.pf.store.DataStore#boatAliases} and returns a JSON array
     * of objects with optional {@code sailNumber} and {@code name} fields. Returns an empty
     * array if no aliases are configured. Returns 404 if the boat ID is not in the store.
     */
    private void handleBoatAliases(String id, HttpServletResponse resp) throws IOException
    {
        Boat boat = store.boats().get(id);
        if (boat == null)
        {
            resp.sendError(404);
            return;
        }
        String normName = IdGenerator.normaliseName(boat.name());
        List<Aliases.SailNumberName> aliases = store.boatAliases(boat.sailNumber(), normName);
        List<Map<String, String>> result = new ArrayList<>();
        for (Aliases.SailNumberName snn : aliases)
        {
            Map<String, String> entry = new LinkedHashMap<>();
            if (snn.sailNumber() != null) entry.put("sailNumber", snn.sailNumber());
            if (snn.name() != null) entry.put("name", snn.name());
            result.add(entry);
        }
        writeJson(resp, result);
    }

    private Map<String, Object> pfVariantMap(Factor f, double referenceDelta, int raceCount)
    {
        if (f == null) return null;
        if (Double.isNaN(f.value()) || Double.isNaN(f.weight())) return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("value", f.value());
        m.put("weight", f.weight());
        m.put("referenceDelta", referenceDelta);
        m.put("raceCount", raceCount);
        return m;
    }

    private Map<String, Object> profileMap(PerformanceProfile p)
    {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("frequency",    p.frequency());
        m.put("diversity",    p.diversity());
        m.put("consistency",  p.consistency());
        m.put("stability",    p.stability());
        m.put("nonChaotic",   p.nonChaotic());
        m.put("overallScore", p.overallScore());
        return m;
    }

    /**
     * GET /api/clubs[/{id}] — club listing or single-club detail.
     * <p>
     * <b>List</b>: merges the in-memory club seed with persisted club records (persisted wins on
     * conflict), then filters and paginates. Supports free-text search ({@code q} matches id,
     * shortName, or longName), {@code showExcluded} (default false hides excluded clubs), and
     * {@code excludeNulls}. Each row includes race count, boat count, non-catch-all series count,
     * and the excluded flag.
     * <p>
     * <b>Detail</b>: returns the raw club record as JSON; 404 if not found.
     */
    private void handleClubs(String sub, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        if (sub.isEmpty() || "/".equals(sub))
        {
            int page = parseIntParam(req, "page", 0);
            int size = parseIntParam(req, "size", DEFAULT_PAGE_SIZE);
            String q = req.getParameter("q");
            String sort = req.getParameter("sort");
            boolean asc = !"desc".equals(req.getParameter("dir"));
            String lower = q != null && !q.isBlank() ? q.toLowerCase() : null;
            boolean showExcluded = "true".equals(req.getParameter("showExcluded"));
            boolean hideEmpty = "true".equals(req.getParameter("hideEmpty"));
            String filterId = req.getParameter("id");

            // Merge persisted clubs and seed stubs into a unified view
            Map<String, Club> all = new LinkedHashMap<>(store.clubSeed());
            all.putAll(store.clubs());  // persisted overrides seed

            List<Map<String, Object>> rows = all.values().stream()
                .filter(c -> filterId == null || filterId.equals(c.id()))
                .filter(c -> showExcluded || !c.excluded())
                .filter(c -> lower == null
                    || c.id().toLowerCase().contains(lower)
                    || (c.shortName() != null && c.shortName().toLowerCase().contains(lower))
                    || (c.longName() != null && c.longName().toLowerCase().contains(lower)))
                .map(c ->
                {
                    // Count races and boats for this club
                    long raceCount = store.races().values().stream()
                        .filter(r -> c.id().equals(r.clubId()))
                        .count();
                    long boatCount = store.boats().values().stream()
                        .filter(b -> c.id().equals(b.clubId()))
                        .count();

                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", c.id());
                    row.put("shortName", c.shortName());
                    row.put("longName", c.longName());
                    row.put("state", c.state());
                    long seriesCount = c.series() == null ? 0
                        : c.series().stream().filter(s -> !s.isCatchAll()).count();
                    row.put("boats", boatCount > 0 ? boatCount : null);
                    row.put("series", seriesCount > 0 ? seriesCount : null);
                    row.put("races", raceCount);
                    row.put("excluded", c.excluded());
                    return row;
                })
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
                rows.sort(mapComparator(sort, asc));

            if (hideEmpty && sort != null && !sort.isBlank())
                rows.removeIf(r -> isEmptyValue(r.get(sort)));

            writeJson(resp, paginate(rows, page, size));
        }
        else
        {
            String id = sub.startsWith("/") ? sub.substring(1) : sub;
            // Check persisted clubs first, then seed
            Club club = store.clubs().get(id);
            if (club == null)
                club = store.clubSeed().get(id);
            if (club == null)
            {
                resp.sendError(404);
                return;
            }
            writeJson(resp, club);
        }
    }

    /**
     * POST /api/clubs/exclude — toggles the excluded flag on a club.
     * <p>
     * Reads a JSON body with {@code id} and {@code excluded}. Unlike boats/designs/races,
     * club exclusion is stored on the club record itself (not in {@code exclusions.json}) and
     * persisted via {@link org.mortbay.sailing.pf.store.DataStore#setClubExcluded}. Excluded
     * clubs are hidden from the data browser by default and their races are skipped during
     * RF and PF computation. Responds 400 if {@code id} is missing.
     */
    @SuppressWarnings("unchecked")
    private void handleSetClubExcluded(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            List<String> ids = readIds(body, "id", "ids");
            if (ids.isEmpty())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "id or ids is required"));
                return;
            }
            boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
            for (String id : ids)
                store.setClubExcluded(id, excluded);
            writeJson(resp, Map.of("ok", true, "excluded", excluded, "count", ids.size()));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/boats/merge — merges two or more duplicate boat records into one.
     * <p>
     * Reads a JSON body with {@code keepId} (the canonical boat to retain) and
     * {@code mergeIds} (list of boat IDs to fold in). Before merging, collects alias entries
     * for any merged boat whose normalised sail number or name differs from the keep boat.
     * Calls {@link org.mortbay.sailing.pf.store.DataStore#mergeBoats} to rewrite all race
     * finisher references from merged IDs to {@code keepId} and delete the merged boat records.
     * Saves the store, then appends the new alias entries to {@code aliases.yaml} and reloads
     * the alias seed so future imports honour the merge automatically.
     * Returns 400 for validation errors, 404 if any ID is not found, 500 on unexpected error.
     * On success returns {@code updatedRaces} and {@code updatedFinishers} counts.
     */
    @SuppressWarnings("unchecked")
    private void handleMergeBoats(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String keepId = (String) body.get("keepId");
            List<String> mergeIds = (List<String>) body.get("mergeIds");

            if (keepId == null || keepId.isBlank())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "keepId is required"));
                return;
            }
            if (mergeIds == null || mergeIds.isEmpty())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "mergeIds must be a non-empty list"));
                return;
            }
            if (mergeIds.contains(keepId))
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "keepId must not appear in mergeIds"));
                return;
            }

            // Build alias specs before merging (while we still have all boat records)
            Boat keepBoat = store.boats().get(keepId);
            if (keepBoat == null)
            {
                resp.setStatus(404);
                writeJson(resp, Map.of("error", "Keep boat not found: " + keepId));
                return;
            }
            // Build alias entries from merged boats before merging (while we still have all boat records)
            String keepNormSail = IdGenerator.normaliseSailNumber(keepBoat.sailNumber());
            List<Aliases.SailNumberName> newAliases = new ArrayList<>();
            for (String mergeId : mergeIds)
            {
                Boat mb = store.boats().get(mergeId);
                if (mb == null)
                {
                    resp.setStatus(404);
                    writeJson(resp, Map.of("error", "Merge boat not found: " + mergeId));
                    return;
                }
                String mbNormSail = IdGenerator.normaliseSailNumber(mb.sailNumber());
                String mbNormName = IdGenerator.normaliseName(mb.name());
                // Record alias entry if sail number or name differs from the keep boat
                if (!mbNormSail.equalsIgnoreCase(keepNormSail)
                    || !mbNormName.equalsIgnoreCase(IdGenerator.normaliseName(keepBoat.name())))
                {
                    newAliases.add(new Aliases.SailNumberName(mbNormSail, mbNormName));
                }
            }

            DataStore.MergeResult result = store.mergeBoats(keepId, mergeIds);
            store.save();

            // Update aliases.yaml and reload the alias seed so future imports honour the merge
            if (!newAliases.isEmpty())
            {
                Aliases.addAliases(store.configDir(), keepNormSail, keepBoat.name(), newAliases);
                store.reloadAliases();
            }

            cache.refreshIndexes();

            writeJson(resp, Map.of(
                "ok", true,
                "updatedRaces", result.updatedRaces(),
                "updatedFinishers", result.updatedFinishers()
            ));
        }
        catch (IllegalArgumentException e)
        {
            resp.setStatus(400);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/designs/merge — merges two or more duplicate design records into one.
     * <p>
     * Reads a JSON body with {@code keepId} and {@code mergeIds}. Before merging, collects
     * the canonical name, normalised ID, and existing aliases of each merged-away design to
     * use as alias entries. Calls {@link org.mortbay.sailing.pf.store.DataStore#mergeDesigns}
     * to rewrite all boat {@code designId} references and delete the merged design records.
     * Saves the store, then appends design aliases to {@code aliases.yaml} via
     * {@link org.mortbay.sailing.pf.store.Aliases#appendDesignMergeAliases} and reloads
     * the alias seed. Returns {@code updatedBoats}, {@code updatedRaces}, and
     * {@code updatedFinishers} counts on success.
     */
    @SuppressWarnings("unchecked")
    private void handleMergeDesigns(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String keepId = (String) body.get("keepId");
            List<String> mergeIds = (List<String>) body.get("mergeIds");

            if (keepId == null || keepId.isBlank())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "keepId is required"));
                return;
            }
            if (mergeIds == null || mergeIds.isEmpty())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "mergeIds must be a non-empty list"));
                return;
            }
            if (mergeIds.contains(keepId))
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "keepId must not appear in mergeIds"));
                return;
            }

            Design keepDesign = store.designs().get(keepId);
            if (keepDesign == null)
            {
                resp.setStatus(404);
                writeJson(resp, Map.of("error", "Keep design not found: " + keepId));
                return;
            }

            // Collect alias names (canonical names + IDs of the merged-away designs) before merging
            List<String> aliasNames = new ArrayList<>();
            for (String mergeId : mergeIds)
            {
                var md = store.designs().get(mergeId);
                if (md == null)
                {
                    resp.setStatus(404);
                    writeJson(resp, Map.of("error", "Merge design not found: " + mergeId));
                    return;
                }
                aliasNames.add(md.canonicalName());
                aliasNames.add(mergeId); // normalised ID also becomes an alias
                aliasNames.addAll(md.aliases());
            }

            DataStore.DesignMergeResult result = store.mergeDesigns(keepId, mergeIds);
            store.save();

            Aliases.appendDesignMergeAliases(store.configDir(), keepId, keepDesign.canonicalName(), aliasNames);
            store.reloadAliases();

            cache.refreshIndexes();

            writeJson(resp, Map.of("ok", true,
                "updatedBoats", result.updatedBoats(),
                "updatedRaces", result.updatedRaces(),
                "updatedFinishers", result.updatedFinishers()));
        }
        catch (IllegalArgumentException e)
        {
            resp.setStatus(400);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/boats/edit — edits a boat's sail number, name, design, or club.
     * <p>
     * If the new sail number or name changes the normalised boat ID, the boat is re-keyed:
     * the old record is removed, a new one is created under the new ID, all race finisher
     * references are repointed, and an alias is created so future imports of the old
     * identity resolve to the new one.  Indexes are rebuilt after the edit.
     */
    @SuppressWarnings("unchecked")
    private void handleEditBoat(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String boatId = (String) body.get("boatId");
            String newSailNumber = (String) body.get("sailNumber");
            String newName = (String) body.get("name");
            String newDesignId = body.containsKey("designId") ? (String) body.get("designId") : null;
            String newClubId = body.containsKey("clubId") ? (String) body.get("clubId") : null;

            if (boatId == null || boatId.isBlank())
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "boatId is required"));
                return;
            }
            Boat boat = store.boats().get(boatId);
            if (boat == null)
            {
                resp.setStatus(404);
                writeJson(resp, Map.of("error", "Boat not found: " + boatId));
                return;
            }

            // Apply edits — use existing values where not provided
            String sail = (newSailNumber != null && !newSailNumber.isBlank()) ? newSailNumber.trim() : boat.sailNumber();
            String name = (newName != null && !newName.isBlank()) ? newName.trim() : boat.name();
            String designId = body.containsKey("designId") ? (newDesignId != null && !newDesignId.isBlank() ? newDesignId.trim() : null) : boat.designId();
            String clubId = body.containsKey("clubId") ? (newClubId != null && !newClubId.isBlank() ? newClubId.trim() : null) : boat.clubId();

            // Compute the new boat ID
            Design design = designId != null ? store.designs().get(designId) : null;
            String newBoatId = IdGenerator.generateBoatId(sail, name, design);
            boolean idChanged = !newBoatId.equals(boatId);

            // Check for collision with an existing different boat
            if (idChanged && store.boats().containsKey(newBoatId))
            {
                resp.setStatus(409);
                writeJson(resp, Map.of("error", "A boat with ID " + newBoatId + " already exists. Merge first, then edit."));
                return;
            }

            // Create updated boat record
            Boat updated = new Boat(newBoatId, sail, name, designId, clubId,
                boat.certificates(), boat.sources(), java.time.Instant.now(), null);
            store.putBoat(updated);

            int updatedRaces = 0;
            int updatedFinishers = 0;
            if (idChanged)
            {
                // Remove old boat record
                store.removeBoat(boatId);

                // Repoint all race finisher references
                for (Race race : List.copyOf(store.races().values()))
                {
                    boolean changed = false;
                    List<Division> newDivisions = new ArrayList<>();
                    for (Division div : race.divisions())
                    {
                        List<Finisher> newFinishers = new ArrayList<>();
                        for (Finisher f : div.finishers())
                        {
                            if (f.boatId().equals(boatId))
                            {
                                newFinishers.add(new Finisher(newBoatId, f.elapsedTime(), f.nonSpinnaker(), f.certificateNumber()));
                                changed = true;
                                updatedFinishers++;
                            }
                            else
                            {
                                newFinishers.add(f);
                            }
                        }
                        newDivisions.add(new Division(div.name(), List.copyOf(newFinishers)));
                    }
                    if (changed)
                    {
                        store.putRace(new Race(race.id(), race.clubId(), race.seriesIds(), race.date(),
                            race.number(), race.name(), List.copyOf(newDivisions),
                            race.source(), race.lastUpdated(), null));
                        updatedRaces++;
                    }
                }

                // Create alias so future imports of the old identity resolve to the new
                String oldNormSail = IdGenerator.normaliseSailNumber(boat.sailNumber());
                String oldNormName = IdGenerator.normaliseName(boat.name());
                String newNormSail = IdGenerator.normaliseSailNumber(sail);
                String newNormName = IdGenerator.normaliseName(name);
                if (!oldNormSail.equalsIgnoreCase(newNormSail) || !oldNormName.equalsIgnoreCase(newNormName))
                {
                    List<Aliases.SailNumberName> aliasEntries = List.of(
                        new Aliases.SailNumberName(oldNormSail, oldNormName));
                    Aliases.addAliases(store.configDir(), newNormSail, name, aliasEntries);
                    store.reloadAliases();
                }
            }

            // Write design override to design.yaml if design was changed
            if (body.containsKey("designId") && !Objects.equals(designId, boat.designId()))
            {
                if (designId != null)
                {
                    Design d = store.designs().get(designId);
                    String canonicalName = d != null ? d.canonicalName() : designId;
                    store.addDesignOverride(sail, name, designId, canonicalName);
                }
            }

            // Write club override to clubs.yaml if club was changed
            if (body.containsKey("clubId") && !Objects.equals(clubId, boat.clubId()))
            {
                if (clubId != null)
                    store.addClubOverride(sail, name, clubId);
            }

            store.save();
            cache.refreshIndexes();

            writeJson(resp, Map.of("ok", true, "newBoatId", newBoatId,
                "idChanged", idChanged, "updatedRaces", updatedRaces,
                "updatedFinishers", updatedFinishers));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * POST /api/boats/merge-request, /api/designs/merge-request, /api/boats/edit-request
     * — records a user request to a log file in {@code pf-data/log/} for later admin review.
     * These endpoints are open to unauthenticated (read-only) users.
     */
    @SuppressWarnings("unchecked")
    private void handleUserRequest(String path, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String type = path.replaceAll("^/", "").replace('/', '-'); // e.g. "boats-merge-request"
            Path logDir = store.dataRoot().resolve("log");
            java.nio.file.Files.createDirectories(logDir);
            Path logFile = logDir.resolve("user-requests.log");
            String timestamp = java.time.Instant.now().toString();
            String line = timestamp + " " + type + " " + MAPPER.writeValueAsString(body) + "\n";
            java.nio.file.Files.writeString(logFile, line,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
            writeJson(resp, Map.of("ok", true, "message", "Request recorded for admin review"));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /**
     * GET /api/comparison/candidates — returns candidate boats and designs for the PF comparison chart.
     * <p>
     * Accepts optional {@code boatQ} / {@code designQ} text filters and a comma-separated
     * {@code boatIds} list of already-selected boats. When selected boats are provided and
     * {@code allAvailable} is not {@code true}, the boat list is narrowed to boats that have
     * co-raced (in any division) with <em>all</em> currently selected boats; the design list is
     * similarly restricted to designs represented among those co-racers. Excluded boats and
     * designs are always omitted. Results are capped at 100 boats and 100 designs.
     */
    private void handleComparisonCandidates(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String boatQ  = req.getParameter("boatQ");
        String designQ = req.getParameter("designQ");
        String lowerBoat   = boatQ   != null && !boatQ.isBlank()   ? boatQ.toLowerCase()   : null;
        String lowerDesign = designQ != null && !designQ.isBlank() ? designQ.toLowerCase() : null;
        boolean allAvailable = "true".equals(req.getParameter("allAvailable"));

        String boatIdsParam = req.getParameter("boatIds");
        List<String> selectedBoatIds = (boatIdsParam != null && !boatIdsParam.isBlank())
            ? Arrays.asList(boatIdsParam.split(","))
            : List.of();

        // Intersect co-racer sets to find boats that have raced with ALL selected boats
        Set<String> validBoatIds = null;
        if (!allAvailable && !selectedBoatIds.isEmpty())
        {
            for (String cId : selectedBoatIds)
            {
                BoatDerived cbd = cache.boatDerived().get(cId.trim());
                if (cbd == null) continue;
                Set<String> coRacers = new HashSet<>();
                for (String rId : cbd.raceIds())
                {
                    var race = store.races().get(rId);
                    if (race == null || race.divisions() == null) continue;
                    for (var div : race.divisions())
                        for (var f : div.finishers())
                            coRacers.add(f.boatId());
                }
                if (validBoatIds == null)
                    validBoatIds = new HashSet<>(coRacers);
                else
                    validBoatIds.retainAll(coRacers);
            }
        }

        final Set<String> finalValidBoatIds = validBoatIds;
        final Set<String> selectedSet = new HashSet<>(selectedBoatIds);

        List<Map<String, Object>> boats = cache.boatDerived().values().stream()
            .filter(bd -> !store.isBoatExcluded(bd.boat().id())
                && (bd.boat().designId() == null || !store.isDesignExcluded(bd.boat().designId())))
            .filter(bd -> !selectedSet.contains(bd.boat().id()))
            .filter(bd -> finalValidBoatIds == null || finalValidBoatIds.contains(bd.boat().id()))
            .filter(bd -> lowerBoat == null
                || bd.boat().id().toLowerCase().contains(lowerBoat)
                || bd.boat().name().toLowerCase().contains(lowerBoat)
                || (bd.boat().sailNumber() != null && bd.boat().sailNumber().toLowerCase().contains(lowerBoat)))
            .sorted(Comparator.comparing(bd -> bd.boat().name(), Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER)))
            .limit(100)
            .map(bd ->
            {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id",         bd.boat().id());
                m.put("name",       bd.boat().name());
                m.put("sailNumber", bd.boat().sailNumber());
                m.put("designId",   bd.boat().designId());
                return m;
            })
            .collect(Collectors.toList());

        // Design candidates: if filtered, only designs whose boats appear in validBoatIds
        Set<String> validDesignIds = null;
        if (!allAvailable && finalValidBoatIds != null)
        {
            validDesignIds = new HashSet<>();
            for (String bId : finalValidBoatIds)
            {
                BoatDerived bd = cache.boatDerived().get(bId);
                if (bd != null && bd.boat().designId() != null)
                    validDesignIds.add(bd.boat().designId());
            }
        }
        final Set<String> finalValidDesignIds = validDesignIds;

        List<Map<String, Object>> designs = cache.designDerived().values().stream()
            .filter(dd -> !store.isDesignExcluded(dd.design().id()))
            .filter(dd -> finalValidDesignIds == null || finalValidDesignIds.contains(dd.design().id()))
            .filter(dd -> lowerDesign == null
                || dd.design().id().toLowerCase().contains(lowerDesign)
                || dd.design().canonicalName().toLowerCase().contains(lowerDesign))
            .sorted(Comparator.comparing(dd -> dd.design().canonicalName(), Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER)))
            .limit(100)
            .map(dd ->
            {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id",            dd.design().id());
                m.put("canonicalName", dd.design().canonicalName());
                return m;
            })
            .collect(Collectors.toList());

        writeJson(resp, Map.of("boats", boats, "designs", designs));
    }

    /**
     * GET /api/comparison/chart — returns RF and PF data for the multi-boat comparison chart.
     * <p>
     * Accepts comma-separated {@code boatIds} and {@code designIds}. For each boat returns its
     * spin/nonSpin RF and PF factors plus a chronological list of per-race residual points
     * (back-calculated factor, date, race/series name, division, variant, weight) derived from
     * the last PF run. For each design returns its spin/nonSpin RF. Used to render the
     * performance-over-time scatter chart in the analysis panel.
     */
    private void handleComparisonChart(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String boatIdsParam   = req.getParameter("boatIds");
        String designIdsParam = req.getParameter("designIds");
        List<String> boatIds   = (boatIdsParam   != null && !boatIdsParam.isBlank())
            ? Arrays.asList(boatIdsParam.split(","))   : List.of();
        List<String> designIds = (designIdsParam != null && !designIdsParam.isBlank())
            ? Arrays.asList(designIdsParam.split(",")) : List.of();

        List<Map<String, Object>> boatData = new ArrayList<>();
        for (String rawId : boatIds)
        {
            String boatId = rawId.trim();
            if (boatId.isEmpty()) continue;
            BoatDerived bd = cache.boatDerived().get(boatId);
            if (bd == null) continue;

            Map<String, Object> bm = new LinkedHashMap<>();
            bm.put("id",         boatId);
            bm.put("name",       bd.boat().name());
            bm.put("sailNumber", bd.boat().sailNumber());

            ReferenceFactors rf = bd.referenceFactors();
            bm.put("rfSpin",    rf != null ? factorMap(rf.spin())    : null);
            bm.put("rfNonSpin", rf != null ? factorMap(rf.nonSpin()) : null);

            BoatPf pf = bd.pf();
            bm.put("pfSpin",      pf != null ? factorMap(pf.spin())      : null);
            bm.put("pfNonSpin",   pf != null ? factorMap(pf.nonSpin())   : null);
            bm.put("pfTwoHanded", pf != null ? factorMap(pf.twoHanded()) : null);

            List<EntryResidual> residuals = cache.residualsByBoatId().get(boatId);
            List<Map<String, Object>> entries = new ArrayList<>();
            if (residuals != null && pf != null)
            {
                for (EntryResidual r : residuals)
                {
                    Factor pfVariant = r.twoHanded() ? pf.twoHanded()
                        : r.nonSpinnaker() ? pf.nonSpin() : pf.spin();
                    if (pfVariant == null || Double.isNaN(pfVariant.value())) continue;
                    double backCalcFactor = pfVariant.value() * Math.exp(-r.residual());
                    // Look up race name and series name for hover text
                    Race race = store.races().get(r.raceId());
                    String raceName = raceName(race);
                    String seriesName = null;
                    String seriesId   = null;
                    if (race != null && race.seriesIds() != null && !race.seriesIds().isEmpty())
                    {
                        seriesId = race.seriesIds().getFirst();
                        var club = store.clubs().get(race.clubId());
                        if (club != null && club.series() != null)
                            for (var s : club.series())
                                if (seriesId.equals(s.id())) { seriesName = s.name(); break; }
                        if (seriesName == null) seriesName = seriesId;
                    }
                    Map<String, Object> em = new LinkedHashMap<>();
                    em.put("date",           r.raceDate().toString());
                    em.put("raceId",         r.raceId());
                    em.put("raceName",       raceName);
                    em.put("seriesName",     seriesName);
                    em.put("seriesId",       seriesId);
                    em.put("division",       r.divisionName());
                    em.put("backCalcFactor", backCalcFactor);
                    em.put("nonSpinnaker",   r.nonSpinnaker());
                    em.put("twoHanded",      r.twoHanded());
                    em.put("weight",         r.weight());
                    entries.add(em);
                }
            }
            bm.put("entries", entries);
            boatData.add(bm);
        }

        List<Map<String, Object>> designData = new ArrayList<>();
        for (String rawId : designIds)
        {
            String designId = rawId.trim();
            if (designId.isEmpty()) continue;
            DesignDerived dd = cache.designDerived().get(designId);
            if (dd == null) continue;

            Map<String, Object> dm = new LinkedHashMap<>();
            dm.put("id",            designId);
            dm.put("canonicalName", dd.design().canonicalName());
            ReferenceFactors rf = dd.referenceFactors();
            dm.put("rfSpin",    rf != null ? factorMap(rf.spin())    : null);
            dm.put("rfNonSpin", rf != null ? factorMap(rf.nonSpin()) : null);
            designData.add(dm);
        }

        writeJson(resp, Map.of("boats", boatData, "designs", designData));
    }

    /**
     * GET /api/comparison/division — returns elapsed and corrected times for a single race division.
     * <p>
     * Required query params: {@code raceId} and {@code divisionName} (empty string matches
     * legacy null-named divisions). For each finisher with a recorded elapsed time, emits the
     * elapsed seconds, applicable variant (spin/nonSpin), PF and RF values and weights, and
     * PF/RF corrected times. Finishers are sorted by PF value ascending (nulls last). Also
     * returns race metadata (date, series name) and the division's overall variant (spin, nonSpin,
     * or mixed). Used to render the division bar chart in the races tab.
     */
    private void handleComparisonDivision(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String raceId      = req.getParameter("raceId");
        String divisionName = req.getParameter("divisionName");
        if (raceId == null || divisionName == null) { resp.sendError(400); return; }

        Race race = store.races().get(raceId);
        if (race == null) { resp.sendError(404); return; }

        // Empty string is a sentinel for a null-named division (legacy imported data)
        boolean matchNull = divisionName.isEmpty();
        var div = race.divisions() == null ? null
            : race.divisions().stream()
                .filter(d -> matchNull ? (d.name() == null || d.name().isBlank())
                                       : divisionName.equals(d.name()))
                .findFirst().orElse(null);
        if (div == null) { resp.sendError(404); return; }

        int totalFinishers = div.finishers() != null ? div.finishers().size() : 0;
        List<Map<String, Object>> finishers = new ArrayList<>();
        Set<String> variantsUsed = new java.util.LinkedHashSet<>();
        for (var f : div.finishers())
        {
            BoatDerived bd = cache.boatDerived().get(f.boatId());
            if (bd == null || f.elapsedTime() == null) continue;

            ReferenceFactors rf  = bd.referenceFactors();
            BoatPf          pf = bd.pf();

            // Use each finisher's own nonSpinnaker flag to pick the correct variant
            String fVariant = f.nonSpinnaker() ? "nonSpin" : "spin";
            variantsUsed.add(fVariant);

            Factor pfFactor = pf == null ? null : switch (fVariant)
            {
                case "nonSpin" -> pf.nonSpin();
                default        -> pf.spin();
            };
            Factor rfFactor = rf == null ? null : switch (fVariant)
            {
                case "nonSpin" -> rf.nonSpin();
                default        -> rf.spin();
            };

            double elapsedSec = f.elapsedTime().toSeconds();
            Double pfVal = pfFactor != null && !Double.isNaN(pfFactor.value()) ? pfFactor.value() : null;
            Double rfVal  = rfFactor  != null && !Double.isNaN(rfFactor.value())  ? rfFactor.value()  : null;

            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("boatId",      f.boatId());
            fm.put("name",        bd.boat().name());
            fm.put("sailNumber",  bd.boat().sailNumber());
            fm.put("elapsed",     elapsedSec);
            fm.put("variant",     fVariant);
            fm.put("pf",         pfVal);
            fm.put("rf",          rfVal);
            fm.put("pfWeight",   pfFactor != null ? pfFactor.weight() : null);
            fm.put("rfWeight",    rfFactor  != null ? rfFactor.weight()  : null);
            fm.put("pfCorrected", pfVal != null && pfVal > 0 ? elapsedSec * pfVal : null);
            fm.put("rfCorrected",  rfVal  != null && rfVal  > 0 ? elapsedSec * rfVal  : null);
            finishers.add(fm);
        }
        String divisionVariant = variantsUsed.size() == 1 ? variantsUsed.iterator().next() : "mixed";

        // Sort by PF value ascending (nulls last)
        finishers.sort(Comparator.comparing(
            m -> (Double) m.get("pf"), Comparator.nullsFirst(Comparator.naturalOrder())));

        // Race metadata
        String seriesName = null;
        if (race.seriesIds() != null && !race.seriesIds().isEmpty())
        {
            String firstSeriesId = race.seriesIds().getFirst();
            var club = store.clubs().get(race.clubId());
            if (club != null && club.series() != null)
                for (var s : club.series())
                    if (firstSeriesId.equals(s.id())) { seriesName = s.name(); break; }
            if (seriesName == null) seriesName = firstSeriesId;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("raceId",      raceId);
        result.put("raceName",    raceName(race));
        result.put("seriesName",  seriesName);
        result.put("date",        race.date() != null ? race.date().toString() : null);
        result.put("divisionName",    divisionName);
        result.put("divisionVariant", divisionVariant);
        result.put("totalFinishers",  totalFinishers);
        result.put("finishers",       finishers);
        writeJson(resp, result);
    }

    private Map<String, Object> factorMap(Factor f)
    {
        if (f == null)
            return null;
        if (Double.isNaN(f.value()) || Double.isNaN(f.weight()))
            return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("value",  f.value());
        m.put("weight", f.weight());
        return m;
    }

    private Map<String, Object> factorMap(Factor f, int generation)
    {
        if (f == null)
            return null;
        if (Double.isNaN(f.value()) || Double.isNaN(f.weight()))
            return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("value",      f.value());
        m.put("weight",     f.weight());
        m.put("generation", generation);
        return m;
    }

    /**
     * GET /api/designs[/{id}] — design listing or single-design detail.
     * <p>
     * <b>List</b>: supports pagination, free-text search ({@code q} matches id or canonicalName),
     * {@code showExcluded} flag, and sort by {@code canonicalName}, {@code spinRef}, or
     * {@code boats} (number of boats assigned to the design). Each row includes the design's spin
     * RF from the cache, boat count, and excluded status. The {@code excludeNulls} flag removes
     * rows where the sort column is null.
     * <p>
     * <b>Detail</b>: returns the raw {@link org.mortbay.sailing.pf.data.Design} record; 404 if
     * not found.
     */
    private void handleDesigns(String sub, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        if (sub.isEmpty() || "/".equals(sub))
        {
            int page = parseIntParam(req, "page", 0);
            int size = parseIntParam(req, "size", DEFAULT_PAGE_SIZE);
            String q = req.getParameter("q");
            String sort = req.getParameter("sort");
            boolean asc = !"desc".equals(req.getParameter("dir"));
            String lower = q != null && !q.isBlank() ? q.toLowerCase() : null;

            boolean showExcluded = "true".equals(req.getParameter("showExcluded"));
            boolean hideEmpty = "true".equals(req.getParameter("hideEmpty"));
            List<Design> all = store.designs().values().stream()
                .filter(d -> showExcluded || !store.isDesignExcluded(d.id()))
                .filter(d -> lower == null
                    || d.id().toLowerCase().contains(lower)
                    || d.canonicalName().toLowerCase().contains(lower))
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
            {
                Comparator<Design> cmp = switch (sort)
                {
                    case "canonicalName" -> Comparator.comparing(Design::canonicalName, Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                    case "spinRef"       -> Comparator.comparing(
                                               (Design d2) -> { DesignDerived dd2 = cache.designDerived().get(d2.id()); return (dd2 != null && dd2.referenceFactors() != null && dd2.referenceFactors().spin() != null) ? dd2.referenceFactors().spin().value() : 0.0; },
                                               Comparator.<Double>naturalOrder());
                    case "boats"         -> Comparator.comparingInt(
                                               (Design d2) -> { DesignDerived dd2 = cache.designDerived().get(d2.id()); return dd2 != null ? dd2.boatIds().size() : 0; });
                    default              -> Comparator.comparing(Design::id, Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER));
                };
                all.sort(asc ? cmp : cmp.reversed());
            }

            List<Map<String, Object>> rows = all.stream().map(d ->
            {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id",            d.id());
                row.put("canonicalName", d.canonicalName());
                DesignDerived dd = cache.designDerived().get(d.id());
                Factor dspin = (dd != null && dd.referenceFactors() != null) ? dd.referenceFactors().spin() : null;
                row.put("spinRef",       dspin != null ? factorMap(dspin) : null);
                row.put("boats",         dd != null ? dd.boatIds().size() : 0);
                row.put("excluded",      store.isDesignExcluded(d.id()));
                return row;
            }).collect(Collectors.toList());

            if (hideEmpty && sort != null && !sort.isBlank())
                rows.removeIf(r -> isEmptyValue(r.get(sort)));

            writeJson(resp, paginate(rows, page, size));
        }
        else
        {
            String id = sub.startsWith("/") ? sub.substring(1) : sub;
            Design design = store.designs().get(id);
            if (design == null)
            {
                resp.sendError(404);
                return;
            }
            writeJson(resp, design);
        }
    }

    /**
     * GET /api/races[/{id}] — race listing or single-race detail.
     * <p>
     * <b>List</b>: filters by optional {@code boatId}, {@code clubId}, or {@code seriesId},
     * supports free-text search ({@code q} matches race id, race name, or series name), pagination, and sort.
     * The {@code showExcluded} flag (default false) hides individually excluded races and races
     * where every finisher belongs to an excluded design. The {@code excludeNulls} flag removes
     * races with zero finishers. Each row includes series name, finisher count, excluded status,
     * and the PF reference time(s) if available from the cache.
     * <p>
     * <b>Detail</b>: returns the raw {@link org.mortbay.sailing.pf.data.Race} record; 404 if
     * not found.
     */
    private void handleRaces(String sub, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        if (sub.isEmpty() || "/".equals(sub))
        {
            int page = parseIntParam(req, "page", 0);
            int size = parseIntParam(req, "size", DEFAULT_PAGE_SIZE);
            String q = req.getParameter("q");
            String sort = req.getParameter("sort");
            boolean asc = !"desc".equals(req.getParameter("dir"));
            String lower = q != null && !q.isBlank() ? q.toLowerCase() : null;

            String filterBoatId   = req.getParameter("boatId");
            String filterClubId   = req.getParameter("clubId");
            String filterSeriesId = req.getParameter("seriesId");
            boolean showExcluded = "true".equals(req.getParameter("showExcluded"));
            boolean hideEmpty = "true".equals(req.getParameter("hideEmpty"));

            // Build series-id → name index for search matching
            Map<String, String> seriesNameById = new java.util.HashMap<>();
            if (lower != null)
            {
                for (Club c : store.clubs().values())
                    if (c.series() != null)
                        for (var s : c.series())
                            if (s.name() != null)
                                seriesNameById.put(s.id(), s.name().toLowerCase());
            }

            // Enrich all filtered rows — needed to allow sort by seriesName or finishers
            List<Map<String, Object>> enriched = store.races().values().stream()
                .filter(r -> filterBoatId   == null || raceContainsBoat(r, filterBoatId))
                .filter(r -> filterClubId   == null || filterClubId.equals(r.clubId()))
                .filter(r -> filterSeriesId == null || (r.seriesIds() != null && r.seriesIds().contains(filterSeriesId)))
                .filter(r -> lower == null
                    || r.id().toLowerCase().contains(lower)
                    || (r.clubId()  != null && r.clubId().toLowerCase().contains(lower))
                    || (r.name()    != null && r.name().toLowerCase().contains(lower))
                    || (r.seriesIds() != null && r.seriesIds().stream()
                            .anyMatch(sid -> seriesNameById.getOrDefault(sid, "").contains(lower))))
                .filter(r -> showExcluded || !isRaceEffectivelyExcluded(r))
                .map(this::raceRow)
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
                enriched.sort(mapComparator(sort, asc));

            if (hideEmpty)
            {
                // "Hide empty" on the races tab: drop races with no finishers. When a sort
                // column is active, also drop rows whose sort value is empty.
                enriched.removeIf(r -> isEmptyValue(r.get("finishers")));
                if (sort != null && !sort.isBlank())
                    enriched.removeIf(r -> isEmptyValue(r.get(sort)));
            }

            writeJson(resp, paginate(enriched, page, size));
        }
        else
        {
            String id = sub.startsWith("/") ? sub.substring(1) : sub;
            Race race = store.races().get(id);
            if (race == null)
            {
                resp.sendError(404);
                return;
            }
            writeJson(resp, race);
        }
    }

    /**
     * GET /api/series/chart — returns PF-corrected time data for all races in a series,
     * grouped by race and division. Used by the series chart in the data browser.
     */
    private void handleSeriesChart(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String seriesId = req.getParameter("seriesId");
        if (seriesId == null || seriesId.isBlank()) { resp.sendError(400); return; }

        // Find the series and its races
        Series series = null;
        Club owningClub = null;
        for (Club club : store.clubs().values())
        {
            if (club.series() == null) continue;
            for (Series s : club.series())
            {
                if (seriesId.equals(s.id()))
                {
                    series = s;
                    owningClub = club;
                    break;
                }
            }
            if (series != null) break;
        }
        if (series == null || series.raceIds() == null) { resp.sendError(404); return; }

        // Build chart data: one entry per race, each containing divisions with finisher data
        List<Map<String, Object>> racesData = new ArrayList<>();
        for (String raceId : series.raceIds())
        {
            Race race = store.races().get(raceId);
            if (race == null || race.divisions() == null) continue;

            List<Map<String, Object>> divisionsData = new ArrayList<>();
            for (Division div : race.divisions())
            {
                List<Map<String, Object>> finishers = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    BoatDerived bd = cache.boatDerived().get(f.boatId());
                    if (bd == null || f.elapsedTime() == null) continue;

                    BoatPf pf = bd.pf();
                    String variant = f.nonSpinnaker() ? "nonSpin" : "spin";
                    Factor pfFactor = pf == null ? null : switch (variant)
                    {
                        case "nonSpin" -> pf.nonSpin();
                        default -> pf.spin();
                    };

                    double elapsedSec = f.elapsedTime().toSeconds();
                    Double pfVal = pfFactor != null && !Double.isNaN(pfFactor.value()) ? pfFactor.value() : null;

                    Map<String, Object> fm = new LinkedHashMap<>();
                    fm.put("boatId", f.boatId());
                    fm.put("name", bd.boat().name());
                    fm.put("sailNumber", bd.boat().sailNumber());
                    fm.put("pf", pfVal);
                    fm.put("pfCorrected", pfVal != null && pfVal > 0 ? elapsedSec * pfVal : null);
                    finishers.add(fm);
                }

                // Sort by PF ascending, nulls last
                finishers.sort(Comparator.comparing(
                    m -> (Double) m.get("pf"), Comparator.nullsFirst(Comparator.naturalOrder())));

                if (!finishers.isEmpty())
                {
                    Map<String, Object> divMap = new LinkedHashMap<>();
                    divMap.put("name", div.name());
                    divMap.put("finishers", finishers);
                    divisionsData.add(divMap);
                }
            }

            if (!divisionsData.isEmpty())
            {
                Map<String, Object> raceMap = new LinkedHashMap<>();
                raceMap.put("raceId", raceId);
                raceMap.put("raceName", raceName(race));
                raceMap.put("date", race.date() != null ? race.date().toString() : null);
                raceMap.put("divisions", divisionsData);
                racesData.add(raceMap);
            }
        }

        // Sort races by date
        racesData.sort(Comparator.comparing(
            m -> (String) m.get("date"), Comparator.nullsFirst(Comparator.naturalOrder())));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("seriesId", seriesId);
        result.put("seriesName", series.name());
        result.put("club", owningClub.shortName() != null ? owningClub.shortName() : owningClub.id());
        result.put("races", racesData);
        writeJson(resp, result);
    }

    /**
     * GET /api/series — series listing (no single-series detail endpoint exists).
     * <p>
     * Iterates all persisted clubs and their non-catch-all series, joining each series with its
     * races via an in-memory index keyed by series ID. Supports free-text search ({@code q}
     * matches series name, club short name, or club id), optional {@code clubId} filter, and
     * {@code excludeEmpty} flag (default false) to hide series with no imported races.
     * Each row includes first/last race date and race count. Supports pagination and sort.
     */
    private void handleSeries(String sub, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        if (!sub.isEmpty() && !"/".equals(sub)) { resp.sendError(404); return; }

        int page = parseIntParam(req, "page", 0);
        int size = parseIntParam(req, "size", DEFAULT_PAGE_SIZE);
        String q = req.getParameter("q");
        String sort = req.getParameter("sort");
        boolean asc = !"desc".equals(req.getParameter("dir"));
        String lower = q != null && !q.isBlank() ? q.toLowerCase() : null;
        String filterClubId = req.getParameter("clubId");
        boolean excludeEmpty = "true".equals(req.getParameter("excludeEmpty"));
        boolean showExcluded = "true".equals(req.getParameter("showExcluded"));

        // Index races by seriesId for fast lookup
        Map<String, List<Race>> racesBySeries = new java.util.HashMap<>();
        for (Race r : store.races().values())
        {
            if (r.seriesIds() == null) continue;
            for (String sid : r.seriesIds())
                racesBySeries.computeIfAbsent(sid, k -> new ArrayList<>()).add(r);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (Club club : store.clubs().values())
        {
            if (club.series() == null) continue;
            if (filterClubId != null && !filterClubId.equals(club.id())) continue;
            String clubShort = club.shortName() != null ? club.shortName() : club.id();
            for (var s : club.series())
            {
                if (s.isCatchAll()) continue;
                if (lower != null
                    && !s.name().toLowerCase().contains(lower)
                    && !clubShort.toLowerCase().contains(lower)
                    && !club.id().toLowerCase().contains(lower)) continue;

                List<Race> seriesRaces = racesBySeries.getOrDefault(s.id(), List.of());
                if (excludeEmpty && seriesRaces.isEmpty()) continue;
                LocalDate firstDate = seriesRaces.stream()
                    .map(Race::date).filter(java.util.Objects::nonNull)
                    .min(Comparator.naturalOrder()).orElse(null);
                LocalDate lastDate = seriesRaces.stream()
                    .map(Race::date).filter(java.util.Objects::nonNull)
                    .max(Comparator.naturalOrder()).orElse(null);

                boolean excluded = store.isSeriesExcluded(s.name());
                boolean clubExcluded = store.isClubExcluded(club.id());
                if (!showExcluded && (excluded || clubExcluded)) continue;

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id",         s.id());
                row.put("clubId",     club.id());
                row.put("club",       clubShort);
                row.put("clubShortName", clubShort);
                row.put("clubLongName",  club.longName());
                row.put("name",       s.name());
                row.put("firstDate",  firstDate);
                row.put("lastDate",   lastDate);
                row.put("races",      seriesRaces.size());
                row.put("excluded",   excluded);
                row.put("clubExcluded", clubExcluded);
                rows.add(row);
            }
        }

        if (sort != null && !sort.isBlank())
            rows.sort(mapComparator(sort, asc));

        writeJson(resp, paginate(rows, page, size));
    }

    private static boolean raceContainsBoat(Race r, String boatId)
    {
        if (r.divisions() == null) return false;
        for (var div : r.divisions())
            for (var f : div.finishers())
                if (boatId.equals(f.boatId())) return true;
        return false;
    }

    /**
     * True when the race is excluded for the default view: explicit race exclusion,
     * club exclusion of the race's club, explicit exclusion of any of its series,
     * or {@link #isRaceAllExcluded} (all finishers' designs excluded).
     */
    private boolean isRaceEffectivelyExcluded(Race r)
    {
        if (store.isRaceExcluded(r.id())) return true;
        if (r.clubId() != null && store.isClubExcluded(r.clubId())) return true;
        if (r.seriesIds() != null && !r.seriesIds().isEmpty())
        {
            var club = store.clubs().get(r.clubId());
            if (club != null && club.series() != null)
            {
                for (String sid : r.seriesIds())
                    for (var s : club.series())
                        if (sid.equals(s.id()) && store.isSeriesExcluded(s.name()))
                            return true;
            }
        }
        return isRaceAllExcluded(r);
    }

    private boolean isRaceAllExcluded(Race r)
    {
        if (r.divisions() == null || r.divisions().isEmpty())
            return false;
        for (var div : r.divisions())
        {
            if (div.finishers() == null || div.finishers().isEmpty())
                continue;
            for (var f : div.finishers())
            {
                Boat b = store.boats().get(f.boatId());
                if (b == null || b.designId() == null || !store.isDesignExcluded(b.designId()))
                    return false;
            }
        }
        return true;
    }

    private Map<String, Object> raceRow(Race r)
    {
        // Series name: look up via first seriesId in the owning club
        String seriesName = null;
        boolean anySeriesExcluded = false;
        if (r.seriesIds() != null && !r.seriesIds().isEmpty())
        {
            String firstSeriesId = r.seriesIds().getFirst();
            var club = store.clubs().get(r.clubId());
            if (club != null && club.series() != null)
            {
                for (String sid : r.seriesIds())
                {
                    for (var s : club.series())
                    {
                        if (sid.equals(s.id()))
                        {
                            if (sid.equals(firstSeriesId))
                                seriesName = s.name();
                            if (store.isSeriesExcluded(s.name()))
                                anySeriesExcluded = true;
                            break;
                        }
                    }
                }
            }
        }

        // Finisher count: sum across all divisions
        int finishers = 0;
        if (r.divisions() != null)
            for (var div : r.divisions())
                if (div.finishers() != null)
                    finishers += div.finishers().size();

        Map<String, Object> row = new LinkedHashMap<>();
        String firstSeriesId = (r.seriesIds() != null && !r.seriesIds().isEmpty())
            ? r.seriesIds().getFirst() : null;
        row.put("id", r.id());
        row.put("clubId", r.clubId());
        putClubNaming(row, r.clubId());
        row.put("clubExcluded", r.clubId() != null && store.isClubExcluded(r.clubId()));
        row.put("date", r.date());
        row.put("seriesName", seriesName);
        row.put("seriesId", firstSeriesId);
        row.put("seriesExcluded", anySeriesExcluded);
        row.put("name", raceName(r));
        row.put("finishers", finishers);
        row.put("excluded", store.isRaceExcluded(r.id()));

        var rd = cache.raceDerived().get(r.id());
        if (rd != null && rd.divisionPfs() != null && !rd.divisionPfs().isEmpty())
        {
            var divs = rd.divisionPfs();
            String refTime = divs.size() == 1
                ? formatRefTime(divs.getFirst().referenceTimeNanos())
                : IntStream.range(0, divs.size())
                    .mapToObj(i -> {
                        var dh = divs.get(i);
                        String label = dh.divisionName() != null ? dh.divisionName()
                            : "Div " + (char)('A' + i);
                        return label + ": " + formatRefTime(dh.referenceTimeNanos());
                    })
                    .collect(Collectors.joining(" / "));
            row.put("referenceTime", refTime);
        }

        return row;
    }

    private static String formatRefTime(double nanos)
    {
        long seconds = Math.round(nanos / 1_000_000_000.0);
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        return String.format("%d:%02d:%02d", h, m, s);
    }

    /**
     * GET /api/pf/quality — returns convergence and quality metrics from the last PF run.
     * <p>
     * Serialises the {@link org.mortbay.sailing.pf.analysis.PfQuality} snapshot held in the
     * cache: boat count, total entries, iterations (inner/outer, converged flags), final deltas,
     * residual distribution (median, IQR, 95th percentile), down-weighted entry count,
     * high-dispersion division count, median boat confidence, outer-loop delta trace, and
     * the full PF configuration used. Returns 404 if no PF run has completed yet.
     */
    private void handlePfQuality(HttpServletResponse resp) throws IOException
    {
        PfQuality q = cache.pfQuality();
        if (q == null)
        {
            resp.sendError(404);
            return;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatsWithPf", q.boatsWithPf());
        result.put("totalEntries", q.totalEntries());
        result.put("divisionsUsed", q.divisionsUsed());
        result.put("innerIterations", q.innerIterations());
        result.put("outerIterations", q.outerIterations());
        result.put("innerConverged", q.innerConverged());
        result.put("outerConverged", q.outerConverged());
        result.put("finalMaxDelta", q.finalMaxDelta());
        result.put("finalMaxWeightChange", q.finalMaxWeightChange());
        result.put("medianResidual", q.medianResidual());
        result.put("iqrResidual", q.iqrResidual());
        result.put("pct95Residual", q.pct95Residual());
        result.put("downWeightedEntries", q.downWeightedEntries());
        result.put("highDispersionDivisions", q.highDispersionDivisions());
        result.put("medianBoatConfidence", q.medianBoatConfidence());
        result.put("outerDeltaTrace", q.outerDeltaTrace());
        Map<String, Object> cfg = new LinkedHashMap<>();
        cfg.put("lambda", q.config().lambda());
        cfg.put("convergenceThreshold", q.config().convergenceThreshold());
        cfg.put("maxInnerIterations", q.config().maxInnerIterations());
        cfg.put("maxOuterIterations", q.config().maxOuterIterations());
        cfg.put("outlierK", q.config().outlierK());
        cfg.put("asymmetryFactor", q.config().asymmetryFactor());
        cfg.put("outerDampingFactor", q.config().outerDampingFactor());
        cfg.put("outerConvergenceThreshold", q.config().outerConvergenceThreshold());
        result.put("config", cfg);
        writeJson(resp, result);
    }

    /**
     * GET /api/importers — returns the current importer configuration and global schedule.
     * <p>
     * For each configured importer entry emits its name, mode, schedule/startup flags, and
     * current running status. Also includes the global schedule (days and time), key analysis
     * parameters ({@code targetIrcYear}, {@code outlierSigma}, {@code minAnalysisR2}), the full
     * PF optimiser configuration, and sliding-average settings. Used to populate the admin
     * schedule/settings panel.
     */
    private void handleImporters(HttpServletResponse resp) throws IOException
    {
        TaskService.ImportStatus status = _taskService.currentStatus();
        Integer sailsysNextRaceId = _taskService.sailsysNextRaceId();
        List<Map<String, Object>> entries = new ArrayList<>();
        for (TaskService.ImporterEntry e : _taskService.importerEntries())
        {
            boolean isRunning = status != null
                && e.name().equals(status.importerName())
                && e.mode().equals(status.mode());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", e.name());
            row.put("mode", e.mode());
            row.put("includeInSchedule", e.includeInSchedule());
            row.put("runAtStartup", e.runAtStartup());
            row.put("status", isRunning ? "running" : "idle");
            row.put("nextStartId", "sailsys-races".equals(e.name()) ? sailsysNextRaceId : null);
            entries.add(row);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entries", entries);
        result.put("sailsysStartId", _taskService.sailsysNextRaceId());
        result.put("sailsysEndId", _taskService.sailsysEndRaceId());
        result.put("schedule", _taskService.globalSchedule());
        result.put("targetIrcYear", _taskService.targetIrcYear());
        result.put("outlierSigma", _taskService.outlierSigma());
        result.put("minAnalysisR2", _taskService.minAnalysisR2());
        Map<String, Object> pfConfig = new LinkedHashMap<>();
        pfConfig.put("lambda", _taskService.pfLambda());
        pfConfig.put("convergenceThreshold", _taskService.pfConvergenceThreshold());
        pfConfig.put("maxInnerIterations", _taskService.pfMaxInnerIterations());
        pfConfig.put("maxOuterIterations", _taskService.pfMaxOuterIterations());
        pfConfig.put("outlierK", _taskService.pfOutlierK());
        pfConfig.put("asymmetryFactor", _taskService.pfAsymmetryFactor());
        pfConfig.put("outerDampingFactor", _taskService.pfOuterDampingFactor());
        pfConfig.put("outerConvergenceThreshold", _taskService.pfOuterConvergenceThreshold());
        result.put("pfConfig", pfConfig);
        result.put("slidingAverageCount", _taskService.slidingAverageCount());
        result.put("slidingAverageDrops", _taskService.slidingAverageDrops());
        writeJson(resp, result);
    }

    /**
     * GET /api/importers/status — returns the live status of any currently-running import task.
     * <p>
     * If no import is running, returns {@code {"running": false}}. Otherwise returns the
     * importer name, mode, start timestamp, whether the run was scheduler-triggered, and
     * (for the SailSys race importer) the current race ID being fetched.
     */
    private void handleImporterStatus(HttpServletResponse resp) throws IOException
    {
        TaskService.ImportStatus status = _taskService.currentStatus();
        if (status == null)
        {
            writeJson(resp, Map.of("running", false));
        }
        else
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("running", true);
            m.put("name", status.importerName());
            m.put("mode", status.mode());
            m.put("startedAt", status.startedAt().toString());
            m.put("scheduledRun", _taskService.isScheduledRunActive());
            int sailSysId = _taskService.currentSailSysId();
            if (sailSysId > 0)
                m.put("currentId", sailSysId);
            writeJson(resp, m);
        }
    }

    /**
     * POST /api/importers/{name}/run — submits an import task for asynchronous execution.
     * <p>
     * Accepts {@code mode} (query param, e.g. "api" or "cache") and optional {@code startId}
     * (for the SailSys importer, the race ID to resume from). Delegates to
     * {@link TaskService#submit}; returns 202 Accepted if the task was enqueued, or 409 Conflict
     * if another import is already running.
     */
    private void handleImporterRun(String name, String mode, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        // Start ID is now configured in the SailSys config section; use 1 as fallback
        int startId = _taskService.sailsysNextRaceId() != null ? _taskService.sailsysNextRaceId() : 1;
        boolean accepted = _taskService.submit(name, mode, startId);
        if (accepted)
        {
            resp.setStatus(202);
            writeJson(resp, Map.of("accepted", true, "name", name, "mode", mode));
        }
        else
        {
            resp.setStatus(409);
            writeJson(resp, Map.of("error", "An import is already running"));
        }
    }

    /**
     * POST /api/schedule — updates the global import schedule and all tunable analysis parameters.
     * <p>
     * Reads a JSON body containing the importer list (name, mode, includeInSchedule, runAtStartup
     * flags), schedule days and time, and optional overrides for {@code targetIrcYear},
     * {@code outlierSigma}, and all PF optimiser parameters (lambda, convergence thresholds,
     * iteration limits, outlier-K, asymmetry factor, damping factor, cross-variant lambda).
     * Delegates to {@link TaskService#setConfig}, which persists the new config to
     * {@code admin.yaml} and reschedules the next automatic run. Responds 400 if any parameter
     * cannot be parsed.
     */
    @SuppressWarnings("unchecked")
    private void handleSetSchedule(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            List<String> dayStrs = (List<String>) body.getOrDefault("days", List.of());
            List<DayOfWeek> days = dayStrs.stream()
                .map(d -> DayOfWeek.valueOf(d.toUpperCase())).toList();
            String timeStr = (String) body.getOrDefault("time", "03:00");
            LocalTime time = LocalTime.parse(timeStr);

            List<Map<String, Object>> importerMaps =
                (List<Map<String, Object>>) body.getOrDefault("importers", List.of());
            List<TaskService.ImporterEntry> entries = importerMaps.stream()
                .map(m -> new TaskService.ImporterEntry(
                    (String) m.get("name"),
                    (String) m.get("mode"),
                    Boolean.TRUE.equals(m.get("includeInSchedule")),
                    Boolean.TRUE.equals(m.get("runAtStartup"))))
                .toList();

            Object rawSailsysStart = body.get("sailsysStartId");
            Integer sailsysStartRaceId = (rawSailsysStart instanceof Number n && n.intValue() > 0)
                ? n.intValue() : null;
            Object rawSailsysEnd = body.get("sailsysEndId");
            Integer sailsysEndRaceId = (rawSailsysEnd instanceof Number n && n.intValue() > 0)
                ? n.intValue() : null;

            Object rawYear = body.get("targetIrcYear");
            Integer targetIrcYear = (rawYear instanceof Number n && n.intValue() > 0)
                ? n.intValue() : null;

            Object rawSigma = body.get("outlierSigma");
            Double outlierSigma = (rawSigma instanceof Number n && n.doubleValue() > 0)
                ? n.doubleValue() : null;

            // PF config params
            Object rawLambda = body.get("pfLambda");
            Double pfLambda = (rawLambda instanceof Number n2 && n2.doubleValue() > 0)
                ? n2.doubleValue() : null;
            Object rawThreshold = body.get("pfConvergenceThreshold");
            Double pfConvergenceThreshold = (rawThreshold instanceof Number n3 && n3.doubleValue() > 0)
                ? n3.doubleValue() : null;
            Object rawMaxInner = body.get("pfMaxInnerIterations");
            Integer pfMaxInnerIterations = (rawMaxInner instanceof Number n4 && n4.intValue() > 0)
                ? n4.intValue() : null;
            Object rawMaxOuter = body.get("pfMaxOuterIterations");
            Integer pfMaxOuterIterations = (rawMaxOuter instanceof Number n5 && n5.intValue() > 0)
                ? n5.intValue() : null;
            Object rawOutlierK = body.get("pfOutlierK");
            Double pfOutlierK = (rawOutlierK instanceof Number n6 && n6.doubleValue() > 0)
                ? n6.doubleValue() : null;
            Object rawAsymmetry = body.get("pfAsymmetryFactor");
            Double pfAsymmetryFactor = (rawAsymmetry instanceof Number n7 && n7.doubleValue() > 0)
                ? n7.doubleValue() : null;
            Object rawDamping = body.get("pfOuterDampingFactor");
            Double pfOuterDampingFactor = (rawDamping instanceof Number n8 && n8.doubleValue() > 0 && n8.doubleValue() <= 1.0)
                ? n8.doubleValue() : null;
            Object rawOuterConvergence = body.get("pfOuterConvergenceThreshold");
            Double pfOuterConvergenceThreshold = (rawOuterConvergence instanceof Number n9 && n9.doubleValue() > 0)
                ? n9.doubleValue() : null;
            Object rawCrossVariant = body.get("pfCrossVariantLambda");
            Double pfCrossVariantLambda = (rawCrossVariant instanceof Number n10 && n10.doubleValue() >= 0)
                ? n10.doubleValue() : null;

            _taskService.setConfig(entries, new TaskService.GlobalSchedule(days, time),
                sailsysStartRaceId, sailsysEndRaceId,
                targetIrcYear, outlierSigma,
                pfLambda, pfConvergenceThreshold, pfMaxInnerIterations, pfMaxOuterIterations,
                pfOutlierK, pfAsymmetryFactor, pfOuterDampingFactor, pfOuterConvergenceThreshold,
                pfCrossVariantLambda);
            resp.setStatus(200);
            writeJson(resp, Map.of("ok", true));
        }
        catch (Exception e)
        {
            resp.setStatus(400);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    /** Returns the race name, falling back to the ISO date string for unnamed races. */
    private static String raceName(Race race)
    {
        if (race == null) return null;
        String n = race.name();
        return (n != null && !n.isBlank()) ? n
            : race.date() != null ? race.date().toString() : null;
    }

    @SuppressWarnings("unchecked")
    private Comparator<Map<String, Object>> mapComparator(String key, boolean asc)
    {
        Comparator<Map<String, Object>> cmp = (a, b) ->
        {
            Object av = a.get(key);
            Object bv = b.get(key);
            if (av == null && bv == null) return 0;
            if (av == null) return -1;  // empty/null sorts low
            if (bv == null) return 1;
            if (av instanceof String as && bv instanceof String bs)
                return String.CASE_INSENSITIVE_ORDER.compare(as, bs);
            if (av instanceof Comparable && bv instanceof Comparable)
                return ((Comparable<Object>) av).compareTo(bv);
            return av.toString().compareToIgnoreCase(bv.toString());
        };
        return asc ? cmp : cmp.reversed();
    }

    /**
     * Treats an item as "empty" for the purpose of the Hide-empty filter:
     * null, a blank {@link CharSequence}, or a {@link Number} whose value is zero.
     */
    private static boolean isEmptyValue(Object v)
    {
        if (v == null) return true;
        if (v instanceof CharSequence cs) return cs.toString().isBlank();
        if (v instanceof Number n) return n.doubleValue() == 0.0;
        return false;
    }

    /**
     * Resolves the {@link Club} matching an id, preferring persisted records over the seed
     * catalogue (matches the fallback pattern used elsewhere in this servlet).
     */
    private Club resolveClub(String id)
    {
        if (id == null) return null;
        Club c = store.clubs().get(id);
        if (c == null) c = store.clubSeed().get(id);
        return c;
    }

    /**
     * Fills {@code clubShortName} and {@code clubLongName} on the row from the club catalogue.
     * Both fields are set to null if the club id is unknown.
     */
    private void putClubNaming(Map<String, Object> row, String clubId)
    {
        Club c = resolveClub(clubId);
        row.put("clubShortName", c != null ? c.shortName() : null);
        row.put("clubLongName",  c != null ? c.longName()  : null);
    }

    private <T> Map<String, Object> paginate(List<T> all, int page, int size)
    {
        int total = all.size();
        int from = Math.min(page * size, total);
        int to = Math.min(from + size, total);
        return Map.of(
            "items", all.subList(from, to),
            "total", total,
            "page", page,
            "size", size
        );
    }

    private int parseIntParam(HttpServletRequest req, String name, int defaultValue)
    {
        String v = req.getParameter(name);
        if (v == null)
            return defaultValue;
        try
        {
            return Integer.parseInt(v);
        }
        catch (NumberFormatException e)
        {
            return defaultValue;
        }
    }

    private void writeJson(HttpServletResponse resp, Object obj) throws IOException
    {
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(resp.getWriter(), obj);
    }

    /**
     * GET /api/comparison/design-candidates — returns candidate designs for the design-vs-design
     * comparison chart.
     * <p>
     * Accepts an optional {@code designAId} to restrict candidates to designs that have
     * co-raced (in the same division) with any boat of design A, and an optional {@code q}
     * text filter. Excluded designs are always omitted. Results are sorted by canonical name
     * and capped at 200. Each entry includes the design's spin and nonSpin RF.
     */
    private void handleDesignComparisonCandidates(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String designAId = req.getParameter("designAId");
        String q         = req.getParameter("q");
        String lowerQ    = q != null && !q.isBlank() ? q.toLowerCase() : null;

        // When designAId is given, restrict to designs that co-raced with any boat of design A
        Set<String> validDesignIds = null;
        if (designAId != null && !designAId.isBlank())
        {
            DesignDerived dda = cache.designDerived().get(designAId.trim());
            if (dda != null && dda.boatIds() != null)
            {
                validDesignIds = new HashSet<>();
                for (String boatIdA : dda.boatIds())
                {
                    BoatDerived bda = cache.boatDerived().get(boatIdA);
                    if (bda == null) continue;
                    for (String raceId : bda.raceIds())
                    {
                        Race race = store.races().get(raceId);
                        if (race == null || race.divisions() == null) continue;
                        for (var div : race.divisions())
                        {
                            boolean hasA = div.finishers().stream()
                                .anyMatch(f -> dda.boatIds().contains(f.boatId()));
                            if (!hasA) continue;
                            for (var f : div.finishers())
                            {
                                BoatDerived bd = cache.boatDerived().get(f.boatId());
                                if (bd != null && bd.boat().designId() != null)
                                    validDesignIds.add(bd.boat().designId());
                            }
                        }
                    }
                }
                validDesignIds.remove(designAId.trim());
            }
        }

        final Set<String> finalValid = validDesignIds;
        List<Map<String, Object>> designs = cache.designDerived().values().stream()
            .filter(dd -> !store.isDesignExcluded(dd.design().id()))
            .filter(dd -> finalValid == null || finalValid.contains(dd.design().id()))
            .filter(dd -> lowerQ == null
                || dd.design().id().toLowerCase().contains(lowerQ)
                || dd.design().canonicalName().toLowerCase().contains(lowerQ))
            .sorted(Comparator.comparing(dd -> dd.design().canonicalName(),
                Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER)))
            .limit(200)
            .map(dd ->
            {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("id",            dd.design().id());
                m.put("canonicalName", dd.design().canonicalName());
                ReferenceFactors rf = dd.referenceFactors();
                m.put("rfSpin",    rf != null ? factorMap(rf.spin())    : null);
                m.put("rfNonSpin", rf != null ? factorMap(rf.nonSpin()) : null);
                return m;
            })
            .collect(Collectors.toList());

        writeJson(resp, Map.of("designs", designs));
    }

    /**
     * GET /api/comparison/design-chart — returns elapsed-time ratio data for a design-vs-design
     * scatter chart.
     * <p>
     * Required params: {@code designAId} and {@code designBId}. For every race division where
     * at least one boat of each design finished, computes the median elapsed time of design-A
     * boats (y-axis) and design-B boats (x-axis), along with race/series metadata for hover
     * tooltips. Points are sorted by date. Also returns the spin/nonSpin RF for each design.
     * Returns 404 if either design ID is not in the cache.
     */
    private void handleDesignComparisonChart(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String designAId = req.getParameter("designAId");
        String designBId = req.getParameter("designBId");
        if (designAId == null || designBId == null) { resp.sendError(400); return; }
        designAId = designAId.trim();
        designBId = designBId.trim();

        DesignDerived dda = cache.designDerived().get(designAId);
        DesignDerived ddb = cache.designDerived().get(designBId);
        if (dda == null || ddb == null) { resp.sendError(404); return; }

        Set<String> boatIdsA = dda.boatIds() != null ? dda.boatIds() : Set.of();
        Set<String> boatIdsB = ddb.boatIds() != null ? ddb.boatIds() : Set.of();

        // Collect races from all boats of design A
        Set<String> racesToCheck = new HashSet<>();
        for (String bid : boatIdsA)
        {
            BoatDerived bd = cache.boatDerived().get(bid);
            if (bd != null) racesToCheck.addAll(bd.raceIds());
        }

        List<Map<String, Object>> points = new ArrayList<>();
        for (String raceId : racesToCheck)
        {
            Race race = store.races().get(raceId);
            if (race == null || race.divisions() == null) continue;

            for (var div : race.divisions())
            {
                List<Double> aElapsed = new ArrayList<>();
                List<Double> bElapsed = new ArrayList<>();
                List<String> aNames   = new ArrayList<>();
                List<String> bNames   = new ArrayList<>();

                for (var f : div.finishers())
                {
                    if (f.elapsedTime() == null) continue;
                    double elapsed = f.elapsedTime().toSeconds();
                    BoatDerived bd = cache.boatDerived().get(f.boatId());
                    String name = bd != null ? bd.boat().name() : f.boatId();
                    if (boatIdsA.contains(f.boatId())) { aElapsed.add(elapsed); aNames.add(name); }
                    if (boatIdsB.contains(f.boatId())) { bElapsed.add(elapsed); bNames.add(name); }
                }

                if (aElapsed.isEmpty() || bElapsed.isEmpty()) continue;

                String seriesName = null;
                if (race.seriesIds() != null && !race.seriesIds().isEmpty())
                {
                    String seriesId = race.seriesIds().getFirst();
                    var club = store.clubs().get(race.clubId());
                    if (club != null && club.series() != null)
                        for (var s : club.series())
                            if (seriesId.equals(s.id())) { seriesName = s.name(); break; }
                    if (seriesName == null) seriesName = seriesId;
                }

                Map<String, Object> pt = new LinkedHashMap<>();
                pt.put("x",          medianOf(bElapsed));
                pt.put("y",          medianOf(aElapsed));
                pt.put("date",       race.date() != null ? race.date().toString() : null);
                pt.put("raceId",     raceId);
                pt.put("raceName",   raceName(race));
                pt.put("seriesName", seriesName);
                pt.put("division",   div.name());
                pt.put("aBoats",     aNames);
                pt.put("bBoats",     bNames);
                points.add(pt);
            }
        }

        points.sort(Comparator.comparing(m -> (String) m.get("date"),
            Comparator.nullsFirst(Comparator.naturalOrder())));

        ReferenceFactors rfA = dda.referenceFactors();
        ReferenceFactors rfB = ddb.referenceFactors();

        Map<String, Object> designA = new LinkedHashMap<>();
        designA.put("id",            designAId);
        designA.put("canonicalName", dda.design().canonicalName());
        designA.put("rfSpin",    rfA != null ? factorMap(rfA.spin())    : null);
        designA.put("rfNonSpin", rfA != null ? factorMap(rfA.nonSpin()) : null);

        Map<String, Object> designB = new LinkedHashMap<>();
        designB.put("id",            designBId);
        designB.put("canonicalName", ddb.design().canonicalName());
        designB.put("rfSpin",    rfB != null ? factorMap(rfB.spin())    : null);
        designB.put("rfNonSpin", rfB != null ? factorMap(rfB.nonSpin()) : null);

        writeJson(resp, Map.of("designA", designA, "designB", designB, "points", points));
    }

    /**
     * GET /api/comparison/elapsed-chart — returns pairwise elapsed-time comparison data for two boats.
     * <p>
     * Required params: {@code boatAId} and {@code boatBId}. For each race division in which both
     * boats recorded an elapsed time, emits a point with {@code x} = boat B elapsed seconds and
     * {@code y} = boat A elapsed seconds, plus race metadata. Also returns each boat's PF and RF
     * factors for use as reference lines. Used by the pairwise elapsed-time scatter charts on the
     * boat comparison page.
     */
    private void handleElapsedComparisonChart(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String boatAId = req.getParameter("boatAId");
        String boatBId = req.getParameter("boatBId");
        if (boatAId == null || boatBId == null) { resp.sendError(400); return; }
        boatAId = boatAId.trim();
        boatBId = boatBId.trim();

        BoatDerived bda = cache.boatDerived().get(boatAId);
        BoatDerived bdb = cache.boatDerived().get(boatBId);
        if (bda == null || bdb == null) { resp.sendError(404); return; }

        // Collect races where boat A competed, then look for boat B in the same division
        Set<String> racesA = bda.raceIds() != null ? bda.raceIds() : Set.of();

        List<Map<String, Object>> points = new ArrayList<>();
        for (String raceId : racesA)
        {
            Race race = store.races().get(raceId);
            if (race == null || race.divisions() == null) continue;

            for (var div : race.divisions())
            {
                Double aElapsed = null, bElapsed = null;
                for (var f : div.finishers())
                {
                    if (f.elapsedTime() == null) continue;
                    if (f.boatId().equals(boatAId)) aElapsed = (double) f.elapsedTime().toSeconds();
                    if (f.boatId().equals(boatBId)) bElapsed = (double) f.elapsedTime().toSeconds();
                }
                if (aElapsed == null || bElapsed == null) continue;

                String seriesName = null;
                if (race.seriesIds() != null && !race.seriesIds().isEmpty())
                {
                    String seriesId = race.seriesIds().getFirst();
                    var club = store.clubs().get(race.clubId());
                    if (club != null && club.series() != null)
                        for (var s : club.series())
                            if (seriesId.equals(s.id())) { seriesName = s.name(); break; }
                    if (seriesName == null) seriesName = seriesId;
                }

                Map<String, Object> pt = new LinkedHashMap<>();
                pt.put("x",          bElapsed);
                pt.put("y",          aElapsed);
                pt.put("date",       race.date() != null ? race.date().toString() : null);
                pt.put("raceId",     raceId);
                pt.put("raceName",   raceName(race));
                pt.put("seriesName", seriesName);
                pt.put("division",   div.name());
                points.add(pt);
            }
        }

        points.sort(Comparator.comparing(m -> (String) m.get("date"),
            Comparator.nullsFirst(Comparator.naturalOrder())));

        ReferenceFactors rfA = bda.referenceFactors();
        ReferenceFactors rfB = bdb.referenceFactors();
        BoatPf pfA = bda.pf();
        BoatPf pfB = bdb.pf();

        Map<String, Object> boatAMap = new LinkedHashMap<>();
        boatAMap.put("id",         boatAId);
        boatAMap.put("name",       bda.boat().name());
        boatAMap.put("sailNumber", bda.boat().sailNumber());
        boatAMap.put("rfSpin",     rfA != null ? factorMap(rfA.spin())    : null);
        boatAMap.put("rfNonSpin",  rfA != null ? factorMap(rfA.nonSpin()) : null);
        boatAMap.put("pfSpin",      pfA != null ? factorMap(pfA.spin())      : null);
        boatAMap.put("pfNonSpin",   pfA != null ? factorMap(pfA.nonSpin())   : null);
        boatAMap.put("pfTwoHanded", pfA != null ? factorMap(pfA.twoHanded()) : null);

        Map<String, Object> boatBMap = new LinkedHashMap<>();
        boatBMap.put("id",         boatBId);
        boatBMap.put("name",       bdb.boat().name());
        boatBMap.put("sailNumber", bdb.boat().sailNumber());
        boatBMap.put("rfSpin",     rfB != null ? factorMap(rfB.spin())    : null);
        boatBMap.put("rfNonSpin",  rfB != null ? factorMap(rfB.nonSpin()) : null);
        boatBMap.put("pfSpin",      pfB != null ? factorMap(pfB.spin())      : null);
        boatBMap.put("pfNonSpin",   pfB != null ? factorMap(pfB.nonSpin())   : null);
        boatBMap.put("pfTwoHanded", pfB != null ? factorMap(pfB.twoHanded()) : null);

        writeJson(resp, Map.of("boatA", boatAMap, "boatB", boatBMap, "points", points));
    }

    private double medianOf(List<Double> values)
    {
        List<Double> sorted = values.stream().sorted().toList();
        int n = sorted.size();
        return n % 2 == 0
            ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
            : sorted.get(n / 2);
    }
}
