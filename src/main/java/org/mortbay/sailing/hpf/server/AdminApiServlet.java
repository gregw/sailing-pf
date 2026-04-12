package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mortbay.sailing.hpf.analysis.BoatDerived;
import org.mortbay.sailing.hpf.analysis.BoatHpf;
import org.mortbay.sailing.hpf.analysis.DesignDerived;
import org.mortbay.sailing.hpf.analysis.EntryResidual;
import org.mortbay.sailing.hpf.analysis.HpfQuality;
import org.mortbay.sailing.hpf.analysis.PerformanceProfile;
import org.mortbay.sailing.hpf.analysis.ReferenceFactors;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.mortbay.sailing.hpf.store.Aliases;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
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
        else if (path.matches("/boats/[^/]+/hpf"))
            handleBoatHpf(path.replaceAll("^/boats/|/hpf$", ""), resp);
        else if (path.matches("/boats/[^/]+/reference"))
            handleBoatReference(path.replaceAll("^/boats/|/reference$", ""), resp);
        else if (path.startsWith("/boats"))
            handleBoats(path.substring("/boats".length()), req, resp);
        else if (path.startsWith("/designs"))
            handleDesigns(path.substring("/designs".length()), req, resp);
        else if (path.startsWith("/clubs"))
            handleClubs(path.substring("/clubs".length()), req, resp);
        else if (path.startsWith("/races"))
            handleRaces(path.substring("/races".length()), req, resp);
        else if (path.startsWith("/series"))
            handleSeries(path.substring("/series".length()), req, resp);
        else if ("/hpf/quality".equals(path))
            handleHpfQuality(resp);
        else if ("/comparison/candidates".equals(path))
            handleComparisonCandidates(req, resp);
        else if ("/comparison/chart".equals(path))
            handleComparisonChart(req, resp);
        else if ("/comparison/division".equals(path))
            handleComparisonDivision(req, resp);
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
        else
        {
            resp.sendError(404);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleSetExcluded(String entity, HttpServletRequest req,
                                   HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String id = (String) body.get("id");
            if (id == null)
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "id is required"));
                return;
            }
            boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
            switch (entity)
            {
                case "boats"   -> store.setBoatExcluded(id, excluded);
                case "designs" -> store.setDesignExcluded(id, excluded);
                case "races"   -> store.setRaceExcluded(id, excluded);
                default        -> { resp.sendError(400); return; }
            }
            writeJson(resp, Map.of("ok", true, "excluded", excluded));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

    private void handleStats(HttpServletResponse resp) throws IOException
    {
        // Merge seed + persisted to get total club count
        Map<String, Club> allClubs = new LinkedHashMap<>(store.clubSeed());
        allClubs.putAll(store.clubs());
        long seriesCount = store.clubs().values().stream()
            .filter(c -> c.series() != null)
            .flatMap(c -> c.series().stream())
            .filter(s -> !s.isCatchAll())
            .count();
        writeJson(resp, Map.of(
            "races", store.races().size(),
            "boats", store.boats().size(),
            "designs", store.designs().size(),
            "clubs", allClubs.size(),
            "series", seriesCount
        ));
    }

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
            boolean excludeNulls = "true".equals(req.getParameter("excludeNulls"));

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
                    case "sailNumber" -> Comparator.comparing(Boat::sailNumber, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                    case "name"       -> Comparator.comparing(Boat::name,       Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                    case "designId"   -> Comparator.comparing(Boat::designId,   Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                    case "clubId"     -> Comparator.comparing(Boat::clubId,     Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                    case "spinRef"    -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.referenceFactors() != null && bd2.referenceFactors().spin() != null) ? bd2.referenceFactors().spin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "hpf"        -> Comparator.comparing(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return (bd2 != null && bd2.hpf() != null && bd2.hpf().spin() != null) ? bd2.hpf().spin().value() : 0.0; },
                                            Comparator.<Double>naturalOrder());
                    case "finishes"   -> Comparator.comparingInt(
                                            (Boat b2) -> { BoatDerived bd2 = cache.boatDerived().get(b2.id()); return bd2 != null ? bd2.raceIds().size() : 0; });
                    case "profile"    -> Comparator.comparingDouble(
                                            (Boat b2) -> { PerformanceProfile p2 = cache.profilesByBoatId().get(b2.id()); return p2 != null ? p2.overallScore() : 0.0; });
                    default           -> Comparator.comparing(Boat::id,         Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                };
                all.sort(asc ? cmp : cmp.reversed());
            }

            List<Map<String, Object>> rows = all.stream().map(b ->
            {
                boolean excl = store.isBoatExcluded(b.id())
                    || (b.designId() != null && store.isDesignExcluded(b.designId()));
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id",         b.id());
                row.put("sailNumber", b.sailNumber());
                row.put("name",       b.name());
                row.put("designId",   b.designId());
                row.put("clubId",     b.clubId());
                BoatDerived bd = cache.boatDerived().get(b.id());
                Factor spin = (bd != null && bd.referenceFactors() != null) ? bd.referenceFactors().spin() : null;
                row.put("spinRef",    spin != null ? factorMap(spin) : null);
                BoatHpf hpf = (bd != null) ? bd.hpf() : null;
                Factor hpfSpin = (hpf != null) ? hpf.spin() : null;
                row.put("hpf",       hpfSpin != null ? factorMap(hpfSpin) : null);
                row.put("finishes", bd != null ? bd.raceIds().size() : 0);
                PerformanceProfile prof = cache.profilesByBoatId().get(b.id());
                row.put("profile", prof != null ? prof.overallScore() : null);
                row.put("excluded",   excl);
                return row;
            }).collect(Collectors.toList());

            if (excludeNulls && sort != null && !sort.isBlank())
                rows.removeIf(r -> r.get(sort) == null);

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

    private void handleBoatHpf(String id, HttpServletResponse resp) throws IOException
    {
        Boat boat = store.boats().get(id);
        if (boat == null)
        {
            resp.sendError(404);
            return;
        }
        BoatDerived bd = cache.boatDerived().get(id);
        BoatHpf hpf = bd != null ? bd.hpf() : null;
        ReferenceFactors rf = bd != null ? bd.referenceFactors() : null;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatId", id);
        result.put("boatName", boat.name());
        result.put("currentYear", cache.targetYear());

        result.put("spin", hpfVariantMap(hpf != null ? hpf.spin() : null,
            hpf != null ? hpf.referenceDeltaSpin() : 0.0,
            hpf != null ? hpf.spinRaceCount() : 0));
        result.put("nonSpin", hpfVariantMap(hpf != null ? hpf.nonSpin() : null,
            hpf != null ? hpf.referenceDeltaNonSpin() : 0.0,
            hpf != null ? hpf.nonSpinRaceCount() : 0));
        result.put("twoHanded", hpfVariantMap(hpf != null ? hpf.twoHanded() : null,
            hpf != null ? hpf.referenceDeltaTwoHanded() : 0.0,
            hpf != null ? hpf.twoHandedRaceCount() : 0));

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
                Map<String, Object> rm = new LinkedHashMap<>();
                rm.put("raceId", r.raceId());
                rm.put("division", r.divisionName());
                rm.put("date", r.raceDate().toString());
                rm.put("nonSpinnaker", r.nonSpinnaker());
                rm.put("twoHanded", r.twoHanded());
                rm.put("residual", r.residual());
                rm.put("weight", r.weight());
                resList.add(rm);
            }
            result.put("residuals", resList);
        }
        else
        {
            result.put("residuals", List.of());
        }

        // Performance profile — computed fleet-wide after HPF run, read from cache
        PerformanceProfile profile = cache.profilesByBoatId().get(id);
        if (profile != null)
            result.put("profile", profileMap(profile));

        writeJson(resp, result);
    }

    private Map<String, Object> hpfVariantMap(Factor f, double referenceDelta, int raceCount)
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
            boolean excludeNulls = "true".equals(req.getParameter("excludeNulls"));

            // Merge persisted clubs and seed stubs into a unified view
            Map<String, Club> all = new LinkedHashMap<>(store.clubSeed());
            all.putAll(store.clubs());  // persisted overrides seed

            List<Map<String, Object>> rows = all.values().stream()
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

            if (excludeNulls && sort != null && !sort.isBlank())
                rows.removeIf(r -> r.get(sort) == null);

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

    @SuppressWarnings("unchecked")
    private void handleSetClubExcluded(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        try
        {
            Map<String, Object> body = MAPPER.readValue(req.getInputStream(), Map.class);
            String id = (String) body.get("id");
            if (id == null)
            {
                resp.setStatus(400);
                writeJson(resp, Map.of("error", "id is required"));
                return;
            }
            boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
            store.setClubExcluded(id, excluded);
            writeJson(resp, Map.of("ok", true, "excluded", excluded));
        }
        catch (Exception e)
        {
            resp.setStatus(500);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
    }

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
            List<Aliases.SailNumberName> aliases = new ArrayList<>();
            for (String mergeId : mergeIds)
            {
                Boat mb = store.boats().get(mergeId);
                if (mb == null)
                {
                    resp.setStatus(404);
                    writeJson(resp, Map.of("error", "Merge boat not found: " + mergeId));
                    return;
                }
                List<String> names = new ArrayList<>();
                // Always record the merged boat's name (even if it equals the canonical, for completeness)
                names.add(mb.name());
                names.addAll(mb.aliases());
                aliasSpecs.add(new Aliases.MergeAliasSpec(
                    IdGenerator.normaliseSailNumber(mb.sailNumber()),
                    IdGenerator.normaliseSailNumber(keepBoat.sailNumber()),
                    keepBoat.name(),
                    names
                ));
            }

            DataStore.MergeResult result = store.mergeBoats(keepId, mergeIds);
            store.save();

            // Update aliases.yaml and reload the alias seed so future imports honour the merge
            Aliases.appendMergeAliases(store.configDir(), aliasSpecs);
            store.reloadAliases();

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
            .sorted(Comparator.comparing(bd -> bd.boat().name(), Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
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
            .sorted(Comparator.comparing(dd -> dd.design().canonicalName(), Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
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

            BoatHpf hpf = bd.hpf();
            bm.put("hpfSpin",      hpf != null ? factorMap(hpf.spin())      : null);
            bm.put("hpfNonSpin",   hpf != null ? factorMap(hpf.nonSpin())   : null);
            bm.put("hpfTwoHanded", hpf != null ? factorMap(hpf.twoHanded()) : null);

            List<EntryResidual> residuals = cache.residualsByBoatId().get(boatId);
            List<Map<String, Object>> entries = new ArrayList<>();
            if (residuals != null && hpf != null)
            {
                for (EntryResidual r : residuals)
                {
                    Factor hpfVariant = r.twoHanded() ? hpf.twoHanded()
                        : r.nonSpinnaker() ? hpf.nonSpin() : hpf.spin();
                    if (hpfVariant == null || Double.isNaN(hpfVariant.value())) continue;
                    double backCalcFactor = hpfVariant.value() * Math.exp(-r.residual());
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

        List<Map<String, Object>> finishers = new ArrayList<>();
        Set<String> variantsUsed = new java.util.LinkedHashSet<>();
        for (var f : div.finishers())
        {
            BoatDerived bd = cache.boatDerived().get(f.boatId());
            if (bd == null || f.elapsedTime() == null) continue;

            ReferenceFactors rf  = bd.referenceFactors();
            BoatHpf          hpf = bd.hpf();

            // Use each finisher's own nonSpinnaker flag to pick the correct variant
            String fVariant = f.nonSpinnaker() ? "nonSpin" : "spin";
            variantsUsed.add(fVariant);

            Factor hpfFactor = hpf == null ? null : switch (fVariant)
            {
                case "nonSpin" -> hpf.nonSpin();
                default        -> hpf.spin();
            };
            Factor rfFactor = rf == null ? null : switch (fVariant)
            {
                case "nonSpin" -> rf.nonSpin();
                default        -> rf.spin();
            };

            double elapsedSec = f.elapsedTime().toSeconds();
            Double hpfVal = hpfFactor != null && !Double.isNaN(hpfFactor.value()) ? hpfFactor.value() : null;
            Double rfVal  = rfFactor  != null && !Double.isNaN(rfFactor.value())  ? rfFactor.value()  : null;

            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("boatId",      f.boatId());
            fm.put("name",        bd.boat().name());
            fm.put("sailNumber",  bd.boat().sailNumber());
            fm.put("elapsed",     elapsedSec);
            fm.put("variant",     fVariant);
            fm.put("hpf",         hpfVal);
            fm.put("rf",          rfVal);
            fm.put("hpfWeight",   hpfFactor != null ? hpfFactor.weight() : null);
            fm.put("rfWeight",    rfFactor  != null ? rfFactor.weight()  : null);
            fm.put("hpfCorrected", hpfVal != null && hpfVal > 0 ? elapsedSec * hpfVal : null);
            fm.put("rfCorrected",  rfVal  != null && rfVal  > 0 ? elapsedSec * rfVal  : null);
            finishers.add(fm);
        }
        String divisionVariant = variantsUsed.size() == 1 ? variantsUsed.iterator().next() : "mixed";

        // Sort by HPF value ascending (nulls last)
        finishers.sort(Comparator.comparing(
            m -> (Double) m.get("hpf"), Comparator.nullsLast(Comparator.naturalOrder())));

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
            boolean excludeNulls = "true".equals(req.getParameter("excludeNulls"));
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
                    case "canonicalName" -> Comparator.comparing(Design::canonicalName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
                    case "spinRef"       -> Comparator.comparing(
                                               (Design d2) -> { DesignDerived dd2 = cache.designDerived().get(d2.id()); return (dd2 != null && dd2.referenceFactors() != null && dd2.referenceFactors().spin() != null) ? dd2.referenceFactors().spin().value() : 0.0; },
                                               Comparator.<Double>naturalOrder());
                    case "boats"         -> Comparator.comparingInt(
                                               (Design d2) -> { DesignDerived dd2 = cache.designDerived().get(d2.id()); return dd2 != null ? dd2.boatIds().size() : 0; });
                    default              -> Comparator.comparing(Design::id, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER));
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

            if (excludeNulls && sort != null && !sort.isBlank())
                rows.removeIf(r -> r.get(sort) == null);

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
            boolean excludeNulls = "true".equals(req.getParameter("excludeNulls"));
            // Enrich all filtered rows — needed to allow sort by seriesName or finishers
            List<Map<String, Object>> enriched = store.races().values().stream()
                .filter(r -> filterBoatId   == null || raceContainsBoat(r, filterBoatId))
                .filter(r -> filterClubId   == null || filterClubId.equals(r.clubId()))
                .filter(r -> filterSeriesId == null || (r.seriesIds() != null && r.seriesIds().contains(filterSeriesId)))
                .filter(r -> lower == null
                    || r.id().toLowerCase().contains(lower)
                    || (r.clubId() != null && r.clubId().toLowerCase().contains(lower)))
                .filter(r -> showExcluded || (!store.isRaceExcluded(r.id()) && !isRaceAllExcluded(r)))
                .map(this::raceRow)
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
                enriched.sort(mapComparator(sort, asc));

            if (excludeNulls)
                enriched.removeIf(r -> r.get("finishers") == null || ((Number) r.get("finishers")).intValue() == 0);
            else if (sort != null && !sort.isBlank())
                enriched.removeIf(r -> r.get(sort) == null);

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

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id",        s.id());
                row.put("clubId",    club.id());
                row.put("club",      clubShort);
                row.put("name",      s.name());
                row.put("firstDate", firstDate);
                row.put("lastDate",  lastDate);
                row.put("races",     seriesRaces.size());
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
        if (r.seriesIds() != null && !r.seriesIds().isEmpty())
        {
            String firstSeriesId = r.seriesIds().getFirst();
            var club = store.clubs().get(r.clubId());
            if (club != null && club.series() != null)
            {
                for (var s : club.series())
                {
                    if (firstSeriesId.equals(s.id()))
                    {
                        seriesName = s.name();
                        break;
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
        row.put("date", r.date());
        row.put("seriesName", seriesName);
        row.put("seriesId", firstSeriesId);
        row.put("name", raceName(r));
        row.put("finishers", finishers);
        row.put("excluded", store.isRaceExcluded(r.id()) || isRaceAllExcluded(r));

        var rd = cache.raceDerived().get(r.id());
        if (rd != null && rd.divisionHpfs() != null && !rd.divisionHpfs().isEmpty())
        {
            var divs = rd.divisionHpfs();
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

    private void handleHpfQuality(HttpServletResponse resp) throws IOException
    {
        HpfQuality q = cache.hpfQuality();
        if (q == null)
        {
            resp.sendError(404);
            return;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatsWithHpf", q.boatsWithHpf());
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

    private void handleImporters(HttpServletResponse resp) throws IOException
    {
        TaskService.ImportStatus status = _taskService.currentStatus();
        Integer nextSailSysRaceId = _taskService.nextSailSysRaceId();
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
            row.put("nextStartId", "sailsys-races".equals(e.name()) ? nextSailSysRaceId : null);
            entries.add(row);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entries", entries);
        result.put("schedule", _taskService.globalSchedule());
        result.put("targetIrcYear", _taskService.targetIrcYear());
        result.put("outlierSigma", _taskService.outlierSigma());
        result.put("minAnalysisR2", _taskService.minAnalysisR2());
        Map<String, Object> hpfConfig = new LinkedHashMap<>();
        hpfConfig.put("lambda", _taskService.hpfLambda());
        hpfConfig.put("convergenceThreshold", _taskService.hpfConvergenceThreshold());
        hpfConfig.put("maxInnerIterations", _taskService.hpfMaxInnerIterations());
        hpfConfig.put("maxOuterIterations", _taskService.hpfMaxOuterIterations());
        hpfConfig.put("outlierK", _taskService.hpfOutlierK());
        hpfConfig.put("asymmetryFactor", _taskService.hpfAsymmetryFactor());
        hpfConfig.put("outerDampingFactor", _taskService.hpfOuterDampingFactor());
        hpfConfig.put("outerConvergenceThreshold", _taskService.hpfOuterConvergenceThreshold());
        result.put("hpfConfig", hpfConfig);
        result.put("slidingAverageCount", _taskService.slidingAverageCount());
        result.put("slidingAverageDrops", _taskService.slidingAverageDrops());
        writeJson(resp, result);
    }

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

    private void handleImporterRun(String name, String mode, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        int startId = parseIntParam(req, "startId", 1);
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

            Object rawYear = body.get("targetIrcYear");
            Integer targetIrcYear = (rawYear instanceof Number n && n.intValue() > 0)
                ? n.intValue() : null;

            Object rawSigma = body.get("outlierSigma");
            Double outlierSigma = (rawSigma instanceof Number n && n.doubleValue() > 0)
                ? n.doubleValue() : null;

            // HPF config params
            Object rawLambda = body.get("hpfLambda");
            Double hpfLambda = (rawLambda instanceof Number n2 && n2.doubleValue() > 0)
                ? n2.doubleValue() : null;
            Object rawThreshold = body.get("hpfConvergenceThreshold");
            Double hpfConvergenceThreshold = (rawThreshold instanceof Number n3 && n3.doubleValue() > 0)
                ? n3.doubleValue() : null;
            Object rawMaxInner = body.get("hpfMaxInnerIterations");
            Integer hpfMaxInnerIterations = (rawMaxInner instanceof Number n4 && n4.intValue() > 0)
                ? n4.intValue() : null;
            Object rawMaxOuter = body.get("hpfMaxOuterIterations");
            Integer hpfMaxOuterIterations = (rawMaxOuter instanceof Number n5 && n5.intValue() > 0)
                ? n5.intValue() : null;
            Object rawOutlierK = body.get("hpfOutlierK");
            Double hpfOutlierK = (rawOutlierK instanceof Number n6 && n6.doubleValue() > 0)
                ? n6.doubleValue() : null;
            Object rawAsymmetry = body.get("hpfAsymmetryFactor");
            Double hpfAsymmetryFactor = (rawAsymmetry instanceof Number n7 && n7.doubleValue() > 0)
                ? n7.doubleValue() : null;
            Object rawDamping = body.get("hpfOuterDampingFactor");
            Double hpfOuterDampingFactor = (rawDamping instanceof Number n8 && n8.doubleValue() > 0 && n8.doubleValue() <= 1.0)
                ? n8.doubleValue() : null;
            Object rawOuterConvergence = body.get("hpfOuterConvergenceThreshold");
            Double hpfOuterConvergenceThreshold = (rawOuterConvergence instanceof Number n9 && n9.doubleValue() > 0)
                ? n9.doubleValue() : null;
            Object rawCrossVariant = body.get("hpfCrossVariantLambda");
            Double hpfCrossVariantLambda = (rawCrossVariant instanceof Number n10 && n10.doubleValue() >= 0)
                ? n10.doubleValue() : null;

            _taskService.setConfig(entries, new TaskService.GlobalSchedule(days, time),
                targetIrcYear, outlierSigma,
                hpfLambda, hpfConvergenceThreshold, hpfMaxInnerIterations, hpfMaxOuterIterations,
                hpfOutlierK, hpfAsymmetryFactor, hpfOuterDampingFactor, hpfOuterConvergenceThreshold,
                hpfCrossVariantLambda);
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
            if (av == null) return 1;   // nulls last
            if (bv == null) return -1;
            if (av instanceof String as && bv instanceof String bs)
                return String.CASE_INSENSITIVE_ORDER.compare(as, bs);
            if (av instanceof Comparable && bv instanceof Comparable)
                return ((Comparable<Object>) av).compareTo(bv);
            return av.toString().compareToIgnoreCase(bv.toString());
        };
        return asc ? cmp : cmp.reversed();
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
                Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
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
            Comparator.nullsLast(Comparator.naturalOrder())));

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

    private double medianOf(List<Double> values)
    {
        List<Double> sorted = values.stream().sorted().toList();
        int n = sorted.size();
        return n % 2 == 0
            ? (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0
            : sorted.get(n / 2);
    }
}
