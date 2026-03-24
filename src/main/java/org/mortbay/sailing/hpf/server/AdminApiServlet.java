package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mortbay.sailing.hpf.analysis.BoatReferenceFactors;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.data.Factor;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.mortbay.sailing.hpf.store.AliasSeedLoader;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AdminApiServlet extends HttpServlet
{
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();
    private static final int DEFAULT_PAGE_SIZE = 50;

    private final DataStore store;
    private final ImporterService importerService;
    private final AnalysisCache cache;

    public AdminApiServlet(DataStore store, ImporterService importerService, AnalysisCache cache)
    {
        this.store = store;
        this.importerService = importerService;
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
        else if (path.matches("/boats/[^/]+/reference"))
            handleBoatReference(path.replaceAll("^/boats/|/reference$", ""), resp);
        else if (path.startsWith("/boats"))
            handleBoats(path.substring("/boats".length()), req, resp);
        else if (path.startsWith("/designs"))
            handleDesigns(path.substring("/designs".length()), req, resp);
        else if (path.startsWith("/races"))
            handleRaces(path.substring("/races".length()), req, resp);
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

        if (path.startsWith("/importers/") && path.endsWith("/run"))
        {
            String name = path.substring("/importers/".length(), path.length() - "/run".length());
            String mode = req.getParameter("mode");
            if (mode == null)
                mode = "api";
            handleImporterRun(name, mode, req, resp);
        }
        else if ("/importers/stop".equals(path))
        {
            importerService.requestStop();
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
        else
        {
            resp.sendError(404);
        }
    }

    private void handleStats(HttpServletResponse resp) throws IOException
    {
        writeJson(resp, Map.of(
            "races", store.races().size(),
            "boats", store.boats().size(),
            "designs", store.designs().size()
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

            List<Boat> all = store.boats().values().stream()
                .filter(b -> lower == null
                    || b.id().toLowerCase().contains(lower)
                    || b.name().toLowerCase().contains(lower))
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
            {
                Comparator<Boat> cmp = switch (sort)
                {
                    case "sailNumber" -> Comparator.comparing(Boat::sailNumber, Comparator.nullsLast(Comparator.naturalOrder()));
                    case "name"       -> Comparator.comparing(Boat::name,       Comparator.nullsLast(Comparator.naturalOrder()));
                    case "designId"   -> Comparator.comparing(Boat::designId,   Comparator.nullsLast(Comparator.naturalOrder()));
                    case "clubId"     -> Comparator.comparing(Boat::clubId,     Comparator.nullsLast(Comparator.naturalOrder()));
                    default           -> Comparator.comparing(Boat::id,         Comparator.nullsLast(Comparator.naturalOrder()));
                };
                all.sort(asc ? cmp : cmp.reversed());
            }

            writeJson(resp, paginate(all, page, size));
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
        BoatReferenceFactors factors = cache.referenceFactors().get(id);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("boatId", id);
        result.put("currentYear", LocalDate.now().getYear());
        result.put("spin",      factors != null ? factorMap(factors.spin())      : null);
        result.put("nonSpin",   factors != null ? factorMap(factors.nonSpin())   : null);
        result.put("twoHanded", factors != null ? factorMap(factors.twoHanded()) : null);
        writeJson(resp, result);
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
            List<AliasSeedLoader.MergeAliasSpec> aliasSpecs = new ArrayList<>();
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
                aliasSpecs.add(new AliasSeedLoader.MergeAliasSpec(
                    IdGenerator.normaliseSailNumber(mb.sailNumber()),
                    IdGenerator.normaliseSailNumber(keepBoat.sailNumber()),
                    keepBoat.name(),
                    names
                ));
            }

            DataStore.MergeResult result = store.mergeBoats(keepId, mergeIds);
            store.save();

            // Update aliases.yaml and reload the alias seed so future imports honour the merge
            AliasSeedLoader.appendMergeAliases(store.configDir(), aliasSpecs);
            store.reloadAliasSeed();

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

    private Map<String, Object> factorMap(Factor f)
    {
        if (f == null)
            return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("value",  f.value());
        m.put("weight", f.weight());
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

            List<Design> all = store.designs().values().stream()
                .filter(d -> lower == null
                    || d.id().toLowerCase().contains(lower)
                    || d.canonicalName().toLowerCase().contains(lower))
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
            {
                Comparator<Design> cmp = "canonicalName".equals(sort)
                    ? Comparator.comparing(Design::canonicalName, Comparator.nullsLast(Comparator.naturalOrder()))
                    : Comparator.comparing(Design::id,            Comparator.nullsLast(Comparator.naturalOrder()));
                all.sort(asc ? cmp : cmp.reversed());
            }

            writeJson(resp, paginate(all, page, size));
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

            // Enrich all filtered rows — needed to allow sort by seriesName or finishers
            List<Map<String, Object>> enriched = store.races().values().stream()
                .filter(r -> lower == null
                    || r.id().toLowerCase().contains(lower)
                    || (r.clubId() != null && r.clubId().toLowerCase().contains(lower)))
                .map(this::raceRow)
                .collect(Collectors.toList());

            if (sort != null && !sort.isBlank())
                enriched.sort(mapComparator(sort, asc));

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
            if (seriesName == null)
                seriesName = firstSeriesId;
        }

        // Finisher count: sum across all divisions
        int finishers = 0;
        if (r.divisions() != null)
            for (var div : r.divisions())
                if (div.finishers() != null)
                    finishers += div.finishers().size();

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", r.id());
        row.put("clubId", r.clubId());
        row.put("date", r.date());
        row.put("seriesName", seriesName);
        row.put("name", r.name());
        row.put("finishers", finishers);
        return row;
    }

    private void handleImporters(HttpServletResponse resp) throws IOException
    {
        ImporterService.ImportStatus status = importerService.currentStatus();
        Map<String, Integer> lastIds = importerService.lastSailSysIds();
        List<Map<String, Object>> entries = new ArrayList<>();
        for (ImporterService.ImporterEntry e : importerService.importerEntries())
        {
            boolean isRunning = status != null
                && e.name().equals(status.importerName())
                && e.mode().equals(status.mode());
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", e.name());
            row.put("mode", e.mode());
            row.put("includeInSchedule", e.includeInSchedule());
            row.put("status", isRunning ? "running" : "idle");
            row.put("lastId", lastIds.get(e.name() + "-" + e.mode()));
            entries.add(row);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entries", entries);
        result.put("schedule", importerService.globalSchedule());
        result.put("targetIrcYear", importerService.targetIrcYear());
        result.put("outlierSigma", importerService.outlierSigma());
        writeJson(resp, result);
    }

    private void handleImporterStatus(HttpServletResponse resp) throws IOException
    {
        ImporterService.ImportStatus status = importerService.currentStatus();
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
            int sailSysId = importerService.currentSailSysId();
            if (sailSysId > 0)
                m.put("currentId", sailSysId);
            writeJson(resp, m);
        }
    }

    private void handleImporterRun(String name, String mode, HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        int startId = parseIntParam(req, "startId", 1);
        boolean accepted = importerService.submit(name, mode, startId);
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
            List<ImporterService.ImporterEntry> entries = importerMaps.stream()
                .map(m -> new ImporterService.ImporterEntry(
                    (String) m.get("name"),
                    (String) m.get("mode"),
                    Boolean.TRUE.equals(m.get("includeInSchedule"))))
                .toList();

            Object rawYear = body.get("targetIrcYear");
            Integer targetIrcYear = (rawYear instanceof Number n && n.intValue() > 0)
                ? n.intValue() : null;

            Object rawSigma = body.get("outlierSigma");
            Double outlierSigma = (rawSigma instanceof Number n && n.doubleValue() > 0)
                ? n.doubleValue() : null;

            importerService.setConfig(entries, new ImporterService.GlobalSchedule(days, time),
                targetIrcYear, outlierSigma);
            resp.setStatus(200);
            writeJson(resp, Map.of("ok", true));
        }
        catch (Exception e)
        {
            resp.setStatus(400);
            writeJson(resp, Map.of("error", e.getMessage()));
        }
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
            if (av instanceof Comparable && bv instanceof Comparable)
                return ((Comparable<Object>) av).compareTo(bv);
            return av.toString().compareTo(bv.toString());
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
}
