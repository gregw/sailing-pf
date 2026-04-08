package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mortbay.sailing.hpf.analysis.ComparisonKey;
import org.mortbay.sailing.hpf.analysis.ComparisonResult;
import org.mortbay.sailing.hpf.analysis.ConversionGraph;
import org.mortbay.sailing.hpf.analysis.LinearFit;
import org.mortbay.sailing.hpf.analysis.ReferenceNetworkBuilder;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * REST API for handicap conversion analysis.
 *
 * <pre>
 * GET /api/analyse              → summary list [ { id, n, r2 }, … ]
 * GET /api/analyse/{id}         → full ComparisonResult (pairs + fit)
 * GET /api/analyse/{id}/table   → conversion table
 *     ?min=0.85&max=1.15&step=0.01
 *     → [ { x, predicted, weight }, … ]
 * </pre>
 */
public class AnalysisServlet extends HttpServlet
{
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();

    private final DataStore store;
    private final AnalysisCache cache;

    public AnalysisServlet(DataStore store, AnalysisCache cache)
    {
        this.store = store;
        this.cache = cache;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        String path = req.getPathInfo();
        if (path == null || path.isEmpty() || "/".equals(path))
        {
            handleList(resp);
            return;
        }

        // Strip leading slash
        String sub = path.startsWith("/") ? path.substring(1) : path;

        if ("network".equals(sub))
        {
            handleNetwork(resp);
        }
        else if (sub.endsWith("/table"))
        {
            String id = sub.substring(0, sub.length() - "/table".length());
            handleTable(id, req, resp);
        }
        else
        {
            handleDetail(sub, resp);
        }
    }

    private void handleList(HttpServletResponse resp) throws IOException
    {
        List<ComparisonResult> results = cache.comparisons();

        List<Map<String, Object>> summaries = new ArrayList<>();
        for (ComparisonResult r : results)
        {
            Map<String, Object> s;
            int nTotal = r.nTotal();
            int trimmed = r.trimmedPairs().size();
            if (r.fit() != null && trimmed > 0)
                s = Map.of("id", r.key().toId(), "n", nTotal, "r2", r.fit().r2(), "trimmed", trimmed);
            else if (r.fit() != null)
                s = Map.of("id", r.key().toId(), "n", nTotal, "r2", r.fit().r2());
            else
                s = Map.of("id", r.key().toId(), "n", nTotal);
            summaries.add(s);
        }
        writeJson(resp, summaries);
    }

    private void handleDetail(String id, HttpServletResponse resp) throws IOException
    {
        ComparisonResult result = cache.comparisons().stream()
            .filter(r -> r.key().toId().equals(id))
            .findFirst().orElse(null);
        if (result == null)
        {
            resp.sendError(404, "Unknown comparison id: " + id);
            return;
        }
        writeJson(resp, result);
    }

    private void handleTable(String id, HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        ComparisonResult result = cache.comparisons().stream()
            .filter(r -> r.key().toId().equals(id))
            .findFirst().orElse(null);
        if (result == null)
        {
            resp.sendError(404, "Unknown comparison id: " + id);
            return;
        }

        LinearFit fit = result.fit();
        if (fit == null)
        {
            resp.setStatus(422);
            writeJson(resp, Map.of("error", "Insufficient data for a fit (need ≥ 3 pairs)"));
            return;
        }

        double min  = parseDouble(req.getParameter("min"),  0.85);
        double max  = parseDouble(req.getParameter("max"),  1.15);
        double step = parseDouble(req.getParameter("step"), 0.01);

        if (step <= 0 || step > 1)
            step = 0.01;

        List<Map<String, Object>> table = new ArrayList<>();
        for (double x = min; x <= max + step * 0.001; x += step)
        {
            double roundedX = Math.round(x / step) * step;  // avoid floating-point drift
            table.add(Map.of(
                "x",         roundedX,
                "predicted", fit.predict(roundedX),
                "weight",    fit.weight(roundedX)
            ));
        }
        writeJson(resp, table);
    }

    private void handleNetwork(HttpServletResponse resp) throws IOException
    {
        List<ComparisonResult> results = cache.comparisons();

        Map<String, Map<String, Object>> nodeMap = new LinkedHashMap<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        for (ComparisonResult r : results)
        {
            LinearFit fit = r.fit();
            boolean weak = (fit == null || fit.r2() < cache.minAnalysisR2());

            ComparisonKey k = r.key();
            String fromId = nodeId(k.systemA(), k.yearA(), k.nonSpinA(), k.twoHandedA());
            String toId   = nodeId(k.systemB(), k.yearB(), k.nonSpinB(), k.twoHandedB());
            nodeMap.putIfAbsent(fromId, nodeMap(fromId, k.systemA(), k.yearA(), k.nonSpinA(), k.twoHandedA()));
            nodeMap.putIfAbsent(toId,   nodeMap(toId,   k.systemB(), k.yearB(), k.nonSpinB(), k.twoHandedB()));

            double medianWeight = fit != null ? medianWeight(r) : 0.0;

            Map<String, Object> edge = new LinkedHashMap<>();
            edge.put("fromId",       fromId);
            edge.put("toId",         toId);
            edge.put("analysisId",   k.toId());
            edge.put("r2",           fit != null ? fit.r2() : 0.0);
            edge.put("n",            r.nTotal());
            edge.put("medianWeight", medianWeight);
            if (weak)
                edge.put("weak", true);
            edges.add(edge);
        }

        // Count boats per (system, year, variant) node across the whole store
        // and ensure every cert-bearing node appears even if it has no edges.
        Map<String, Integer> certCounts = new LinkedHashMap<>();
        for (Boat boat : store.boats().values())
        {
            Set<String> seen = new HashSet<>();
            for (Certificate c : boat.certificates())
            {
                String sys = c.system();
                if (!"IRC".equals(sys) && !"ORC".equals(sys) && !"AMS".equals(sys))
                    continue;
                String nid = nodeId(sys, c.year(), c.nonSpinnaker(), c.twoHanded());
                nodeMap.putIfAbsent(nid, nodeMap(nid, sys, c.year(), c.nonSpinnaker(), c.twoHanded()));
                if (seen.add(nid))
                    certCounts.merge(nid, 1, Integer::sum);
            }
        }
        // Attach certCount to every node (0 if it appeared only via an edge)
        for (Map<String, Object> node : nodeMap.values())
            node.put("certCount", certCounts.getOrDefault((String) node.get("id"), 0));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodes", new ArrayList<>(nodeMap.values()));
        result.put("edges", edges);
        result.put("targetYear", cache.targetYear());
        result.put("certAgeYears", ReferenceNetworkBuilder.MAX_CERT_AGE_YEARS);
        writeJson(resp, result);
    }

    private static String nodeId(String system, int year, boolean nonSpin, boolean twoHanded)
    {
        String variant = nonSpin ? "nonspin" : (twoHanded ? "2h" : "spin");
        return system + "-" + year + "-" + variant;
    }

    private static Map<String, Object> nodeMap(String id, String system, int year,
                                                boolean nonSpin, boolean twoHanded)
    {
        String variantLabel = nonSpin ? " NS" : (twoHanded ? " 2H" : "");
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("id",        id);
        n.put("system",    system);
        n.put("year",      year);
        n.put("nonSpin",   nonSpin);
        n.put("twoHanded", twoHanded);
        n.put("label",     system + " " + year + variantLabel);
        return n;
    }

    private static double medianWeight(ComparisonResult r)
    {
        List<Double> weights = r.pairs().stream()
            .map(p -> r.fit().weight(p.x()))
            .sorted()
            .collect(Collectors.toList());
        if (weights.isEmpty())
            return 0.0;
        int n = weights.size();
        return (n % 2 == 0)
            ? (weights.get(n / 2 - 1) + weights.get(n / 2)) / 2.0
            : weights.get(n / 2);
    }

    private double parseDouble(String s, double def)
    {
        if (s == null)
            return def;
        try
        {
            return Double.parseDouble(s);
        }
        catch (NumberFormatException e)
        {
            return def;
        }
    }

    private void writeJson(HttpServletResponse resp, Object obj) throws IOException
    {
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(resp.getWriter(), obj);
    }
}
