package org.mortbay.sailing.hpf.server;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.mortbay.sailing.hpf.analysis.ComparisonResult;
import org.mortbay.sailing.hpf.analysis.LinearFit;
import org.mortbay.sailing.hpf.store.DataStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        if (sub.endsWith("/table"))
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
