package org.mortbay.sailing.pf.links;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.StringRequestContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage 3 of finding web pages about designs: applies the reviewed rows of
 * {@code design-link-candidates.csv} (see {@link DesignLinkMatcher}) through a running server's
 * design edit API, so it is safe while the server runs.
 * <p>
 * For each row with {@code approve} set to {@code y}, the design's links become its existing
 * links that were not added from this source, followed by the row's {@code links} (space
 * separated, those not already present) with the source given by {@code --source=} (default
 * {@value #SOURCE}). Re-applying therefore replaces that source's links and never touches links
 * entered by hand or from another source. Rows may be edited before applying: remove a URL from
 * {@code links} to drop it. An optional {@code labels} column gives the links' labels, in the
 * same order, separated by {@code |} (blank for none).
 * <p>
 * An optional {@code base} column names the design the row's design really is, which then gets
 * the links:
 * <ul>
 *   <li>{@code y}: the row's design is merged into the base (its names become aliases).</li>
 *   <li>{@code M}: the row's design is a modification of the base. It is renamed
 *   "&lt;base&gt; Modified", so imports of its names fold into the base ticked modified, and its
 *   boats move to the base ticked modified.</li>
 * </ul>
 * <p>
 * Without {@code --apply} it only reports what it would change.
 * <p>
 * Usage: {@code ApplyDesignLinks <candidates.csv> [admin-url, default http://localhost:8888] [--source=name] [--apply]}
 */
public class ApplyDesignLinks
{
    private static final Logger LOG = LoggerFactory.getLogger(ApplyDesignLinks.class);

    static final String SOURCE = "wikipedia";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception
    {
        List<String> positional = new ArrayList<>();
        boolean apply = false;
        String source = SOURCE;
        for (String a : args)
        {
            if ("--apply".equals(a))
                apply = true;
            else if (a.startsWith("--source="))
                source = a.substring("--source=".length());
            else
                positional.add(a);
        }
        if (positional.isEmpty())
        {
            System.err.println("Usage: ApplyDesignLinks <candidates.csv> [admin-url] [--source=name] [--apply]");
            System.exit(2);
        }
        Path csvFile = Path.of(positional.get(0));
        String base = positional.size() > 1 ? positional.get(1) : "http://localhost:8888";

        List<List<String>> rows = parseCsv(Files.readString(csvFile, StandardCharsets.UTF_8));
        List<String> header = rows.getFirst();
        int approveCol = header.indexOf("approve");
        int idCol = header.indexOf("designId");
        int linksCol = header.indexOf("links");
        int labelsCol = header.indexOf("labels");
        int baseCol = header.indexOf("base");

        HttpClient client = new HttpClient();
        client.start();
        int changed = 0, unchanged = 0, failed = 0;
        try
        {
            Api api = new Api(client, base, apply);
            for (List<String> row : rows.subList(1, rows.size()))
            {
                String approve = cell(row, approveCol);
                boolean modified = "m".equalsIgnoreCase(approve);
                if (row.size() <= linksCol || !("y".equalsIgnoreCase(approve) || modified))
                    continue;
                String designId = cell(row, idCol);
                String baseId = cell(row, baseCol);
                try
                {
                    if (modified && baseId.isEmpty())
                        throw new IllegalStateException("M needs a base design");
                    String target = baseId.isEmpty() ? designId : baseId;
                    boolean folded = !baseId.isEmpty() && foldIntoBase(api, designId, baseId, modified);

                    JsonNode current = api.get("/api/designs/" + URLEncoder.encode(target, StandardCharsets.UTF_8) + "/profile");
                    List<Map<String, String>> existing = new ArrayList<>();
                    for (JsonNode l : current.path("links"))
                        existing.add(linkMap(l.path("url").asText(), l.path("label").asText(null), l.path("source").asText(null)));

                    List<Map<String, String>> links = merge(existing, List.of(cell(row, linksCol).split("\\s+")),
                        List.of(cell(row, labelsCol).split("\\|", -1)), source);
                    if (links.equals(existing) && !folded)
                    {
                        unchanged++;
                        continue;
                    }
                    LOG.info("{}: {}", target, links.stream().map(l -> l.containsKey("label")
                        ? l.get("url") + " [" + l.get("label") + "]" : l.get("url")).toList());
                    if (!links.equals(existing))
                        api.post("/api/designs/edit", Map.of("designId", target, "links", links));
                    changed++;
                }
                catch (Exception e)
                {
                    LOG.warn("{}: {}", designId, e.getMessage());
                    failed++;
                }
            }
        }
        finally
        {
            client.stop();
        }
        LOG.info("{} designs {}, {} unchanged, {} failed", changed, apply ? "updated" : "to update (dry run; --apply to apply)",
            unchanged, failed);
    }

    /**
     * Folds {@code designId} into {@code baseId}: merged into it, or, when {@code modified},
     * renamed "&lt;base&gt; Modified" with its boats moved to the base ticked modified.
     * False if {@code designId} is already gone (folded on an earlier run), so there is nothing to do.
     */
    private static boolean foldIntoBase(Api api, String designId, String baseId, boolean modified) throws Exception
    {
        String enc = URLEncoder.encode(designId, StandardCharsets.UTF_8);
        JsonNode design = api.getOrNull("/api/designs/" + enc + "/profile");
        if (design == null)
            return false;
        JsonNode baseDesign = api.get("/api/designs/" + URLEncoder.encode(baseId, StandardCharsets.UTF_8) + "/profile");
        List<String> boatIds = new ArrayList<>();
        for (JsonNode b : api.get("/api/boats?showExcluded=true&size=100000&designId=" + enc).path("items"))
            boatIds.add(b.path("id").asText());
        if (!modified)
        {
            LOG.info("{}: merge into {} ({} boats)", designId, baseId, boatIds.size());
            api.post("/api/designs/merge", Map.of("keepId", baseId, "mergeIds", List.of(designId)));
            return true;
        }
        String name = baseDesign.path("designName").asText(baseId) + " Modified";
        LOG.info("{}: rename to \"{}\", boats {} to {} modified", designId, name, boatIds, baseId);
        JsonNode renamed = api.post("/api/designs/edit", Map.of("designId", designId, "canonicalName", name));
        String modifiedId = renamed == null ? designId : renamed.path("newDesignId").asText(designId);
        if (renamed == null)
            boatIds.clear(); // dry run
        else if (!modifiedId.equals(designId))
        {
            boatIds.clear();
            for (JsonNode b : api.get("/api/boats?showExcluded=true&size=100000&designId="
                + URLEncoder.encode(modifiedId, StandardCharsets.UTF_8)).path("items"))
                boatIds.add(b.path("id").asText());
        }
        for (String boatId : boatIds)
            api.post("/api/boats/edit", Map.of("boatId", boatId, "designId", baseId, "modified", true));
        return true;
    }

    private static String cell(List<String> row, int col)
    {
        return col < 0 || col >= row.size() ? "" : row.get(col).trim();
    }

    /** JSON calls on the admin API; posts are only logged unless applying. */
    private record Api(HttpClient client, String base, boolean apply)
    {
        JsonNode get(String path) throws Exception
        {
            JsonNode node = getOrNull(path);
            if (node == null)
                throw new IllegalStateException("not found: " + path);
            return node;
        }

        JsonNode getOrNull(String path) throws Exception
        {
            ContentResponse response = client.GET(base + path);
            if (response.getStatus() == 404)
                return null;
            if (response.getStatus() != 200)
                throw new IllegalStateException("GET " + path + ": HTTP " + response.getStatus());
            return MAPPER.readTree(response.getContentAsString());
        }

        /** The response, or null on a dry run. */
        JsonNode post(String path, Map<String, Object> body) throws Exception
        {
            if (!apply)
                return null;
            ContentResponse response = client.POST(base + path)
                .body(new StringRequestContent("application/json", MAPPER.writeValueAsString(body)))
                .send();
            if (response.getStatus() != 200)
                throw new IllegalStateException("POST " + path + ": HTTP " + response.getStatus() + " " + response.getContentAsString());
            return MAPPER.readTree(response.getContentAsString());
        }
    }

    /**
     * The design's existing links not from the source, then the harvested URLs not already among
     * them, from the source, each labelled by the label at the same index (if any and not blank).
     */
    static List<Map<String, String>> merge(List<Map<String, String>> existing, List<String> harvested,
        List<String> labels, String source)
    {
        List<Map<String, String>> links = new ArrayList<>();
        for (Map<String, String> l : existing)
            if (!source.equals(l.get("source")))
                links.add(l);
        for (int i = 0; i < harvested.size(); i++)
        {
            String url = harvested.get(i);
            if (url.isBlank() || links.stream().anyMatch(l -> url.equals(l.get("url"))))
                continue;
            String label = i < labels.size() && !labels.get(i).isBlank() ? labels.get(i).trim() : null;
            links.add(linkMap(url, label, source));
        }
        return links;
    }

    private static Map<String, String> linkMap(String url, String label, String source)
    {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("url", url);
        if (label != null)
            m.put("label", label);
        if (source != null)
            m.put("source", source);
        return m;
    }

    /** Parses CSV text with double-quoted fields (as written by {@link DesignLinkMatcher#csv}). */
    static List<List<String>> parseCsv(String text)
    {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < text.length(); i++)
        {
            char c = text.charAt(i);
            if (quoted)
            {
                if (c == '"' && i + 1 < text.length() && text.charAt(i + 1) == '"')
                {
                    field.append('"');
                    i++;
                }
                else if (c == '"')
                    quoted = false;
                else
                    field.append(c);
            }
            else if (c == '"')
                quoted = true;
            else if (c == ',')
            {
                row.add(field.toString());
                field.setLength(0);
            }
            else if (c == '\n' || c == '\r')
            {
                if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n')
                    i++;
                row.add(field.toString());
                field.setLength(0);
                rows.add(row);
                row = new ArrayList<>();
            }
            else
                field.append(c);
        }
        if (!field.isEmpty() || !row.isEmpty())
        {
            row.add(field.toString());
            rows.add(row);
        }
        return rows;
    }
}
