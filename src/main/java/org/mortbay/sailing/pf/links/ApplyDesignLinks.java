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
 * links that were not added by this harvest, followed by the row's {@code links} (space
 * separated) with source {@value #SOURCE}. Re-applying therefore replaces this harvest's links and
 * never touches links entered by hand. Rows may be edited before applying: remove a URL from
 * {@code links} to drop it.
 * <p>
 * Without {@code --apply} it only reports what it would change.
 * <p>
 * Usage: {@code ApplyDesignLinks <candidates.csv> [admin-url, default http://localhost:8888] [--apply]}
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
        for (String a : args)
        {
            if ("--apply".equals(a))
                apply = true;
            else
                positional.add(a);
        }
        if (positional.isEmpty())
        {
            System.err.println("Usage: ApplyDesignLinks <candidates.csv> [admin-url] [--apply]");
            System.exit(2);
        }
        Path csvFile = Path.of(positional.get(0));
        String base = positional.size() > 1 ? positional.get(1) : "http://localhost:8888";

        List<List<String>> rows = parseCsv(Files.readString(csvFile, StandardCharsets.UTF_8));
        List<String> header = rows.getFirst();
        int approveCol = header.indexOf("approve");
        int idCol = header.indexOf("designId");
        int linksCol = header.indexOf("links");

        HttpClient client = new HttpClient();
        client.start();
        int changed = 0, unchanged = 0, failed = 0;
        try
        {
            for (List<String> row : rows.subList(1, rows.size()))
            {
                if (row.size() <= linksCol || !"y".equalsIgnoreCase(row.get(approveCol).trim()))
                    continue;
                String designId = row.get(idCol).trim();
                ContentResponse current = client.GET(base + "/api/designs/"
                    + URLEncoder.encode(designId, StandardCharsets.UTF_8) + "/profile");
                if (current.getStatus() != 200)
                {
                    LOG.warn("{}: design not found (HTTP {})", designId, current.getStatus());
                    failed++;
                    continue;
                }
                List<Map<String, String>> existing = new ArrayList<>();
                for (JsonNode l : MAPPER.readTree(current.getContentAsString()).path("links"))
                    existing.add(linkMap(l.path("url").asText(), l.path("label").asText(null), l.path("source").asText(null)));

                List<Map<String, String>> links = merge(existing, List.of(row.get(linksCol).trim().split("\\s+")));
                if (links.equals(existing))
                {
                    unchanged++;
                    continue;
                }
                LOG.info("{}: {}", designId, links.stream().map(l -> l.get("url")).toList());
                changed++;
                if (!apply)
                    continue;
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("designId", designId);
                body.put("links", links);
                ContentResponse response = client.POST(base + "/api/designs/edit")
                    .body(new StringRequestContent("application/json", MAPPER.writeValueAsString(body)))
                    .send();
                if (response.getStatus() != 200)
                {
                    LOG.warn("{}: edit failed: HTTP {} {}", designId, response.getStatus(), response.getContentAsString());
                    failed++;
                    changed--;
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
     * The design's existing links that this harvest did not add, then the harvested URLs not
     * already among them.
     */
    static List<Map<String, String>> merge(List<Map<String, String>> existing, List<String> harvested)
    {
        List<Map<String, String>> links = new ArrayList<>();
        for (Map<String, String> l : existing)
            if (!SOURCE.equals(l.get("source")))
                links.add(l);
        for (String url : harvested)
        {
            if (url.isBlank() || links.stream().anyMatch(l -> url.equals(l.get("url"))))
                continue;
            links.add(linkMap(url, null, SOURCE));
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
