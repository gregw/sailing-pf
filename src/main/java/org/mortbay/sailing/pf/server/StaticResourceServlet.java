package org.mortbay.sailing.pf.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

public class StaticResourceServlet extends HttpServlet
{
    private static final Pattern INCLUDE = Pattern.compile("<!--\\s*INCLUDE\\s+(\\S+)\\s*-->");

    private static final String WIKI_RAW_BASE =
        "https://raw.githubusercontent.com/wiki/gregw/sailing-pf/";
    private static final String WIKI_HTML_BASE =
        "https://github.com/gregw/sailing-pf/wiki/";
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final Pattern A_HREF =
        Pattern.compile("<a href=\"([^\"]+)\"");

    /** In-app paths served from GitHub wiki pages (raw markdown, rendered on request). */
    private static final Map<String, String> WIKI_PAGES = Map.of(
        "/docs.md",     "Home",
        "/ui-tips.md",  "UI-Tips"
    );

    private static final List<Extension> MD_EXTENSIONS = List.of(TablesExtension.create());
    private static final Parser MD_PARSER = Parser.builder().extensions(MD_EXTENSIONS).build();
    private static final HtmlRenderer MD_RENDERER = HtmlRenderer.builder().extensions(MD_EXTENSIONS).build();

    private final Path filesystemContentRoot;

    public StaticResourceServlet()
    {
        this(null);
    }

    public StaticResourceServlet(Path filesystemContentRoot)
    {
        this.filesystemContentRoot = filesystemContentRoot;
    }

    /**
     * Opens a content resource by filename, falling back to the filesystem root if not on classpath.
     */
    private InputStream openContentResource(String filename) throws IOException
    {
        InputStream in = getClass().getResourceAsStream("/content/" + filename);
        if (in != null || filesystemContentRoot == null)
            return in;
        Path target = filesystemContentRoot.resolve(filename).normalize();
        if (!target.startsWith(filesystemContentRoot.normalize()))
            return null;
        try
        {
            return Files.newInputStream(target);
        }
        catch (IOException e)
        {
            return null;
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String path = req.getPathInfo();
        if (path == null || "/".equals(path))
            path = "/index.html";

        String wikiPage = WIKI_PAGES.get(path);
        if (wikiPage != null)
        {
            serveWikiPage(resp, wikiPage);
            return;
        }

        InputStream in = openContentResource(path.substring(1));
        if (in == null)
        {
            resp.sendError(404);
            return;
        }

        resp.setContentType(guessContentType(path));
        try (in)
        {
            if (path.endsWith(".html"))
            {
                String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                html = resolveIncludes(html);
                resp.getWriter().write(html);
            }
            else if (path.endsWith(".md"))
            {
                String md = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                String body = MD_RENDERER.render(MD_PARSER.parse(md));
                resp.getWriter().write(wrapMarkdownPage(body, deriveTitleFromPath(path)));
            }
            else
            {
                in.transferTo(resp.getOutputStream());
            }
        }
    }

    private void serveWikiPage(HttpServletResponse resp, String wikiPage) throws IOException
    {
        HttpRequest fetch = HttpRequest.newBuilder()
            .uri(URI.create(WIKI_RAW_BASE + wikiPage + ".md"))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();
        HttpResponse<String> response;
        try
        {
            response = HTTP.send(fetch, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            resp.sendError(502, "Wiki fetch interrupted");
            return;
        }
        if (response.statusCode() != 200)
        {
            resp.sendError(502, "Wiki fetch returned HTTP " + response.statusCode());
            return;
        }
        String body = MD_RENDERER.render(MD_PARSER.parse(response.body()));
        body = rewriteWikiLinks(body);
        String title = wikiPage.equals("Home") ? "Documentation" : wikiPage.replace('-', ' ');
        resp.setContentType("text/html; charset=UTF-8");
        resp.getWriter().write(wrapMarkdownPage(body, title));
    }

    private String wrapMarkdownPage(String body, String title) throws IOException
    {
        String page = "<!DOCTYPE html><html lang=\"en\"><head>" +
            "<meta charset=\"UTF-8\">" +
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">" +
            "<title>PF — " + escapeHtml(title) + "</title>" +
            "<link rel=\"stylesheet\" href=\"/style.css\">" +
            "<style>" +
            ".md-body{max-width:860px}" +
            ".md-body pre{background:#f5f5f5;padding:0.75rem 1rem;overflow-x:auto;border-radius:4px;font-size:0.85rem}" +
            ".md-body code{font-family:monospace;font-size:0.9em}" +
            ".md-body pre code{font-size:inherit}" +
            ".md-body table{border-collapse:collapse;margin:0.5rem 0}" +
            ".md-body th,.md-body td{border:1px solid #ddd;padding:0.3rem 0.6rem;text-align:left}" +
            ".md-body th{background:#f8f8f8}" +
            ".md-body h1,.md-body h2{border-bottom:1px solid #eee;padding-bottom:0.25rem}" +
            ".md-body h2{margin-top:2rem}" +
            ".md-body h3{margin-top:1.25rem;color:#444}" +
            "</style>" +
            "</head><body>" +
            "<!-- INCLUDE _nav.html -->" +
            "<div class=\"md-body\">" + body + "</div>" +
            "<script src=\"/common.js\"></script>" +
            "</body></html>";
        return resolveIncludes(page);
    }

    private static String rewriteWikiLinks(String html)
    {
        Matcher m = A_HREF.matcher(html);
        StringBuilder sb = new StringBuilder();
        while (m.find())
        {
            String href = m.group(1);
            String replacement;
            if (href.startsWith("#") || href.startsWith("http://") || href.startsWith("https://"))
                replacement = m.group(0);
            else
                replacement = "<a target=\"_blank\" rel=\"noopener\" href=\"" + WIKI_HTML_BASE + href + "\"";
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String deriveTitleFromPath(String path)
    {
        String name = path.substring(path.lastIndexOf('/') + 1);
        if (name.endsWith(".md"))
            name = name.substring(0, name.length() - 3);
        return name.replace('-', ' ');
    }

    private static String escapeHtml(String s)
    {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private String resolveIncludes(String html) throws IOException
    {
        Matcher m = INCLUDE.matcher(html);
        StringBuilder sb = new StringBuilder();
        while (m.find())
        {
            String file = m.group(1);
            InputStream inc = openContentResource(file);
            String replacement = "";
            if (inc != null)
            {
                String content = new String(inc.readAllBytes(), StandardCharsets.UTF_8);
                replacement = file.endsWith(".md")
                    ? MD_RENDERER.render(MD_PARSER.parse(content))
                    : content;
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String guessContentType(String path)
    {
        if (path.endsWith(".html") || path.endsWith(".md"))
            return "text/html; charset=UTF-8";
        if (path.endsWith(".js"))
            return "application/javascript; charset=UTF-8";
        if (path.endsWith(".css"))
            return "text/css; charset=UTF-8";
        if (path.endsWith(".json"))
            return "application/json";
        if (path.endsWith(".png"))
            return "image/png";
        if (path.endsWith(".ico"))
            return "image/x-icon";
        return "application/octet-stream";
    }
}
