package org.mortbay.sailing.hpf.server;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;

import java.util.List;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StaticResourceServlet extends HttpServlet
{
    private static final Pattern INCLUDE = Pattern.compile("<!--\\s*INCLUDE\\s+(\\S+)\\s*-->");

    private static final List<Extension> MD_EXTENSIONS = List.of(TablesExtension.create());
    private static final Parser MD_PARSER = Parser.builder().extensions(MD_EXTENSIONS).build();
    private static final HtmlRenderer MD_RENDERER = HtmlRenderer.builder().extensions(MD_EXTENSIONS).build();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String path = req.getPathInfo();
        if (path == null || "/".equals(path))
            path = "/index.html";

        InputStream in = getClass().getResourceAsStream("/content" + path);
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
                String title = deriveTitleFromPath(path);
                String page = "<!DOCTYPE html><html lang=\"en\"><head>" +
                    "<meta charset=\"UTF-8\">" +
                    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">" +
                    "<title>HPF — " + escapeHtml(title) + "</title>" +
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
                page = resolveIncludes(page);
                resp.getWriter().write(page);
            }
            else
            {
                in.transferTo(resp.getOutputStream());
            }
        }
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
            InputStream inc = getClass().getResourceAsStream("/content/" + file);
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
