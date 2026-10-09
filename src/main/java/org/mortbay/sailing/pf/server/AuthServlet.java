package org.mortbay.sailing.pf.server;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The sign-in pages under {@code /auth/}:
 * <ul>
 *   <li>{@code status} — who is signed in and whether they may change data;</li>
 *   <li>{@code login} — the one constrained path ({@link PfServer}): asking for it triggers the
 *       OpenID sign-in, after which this sends the browser back to the app;</li>
 *   <li>{@code logout} — ends the session;</li>
 *   <li>{@code error} — where Jetty sends a failed sign-in, with the reason.</li>
 * </ul>
 * The sign-in callback itself ({@link AuthConfig#redirectPath}) is handled by Jetty's
 * authenticator before it reaches here.
 */
class AuthServlet extends HttpServlet
{
    private static final Logger LOG = LoggerFactory.getLogger(AuthServlet.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String LOGIN_PATH = "/auth/login";
    static final String ERROR_PATH = "/auth/error";

    private final AuthConfig authConfig;

    AuthServlet(AuthConfig authConfig)
    {
        this.authConfig = authConfig;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String path = req.getPathInfo();
        if (path == null)
            path = "";
        switch (path)
        {
            case "/status" -> handleStatus(req, resp);
            // /protected is the old name of /login, kept for bookmarks and open pages.
            case "/login", "/protected" -> handleLogin(req, resp);
            case "/logout" -> handleLogout(req, resp);
            case "/error" -> handleError(req, resp);
            default -> resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    /**
     * {@code {enabled, signedIn, email, editor, adminConnector, authenticated}} — where
     * {@code authenticated} (what the pages test before offering changes) means "may change
     * data", i.e. {@code editor}.
     */
    private void handleStatus(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        boolean admin = authConfig.isAdminConnector(req);
        boolean editor = Access.isEditor(req, authConfig);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("enabled", authConfig.enabled());
        body.put("signedIn", Access.claims(req) != null);
        body.put("email", admin ? "admin@local" : Access.email(req));
        body.put("editor", editor);
        body.put("adminConnector", admin);
        body.put("authenticated", editor);
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        MAPPER.writeValue(resp.getWriter(), body);
    }

    /**
     * Reached only once signed in (the security handler constrains this path). Sends the
     * browser to {@code ?return=} when that is a path on this site, else to the home page.
     */
    private void handleLogin(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String ret = req.getParameter("return");
        // Only a local path: "//host" or "/\host" would be a redirect to another site.
        boolean local = ret != null && ret.startsWith("/") && !ret.startsWith("//") && !ret.startsWith("/\\");
        resp.sendRedirect(local ? ret : "/");
    }

    /** Works for anyone, so a wrong account can always get out without clearing cookies. */
    private void handleLogout(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        HttpSession session = req.getSession(false);
        if (session != null)
            session.invalidate();
        resp.sendRedirect("/");
    }

    /**
     * Explains a failed sign-in. Jetty passes the reason as query parameters:
     * {@code error_description_jetty} when its own check failed (a state nobody issued, a
     * session lost to a restart), else the provider's {@code error_description} / {@code error}.
     * It is logged as well, since whoever can fix it is usually reading the server log.
     */
    private void handleError(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        String reason = firstOf(req.getParameter(OpenIdAuthenticator.ERROR_PARAMETER),
            req.getParameter("error_description"), req.getParameter("error"));
        if (reason == null)
            reason = "no reason given";
        LOG.warn("Sign-in failed: {}", reason);
        resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
        resp.setContentType("text/html; charset=utf-8");
        resp.getWriter().write("""
            <!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
            <title>Sailing PF — sign-in failed</title>
            <style>body{font:16px/1.5 system-ui,sans-serif;margin:4rem auto;max-width:34rem;
            padding:0 1rem;color:#243}a{color:#2255aa}code{background:#eef2f5;padding:.1em .3em;
            border-radius:3px}</style></head><body>
            <h1>Sign-in failed</h1>
            <p><code>%s</code></p>
            <p><a href="/">Back to Sailing PF</a> · <a href="%s">Try again</a>. If it keeps
            failing, the reason above is the thing to fix — it is in the server log too.</p>
            </body></html>
            """.formatted(esc(reason), LOGIN_PATH));
    }

    private static String firstOf(String... values)
    {
        for (String v : values)
        {
            if (v != null && !v.isBlank())
                return v;
        }
        return null;
    }

    private static String esc(String s)
    {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
