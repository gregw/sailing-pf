package org.mortbay.sailing.hpf.server;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.util.Map;

class AuthServlet extends HttpServlet
{
    private static final String CLAIMS_ATTR = "org.eclipse.jetty.security.openid.claims";
    private final AuthConfig authConfig;

    AuthServlet(AuthConfig authConfig)
    {
        this.authConfig = authConfig;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException
    {
        String path = req.getPathInfo();
        if (path == null)
            path = "";
        switch (path)
        {
            case "/status" -> handleStatus(req, resp);
            case "/protected" -> handleProtected(req, resp);
            case "/logout" -> handleLogout(req, resp);
            default -> resp.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void handleStatus(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        resp.setContentType("application/json");
        if (authConfig.devMode())
        {
            resp.getWriter().write("{\"authenticated\":true,\"email\":\"dev@localhost\",\"devMode\":true}");
            return;
        }
        if (authConfig.isAdminConnector(req))
        {
            resp.getWriter().write("{\"authenticated\":true,\"email\":\"admin@local\",\"adminConnector\":true}");
            return;
        }
        HttpSession session = req.getSession(false);
        @SuppressWarnings("unchecked")
        Map<String, Object> claims = session != null
            ? (Map<String, Object>)session.getAttribute(CLAIMS_ATTR) : null;
        if (claims == null)
        {
            resp.getWriter().write("{\"authenticated\":false}");
        }
        else
        {
            String email = (String)claims.get("email");
            resp.getWriter().write("{\"authenticated\":true,\"email\":\"" + escJson(email) + "\"}");
        }
    }

    private void handleProtected(HttpServletRequest req, HttpServletResponse resp)
        throws IOException
    {
        HttpSession session = req.getSession(false);
        @SuppressWarnings("unchecked")
        Map<String, Object> claims = session != null
            ? (Map<String, Object>)session.getAttribute(CLAIMS_ATTR) : null;
        if (claims == null)
        {
            resp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        String email = (String)claims.get("email");
        String allowedDomain = authConfig.allowedDomain();
        if (allowedDomain != null && !allowedDomain.isBlank()
            && (email == null || !email.endsWith("@" + allowedDomain)))
        {
            session.invalidate();
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Login restricted to " + allowedDomain);
            return;
        }
        resp.sendRedirect("/");
    }

    private void handleLogout(HttpServletRequest req, HttpServletResponse resp) throws IOException
    {
        HttpSession session = req.getSession(false);
        if (session != null)
            session.invalidate();
        resp.sendRedirect("/");
    }

    private static String escJson(String s)
    {
        if (s == null)
            return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
