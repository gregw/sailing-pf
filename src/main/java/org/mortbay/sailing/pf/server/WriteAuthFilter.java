package org.mortbay.sailing.pf.server;

import java.io.IOException;
import java.util.Set;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Lets only editors ({@link Access#isEditor}) make POST requests, apart from the open
 * request-logging and lookup endpoints. Not signed in → 401 with a sign-in URL; signed in
 * but not an editor → 403.
 */
class WriteAuthFilter implements Filter
{
    /**
     * POST endpoints that are open to unauthenticated users (read-only: request logging and
     * boat lookup).
     */
    private static final Set<String> OPEN_POST_PATHS = Set.of(
        "/api/boats/merge-request", "/api/designs/merge-request",
        "/api/boats/edit-request", "/api/designs/edit-request", "/api/clubs/edit-request",
        "/api/series/edit-request", "/api/boats/resolve",
        "/api/boats/exclude-request", "/api/designs/exclude-request",
        "/api/clubs/exclude-request", "/api/races/exclude-request",
        "/api/series/exclude-request",
        "/api/designs/ignore-request",
        "/api/boats/dubious-request", "/api/designs/dubious-request",
        "/api/series/dubious-request", "/api/races/dubious-request");
    private final AuthConfig authConfig;

    WriteAuthFilter(AuthConfig authConfig)
    {
        this.authConfig = authConfig;
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
        throws IOException, ServletException
    {
        HttpServletRequest request = (HttpServletRequest)req;
        HttpServletResponse response = (HttpServletResponse)res;

        // Admin-connector requests are pre-authenticated — allow everything
        if (authConfig.isAdminConnector(request))
        {
            chain.doFilter(req, res);
            return;
        }
        if (!"POST".equalsIgnoreCase(request.getMethod()))
        {
            chain.doFilter(req, res);
            return;
        }
        // Allow specific POST paths for unauthenticated users (read-only request logging)
        String pathInfo = request.getServletPath() + (request.getPathInfo() != null ? request.getPathInfo() : "");
        if (OPEN_POST_PATHS.contains(pathInfo))
        {
            chain.doFilter(req, res);
            return;
        }
        if (Access.isEditor(request, authConfig))
        {
            chain.doFilter(req, res);
            return;
        }
        response.setContentType("application/json");
        if (Access.claims(request) == null)
        {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("{\"error\":\"unauthenticated\",\"loginUrl\":\"/auth/login\"}");
        }
        else
        {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.getWriter().write("{\"error\":\"forbidden\",\"message\":"
                + "\"This account can view but not change data\"}");
        }
    }

    @Override
    public void init(FilterConfig config)
    {
    }

    @Override
    public void destroy()
    {
    }
}
