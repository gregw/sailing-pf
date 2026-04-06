package org.mortbay.sailing.hpf.server;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;

class WriteAuthFilter implements Filter
{
    private static final String CLAIMS_ATTR = "org.eclipse.jetty.security.openid.claims";
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

        // Dev mode and admin-connector requests are pre-authenticated — allow everything
        if (authConfig.devMode() || authConfig.isAdminConnector(request))
        {
            chain.doFilter(req, res);
            return;
        }
        if (!"POST".equalsIgnoreCase(request.getMethod()))
        {
            chain.doFilter(req, res);
            return;
        }
        HttpSession session = request.getSession(false);
        Object claims = session != null ? session.getAttribute(CLAIMS_ATTR) : null;
        if (claims == null)
        {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"unauthenticated\",\"loginUrl\":\"/auth/protected\"}");
            return;
        }
        chain.doFilter(req, res);
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
