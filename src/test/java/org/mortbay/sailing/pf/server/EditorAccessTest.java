package org.mortbay.sailing.pf.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.SessionHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Who may change data with sign-in on: editors only. A test filter stands in for Jetty's
 * OpenID authenticator, putting the claims named by X-Test-* headers into the session.
 */
class EditorAccessTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    private DataStore store;
    private Server server;
    private HttpClient client;
    private int port;

    /** Puts {email, hd, email_verified} from X-Test-* headers into the session as claims. */
    static class FakeSignIn implements Filter
    {
        @Override
        public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException
        {
            HttpServletRequest request = (HttpServletRequest)req;
            String email = request.getHeader("X-Test-Email");
            if (email != null)
            {
                Map<String, Object> claims = new LinkedHashMap<>();
                claims.put("email", email);
                if (request.getHeader("X-Test-Hd") != null)
                    claims.put("hd", request.getHeader("X-Test-Hd"));
                claims.put("email_verified", !"false".equals(request.getHeader("X-Test-Verified")));
                request.getSession(true).setAttribute(OpenIdAuthenticator.CLAIMS, claims);
            }
            chain.doFilter(req, res);
        }
    }

    @BeforeEach
    void setUp() throws Exception
    {
        store = new DataStore(tempDir);
        store.start();

        // adminPort = -1 so no request counts as the admin connector.
        AuthConfig auth = new AuthConfig(true, null, "id", "secret", null,
            List.of("myc.org.au"), List.of("greg@example.com"), false, -1, 0, null);

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler("/");
        context.setSessionHandler(new SessionHandler());
        context.addFilter(new FilterHolder(new FakeSignIn()), "/*", EnumSet.of(DispatcherType.REQUEST));
        context.addFilter(new FilterHolder(new WriteAuthFilter(auth)), "/api/*",
            EnumSet.of(DispatcherType.REQUEST));
        context.addServlet(new ServletHolder(new AuthServlet(auth)), "/auth/*");
        context.addServlet(new ServletHolder(new AdminApiServlet(store, null, null, null, auth)), "/api/*");
        server.setHandler(context);
        server.start();
        port = connector.getLocalPort();

        client = new HttpClient();
        client.start();
        client.getProtocolHandlers().clear();
        client.setFollowRedirects(false);
    }

    @AfterEach
    void tearDown() throws Exception
    {
        if (client != null)
            client.stop();
        if (server != null)
            server.stop();
        if (store != null)
            store.stop();
    }

    /** A write that reaches the servlet fails there (unknown series), not at the filter. */
    private int edit(String... headers) throws Exception
    {
        var request = client.newRequest("http://localhost:" + port + "/api/series/edit")
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json",
                "{\"seriesIds\":[\"x/y\"],\"seriesType\":\"ns\"}"));
        for (int i = 0; i < headers.length; i += 2)
        {
            String name = headers[i], value = headers[i + 1];
            request.headers(h -> h.put(name, value));
        }
        return request.send().getStatus();
    }

    @Test
    void notSignedInIs401() throws Exception
    {
        assertEquals(401, edit());
    }

    @Test
    void signedInButNotAnEditorIs403() throws Exception
    {
        assertEquals(403, edit("X-Test-Email", "someone@gmail.com"));
    }

    @Test
    void unverifiedAddressIsNotAnEditor() throws Exception
    {
        assertEquals(403, edit("X-Test-Email", "someone@myc.org.au", "X-Test-Verified", "false"));
    }

    @Test
    void editorsGetPastTheFilter() throws Exception
    {
        for (String[] h : List.of(
            new String[]{"X-Test-Email", "greg@example.com"},
            new String[]{"X-Test-Email", "someone@myc.org.au"},
            new String[]{"X-Test-Email", "someone@gmail.com", "X-Test-Hd", "myc.org.au"}))
        {
            int status = edit(h);
            assertNotEquals(401, status, String.join(" ", h));
            assertNotEquals(403, status, String.join(" ", h));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusReportsSignedInAndEditor() throws Exception
    {
        ContentResponse resp = client.newRequest("http://localhost:" + port + "/auth/status")
            .headers(h -> h.put("X-Test-Email", "someone@gmail.com")).send();
        Map<String, Object> body = MAPPER.readValue(resp.getContentAsString(), Map.class);
        assertEquals(true, body.get("enabled"));
        assertEquals(true, body.get("signedIn"));
        assertEquals("someone@gmail.com", body.get("email"));
        assertEquals(false, body.get("editor"));
        assertEquals(false, body.get("authenticated"));
    }

    @Test
    void loginReturnsOnlyToLocalPaths() throws Exception
    {
        assertEquals("/series.html?id=a", location("/auth/login?return=%2Fseries.html%3Fid%3Da"));
        assertEquals("/", location("/auth/login?return=%2F%2Fevil.example"));
        assertEquals("/", location("/auth/login?return=https%3A%2F%2Fevil.example"));
        assertEquals("/", location("/auth/login"));
    }

    private String location(String path) throws Exception
    {
        ContentResponse resp = client.newRequest("http://localhost:" + port + path).send();
        assertEquals(302, resp.getStatus());
        String loc = resp.getHeaders().get("Location");
        return loc.replaceFirst("^https?://[^/]+", "");
    }
}
