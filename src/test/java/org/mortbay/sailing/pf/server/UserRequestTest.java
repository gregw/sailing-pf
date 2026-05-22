package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;

import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.SessionHandler;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Servlet-level tests for the user-request logging endpoints, covering the
 * "flag as dubious" feature: a POST to {@code /api/{entity}/dubious-request}
 * must be accepted from unauthenticated users and append a line to
 * {@code <dataRoot>/log/user-requests.log}. Also guards that adding the dubious
 * paths to {@link WriteAuthFilter} did not open unrelated write endpoints.
 */
class UserRequestTest
{
    @TempDir
    Path tempDir;

    private DataStore store;
    private Server server;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws Exception
    {
        store = new DataStore(tempDir);
        store.start();

        // adminPort = -1 (an impossible local port) so isAdminConnector() is always
        // false and WriteAuthFilter enforces authentication for non-open paths.
        AuthConfig authConfig = new AuthConfig(null, null, "http://localhost", null, -1, 0, null);

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler("/");
        context.setSessionHandler(new SessionHandler());
        context.addFilter(new FilterHolder(new WriteAuthFilter(authConfig)), "/api/*",
            EnumSet.of(DispatcherType.REQUEST));
        // taskService/cache/httpClient are unused by the dubious-request path.
        context.addServlet(
            new ServletHolder(new AdminApiServlet(store, null, null, null, authConfig)), "/api/*");
        server.setHandler(context);
        server.start();
        port = connector.getLocalPort();

        client = new HttpClient();
        // Drop the built-in protocol handlers: the auth handler treats a 401 without a
        // WWW-Authenticate header as a protocol violation, but we want to inspect the 401.
        client.start();
        client.getProtocolHandlers().clear();
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

    private ContentResponse post(String path, String json) throws Exception
    {
        return client.newRequest("http://localhost:" + port + path)
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", json))
            .send();
    }

    @Test
    void dubiousRequestLogsLineForEachEntity() throws Exception
    {
        for (String entity : List.of("boats", "designs", "series", "races"))
        {
            ContentResponse resp = post("/api/" + entity + "/dubious-request",
                "{\"ids\":[\"" + entity + "-x1\"],\"message\":\"looks wrong\"}");
            assertEquals(200, resp.getStatus(), entity + " dubious-request should be accepted");
            // Response is pretty-printed JSON; strip whitespace before matching.
            assertTrue(resp.getContentAsString().replaceAll("\\s", "").contains("\"ok\":true"),
                entity + " response should report ok");
        }

        Path log = tempDir.resolve("log").resolve("user-requests.log");
        assertTrue(Files.exists(log), "user-requests.log should have been created");
        List<String> lines = Files.readAllLines(log);
        assertEquals(4, lines.size());
        assertTrue(lines.get(0).contains("boats-dubious-request"), lines.get(0));
        assertTrue(lines.get(1).contains("designs-dubious-request"), lines.get(1));
        assertTrue(lines.get(2).contains("series-dubious-request"), lines.get(2));
        assertTrue(lines.get(3).contains("races-dubious-request"), lines.get(3));
        assertTrue(lines.get(0).contains("looks wrong"), "the user note should be logged");
    }

    @Test
    void nonOpenWriteEndpointStillRejectsUnauthenticatedUsers() throws Exception
    {
        // /api/boats/edit is not in OPEN_POST_PATHS — confirms the filter allowlist
        // was not over-broadened when the dubious-request paths were added.
        ContentResponse resp = post("/api/boats/edit", "{}");
        assertEquals(401, resp.getStatus());
    }
}
