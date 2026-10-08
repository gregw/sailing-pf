package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * POST /api/boats/resolve — looks boats up by sail number and/or name for a hand-written
 * handicap file, and is open to unauthenticated users.
 */
class BoatResolveApiTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    private DataStore store;
    private Server server;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws Exception
    {
        // Empty aliases so the classpath test aliases cannot rename these boats.
        Files.createDirectories(tempDir.resolve("config"));
        Files.writeString(tempDir.resolve("config/aliases.yaml"), "boats: {}\ndesigns: {}\n");
        store = new DataStore(tempDir);
        store.start();
        store.putBoat(new Boat("R350-absolut", "R350", "Absolut", null, List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("MYC7-daydreamin", "MYC7", "Daydreamin", null, List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("100-twin-a", "100", "Twin", null, List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("200-twin-b", "200", "Twin", null, List.of(), List.of(), List.of(), null, null));

        // adminPort = -1 so WriteAuthFilter treats every request as unauthenticated.
        AuthConfig authConfig = new AuthConfig(null, null, "http://localhost", null, -1, 0, null);
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler("/");
        context.setSessionHandler(new SessionHandler());
        context.addFilter(new FilterHolder(new WriteAuthFilter(authConfig)), "/api/*",
            EnumSet.of(DispatcherType.REQUEST));
        context.addServlet(
            new ServletHolder(new AdminApiServlet(store, null, null, null, authConfig)), "/api/*");
        server.setHandler(context);
        server.start();
        port = connector.getLocalPort();

        client = new HttpClient();
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

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> resolve(String rowsJson) throws Exception
    {
        ContentResponse resp = client.newRequest("http://localhost:" + port + "/api/boats/resolve")
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", "{\"rows\":" + rowsJson + "}"))
            .send();
        assertEquals(200, resp.getStatus(), "open to unauthenticated users");
        return (List<Map<String, Object>>)MAPPER.readValue(resp.getContentAsString(), Map.class).get("boats");
    }

    @Test
    void resolvesBySailAndNameThenSailThenName() throws Exception
    {
        List<Map<String, Object>> boats = resolve("""
            [{"sailno":"R350","name":"Absolut"},
             {"sailno":"MYC7","name":"Daydreamin’"},
             {"sailno":"R350","name":"Renamed"},
             {"name":"Absolut"},
             {"name":"Twin"},
             {"sailno":"X999","name":"Ghost"}]
            """);
        assertEquals("R350-absolut", boats.get(0).get("boatId"));
        assertEquals("MYC7-daydreamin", boats.get(1).get("boatId"), "punctuation ignored");
        assertEquals("R350-absolut", boats.get(2).get("boatId"), "falls back to sail alone");
        assertEquals("R350-absolut", boats.get(3).get("boatId"), "name alone when unique");
        assertNull(boats.get(4), "ambiguous name is not guessed");
        assertNull(boats.get(5));
    }
}
