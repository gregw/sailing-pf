package org.mortbay.sailing.pf.server;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.data.SeriesType;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Servlet-level tests for the series listing's type column and filter, and the series
 * edit endpoints.
 */
class SeriesApiTest
{
    private static final String CLUB = "test.club.au";
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
        store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);

        Race spinRace = race("spin-race", CLUB + "/summer-spinnaker", false, false);
        Race nsRace = race("ns-race", CLUB + "/wednesday-non-spinnaker", false, true);
        Race mixedRace = race("mixed-race", CLUB + "/twilight", false, true);
        store.putClub(new Club(CLUB, "TC", null, null, false, null, List.of(), List.of(), List.of(),
            List.of(
                new Series(CLUB + "/summer-spinnaker", "Summer Spinnaker", false, List.of(spinRace.id())),
                new Series(CLUB + "/wednesday-non-spinnaker", "Wednesday Non-Spinnaker", false,
                    List.of(nsRace.id())),
                new Series(CLUB + "/twilight", "Twilight", false, List.of(mixedRace.id()))),
            null));
        store.putRace(spinRace);
        store.putRace(nsRace);
        store.putRace(mixedRace);
        store.setSeriesType(CLUB + "/summer-spinnaker", SeriesType.SPIN);

        // adminPort = -1 so WriteAuthFilter enforces authentication for non-open paths.
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

    /** One division of two finishers: the first spin, the second as given. */
    private static Race race(String id, String seriesId, boolean firstNs, boolean secondNs)
    {
        return new Race(id, CLUB, List.of(seriesId), LocalDate.of(2026, 1, 7), 1, null,
            List.of(new Division("Open", List.of(
                new Finisher("boat-a", Duration.ofMinutes(60), firstNs, null),
                new Finisher("boat-b", Duration.ofMinutes(62), secondNs, null)))),
            null, null, null);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> listSeries(String query) throws Exception
    {
        ContentResponse resp = client.GET("http://localhost:" + port + "/api/series?size=100" + query);
        assertEquals(200, resp.getStatus());
        Map<String, Object> body = MAPPER.readValue(resp.getContentAsString(), Map.class);
        List<Map<String, Object>> items = (List<Map<String, Object>>)body.get("items");
        return items.stream().collect(Collectors.toMap(m -> (String)m.get("id"), m -> m));
    }

    private ContentResponse post(String path, String json) throws Exception
    {
        return client.newRequest("http://localhost:" + port + path)
            .method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", json))
            .send();
    }

    @Test
    void listingReportsTypeAndActualEntries() throws Exception
    {
        Map<String, Map<String, Object>> rows = listSeries("");

        Map<String, Object> spin = rows.get(CLUB + "/summer-spinnaker");
        assertEquals("spin", spin.get("seriesType"));
        assertEquals(true, spin.get("seriesTypeSet"));

        Map<String, Object> ns = rows.get(CLUB + "/wednesday-non-spinnaker");
        assertEquals("ns", ns.get("seriesType"), "derived from the name");
        assertEquals(false, ns.get("seriesTypeSet"));

        Map<String, Object> unknown = rows.get(CLUB + "/twilight");
        assertEquals("unknown", unknown.get("seriesType"));
        assertFalse(unknown.containsKey("entriesType"), "only worked out when filtering for Unknown");

        Map<String, Object> filtered = listSeries("&type=unknown").get(CLUB + "/twilight");
        assertEquals("mixed", filtered.get("entriesType"), "source flags left alone");
    }

    @Test
    void typeFilterSelectsMatchingSeries() throws Exception
    {
        assertEquals(List.of(CLUB + "/summer-spinnaker"), List.copyOf(listSeries("&type=spin").keySet()));
        assertEquals(List.of(CLUB + "/wednesday-non-spinnaker"), List.copyOf(listSeries("&type=ns").keySet()));
        assertEquals(List.of(CLUB + "/twilight"), List.copyOf(listSeries("&type=unknown").keySet()));
        assertTrue(listSeries("&type=mixed").isEmpty());
        assertEquals(3, listSeries("").size());
    }

    @Test
    void editRequiresAuthButEditRequestIsOpen() throws Exception
    {
        String body = "{\"seriesIds\":[\"" + CLUB + "/twilight\"],\"seriesType\":\"mixed\"}";
        assertEquals(401, post("/api/series/edit", body).getStatus());
        assertEquals(200, post("/api/series/edit-request", body).getStatus());
        assertEquals(SeriesType.UNKNOWN, store.seriesType(CLUB + "/twilight"), "request does not change it");
    }

    @Test
    void entriesTypeNullWithoutFinishers() throws Exception
    {
        store.putClub(new Club(CLUB, "TC", null, null, false, null, List.of(), List.of(), List.of(),
            List.of(new Series(CLUB + "/empty", "Empty", false, List.of())), null));
        Map<String, Object> row = listSeries("&type=unknown").get(CLUB + "/empty");
        assertNull(row.get("entriesType"));
        assertFalse((Boolean)row.get("seriesTypeSet"));
    }
}
