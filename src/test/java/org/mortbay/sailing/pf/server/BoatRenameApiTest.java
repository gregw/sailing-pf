package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.StringRequestContent;
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
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.Aliases;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Merging a renamed boat with a rename date: the merge plan suggests the date from the
 * races, the merge records the old name and when it was used, the dates can be edited, and
 * the old name has its own row in the boat list.
 */
class BoatRenameApiTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir Path tempDir;

    private DataStore store;
    private Server server;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws Exception
    {
        Files.createDirectories(tempDir.resolve("config"));
        Files.writeString(tempDir.resolve("config/aliases.yaml"), "boats: {}\ndesigns: {}\n");
        store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        store.putBoat(new Boat("7709-sailexchange", "7709", "Sail Exchange", null, List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("7709-kanreki", "7709", "Kanreki", null, List.of(), List.of(), List.of(), null, null));
        race(LocalDate.of(2019, 3, 2), 1, "7709-sailexchange");
        race(LocalDate.of(2020, 2, 1), 2, "7709-sailexchange");
        race(LocalDate.of(2021, 10, 9), 3, "7709-kanreki");
        race(LocalDate.of(2023, 1, 14), 4, "7709-kanreki");
        AnalysisCache cache = new AnalysisCache(store);
        cache.refreshIndexes();

        AuthConfig authConfig = AuthConfig.disabled(-1, 0, null);
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler("/");
        context.setSessionHandler(new SessionHandler());
        // No WriteAuthFilter: these tests are of the API, not who may use it.
        context.addServlet(new ServletHolder(new AdminApiServlet(store, null, cache, null, authConfig)), "/api/*");
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
        client.stop();
        server.stop();
        store.stop();
    }

    @Test
    void proposeRenameFromRaceDates()
    {
        LocalDate a = LocalDate.of(2019, 1, 1), b = LocalDate.of(2020, 1, 1), c = LocalDate.of(2021, 1, 1), d = LocalDate.of(2022, 1, 1);
        assertEquals(new AdminApiServlet.RenameProposal(true, c), AdminApiServlet.proposeRename(c, d, a, b),
            "merged boat raced first: its name was used until the kept boat's first race");
        assertEquals(new AdminApiServlet.RenameProposal(false, c), AdminApiServlet.proposeRename(a, b, c, d),
            "merged boat raced after: its name was used from its own first race");
        assertNull(AdminApiServlet.proposeRename(a, c, b, d).date(), "overlapping races: no date");
        assertNull(AdminApiServlet.proposeRename(null, null, a, b).date(), "no races: no date");
    }

    @Test
    @SuppressWarnings("unchecked")
    void mergeWithRenameDateKeepsTheOldNameAndWhen() throws Exception
    {
        Map<String, Object> plan = MAPPER.readValue(get("/api/boats/merge-plan?keep=7709-kanreki&merge=7709-sailexchange"), Map.class);
        Map<String, Object> proposal = ((List<Map<String, Object>>) plan.get("boats")).getFirst();
        assertEquals(true, proposal.get("alias"));
        assertEquals("until", proposal.get("direction"));
        assertEquals("2021-10-09", proposal.get("date"));

        ContentResponse merged = post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-sailexchange"],
             "renames":{"7709-sailexchange":{"direction":"until","date":"2021-08-14"}}}""");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());

        Boat kanreki = store.boats().get("7709-kanreki");
        List<Aliases.SailNumberName> aliases = store.writtenBoatAliases(kanreki);
        assertEquals(1, aliases.size());
        assertEquals("Sail Exchange", aliases.getFirst().displayName());
        assertEquals(LocalDate.of(2021, 8, 14), aliases.getFirst().until());
        assertTrue(store.findOrCreateBoat("7709", "Sail Exchange", null).id().equals("7709-kanreki"),
            "imports still resolve the old name to the boat");

        // The old name has its own row, opening the merged boat
        List<Map<String, Object>> rows = (List<Map<String, Object>>) MAPPER.readValue(get("/api/boats?q=exchange"), Map.class).get("items");
        assertEquals(1, rows.size());
        assertEquals("7709-kanreki", rows.getFirst().get("id"));
        assertEquals("Sail Exchange", rows.getFirst().get("name"));
        assertEquals("Kanreki", rows.getFirst().get("aliasOf"));
        assertEquals("2021-08-14", rows.getFirst().get("aliasUntil"));

        // Clearing the date leaves a plain alias, no longer listed
        ContentResponse cleared = post("/api/boats/rename-dates", """
            {"boatId":"7709-kanreki","aliases":[{"sailNumber":"7709","name":"sailexchange","from":null,"until":null}]}""");
        assertEquals(200, cleared.getStatus(), cleared.getContentAsString());
        assertFalse(store.writtenBoatAliases(kanreki).getFirst().isRename());
        rows = (List<Map<String, Object>>) MAPPER.readValue(get("/api/boats?q=exchange"), Map.class).get("items");
        assertTrue(rows.isEmpty(), "a merge without a date is not listed: " + rows);
    }

    @Test
    void mergeWithoutDateIsAPlainAlias() throws Exception
    {
        ContentResponse merged = post("/api/boats/merge", "{\"keepId\":\"7709-kanreki\",\"mergeIds\":[\"7709-sailexchange\"]}");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());
        Aliases.SailNumberName alias = store.writtenBoatAliases(store.boats().get("7709-kanreki")).getFirst();
        assertEquals("sailexchange", alias.name());
        assertFalse(alias.isRename());
    }

    private void race(LocalDate date, int n, String boatId)
    {
        store.putRace(new Race("club-" + date + "-000" + n, "club", List.of("series"), date, n, null,
            List.of(new Division("Div 1", List.of(new Finisher(boatId, Duration.ofHours(1), false, null)))),
            "test", null, null));
    }

    private String get(String path) throws Exception
    {
        return client.GET("http://localhost:" + port + path).getContentAsString();
    }

    private ContentResponse post(String path, String json) throws Exception
    {
        return client.newRequest("http://localhost:" + port + path).method(HttpMethod.POST)
            .body(new StringRequestContent("application/json", json)).send();
    }
}
