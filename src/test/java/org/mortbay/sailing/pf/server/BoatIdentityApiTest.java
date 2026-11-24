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
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boat identities through the API: the merge plan says whether boats look like one boat
 * renamed; a link keeps each name as an identity with its results, a spelling merge makes an
 * alias; identities are listed, edited, unlinked, and shown in the boat list and PF history.
 */
class BoatIdentityApiTest
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
        store.putBoat(new Boat("7709-abouttime", "7709", "About Time", null, List.of(), List.of(), List.of(), null, null));
        race(LocalDate.of(2016, 11, 5), 5, "7709-abouttime");
        race(LocalDate.of(2017, 12, 2), 6, "7709-abouttime");
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> json(String path) throws Exception
    {
        return MAPPER.readValue(get(path), Map.class);
    }

    @Test
    void mergePlanSaysWhetherTheBoatsLookLikeARename() throws Exception
    {
        Map<String, Object> plan = json("/api/boats/merge-plan?keep=7709-kanreki&merge=7709-abouttime,7709-sailexchange");
        assertEquals(true, plan.get("looksLikeRename"), "three periods, one after the other");
        assertEquals(true, plan.get("sameDesign"));
        // An overlapping boat makes it look like a misspelling instead
        race(LocalDate.of(2022, 6, 4), 7, "7709-abouttime");
        plan = json("/api/boats/merge-plan?keep=7709-kanreki&merge=7709-abouttime,7709-sailexchange");
        assertEquals(false, plan.get("looksLikeRename"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void linkKeepsEachNameAsAnIdentity() throws Exception
    {
        ContentResponse merged = post("/api/boats/merge",
            "{\"keepId\":\"7709-abouttime\",\"mergeIds\":[\"7709-kanreki\",\"7709-sailexchange\"],\"link\":true}");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());
        assertEquals("7709-kanreki", MAPPER.readValue(merged.getContentAsString(), Map.class).get("keptId"),
            "the most recently raced name is current");

        List<Map<String, Object>> identities = MAPPER.readValue(get("/api/boats/7709-abouttime/identities"), List.class);
        assertEquals(List.of("7709-abouttime", "7709-sailexchange", "7709-kanreki"),
            identities.stream().map(i -> i.get("id")).toList());
        assertEquals(true, identities.get(2).get("current"));
        assertEquals("2019-03-02", identities.get(1).get("firstRace"));

        // The list shows the boat, and a row for each former name that opens it
        List<Map<String, Object>> rows = (List<Map<String, Object>>) json("/api/boats?q=7709").get("items");
        assertEquals(3, rows.size());
        Map<String, Object> former = rows.stream().filter(r -> "About Time".equals(r.get("name"))).findFirst().orElseThrow();
        assertEquals("7709-kanreki", former.get("id"));
        assertEquals("Kanreki", former.get("aliasOf"));
        assertEquals("2017-12-02", former.get("aliasUntil"), "until its last race");
    }

    @Test
    void spellingMergeMakesAnAliasAndMovesTheResults() throws Exception
    {
        ContentResponse merged = post("/api/boats/merge",
            "{\"keepId\":\"7709-kanreki\",\"mergeIds\":[\"7709-sailexchange\"]}");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());
        Boat kanreki = store.boat("7709-kanreki");
        assertEquals(1, kanreki.identities().size());
        assertTrue(kanreki.identity().matches("7709", "sailexchange"));
        assertNull(store.boat("7709-sailexchange"), "no longer an identity: an alias");
    }

    @Test
    @SuppressWarnings("unchecked")
    void identitiesCanBeDatedGivenAliasesMadeCurrentAndUnlinked() throws Exception
    {
        post("/api/boats/merge", "{\"keepId\":\"7709-kanreki\",\"mergeIds\":[\"7709-abouttime\"],\"link\":true}");
        ContentResponse edited = post("/api/boats/identities", """
            {"boatId":"7709-kanreki","current":"7709-abouttime","identities":[
              {"id":"7709-abouttime","until":"2018-07-28","aliases":[{"name":"About Time II"},{"sailNumber":"AUS7709"}]}]}""");
        assertEquals(200, edited.getStatus(), edited.getContentAsString());
        Boat b = store.boat("7709-abouttime");
        assertEquals("7709-abouttime", b.currentId(), "made current");
        assertEquals(LocalDate.of(2018, 7, 28), b.identity().until());
        assertTrue(b.identity().matches("7709", "abouttimeii"));
        assertTrue(b.identity().matches("AUS7709", "abouttime"));

        ContentResponse unlinked = post("/api/boats/unlink", "{\"identityId\":\"7709-kanreki\"}");
        assertEquals(200, unlinked.getStatus(), unlinked.getContentAsString());
        assertEquals("7709-kanreki", store.canonicalBoatId("7709-kanreki"));
        assertEquals("7709-abouttime", store.canonicalBoatId("7709-abouttime"));
    }

    @Test
    void editingAFormerIdentityRenamesOnlyIt() throws Exception
    {
        post("/api/boats/merge", "{\"keepId\":\"7709-kanreki\",\"mergeIds\":[\"7709-abouttime\"],\"link\":true}");
        ContentResponse edited = post("/api/boats/edit",
            "{\"boatId\":\"7709-abouttime\",\"sailNumber\":\"7700\",\"name\":\"About Time\"}");
        assertEquals(200, edited.getStatus(), edited.getContentAsString());
        Boat b = store.boat("7700-abouttime");
        assertEquals("7700", b.sailNumber());
        assertEquals("7709-kanreki", b.currentId(), "Kanreki is still the boat's name");
        assertTrue(b.identity().matches("7709", "abouttime"), "the old sail number kept as an alias");
    }

    @Test
    void sameCertificateFilter() throws Exception
    {
        org.mortbay.sailing.pf.data.Certificate irc = new org.mortbay.sailing.pf.data.Certificate(
            "IRC", 2023, 1.061, false, false, false, false, null, null);
        org.mortbay.sailing.pf.data.Certificate orc = new org.mortbay.sailing.pf.data.Certificate(
            "ORC", 2024, 1.05, false, false, false, false, "261233", null);
        // One boat under two designs, sharing its IRC certificate
        store.putBoat(new Boat("6755-ausreo-first447", "6755", "Ausreo", "first447", List.of(), List.of(irc), List.of(), null, null));
        store.putBoat(new Boat("6755-ausreo-first477", "6755", "Ausreo", "first477", List.of(), List.of(irc), List.of(), null, null));
        // The same IRC rating on unrelated boats (a one-design class): not a match
        store.putBoat(new Boat("1-alpha-j70", "1", "Alpha", "j70", List.of(), List.of(irc), List.of(), null, null));
        // The same ORC certificate number under different details: a match
        store.putBoat(new Boat("2-beta-x", "2", "Beta", "x", List.of(), List.of(orc), List.of(), null, null));
        store.putBoat(new Boat("3-gamma-y", "3", "Gamma", "y", List.of(), List.of(orc), List.of(), null, null));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) MAPPER.readValue(
            get("/api/boats?dupeFilter=sameCert&size=100"), Map.class).get("items");
        assertEquals(java.util.Set.of("6755-ausreo-first447", "6755-ausreo-first477", "2-beta-x", "3-gamma-y"),
            rows.stream().map(r -> (String) r.get("id")).collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void renameCandidatesFilter() throws Exception
    {
        // Same sail and design, different names, one after the other: a likely rename.
        store.putBoat(new Boat("100-alpha-x", "100", "Alpha", "x", List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("100-beta-x", "100", "Beta", "x", List.of(), List.of(), List.of(), null, null));
        race(LocalDate.of(2015, 1, 3), 1, "100-alpha-x");
        race(LocalDate.of(2016, 1, 3), 1, "100-alpha-x");
        race(LocalDate.of(2017, 1, 3), 1, "100-beta-x");
        // Same sail and design, different names, racing at the same time: two boats.
        store.putBoat(new Boat("200-gamma-x", "200", "Gamma", "x", List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("200-delta-x", "200", "Delta", "x", List.of(), List.of(), List.of(), null, null));
        race(LocalDate.of(2015, 2, 7), 1, "200-gamma-x");
        race(LocalDate.of(2015, 6, 7), 1, "200-delta-x");
        race(LocalDate.of(2016, 2, 7), 1, "200-gamma-x");
        // Same sail, different design: not a rename candidate.
        store.putBoat(new Boat("300-eps-x", "300", "Eps", "x", List.of(), List.of(), List.of(), null, null));
        store.putBoat(new Boat("300-zeta-y", "300", "Zeta", "y", List.of(), List.of(), List.of(), null, null));
        race(LocalDate.of(2015, 3, 7), 1, "300-eps-x");
        race(LocalDate.of(2018, 3, 7), 1, "300-zeta-y");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = (List<Map<String, Object>>) MAPPER.readValue(
            get("/api/boats?dupeFilter=rename&size=100"), Map.class).get("items");
        assertEquals(java.util.Set.of("100-alpha-x", "100-beta-x"),
            rows.stream().map(r -> (String) r.get("id")).collect(java.util.stream.Collectors.toSet()));
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
