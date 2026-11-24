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
    private Map<String, Map<String, Object>> plan(String keep, String merge) throws Exception
    {
        Map<String, Object> plan = MAPPER.readValue(get("/api/boats/merge-plan?keep=" + keep + "&merge=" + merge), Map.class);
        Map<String, Map<String, Object>> byId = new java.util.HashMap<>();
        for (Map<String, Object> b : (List<Map<String, Object>>) plan.get("boats"))
            byId.put((String) b.get("id"), b);
        return byId;
    }

    @Test
    void threeNamesMergedIntoTheLatestFormAChain() throws Exception
    {
        Map<String, Map<String, Object>> p = plan("7709-kanreki", "7709-abouttime,7709-sailexchange");
        // About Time was renamed Sail Exchange (not Kanreki), on Sail Exchange's first race
        assertEquals("until", p.get("7709-abouttime").get("direction"));
        assertEquals("2019-03-02", p.get("7709-abouttime").get("date"));
        assertEquals("Sail Exchange", p.get("7709-abouttime").get("otherName"));
        assertEquals("until", p.get("7709-sailexchange").get("direction"));
        assertEquals("2021-10-09", p.get("7709-sailexchange").get("date"));
        assertEquals("Kanreki", p.get("7709-sailexchange").get("otherName"));
    }

    @Test
    void threeNamesMergedIntoTheEarliestFormAChain() throws Exception
    {
        Map<String, Map<String, Object>> p = plan("7709-abouttime", "7709-sailexchange,7709-kanreki");
        // Sail Exchange was renamed from About Time; Kanreki from Sail Exchange (not About Time)
        assertEquals("from", p.get("7709-sailexchange").get("direction"));
        assertEquals("2019-03-02", p.get("7709-sailexchange").get("date"));
        assertEquals("About Time", p.get("7709-sailexchange").get("otherName"));
        assertEquals("from", p.get("7709-kanreki").get("direction"));
        assertEquals("2021-10-09", p.get("7709-kanreki").get("date"));
        assertEquals("Sail Exchange", p.get("7709-kanreki").get("otherName"));
        assertEquals("2019-03-02", p.get("7709-kanreki").get("otherFirstRace"));
        assertEquals("2020-02-01", p.get("7709-kanreki").get("otherLastRace"));
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
    @SuppressWarnings("unchecked")
    void aSecondRenameIsPlacedInTheNameHistory() throws Exception
    {
        post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-sailexchange"],
             "renames":{"7709-sailexchange":{"direction":"until","date":"2021-08-14"}}}""");
        // About Time raced before Sail Exchange: it was renamed Sail Exchange, not Kanreki
        Map<String, Object> plan = MAPPER.readValue(get("/api/boats/merge-plan?keep=7709-kanreki&merge=7709-abouttime"), Map.class);
        Map<String, Object> p = ((List<Map<String, Object>>) plan.get("boats")).getFirst();
        assertEquals("until", p.get("direction"));
        assertEquals("2019-03-02", p.get("date"));
        assertEquals("Sail Exchange", p.get("otherName"));
        assertEquals("2019-03-02", p.get("otherFirstRace"));
        assertEquals("2020-02-01", p.get("otherLastRace"));

        post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-abouttime"],
             "renames":{"7709-abouttime":{"direction":"until","date":"2019-03-02"}}}""");
        Boat kanreki = store.boats().get("7709-kanreki");
        List<Aliases.SailNumberName> renames = store.writtenBoatAliases(kanreki);
        assertEquals("About Time", AdminApiServlet.nameAt(kanreki, renames, LocalDate.of(2017, 1, 1)));
        assertEquals("Sail Exchange", AdminApiServlet.nameAt(kanreki, renames, LocalDate.of(2020, 1, 1)));
        assertEquals("Kanreki", AdminApiServlet.nameAt(kanreki, renames, LocalDate.of(2022, 1, 1)));
    }

    @Test
    void mergingARenamedBoatKeepsItsRenameDates() throws Exception
    {
        post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-sailexchange"],
             "renames":{"7709-sailexchange":{"direction":"until","date":"2021-08-14"}}}""");
        // Kanreki, with its own rename, is merged into About Time: its history moves with it
        ContentResponse merged = post("/api/boats/merge", """
            {"keepId":"7709-abouttime","mergeIds":["7709-kanreki"],
             "renames":{"7709-kanreki":{"direction":"from","date":"2019-03-02"}}}""");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());
        Aliases.SailNumberName sailExchange = store.writtenBoatAliases(store.boats().get("7709-abouttime")).stream()
            .filter(a -> "sailexchange".equals(a.name())).findFirst().orElseThrow();
        assertEquals(LocalDate.of(2021, 8, 14), sailExchange.until());
        assertEquals("Sail Exchange", sailExchange.displayName());
    }

    @Test
    void pickingAnAliasAsCurrentRepairsAMergeKeptUnderTheOldName() throws Exception
    {
        // Kanreki (renamed from Sail Exchange) merged into About Time, keeping the old name
        post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-sailexchange"],
             "renames":{"7709-sailexchange":{"direction":"until","date":"2021-10-09"}}}""");
        post("/api/boats/merge", """
            {"keepId":"7709-abouttime","mergeIds":["7709-kanreki"],
             "renames":{"7709-kanreki":{"direction":"from","date":"2021-10-09"}}}""");

        // The edit panel with Kanreki picked as current: an edit to its sail and name, then
        // the dates of the old current name, now an alias
        ContentResponse edited = post("/api/boats/edit",
            "{\"boatId\":\"7709-abouttime\",\"sailNumber\":\"7709\",\"name\":\"Kanreki\"}");
        assertEquals(200, edited.getStatus(), edited.getContentAsString());
        String newId = (String) MAPPER.readValue(edited.getContentAsString(), Map.class).get("newBoatId");
        ContentResponse dated = post("/api/boats/rename-dates", "{\"boatId\":\"" + newId
            + "\",\"aliases\":[{\"sailNumber\":\"7709\",\"name\":\"About Time\",\"until\":\"2019-03-02\"}]}");
        assertEquals(200, dated.getStatus(), dated.getContentAsString());

        Boat kanreki = store.boats().get(newId);
        assertEquals("Kanreki", kanreki.name());
        List<Aliases.SailNumberName> aliases = store.writtenBoatAliases(kanreki);
        assertEquals(2, aliases.size(), aliases.toString());
        assertEquals("About Time", AdminApiServlet.nameAt(kanreki, aliases, LocalDate.of(2017, 1, 1)));
        assertEquals("Sail Exchange", AdminApiServlet.nameAt(kanreki, aliases, LocalDate.of(2020, 1, 1)));
        assertEquals("Kanreki", AdminApiServlet.nameAt(kanreki, aliases, LocalDate.of(2022, 1, 1)));
    }

    @Test
    void aliasesCanBeAddedAndRemoved() throws Exception
    {
        ContentResponse added = post("/api/boats/aliases", """
            {"boatId":"7709-kanreki","add":[
              {"sailNumber":"AUS 7709","name":null},
              {"sailNumber":null,"name":"Kanreki II"},
              {"sailNumber":"7710","name":"Old Kanreki","until":"2015-01-01"},
              {"sailNumber":"7709","name":"KANREKI"}]}""");
        assertEquals(200, added.getStatus(), added.getContentAsString());
        Boat kanreki = store.boats().get("7709-kanreki");
        List<Aliases.SailNumberName> aliases = store.writtenBoatAliases(kanreki);
        // 7709 KANREKI is the boat itself, so it is skipped
        assertEquals(3, aliases.size(), aliases.toString());
        Aliases.SailNumberName old = aliases.stream().filter(a -> "oldkanreki".equals(a.name())).findFirst().orElseThrow();
        assertEquals("Old Kanreki", old.displayName());
        assertEquals(LocalDate.of(2015, 1, 1), old.until());

        ContentResponse removed = post("/api/boats/aliases", """
            {"boatId":"7709-kanreki","remove":[
              {"sailNumber":"AUS7709","name":null},{"sailNumber":null,"name":"kanrekiii"},
              {"sailNumber":"7710","name":"oldkanreki"}]}""");
        assertEquals(200, removed.getStatus(), removed.getContentAsString());
        assertEquals(List.of(), store.writtenBoatAliases(kanreki));
    }

    @Test
    void renamingABoatOffAnotherBoatsNameLeavesThatBoatsAliasesAlone() throws Exception
    {
        // Kanreki with its name history, and another boat wrongly given Kanreki's identity
        post("/api/boats/merge", """
            {"keepId":"7709-kanreki","mergeIds":["7709-sailexchange"],
             "renames":{"7709-sailexchange":{"direction":"until","date":"2021-10-09"}}}""");
        store.putBoat(new Boat("7709-kanreki-neoroma430", "7709", "Kanreki", "neoroma430", List.of(), List.of(), List.of(), null, null));

        ContentResponse edited = post("/api/boats/edit",
            "{\"boatId\":\"7709-kanreki-neoroma430\",\"sailNumber\":\"7700\",\"name\":\"About Time II\"}");
        assertEquals(200, edited.getStatus(), edited.getContentAsString());
        String newId = (String) MAPPER.readValue(edited.getContentAsString(), Map.class).get("newBoatId");
        // No "7709 Kanreki" alias on the renamed boat: the real Kanreki still uses that name
        assertEquals(List.of(), store.writtenBoatAliases(store.boats().get(newId)));
        assertEquals("sailexchange", store.writtenBoatAliases(store.boats().get("7709-kanreki")).getFirst().name());
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
    void mergeWithoutDateIsAPlainAlias() throws Exception
    {
        ContentResponse merged = post("/api/boats/merge", "{\"keepId\":\"7709-kanreki\",\"mergeIds\":[\"7709-sailexchange\"]}");
        assertEquals(200, merged.getStatus(), merged.getContentAsString());
        Aliases.SailNumberName alias = store.writtenBoatAliases(store.boats().get("7709-kanreki")).getFirst();
        assertEquals("sailexchange", alias.name());
        assertFalse(alias.isRename());
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
