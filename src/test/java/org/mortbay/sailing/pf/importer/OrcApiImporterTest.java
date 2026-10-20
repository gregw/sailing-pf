package org.mortbay.sailing.pf.importer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.store.DataStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class OrcApiImporterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir Path tempDir;
    private DataStore store;
    private OrcApiImporter importer;

    @BeforeEach
    void setUp() {
        store = new DataStore(tempDir);
        store.start();
        importer = new OrcApiImporter(store, null, "orc_test"); // client not used by parsing methods
    }

    @AfterEach
    void tearDown() {
        store.stop();
    }

    @Test
    void parseRowsReadsDataRows() throws Exception {
        String json = """
            {"data":{"rows":[
              {"Family":1,"CountryId":"AUS","dxtID":1001,"RefNo":"03420004HB1","YachtName":"A"},
              {"Family":3,"CountryId":"AUS","dxtID":1002,"RefNo":"03420004HB2","YachtName":"B"}
            ]}}""";
        List<JsonNode> rows = importer.parseRows(json);
        assertEquals(2, rows.size());
        assertEquals("03420004HB2", rows.get(1).get("RefNo").asText());
    }

    @Test
    void parseRowsEmptyWhenNoData() throws Exception {
        assertTrue(importer.parseRows("{\"errors\":[\"Missing or invalid API key.\"]}").isEmpty());
    }

    @Test
    void parseGphReadsNumber() {
        assertEquals(660.8, importer.parseGph(certJson(660.8), "R1"));
    }

    @Test
    void parseGphReadsText() {
        assertEquals(612.3, importer.parseGph("{\"rms\":[{\"GPH\":\"612.3\"}]}", "R2"));
    }

    @Test
    void parseGphReturnsNaNWhenMissing() {
        assertEquals(Double.NaN, importer.parseGph("{\"rms\":[]}", "R3"));
        assertEquals(Double.NaN, importer.parseGph("{\"rms\":[{\"APHD\":578}]}", "R4"));
        assertEquals(Double.NaN, importer.parseGph("not json", "R5"));
    }

    @Test
    void errorsJoinsErrorArray() {
        assertEquals("Missing or invalid API key.",
            OrcApiImporter.errors("{\"errors\":[\"Missing or invalid API key.\"]}"));
        assertEquals("a; b", OrcApiImporter.errors("{\"errors\":[\"a\",\"b\"]}"));
    }

    @Test
    void errorsNullWhenNone() {
        assertNull(OrcApiImporter.errors("{\"data\":{\"rows\":[]}}"));
        assertNull(OrcApiImporter.errors("{\"errors\":[]}"));
        assertNull(OrcApiImporter.errors("not json"));
    }

    @Test
    void processRowCreatesDesignBoatAndCertificate() throws Exception {
        importer.processRow(row(2001, "AUS2024", "Wild Thing", "AUS1234", 2024, 3, "2024-12-31T00:00:00", "J/24", "ORC Standard"),
            certJson(612.3));

        assertNotNull(store.designs().get("j24"));
        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals("1234", boat.sailNumber());
        assertEquals("Wild Thing", boat.name());
        assertTrue(boat.sources().contains("ORC"));

        Certificate cert = boat.certificates().getFirst();
        assertEquals("ORC", cert.system());
        assertEquals(600.0 / 612.3, cert.value(), 1e-10);
        assertEquals(2024, cert.year());
        assertEquals("2001", cert.certificateNumber());
        assertEquals(LocalDate.of(2024, 12, 31), cert.expiryDate());
        assertTrue(cert.club());
        assertFalse(cert.nonSpinnaker());
        assertFalse(cert.twoHanded());
    }

    @Test
    void processRowNumericCertTypeSetsFlags() throws Exception {
        importer.processRow(row(2002, "R2002", "Duo", "AUS22", 2024, 9, "", "J/24", ""), certJson(600));
        importer.processRow(row(2003, "R2003", "Solo", "AUS33", 2024, 10, "", "J/24", ""), certJson(600));

        Certificate dh = boatNamed("Duo").certificates().getFirst();
        assertTrue(dh.twoHanded());
        assertTrue(dh.club());

        Certificate ns = boatNamed("Solo").certificates().getFirst();
        assertTrue(ns.nonSpinnaker());
        assertFalse(ns.club());
    }

    @Test
    void processRowNoGphSkipsCert() throws Exception {
        importer.processRow(row(2004, "R2004", "Ghost", "AUS44", 2024, 3, "", "J/24", ""), "{\"rms\":[]}");
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processRowImplausibleGphSkipsCert() throws Exception {
        importer.processRow(row(2005, "R2005", "Rocket", "AUS55", 2024, 3, "", "J/24", ""), certJson(99));
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processRowUpsertSharesDxtIdWithLegacyImporter() throws Exception {
        // A cert already imported by OrcImporter (same dxtID) is replaced, not duplicated
        new OrcCertRow("2006", "Pilot", "AUS300", "2024", "3", null, "J/24", null).importInto(store, 600.0);
        importer.processRow(row(2006, "R2006", "Pilot", "AUS300", 2024, 3, "", "J/24", ""), certJson(601));
        assertEquals(1, store.boats().values().iterator().next().certificates().size());
        assertTrue(OrcCertRow.importedDxtIds(store).contains("2006"));
    }

    @Test
    void loadApiKeyFromFile() throws Exception {
        assumeTrue(System.getenv(OrcApiImporter.ENV_API_KEY) == null, "ORC_API_KEY is set");
        String key = "orc_" + "0123456789abcdef".repeat(4);
        Files.writeString(tempDir.resolve(OrcApiImporter.API_KEY_FILE), key + "\n");
        assertEquals(key, OrcApiImporter.loadApiKey(tempDir));
    }

    @Test
    void loadApiKeyMissingThrows() {
        assumeTrue(System.getenv(OrcApiImporter.ENV_API_KEY) == null, "ORC_API_KEY is set");
        assertThrows(IllegalStateException.class, () -> OrcApiImporter.loadApiKey(tempDir.resolve("none")));
    }

    private Boat boatNamed(String name) {
        return store.boats().values().stream().filter(b -> b.name().equals(name)).findFirst().orElseThrow();
    }

    /** An activecerts row; numeric fields are JSON numbers as the API returns them; empty strings are omitted. */
    private JsonNode row(int dxtId, String refNo, String yachtName, String sailNo, int vppYear,
                         int certType, String expiry, String className, String familyName) throws Exception {
        var node = MAPPER.createObjectNode()
            .put("dxtID", dxtId).put("RefNo", refNo).put("YachtName", yachtName).put("SailNo", sailNo)
            .put("VPPYear", vppYear).put("CertType", certType).put("CountryId", "AUS");
        if (!expiry.isBlank())     node.put("Expiry", expiry);
        if (!className.isBlank())  node.put("Class", className);
        if (!familyName.isBlank()) node.put("FamilyName", familyName);
        return node;
    }

    /** A minimal DownBoatRMS_byrefno response carrying a GPH. */
    private String certJson(double gph) {
        return "{\"rms\":[{\"NatAuth\":\"AUS\",\"RefNo\":\"X\",\"GPH\":" + gph + ",\"APHD\":578}]}";
    }
}
