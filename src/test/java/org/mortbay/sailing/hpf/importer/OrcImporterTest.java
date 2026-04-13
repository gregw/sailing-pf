package org.mortbay.sailing.hpf.importer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.store.DataStore;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class OrcImporterTest {

    @TempDir Path tempDir;
    private DataStore store;
    private OrcImporter importer;

    @BeforeEach
    void setUp() {
        store = new DataStore(tempDir);
        store.start();
        importer = new OrcImporter(store, null); // client not used by parsing methods
    }

    @AfterEach
    void tearDown() {
        store.stop();
    }

    // --- parseGph ---

    @Test
    void parseGphFindsValueInTableCell() {
        String html = "<table><tr><td filecode=\"GPH\">612.3</td></tr></table>";
        assertEquals(612.3, importer.parseGph(html, "TEST-001"));
    }

    @Test
    void parseGphHandlesWhitespaceBetweenLabelAndValue() {
        String html = "<td scoringkind=\"tod\" filecode=\"GPH\">588.4</td>";
        assertEquals(588.4, importer.parseGph(html, "TEST-002"));
    }

    @Test
    void parseGphReturnsNaNWhenNotPresent() {
        assertEquals(Double.NaN, importer.parseGph("<html>no handicap here</html>", "TEST-003"));
    }

    @Test
    void parseGphIgnoresValuesOutsidePlausibleRange() {
        // Two-digit number should not match (pattern requires 3–4 digits before decimal)
        assertEquals(Double.NaN, importer.parseGph("<td>GPH</td><td>99</td>", "TEST-004"));
    }

    // --- processCertElement ---

    @Test
    void processCertElementCreatesDesignBoatAndCertificate() {
        Element el = row("AUS-2024-001", "Wild Thing", "AUS1234", "2024", "1", "2024-12-31", "J/24", "");
        String certHtml = gphHtml(612.3);

        importer.processCertElement(el, certHtml);

        Design design = store.designs().get("j24");
        assertNotNull(design, "design j24 should exist");
        assertEquals("J/24", design.canonicalName());

        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals("1234", boat.sailNumber());
        assertEquals("Wild Thing", boat.name());
        assertEquals("j24", boat.designId());

        assertEquals(1, boat.certificates().size());
        Certificate cert = boat.certificates().getFirst();
        assertEquals("ORC", cert.system());
        assertEquals(600.0 / 612.3, cert.value(), 1e-10);
        assertEquals(2024, cert.year());
        assertEquals("AUS-2024-001", cert.certificateNumber());
        assertEquals(LocalDate.of(2024, 12, 31), cert.expiryDate());
        assertFalse(cert.nonSpinnaker());
    }

    @Test
    void processCertElementNonSpinnakerByFamilyName() {
        Element el = row("AUS-2024-002", "Breeze", "AUS999", "2024", "1", "", "J/24", "Non Spinnaker");
        importer.processCertElement(el, gphHtml(650.0));
        assertTrue(store.boats().values().iterator().next().certificates().getFirst().nonSpinnaker());
    }

    @Test
    void processCertElementNonSpinnakerByCertType() {
        // CertType "10" indicates non-spinnaker
        Element el = row("AUS-2024-003", "Zephyr", "AUS777", "2024", "10", "", "J/24", "");
        importer.processCertElement(el, gphHtml(620.0));
        assertTrue(store.boats().values().iterator().next().certificates().getFirst().nonSpinnaker());
    }

    @Test
    void processCertElementMissingYachtNameSkipsCert() {
        Element el = row("AUS-2024-004", "", "AUS222", "2024", "1", "", "J/24", "");
        importer.processCertElement(el, gphHtml(600.0));
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processCertElementInvalidYearSkipsCert() {
        Element el = row("AUS-2024-005", "Stormy", "AUS333", "notAYear", "1", "", "J/24", "");
        importer.processCertElement(el, gphHtml(600.0));
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processCertElementNoGphSkipsCert() {
        Element el = row("AUS-2024-006", "Ghost", "AUS444", "2024", "1", "", "J/24", "");
        importer.processCertElement(el, "<html>no handicap value here</html>");
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processCertElementMissingClassLeavesDesignIdNull() {
        Element el = row("AUS-2024-007", "Mystery", "AUS555", "2024", "1", "", "", "");
        importer.processCertElement(el, gphHtml(600.0));
        assertTrue(store.designs().isEmpty());
        assertNull(store.boats().values().iterator().next().designId());
    }

    @Test
    void processCertElementAddsSecondCertToExistingBoat() {
        // First cert
        importer.processCertElement(
            row("AUS-2023-008", "Tornado", "AUS666", "2023", "1", "", "J/24", ""),
            gphHtml(615.0));

        // Second cert — same sail + same first word → same boat
        importer.processCertElement(
            row("AUS-2024-008", "Tornado", "AUS666", "2024", "1", "", "J/24", ""),
            gphHtml(612.0));

        assertEquals(1, store.boats().size());
        assertEquals(2, store.boats().values().iterator().next().certificates().size());
    }

    @Test
    void processCertElementTwoDifferentSailsProduceTwoBoats() {
        importer.processCertElement(
            row("AUS-2024-009", "Alpha", "AUS100", "2024", "1", "", "J/24", ""),
            gphHtml(610.0));
        importer.processCertElement(
            row("AUS-2024-010", "Beta", "AUS200", "2024", "1", "", "J/24", ""),
            gphHtml(620.0));

        assertEquals(2, store.boats().size());
        assertEquals(1, store.designs().size()); // same Class → one design
    }

    @Test
    void processCertElementUpsertReplacesDuplicateDxtId() {
        Element el = row("AUS-2024-011", "Pilot", "AUS300", "2024", "1", "", "J/24", "");
        importer.processCertElement(el, gphHtml(600.0));
        importer.processCertElement(el, gphHtml(600.0));
        assertEquals(1, store.boats().values().iterator().next().certificates().size());
    }

    @Test
    void processCertElementNewerCertIsSortedFirst() {
        importer.processCertElement(
            row("AUS-2023-013", "Pilot", "AUS300", "2023", "1", "", "J/24", ""),
            gphHtml(615.0));
        importer.processCertElement(
            row("AUS-2024-013", "Pilot", "AUS300", "2024", "1", "", "J/24", ""),
            gphHtml(612.0));
        assertEquals(2024, store.boats().values().iterator().next().certificates().getFirst().year());
    }

    @Test
    void processCertElementInvalidExpiryDateStillImportsCert() {
        Element el = row("AUS-2024-012", "Flex", "AUS400", "2024", "1", "not-a-date", "J/24", "");
        importer.processCertElement(el, gphHtml(600.0));
        // Cert is created; expiryDate defaults to null
        Certificate cert = store.boats().values().iterator().next().certificates().getFirst();
        assertNull(cert.expiryDate());
    }

    // --- Helpers ---

    /** Build a ROW element from field values; empty string omits the element. */
    private Element row(String dxtId, String yachtName, String sailNo, String vppYear,
                        String certType, String expiry, String className, String familyName) {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\"?><ROOT><ROW>");
        if (!dxtId.isBlank())      sb.append("<dxtID>").append(dxtId).append("</dxtID>");
        if (!yachtName.isBlank())  sb.append("<YachtName>").append(yachtName).append("</YachtName>");
        if (!sailNo.isBlank())     sb.append("<SailNo>").append(sailNo).append("</SailNo>");
        if (!vppYear.isBlank())    sb.append("<VPPYear>").append(vppYear).append("</VPPYear>");
        if (!certType.isBlank())   sb.append("<CertType>").append(certType).append("</CertType>");
        if (!expiry.isBlank())     sb.append("<Expiry>").append(expiry).append("</Expiry>");
        if (!className.isBlank())  sb.append("<Class>").append(className).append("</Class>");
        if (!familyName.isBlank()) sb.append("<FamilyName>").append(familyName).append("</FamilyName>");
        sb.append("</ROW></ROOT>");
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            var doc = factory.newDocumentBuilder()
                    .parse(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
            return (Element) doc.getElementsByTagName("ROW").item(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Minimal HTML page containing a GPH value in the ORC cert page format. */
    private String gphHtml(double gph) {
        return "<table><tr><td scoringkind=\"tod\" filecode=\"GPH\">" + gph + "</td></tr></table>";
    }
}
