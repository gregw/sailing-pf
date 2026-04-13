package org.mortbay.sailing.hpf.importer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.store.DataStore;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AmsImporterTest
{
    @TempDir Path tempDir;
    private DataStore store;
    private AmsImporter importer;

    @BeforeEach
    void setUp()
    {
        store = new DataStore(tempDir);
        store.start();
        importer = new AmsImporter(store, null); // client not used by parsing methods
    }

    @AfterEach
    void tearDown()
    {
        store.stop();
    }

    @Test
    void processRowCreatesBoatAndThreeCerts()
    {
        importer.processRow("2XTREME", "SB4272", "28108", "2025-11-07", "0.879", "0.822", "0.858", "", "");

        assertEquals(1, store.boats().size());
        Boat boat = store.boats().values().iterator().next();
        assertEquals("SB4272", boat.sailNumber());
        assertEquals("2XTREME", boat.name());
        assertEquals(3, boat.certificates().size());
    }

    @Test
    void processRowSpinnonSpinTwoHandedFlags()
    {
        importer.processRow("WIND RIDER", "AUS100", "99001", "2025-06-01", "0.900", "0.850", "0.875", "", "");

        List<Certificate> certs = store.boats().values().iterator().next().certificates();

        Certificate spin = certs.stream()
                .filter(c -> !c.nonSpinnaker() && !c.twoHanded()).findFirst().orElse(null);
        assertNotNull(spin, "spinnaker cert should exist");
        assertEquals(0.900, spin.value());
        assertEquals("99001", spin.certificateNumber());

        Certificate noSpin = certs.stream()
                .filter(Certificate::nonSpinnaker).findFirst().orElse(null);
        assertNotNull(noSpin, "non-spinnaker cert should exist");
        assertEquals(0.850, noSpin.value());
        assertEquals("99001-ns", noSpin.certificateNumber());

        Certificate twoH = certs.stream()
                .filter(Certificate::twoHanded).findFirst().orElse(null);
        assertNotNull(twoH, "two-handed cert should exist");
        assertEquals(0.875, twoH.value());
        assertEquals("99001-2h", twoH.certificateNumber());
    }

    @Test
    void processRowUpsertsOnSameCertNo()
    {
        importer.processRow("WIND RIDER", "AUS100", "99001", "2025-06-01", "0.900", "0.850", "0.875", "", "");
        importer.processRow("WIND RIDER", "AUS100", "99001", "2025-06-01", "0.901", "0.851", "0.876", "", "");

        Boat boat = store.boats().values().iterator().next();
        assertEquals(3, boat.certificates().size(), "Upsert should retain only one set of 3 certs");
        double spinVal = boat.certificates().stream()
                .filter(c -> !c.nonSpinnaker() && !c.twoHanded())
                .findFirst().get().value();
        assertEquals(0.901, spinVal);
    }

    @Test
    void processRowSkipsBlankRatingValues()
    {
        importer.processRow("LIGHT WIND", "AUS200", "99002", "2025-06-01", "0.900", "", "", "", "");

        Boat boat = store.boats().values().iterator().next();
        assertEquals(1, boat.certificates().size());
        assertFalse(boat.certificates().getFirst().nonSpinnaker());
        assertFalse(boat.certificates().getFirst().twoHanded());
    }

    @Test
    void processRowSkipsMissingRequiredFields()
    {
        importer.processRow("", "AUS300", "99003", "2025-06-01", "0.900", "0.850", "0.875", "", "");
        assertTrue(store.boats().isEmpty(), "Missing boat name should skip row");

        importer.processRow("STORM", "", "99003", "2025-06-01", "0.900", "0.850", "0.875", "", "");
        assertTrue(store.boats().isEmpty(), "Missing sail number should skip row");
    }

    @Test
    void processRowInvalidYearSkipsRow()
    {
        importer.processRow("FAST BOAT", "AUS400", "99004", "not-a-date", "0.900", "0.850", "0.875", "", "");
        assertTrue(store.boats().isEmpty());
    }

    @Test
    void processRowInvalidRatingSkipsThatCert()
    {
        importer.processRow("TRICKY", "AUS500", "99005", "2025-06-01", "n/a", "0.850", "0.875", "", "");

        Boat boat = store.boats().values().iterator().next();
        assertEquals(2, boat.certificates().size(), "Invalid spin rating — only 2 certs expected");
        assertTrue(boat.certificates().stream().noneMatch(c -> !c.nonSpinnaker() && !c.twoHanded()),
                "No spinnaker cert should exist");
    }

    @Test
    void parseAndImportExtractsMultipleRows()
    {
        // Mirrors the actual AMS HTML: plain <tr> in <tbody>, header <tr> in <thead>
        String html = "<table class=\"tablesorter\" id=\"tstable\">"
                + "<thead><tr>"
                + "<th>Boat Name</th><th>Sail No</th><th>Cert No</th>"
                + "<th>Cert Date</th><th>Rating</th><th>No Spin</th><th>Two hnd</th>"
                + "<th>Club</th><th>Loct'n</th>"
                + "</tr></thead>"
                + "<tbody>"
                + "<tr>"
                + "<td class=\"pc24 ws overflowHide\">ALPHA</td>"
                + "<td class=\"pc12\">AUS601</td>"
                + "<td class=\"pc9\">30001</td>"
                + "<td class=\"pc10 ws overflowHide\">2025-01-15</td>"
                + "<td class=\"pc7\">0.910</td>"
                + "<td class=\"pc7\">0.860</td>"
                + "<td class=\"pc7\">0.880</td>"
                + "<td class=\"pc12\">MYC</td>"
                + "<td class=\"pc12\">NSW</td>"
                + "</tr>"
                + "<tr>"
                + "<td class=\"pc24 ws overflowHide\">BETA</td>"
                + "<td class=\"pc12\">AUS602</td>"
                + "<td class=\"pc9\">30002</td>"
                + "<td class=\"pc10 ws overflowHide\">2025-02-20</td>"
                + "<td class=\"pc7\">0.920</td>"
                + "<td class=\"pc7\">0.870</td>"
                + "<td class=\"pc7\">0.890</td>"
                + "<td class=\"pc12\">SBSC</td>"
                + "<td class=\"pc12\">VIC</td>"
                + "</tr>"
                + "</tbody></table>";

        importer.parseAndImport(html);

        assertEquals(2, store.boats().size());
        assertTrue(store.boats().values().stream().anyMatch(b -> "601".equals(b.sailNumber())));
        assertTrue(store.boats().values().stream().anyMatch(b -> "602".equals(b.sailNumber())));
    }
}
