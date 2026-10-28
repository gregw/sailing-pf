package org.mortbay.sailing.pf.server;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Link;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LinksApiTest
{
    @Test
    void parseLinksKeepsSourceOfExistingUrlsAndMarksNewOnesManual()
    {
        List<Link> existing = List.of(new Link("https://en.wikipedia.org/wiki/X", null, "wikipedia"));
        List<Link> links = AdminApiServlet.parseLinks(List.of(
            Map.of("url", "https://en.wikipedia.org/wiki/X", "label", "Wiki"),
            Map.of("url", " "),
            Map.of("url", "https://example.com/x")), existing);
        assertEquals(List.of(
            new Link("https://en.wikipedia.org/wiki/X", "Wiki", "wikipedia"),
            new Link("https://example.com/x", null, Link.MANUAL)), links);
    }

    @Test
    void parseLinksRejectsNonHttpUrls()
    {
        assertThrows(IllegalArgumentException.class,
            () -> AdminApiServlet.parseLinks(List.of(Map.of("url", "javascript:alert(1)")), List.of()));
    }

    @Test
    void boatLinksAddOrcCertificatesNewestFirst()
    {
        Certificate orc2024 = new Certificate("ORC", 2024, 1.0, false, false, false, false, "263603", LocalDate.of(2024, 12, 31));
        Certificate orc2025dh = new Certificate("ORC", 2025, 1.0, false, true, false, false, "300001", LocalDate.of(2025, 12, 31));
        Certificate irc = new Certificate("IRC", 2025, 1.0, false, false, false, false, "bwps-irc-2021-1.4060", null);
        Certificate orc2024dh = new Certificate("ORC", 2024, 1.0, false, true, false, false, "263603", LocalDate.of(2024, 12, 31));
        Certificate inferred = new Certificate("ORC", 2025, 1.0, false, false, false, false, "orc-inferred-1", null);
        Boat boat = new Boat("AUS1-foo", "AUS1", "Foo", null, List.of(),
            List.of(orc2024, orc2024dh, irc, orc2025dh, inferred), List.of("test"),
            List.of(new Link("https://www.foo.com", "Foo", null)), null, null);

        List<Link> links = AdminApiServlet.boatLinks(boat);

        assertEquals(List.of(
            new Link("https://www.foo.com", "Foo", Link.MANUAL),
            new Link("https://data.orc.org/public/WPub.dll/CC/300001", "ORC 2025", "ORC"),
            new Link("https://data.orc.org/public/WPub.dll/CC/263603", "ORC 2024", "ORC")), links);
    }
}
