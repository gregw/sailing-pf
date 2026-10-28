package org.mortbay.sailing.pf.data;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LinkTest
{
    @Test
    void validUrls()
    {
        assertTrue(Link.isValidUrl("https://sailboatdata.com/sailboat/grand-surpise/"));
        assertTrue(Link.isValidUrl("http://www.grandsurprise.com"));
        assertFalse(Link.isValidUrl("javascript:alert(1)"));
        assertFalse(Link.isValidUrl("ftp://example.com/x"));
        assertFalse(Link.isValidUrl("example.com"));
        assertFalse(Link.isValidUrl("https://example.com/a\"b"));
        assertFalse(Link.isValidUrl(" "));
        assertFalse(Link.isValidUrl(null));
    }

    @Test
    void defaultsSourceAndBlankLabel()
    {
        Link l = new Link(" https://a.com ", " ", null);
        assertEquals("https://a.com", l.url());
        assertNull(l.label());
        assertEquals(Link.MANUAL, l.source());
    }

    @Test
    void normaliseDropsInvalidAndDuplicateUrls()
    {
        List<Link> links = Link.normalise(List.of(
            new Link("https://a.com", "first", "wikipedia"),
            new Link("not a url", null, null),
            new Link("https://b.com", null, null),
            new Link("https://a.com", "second", null)));
        assertEquals(2, links.size());
        assertEquals("first", links.get(0).label());
        assertEquals("https://b.com", links.get(1).url());
    }

    @Test
    void mergeKeepsOrderAndFirstOfEachUrl()
    {
        List<Link> merged = Link.merge(
            List.of(new Link("https://a.com", "keep", null)),
            List.of(new Link("https://a.com", "other", null), new Link("https://b.com", null, null)));
        assertEquals(List.of("https://a.com", "https://b.com"), merged.stream().map(Link::url).toList());
        assertEquals("keep", merged.getFirst().label());
    }
}
