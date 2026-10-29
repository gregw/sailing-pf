package org.mortbay.sailing.pf.links;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApplyDesignLinksTest
{
    @Test
    void parseCsvReadsWhatTheMatcherWrites()
    {
        String text = DesignLinkMatcher.csv("approve", "designId", "links") + "\n"
            + DesignLinkMatcher.csv("y", "a,b", "say \"hi\"") + "\r\n"
            + DesignLinkMatcher.csv("?", "c", "") + "\n";
        assertEquals(List.of(
            List.of("approve", "designId", "links"),
            List.of("y", "a,b", "say \"hi\""),
            List.of("?", "c", "")), ApplyDesignLinks.parseCsv(text));
    }

    @Test
    void mergeReplacesHarvestedLinksAndKeepsManualOnes()
    {
        List<Map<String, String>> existing = List.of(
            Map.of("url", "https://class.org", "label", "Class", "source", "manual"),
            Map.of("url", "https://old.harvest", "source", "wikipedia"),
            Map.of("url", "https://sailboatdata.com/sailboat/x", "source", "manual"));
        List<Map<String, String>> merged = ApplyDesignLinks.merge(existing,
            List.of("https://en.wikipedia.org/wiki/X", "https://sailboatdata.com/sailboat/x"), List.of(), "wikipedia");
        assertEquals(List.of(
            Map.of("url", "https://class.org", "label", "Class", "source", "manual"),
            Map.of("url", "https://sailboatdata.com/sailboat/x", "source", "manual"),
            Map.of("url", "https://en.wikipedia.org/wiki/X", "source", "wikipedia")), merged);
    }

    @Test
    void mergeLeavesOtherSourcesAlone()
    {
        List<Map<String, String>> existing = List.of(
            Map.of("url", "https://en.wikipedia.org/wiki/X", "source", "wikipedia"),
            Map.of("url", "https://sailboatdata.com/sailboat/old", "source", "speclinks"));
        List<Map<String, String>> merged = ApplyDesignLinks.merge(existing,
            List.of("https://en.wikipedia.org/wiki/X", "https://sailboatdata.com/sailboat/x"), List.of(), "speclinks");
        assertEquals(List.of(
            Map.of("url", "https://en.wikipedia.org/wiki/X", "source", "wikipedia"),
            Map.of("url", "https://sailboatdata.com/sailboat/x", "source", "speclinks")), merged);
    }

    @Test
    void mergeLabelsLinksByPosition()
    {
        List<Map<String, String>> merged = ApplyDesignLinks.merge(List.of(),
            List.of("https://a", "https://b", "https://c"), List.of("A one", " ", "C one "), "speclinks");
        assertEquals(List.of(
            Map.of("url", "https://a", "label", "A one", "source", "speclinks"),
            Map.of("url", "https://b", "source", "speclinks"),
            Map.of("url", "https://c", "label", "C one", "source", "speclinks")), merged);
    }
}
