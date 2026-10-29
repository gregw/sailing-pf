package org.mortbay.sailing.pf.links;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.links.SpecLinkMatcher.Kind;
import org.mortbay.sailing.pf.links.SpecLinkMatcher.Proposal;
import org.mortbay.sailing.pf.links.SpecLinkMatcher.Site;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpecLinkMatcherTest
{
    private final SpecLinkMatcher matcher = new SpecLinkMatcher(List.of(
        "http://www.sailboatdata.com/sailboat/hunter-30/",
        "https://sailboatdata.com/sailboat/hunter-30-2",
        "https://sailboatdata.com/sailboat/grand-surpise",
        "https://sailboatdata.com/sailboat/beneteau-first-40",
        "https://sailboatdata.com/sailboat/surprise-25-archambault",
        "https://www.boat-specs.com/sailing/sailboats/archambault/grand-surprise",
        "https://www.boat-specs.com/sailing/sailboats/hunter-marine/hunter-30",
        "https://sailboatdata.com/sailboat/farr-30",
        "https://en.wikipedia.org/wiki/Hunter_30"));

    private static Design design(String id, String name, String... aliases)
    {
        return new Design(id, name, List.of(aliases), List.of(), null, false, null);
    }

    @Test
    void exactMatchOnBothSitesIsApprovable()
    {
        Proposal p = matcher.propose(design("hunter30", "Hunter 30"), 5, false, Set.of());
        assertEquals(Kind.exact, p.bySite().get(Site.sailboatdata).getFirst().kind());
        assertEquals(Kind.exact, p.bySite().get(Site.boatspecs).getFirst().kind());
        assertTrue(p.approvable());
        assertEquals(List.of("https://sailboatdata.com/sailboat/hunter-30",
            "https://www.boat-specs.com/sailing/sailboats/hunter-marine/hunter-30"), p.links());

        p = matcher.propose(design("hunter30", "Hunter 30"), 5, false, Set.of(Site.boatspecs));
        assertEquals(List.of("https://sailboatdata.com/sailboat/hunter-30"), p.links());
    }

    @Test
    void builderStrippedAndFuzzyMatches()
    {
        Proposal p = matcher.propose(design("grandsurprise", "Grand Surprise"), 5, false, Set.of());
        assertEquals(List.of(new SpecLinkMatcher.Match(SpecLinkMatcher.page("https://sailboatdata.com/sailboat/grand-surpise"),
            Kind.fuzzy)), p.bySite().get(Site.sailboatdata));
        assertEquals(Kind.exact, p.bySite().get(Site.boatspecs).getFirst().kind());
        assertFalse(p.approvable());

        assertEquals(Kind.stripped,
            matcher.propose(design("first40", "First 40"), 1, false, Set.of()).bySite().get(Site.sailboatdata).getFirst().kind());
        assertEquals(Kind.stripped,
            matcher.propose(design("surprise25", "Surprise 25"), 1, false, Set.of()).bySite().get(Site.sailboatdata).getFirst().kind());
        assertEquals(Kind.stripped,
            matcher.propose(design("archambaulthunter30", "Archambault Hunter 30"), 1, false, Set.of(Site.boatspecs))
                .bySite().get(Site.sailboatdata).getFirst().kind());
    }

    @Test
    void trailingBuilderAndTwoWordBuilderPrefix()
    {
        SpecLinkMatcher m = new SpecLinkMatcher(List.of("https://sailboatdata.com/sailboat/first-407-beneteau/",
            "https://sailboatdata.com/sailboat/j70/"));
        Proposal p = m.propose(design("beneteaufirst407", "Beneteau First 40.7"), 1, false, Set.of());
        assertEquals(Kind.exact, p.bySite().get(Site.sailboatdata).getFirst().kind());
        assertEquals(List.of("https://sailboatdata.com/sailboat/first-407-beneteau"), p.links());
        p = m.propose(design("jboatsj70", "J Boats J/70"), 1, false, Set.of());
        assertEquals(Kind.stripped, p.bySite().get(Site.sailboatdata).getFirst().kind());
    }

    @Test
    void genericWordsDoNotMatch()
    {
        SpecLinkMatcher m = new SpecLinkMatcher(List.of("https://sailboatdata.com/sailboat/ranger-24",
            "https://sailboatdata.com/sailboat/columbia-24-contender", "https://sailboatdata.com/sailboat/beverly-dinghy",
            "https://sailboatdata.com/sailboat/classic-26"));
        assertNull(m.propose(design("ranger", "Ranger"), 1, false, Set.of()));
        assertNull(m.propose(design("contender", "Contender"), 1, false, Set.of()));
        assertNull(m.propose(design("okdinghy", "OK Dinghy"), 1, false, Set.of()));
        assertNull(m.propose(design("devoticlassic", "Devoti Classic"), 1, false, Set.of()));

        m = new SpecLinkMatcher(List.of("https://sailboatdata.com/sailboat/hunter-45-ds"));
        assertNull(m.propose(design("jeanneau45ds", "Jeanneau 45DS"), 1, false, Set.of()));
    }

    @Test
    void numbersMustAgree()
    {
        assertNull(matcher.propose(design("farr40", "Farr 40"), 1, false, Set.of()));
        assertNull(matcher.propose(design("hunter31", "Hunter 31"), 1, false, Set.of()));
    }

    @Test
    void oneEditApart()
    {
        assertTrue(SpecLinkMatcher.oneEditApart("grandsurpise", "grandsurprise"));
        assertTrue(SpecLinkMatcher.oneEditApart("grandsurprize", "grandsurprise"));
        assertFalse(SpecLinkMatcher.oneEditApart("grandsurprise", "grandsurprise"));
        assertFalse(SpecLinkMatcher.oneEditApart("grndsurpise", "grandsurprise"));
    }

    @Test
    void coveredReadsApprovedRows(@TempDir Path dir) throws Exception
    {
        Path csv = dir.resolve("reviewed.csv");
        Files.writeString(csv, """
            approve,designId,links,review
            y,hunter30,https://en.wikipedia.org/wiki/Hunter_30 http://sailboatdata.com/sailboat/hunter-30,
            n,farr40,https://sailboatdata.com/sailboat/farr-40,wrong
            """);
        assertEquals(Map.of("hunter30", Set.of(Site.sailboatdata)), SpecLinkMatcher.covered(csv));
    }
}
