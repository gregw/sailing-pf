package org.mortbay.sailing.pf.links;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.links.DesignLinkMatcher.Kind;
import org.mortbay.sailing.pf.links.DesignLinkMatcher.Proposal;
import org.mortbay.sailing.pf.links.WikipediaDesignHarvest.Article;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DesignLinkMatcherTest
{
    private static final Article GRAND_SURPRISE = new Article("Archambault Grand Surprise", 1, "Q104543351",
        "Sailboat class", List.of(),
        List.of("Category:Sailboat type designs by Joubert-Nivelt", "Category:Sailboat types built by Archambault Boats"),
        List.of("https://sailboatdata.com/sailboat/grand-surpise",
            "https://www.boat-specs.com/sailing/sailboats/archambault/grand-surprise",
            "https://sailboatdata.com/sailboat/surprise-25-archambault",
            "https://www.boat-specs.com/sailing/sailboats/archambault/surprise-fin-keel"));

    private static final Article HUNTER_30 = new Article("Hunter 30", 2, null, "Sailboat class",
        List.of("Hunter 30-2"),
        List.of("Category:Sailboat types built by Hunter Marine"),
        List.of("https://sailboatdata.com/sailboat/hunter-30", "https://sailboatdata.com/sailboat/hunter-30-2",
            "https://sailboatdata.com/sailboat/hunter-30t"));

    private static final Article FARR_40 = new Article("Farr 40", 3, null, null, List.of(),
        List.of("Category:Sailboat type designs by Bruce Farr"),
        List.of("https://sailboatdata.com/sailboat/farr-40"));

    private static final Article FARR_40_YACHT = new Article("Farr 40 (yacht)", 4, null, null, List.of(), List.of(), List.of());

    private final DesignLinkMatcher matcher = new DesignLinkMatcher(List.of(GRAND_SURPRISE, HUNTER_30, FARR_40));

    private static Design design(String id, String name, String... aliases)
    {
        return new Design(id, name, List.of(aliases), List.of(), null, false, null);
    }

    @Test
    void builderStrippedTitleMatchesAndPicksTheMatchingSpecPages()
    {
        Proposal p = matcher.propose(design("grandsurprise", "Grand Surprise"), 12, false);
        assertEquals(Kind.builder, p.candidates().getFirst().kind());
        assertFalse(p.approvable());
        assertEquals(List.of("https://en.wikipedia.org/wiki/Archambault_Grand_Surprise",
            "https://www.boat-specs.com/sailing/sailboats/archambault/grand-surprise"), p.links());
    }

    @Test
    void titleMatchIsApprovableAndTakesTheSameNamedSpecPage()
    {
        Proposal p = matcher.propose(design("hunter30", "Hunter 30"), 3, false);
        assertEquals(Kind.title, p.candidates().getFirst().kind());
        assertTrue(p.approvable());
        assertEquals(List.of("https://en.wikipedia.org/wiki/Hunter_30", "https://sailboatdata.com/sailboat/hunter-30"),
            p.links());
    }

    @Test
    void redirectMatchUsesTheRedirectNameForTheSpecPage()
    {
        Proposal p = matcher.propose(design("hunter302", "Hunter 30-2"), 1, false);
        assertEquals(Kind.redirect, p.candidates().getFirst().kind());
        assertTrue(p.links().contains("https://sailboatdata.com/sailboat/hunter-30-2"));
    }

    @Test
    void aliasMatches()
    {
        Proposal p = matcher.propose(design("farr40od", "Farr 40 OD", "Farr 40"), 20, false);
        assertEquals("Farr 40", p.candidates().getFirst().article().title());
    }

    @Test
    void bareNumberAfterBuilderDoesNotMatch()
    {
        assertNull(matcher.propose(design("30", "30"), 1, false));
    }

    @Test
    void twoArticlesWithTheSameNameNeedReview()
    {
        DesignLinkMatcher twoFarrs = new DesignLinkMatcher(List.of(FARR_40, FARR_40_YACHT));
        Proposal p = twoFarrs.propose(design("farr40", "Farr 40"), 20, false);
        assertEquals(2, p.candidates().size());
        assertFalse(p.approvable());
    }

    @Test
    void articleAboutAKindOfBoatNeedsReview()
    {
        Article trailerSailer = new Article("Trailer sailer", 5, null, null, List.of("E boat"),
            List.of("Category:Sailboat types"), List.of());
        Proposal p = new DesignLinkMatcher(List.of(trailerSailer)).propose(design("eboat", "E boat"), 51, false);
        assertEquals(Kind.redirect, p.candidates().getFirst().kind());
        assertFalse(p.approvable());
    }

    @Test
    void wikipediaUrlEncodesTheTitle()
    {
        assertEquals("https://en.wikipedia.org/wiki/J/24_(keelboat)", DesignLinkMatcher.wikipediaUrl("J/24 (keelboat)"));
        assertEquals("https://en.wikipedia.org/wiki/B%C3%A9n%C3%A9teau_First_40",
            DesignLinkMatcher.wikipediaUrl("Bénéteau First 40"));
    }

    @Test
    void csvQuotes()
    {
        assertEquals("a,\"b,c\",\"d\"\"e\"", DesignLinkMatcher.csv("a", "b,c", "d\"e"));
    }
}
