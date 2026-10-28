package org.mortbay.sailing.pf.links;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mortbay.sailing.pf.links.WikipediaDesignHarvest.Article;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WikipediaDesignHarvestTest
{
    @Test
    void titleGuessesDropBuilderWordsAndTitleCase()
    {
        assertEquals(Set.of("J Boats J/70", "Boats J/70", "J/70"), WikipediaDesignHarvest.titleGuesses("J Boats J/70"));
        assertEquals(Set.of("NORTHSHORE 38", "Northshore 38"), WikipediaDesignHarvest.titleGuesses("NORTHSHORE 38"));
    }

    @Test
    void onlyArticlesAboutBoatsAreKept()
    {
        assertTrue(WikipediaDesignHarvest.isAboutBoats(article("Sailboat class", List.of())));
        assertTrue(WikipediaDesignHarvest.isAboutBoats(article(null, List.of("Category:Keelboats"))));
        assertFalse(WikipediaDesignHarvest.isAboutBoats(article("City in Queensland, Australia", List.of("Category:Cities in Queensland"))));
        assertFalse(WikipediaDesignHarvest.isAboutBoats(article("Topics referred to by the same term",
            List.of("Category:Disambiguation pages", "Category:Sailing"))));
    }

    private static Article article(String description, List<String> categories)
    {
        return new Article("X", 1, null, description, List.of(), categories, List.of());
    }
}
