package org.mortbay.sailing.pf.store;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Link;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Links on designs and boats must survive persistence and every path that rebuilds or merges
 * the records.
 */
class DataStoreLinksTest
{
    private static final Link WIKI = new Link("https://en.wikipedia.org/wiki/Archambault_Grand_Surprise", null, "wikipedia");
    private static final Link SBD = new Link("https://sailboatdata.com/sailboat/grand-surpise/", "Specs", Link.MANUAL);
    private static final Link CLASS = new Link("https://www.grandsurprise.com", "Class association", Link.MANUAL);

    private static Design design(String id, String name, List<Link> links)
    {
        return new Design(id, name, List.of(), List.of("test"), links, null, false, null);
    }

    private static Boat boat(String id, String sail, String name, String designId, List<Link> links)
    {
        return new Boat(id, sail, name, designId, List.of(), List.of(), List.of("test"), links, null, null);
    }

    @Test
    void linksPersistAcrossRestart(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putDesign(design("grandsurprise", "Grand Surprise", List.of(WIKI, SBD)));
        store.putBoat(boat("AUS1-foo-grandsurprise", "AUS1", "Foo", "grandsurprise", List.of(CLASS)));
        store.save();
        store.stop();

        DataStore reloaded = new DataStore(tempDir);
        reloaded.start();
        assertEquals(List.of(WIKI, SBD), reloaded.designs().get("grandsurprise").links());
        assertEquals(List.of(CLASS), reloaded.boats().get("AUS1-foo-grandsurprise").links());
    }

    @Test
    void recordsWithoutLinksLoadWithNone() throws Exception
    {
        DataStore store = new DataStore(Path.of(getClass().getClassLoader().getResource("testdata").toURI()));
        store.start();
        assertTrue(store.boats().values().stream().allMatch(b -> b.links().isEmpty()));
        assertTrue(store.designs().values().stream().allMatch(d -> d.links().isEmpty()));
    }

    @Test
    void mergeDesignsUnionsDesignLinksAndKeepsBoatLinks(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putDesign(design("keep", "Keep", List.of(WIKI)));
        store.putDesign(design("old", "Old", List.of(SBD, WIKI)));
        store.putBoat(boat("AUS1-foo-old", "AUS1", "Foo", "old", List.of(CLASS)));

        store.mergeDesigns("keep", List.of("old"));

        assertEquals(List.of(WIKI, SBD), store.designs().get("keep").links());
        assertEquals(List.of(CLASS), store.boats().get("AUS1-foo-keep").links());
    }

    @Test
    void mergeDesignsCollisionUnionsBoatLinks(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putDesign(design("keep", "Keep", List.of()));
        store.putDesign(design("old", "Old", List.of()));
        store.putBoat(boat("AUS1-foo-old", "AUS1", "Foo", "old", List.of(SBD)));
        store.putBoat(boat("AUS1-foo-keep", "AUS1", "Foo", "keep", List.of(CLASS)));

        store.mergeDesigns("keep", List.of("old"));

        assertEquals(List.of(CLASS, SBD), store.boats().get("AUS1-foo-keep").links());
    }

    @Test
    void mergeBoatsUnionsLinks(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(boat("AUS1-foo", "AUS1", "Foo", null, List.of(CLASS)));
        store.putBoat(boat("AUS2-foo", "AUS2", "Foo", null, List.of(SBD, CLASS)));

        store.mergeBoats("AUS1-foo", List.of("AUS2-foo"));

        assertEquals(List.of(CLASS, SBD), store.boats().get("AUS1-foo").links());
    }
}
