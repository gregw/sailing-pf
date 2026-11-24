package org.mortbay.sailing.pf.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.BoatAlias;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Identity;
import org.mortbay.sailing.pf.data.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A boat with several identities: any identity ID refers to it, its finishers are counted for
 * its current identity in memory but saved as the identity entered as, and its file follows
 * its current identity.
 */
class BoatIdentityStoreTest
{
    private static final String ABOUT_TIME = "7700-abouttime-cookson12";
    private static final String KANREKI = "7709-kanreki-cookson12";

    private static Identity identity(String sail, String name, String club)
    {
        return new Identity(sail, name, List.of(club), List.of(), List.of("test"));
    }

    private static Boat aboutTimeThenKanreki()
    {
        Map<String, Identity> ids = new LinkedHashMap<>();
        ids.put(ABOUT_TIME, identity("7700", "About Time", "cyca.com.au"));
        ids.put(KANREKI, identity("7709", "Kanreki", "myc.org.au"));
        return new Boat(null, "cookson12", List.of(), ids, null, null);
    }

    private static Race race(LocalDate date, String identityId)
    {
        return new Race("club-" + date + "-0001", "club", List.of("club/s"), date, 1, null,
            List.of(new Division("Div", List.of(new Finisher(identityId, Duration.ofHours(1), false, null)))),
            "test", null, null);
    }

    @Test
    void anyIdentityRefersToTheBoat(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(aboutTimeThenKanreki());

        assertEquals(List.of(KANREKI), List.copyOf(store.boats().keySet()), "stored once, under its current identity");
        assertEquals("Kanreki", store.boat(KANREKI).name());
        Boat asAboutTime = store.boat(ABOUT_TIME);
        assertEquals("About Time", asAboutTime.name());
        assertEquals("7700", asAboutTime.sailNumber());
        assertEquals("cyca.com.au", asAboutTime.primaryClubId(), "seen through About Time, its CYCA club first");
        assertEquals(KANREKI, store.canonicalBoatId(ABOUT_TIME));
        assertTrue(store.boat(KANREKI).hasClub("cyca.com.au"));
        assertNull(store.boat("1-nosuch"));
    }

    @Test
    void finishersCountForTheBoatButAreSavedAsEntered(@TempDir Path tempDir) throws Exception
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(aboutTimeThenKanreki());
        store.putRace(race(LocalDate.of(2004, 1, 3), ABOUT_TIME));

        Finisher f = store.races().values().iterator().next().divisions().getFirst().finishers().getFirst();
        assertEquals(KANREKI, f.boatId(), "counted for the boat");
        assertEquals(ABOUT_TIME, f.identityId(), "entered as About Time");

        store.save();
        String json;
        try (var files = Files.walk(tempDir.resolve("races")))
        {
            json = Files.readString(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
        }
        assertTrue(json.contains("\"boatId\" : \"" + ABOUT_TIME + "\""), json);
        assertFalse(json.contains(KANREKI), json);

        // Reloaded, the finisher is again counted for the boat
        DataStore reloaded = new DataStore(tempDir);
        reloaded.start();
        Finisher r = reloaded.races().values().iterator().next().divisions().getFirst().finishers().getFirst();
        assertEquals(KANREKI, r.boatId());
        assertEquals(ABOUT_TIME, r.identityId());
        assertEquals(2, reloaded.boat(ABOUT_TIME).identities().size());
    }

    @Test
    void aNewCurrentIdentityMovesTheRecordAndItsFinishers(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(aboutTimeThenKanreki());
        store.putRace(race(LocalDate.of(2020, 1, 4), KANREKI));
        store.save();
        assertTrue(Files.exists(tempDir.resolve("boats/" + KANREKI + ".json")));

        // The boat is renamed again: a third identity becomes current
        String next = "7709-kanrekiii-cookson12";
        store.putBoat(store.boat(KANREKI).withIdentity(next, identity("7709", "Kanreki II", "myc.org.au")));
        store.save();

        assertEquals(List.of(next), List.copyOf(store.boats().keySet()));
        assertFalse(Files.exists(tempDir.resolve("boats/" + KANREKI + ".json")), "old file removed");
        assertTrue(Files.exists(tempDir.resolve("boats/" + next + ".json")));
        Finisher f = store.races().values().iterator().next().divisions().getFirst().finishers().getFirst();
        assertEquals(next, f.boatId());
        assertEquals(KANREKI, f.identityId());
        assertEquals(next, store.canonicalBoatId(ABOUT_TIME));
    }

    @Test
    void removingByAFormerIdentityRemovesTheBoat(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.putBoat(aboutTimeThenKanreki());
        store.removeBoat(ABOUT_TIME);
        assertTrue(store.boats().isEmpty());
        assertNull(store.boat(KANREKI));
        assertNull(store.boat(ABOUT_TIME));
    }

    @Test
    void aNewDatabaseIsMarkedFormat2AndAV1OneIsRefused(@TempDir Path tempDir) throws Exception
    {
        Path fresh = tempDir.resolve("fresh");
        new DataStore(fresh).start();
        assertEquals(DataStore.FORMAT, Files.readString(fresh.resolve(DataStore.FORMAT_FILE)).trim());

        Path v1 = tempDir.resolve("v1");
        Files.createDirectories(v1.resolve("imported/boats"));
        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
            () -> new DataStore(v1).start());
        assertTrue(e.getMessage().contains("v1 database"), e.getMessage());
    }

    @Test
    void identityMatchesItsNameAndAliases()
    {
        Identity i = new Identity("7709", "Kanreki", null, null, List.of(), false,
            List.of(new BoatAlias("AUS7709", null), new BoatAlias(null, "Kanreki II"), new BoatAlias("7710", "Old Kanreki")),
            List.of(), List.of());
        assertTrue(i.matches("7709", "kanreki"));
        assertTrue(i.matches("AUS7709", "kanreki"), "sail-number-only alias, with its name");
        assertTrue(i.matches("AUS7709", "kanrekiii"), "sail-number-only alias, with a name-only alias");
        assertTrue(i.matches("7709", "kanrekiii"), "name-only alias, with its sail number");
        assertTrue(i.matches("7710", "oldkanreki"), "full alias");
        assertFalse(i.matches("7710", "kanreki"), "a full alias's sail number does not pair with other names");
        assertFalse(i.matches("7700", "kanreki"));
    }
}
