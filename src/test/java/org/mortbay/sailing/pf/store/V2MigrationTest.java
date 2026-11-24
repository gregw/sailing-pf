package org.mortbay.sailing.pf.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Converting a v1 database: boats become identities, aliases.yaml boat aliases become identity
 * aliases or (dated) identities with their results, club overrides become curated clubs, and
 * the easily re-imported races and their certificates are purged.
 */
class V2MigrationTest
{
    private static void write(Path file, String content) throws Exception
    {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private static String boat(String id, String sail, String name, String design, String certs)
    {
        return "{\"id\":\"" + id + "\",\"sailNumber\":\"" + sail + "\",\"name\":\"" + name + "\",\"designId\":\""
            + design + "\",\"clubIds\":[\"myc.org.au\"],\"certificates\":[" + certs + "],\"sources\":[\"SailSys\"]}";
    }

    private static String race(String id, String date, String source, String boatId)
    {
        return "{\"id\":\"" + id + "\",\"clubId\":\"myc.org.au\",\"seriesIds\":[\"myc.org.au/s\"],\"date\":\"" + date
            + "\",\"number\":1,\"divisions\":[{\"name\":\"A\",\"finishers\":[{\"boatId\":\"" + boatId
            + "\",\"elapsedTime\":\"PT1H\",\"nonSpinnaker\":false,\"certificateNumber\":null}]}],\"source\":\"" + source + "\"}";
    }

    @Test
    void convertsAV1Database(@TempDir Path tempDir) throws Exception
    {
        Path v1 = tempDir.resolve("v1");
        String bwpsCert = "{\"system\":\"IRC\",\"year\":2022,\"value\":1.1,\"nonSpinnaker\":false,\"twoHanded\":false,"
            + "\"club\":false,\"windwardLeeward\":false,\"certificateNumber\":\"bwps-irc-2022-1.1000\",\"expiryDate\":null}";
        write(v1.resolve("imported/boats/7709-kanreki-cookson12.json"),
            boat("7709-kanreki-cookson12", "7709", "Kanreki", "cookson12", bwpsCert));
        write(v1.resolve("imported/boats/100-alpha-j24.json"), boat("100-alpha-j24", "100", "Alpha", "j24", ""));
        write(v1.resolve("imported/races/myc.org.au/s/r1.json"), race("r1", "2004-01-03", "SailSys", "7709-kanreki-cookson12"));
        write(v1.resolve("imported/races/myc.org.au/s/r2.json"), race("r2", "2020-01-04", "SailSys", "7709-kanreki-cookson12"));
        write(v1.resolve("imported/races/myc.org.au/s/r3.json"), race("r3", "2025-01-04", "SailSys", "7709-kanreki-cookson12"));
        write(v1.resolve("imported/races/myc.org.au/s/r4.json"), race("r4", "2019-12-26", "RSHYR", "7709-kanreki-cookson12"));
        write(v1.resolve("imported/designs/cookson12.json"), "{\"id\":\"cookson12\",\"canonicalName\":\"Cookson 12\"}");
        write(v1.resolve("config/aliases.yaml"), """
            boats:
              "7709-kanreki":
                canonicalName: "Kanreki"
                aliases:
                - sailNumber: "7700"
                  name: "abouttime"
                  displayName: "About Time"
                  until: "2006-01-01"
                - sailNumber: "7709"
                  name: "sailexchange"
                  displayName: "Sail Exchange"
                  until: "2024-03-29"
                - sailNumber: "AUS7709"
                  name: "kanreki"
              "100-alpha":
                canonicalName: "Alpha"
                aliases:
                - sailNumber: "100"
                  name: "alfa"
            designs: {}
            """);
        write(v1.resolve("config/clubs.yaml"), """
            clubs: {}
            noclub:
            - "100-alpha-j24"
            """);
        write(v1.resolve("myc-archive/a.xlsx"), "x");
        // Corum: overridden to a Mumm 36 (entered as a Farr Mumm 36), and modified
        write(v1.resolve("imported/boats/1088-corum-farrmumm36.json"), boat("1088-corum-farrmumm36", "1088", "Corum", "farrmumm36", ""));
        write(v1.resolve("imported/races/myc.org.au/s/r5.json"), race("r5", "2021-01-09", "SailSys", "1088-corum-farrmumm36"));
        write(v1.resolve("config/design.yaml"), """
            excluded:
            - "Optimist"
            designYears:
              cookson12: 1998
            modifiedBoats:
            - sailNumber: "1088"
              name: "Corum"
            boatDesignOverrides:
            - designId: "mumm36"
              canonicalName: "Mumm 36"
              boats:
              - sailNumber: "1088"
                name: "Corum"
                from: null
                until: null
            """);

        Path v2 = tempDir.resolve("v2");
        V2Migration.Report report = V2Migration.convert(v1, v2);

        assertEquals("2", Files.readString(v2.resolve("FORMAT")).trim());
        assertTrue(Files.exists(v2.resolve("myc-archive/a.xlsx")), "the MYC archive is carried over");
        assertTrue(Files.exists(v2.resolve("designs/cookson12.json")));
        assertFalse(Files.exists(v2.resolve("imported")));
        assertEquals(1, report.racesPurged);
        assertEquals(1, report.certificatesPurged);

        DataStore store = new DataStore(v2);
        store.start();
        Boat kanreki = store.boat("7709-kanreki-cookson12");
        assertEquals(List.of("7700-abouttime-cookson12", "7709-sailexchange-cookson12", "7709-kanreki-cookson12"),
            List.copyOf(kanreki.identities().keySet()), "a renamed boat's names, oldest first");
        assertEquals(LocalDate.of(2006, 1, 1), kanreki.identities().get("7700-abouttime-cookson12").until());
        assertTrue(kanreki.identities().get("7709-kanreki-cookson12").matches("AUS7709", "kanreki"));
        assertTrue(kanreki.certificates().isEmpty(), "the BWPS-derived certificate is purged");
        assertNull(store.races().get("r4"), "the Sydney Hobart race is purged");
        assertEquals("7700-abouttime-cookson12", identityOf(store, "r1"), "2004: About Time");
        assertEquals("7709-sailexchange-cookson12", identityOf(store, "r2"), "2020: Sail Exchange");
        assertEquals("7709-kanreki-cookson12", identityOf(store, "r3"), "2025: Kanreki");

        Boat alpha = store.boat("100-alpha-j24");
        assertTrue(alpha.identity().matches("100", "alfa"), "an undated alias");
        assertTrue(alpha.identity().clubsCurated(), "noclub: an empty curated club list");
        assertTrue(alpha.identity().clubIds().isEmpty());

        Boat corum = store.boat("1088-corum-mumm36");
        assertEquals("mumm36", corum.designId(), "the override's design");
        assertTrue(corum.modified());
        assertTrue(corum.identity().enteredAs("1088", "corum", "farrmumm36"), "results of any design are Corum's");
        assertEquals("1088-corum-mumm36", identityOf(store, "r5"), "its result moved with it");
        assertTrue(Files.exists(v2.resolve("designs/mumm36.json")), "the override's design is created");
        assertEquals("1088-corum-mumm36",
            store.findOrCreateBoat("1088", "Corum", "Farr Mumm 36", LocalDate.of(2025, 1, 4), "test").id());
        assertFalse(Files.exists(v2.resolve("config/design.yaml")), "design.yaml is retired");
        assertTrue(store.isDesignExcluded("optimist"), "a flagged design not held is created, flagged");
        assertEquals(1998, store.designYearOverride("cookson12"));
        assertFalse(Files.exists(v2.resolve("config/aliases.yaml")), "aliases.yaml is retired");

        assertThrows(IllegalArgumentException.class, () -> V2Migration.convert(v1, v2), "will not overwrite");
    }

    private static String identityOf(DataStore store, String raceId)
    {
        Race r = store.races().get(raceId);
        Finisher f = r.divisions().getFirst().finishers().getFirst();
        return f.identityId();
    }
}
