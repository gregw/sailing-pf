package org.mortbay.sailing.pf.store;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link DataStore#asOf}: the data as it was at a cutoff, read-only. */
class DatedDataStoreTest
{
    @TempDir Path tempDir;

    @Test
    void viewHoldsOnlyRacesBeforeTheCutoffAndCertificatesUpToItsYear()
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        store.setAutoSanityCheck(false);
        store.putBoat(new Boat("b1", "B1", "b1", null, null, List.of(
            cert(2018), cert(2019), cert(2020)), List.of(), null, null));
        race(store, LocalDate.of(2019, 6, 30), 1);
        race(store, LocalDate.of(2019, 7, 1), 2);
        race(store, LocalDate.of(2020, 1, 1), 3);
        store.setRaceExcluded("club-2019-06-30-0001", true);

        DataStore view = store.asOf(LocalDate.of(2019, 7, 1));
        assertEquals(List.of("club-2019-06-30-0001"), List.copyOf(view.races().keySet()), "a race on the cutoff is after it");
        assertEquals(List.of(2018, 2019), view.boats().get("b1").certificates().stream().map(Certificate::year).toList());
        assertTrue(view.isRaceExcluded("club-2019-06-30-0001"), "exclusions pass through");
        assertNull(view.dataRoot(), "no data root, so the analysis writes no logs");
        assertEquals(3, store.races().size(), "the store itself is unchanged");
        assertEquals(3, store.boats().get("b1").certificates().size());
        assertThrows(IllegalStateException.class, () -> view.putBoat(store.boats().get("b1")), "read-only");
    }

    private static Certificate cert(int year)
    {
        return new Certificate("IRC", year, 1.0, false, false, false, false, "C" + year, null);
    }

    private static void race(DataStore store, LocalDate date, int n)
    {
        store.putRace(new Race("club-" + date + "-" + String.format("%04d", n), "club", List.of("series"), date, n, null,
            List.of(new Division("Div 1", List.of(new Finisher("b1", Duration.ofHours(1), false, null)))),
            "test", null, null));
    }
}
