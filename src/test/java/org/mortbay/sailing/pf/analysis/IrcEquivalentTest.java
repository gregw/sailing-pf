package org.mortbay.sailing.pf.analysis;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Factor;
import org.mortbay.sailing.pf.store.DataStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link ReferenceNetworkBuilder#ircEquivalent}: a certificate as the IRC TCC of its own
 * year and variant, for the certificate history on the boat page.
 */
class IrcEquivalentTest
{
    @Test
    void convertsOrcToIrcOfTheSameYear(@TempDir Path tempDir)
    {
        DataStore store = new DataStore(tempDir);
        store.start();
        // An old year, beyond the RF's 5 year certificate window: ORC = 1.1 × IRC
        for (int i = 0; i < 20; i++)
        {
            double irc = 0.9 + 0.01 * i;
            store.putDesign(new Design("d" + i, "D " + i, List.of(), List.of(), null, false, null));
            store.putBoat(new Boat("B" + i + "-b-d" + i, "B" + i, "B", "d" + i, null,
                List.of(new Certificate("IRC", 2010, irc, false, false, false, false, null, null),
                        new Certificate("ORC", 2010, irc * 1.1, false, false, false, false, null, null)),
                List.of(), null, null));
        }
        ConversionGraph graph = ConversionGraph.from(new HandicapAnalyser(store, 2.5).analyseAll());

        Factor orc = ReferenceNetworkBuilder.ircEquivalent(
            new Certificate("ORC", 2010, 1.1, false, false, false, false, null, null), graph);
        assertNotNull(orc);
        assertEquals(1.0, orc.value(), 0.005);

        // An IRC certificate is its own equivalent
        Factor irc = ReferenceNetworkBuilder.ircEquivalent(
            new Certificate("IRC", 2010, 1.03, false, false, false, false, null, null), graph);
        assertEquals(1.03, irc.value(), 1e-9);
        assertEquals(1.0, irc.weight(), 1e-9);

        // No path from a year the graph has no comparisons for
        assertNull(ReferenceNetworkBuilder.ircEquivalent(
            new Certificate("AMS", 1999, 1.0, false, false, false, false, null, null), graph));
    }
}
