package org.mortbay.sailing.hpf.importer;

import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One-time cleanup: removes inferred certificates whose {@code system} is not one of the
 * three recognised measurement systems (IRC, ORC, AMS).
 * <p>
 * These were created by {@link SailSysRaceImporter} and {@link TopYachtImporter} before the
 * importer was fixed to restrict certificate creation to measurement systems only.  All
 * other system codes (TCF, YRD, YV, CBH, PHSDIV*, etc.) are performance-based handicaps
 * and must not be stored as certificates.
 * <p>
 * Run once after deploying the importer fix, then delete this class.
 * <p>
 * Usage: {@code java RemoveNonMeasurementCertificates [dataRoot]}
 */
public class RemoveNonMeasurementCertificates
{
    private static final Logger LOG = LoggerFactory.getLogger(RemoveNonMeasurementCertificates.class);

    public static void main(String[] args)
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        DataStore store = new DataStore(dataRoot);
        store.start();

        try
        {
            int boatsModified = 0;
            int certsRemoved = 0;
            Map<String, Integer> bySystem = new TreeMap<>();

            for (Boat boat : store.boats().values())
            {
                if (boat.certificates().isEmpty())
                    continue;

                List<Certificate> kept = boat.certificates().stream()
                    .filter(c -> isMeasurementSystem(c.system()))
                    .toList();

                if (kept.size() == boat.certificates().size())
                    continue;

                for (Certificate c : boat.certificates())
                {
                    if (!isMeasurementSystem(c.system()))
                        bySystem.merge(c.system(), 1, Integer::sum);
                }

                certsRemoved += boat.certificates().size() - kept.size();
                boatsModified++;
                store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
                    boat.designId(), boat.clubId(), boat.aliases(), kept, null));
            }

            LOG.info("Removed {} non-measurement certificate(s) from {} boat(s)", certsRemoved, boatsModified);
            if (!bySystem.isEmpty())
                LOG.info("Removed by system: {}", bySystem);

            store.save();
            LOG.info("Done.");
        }
        finally
        {
            store.stop();
        }
    }

    private static boolean isMeasurementSystem(String system)
    {
        return "IRC".equalsIgnoreCase(system)
            || "ORC".equalsIgnoreCase(system)
            || "AMS".equalsIgnoreCase(system);
    }
}
