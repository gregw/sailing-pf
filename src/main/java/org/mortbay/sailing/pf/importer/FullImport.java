package org.mortbay.sailing.pf.importer;

import java.nio.file.Path;

import org.eclipse.jetty.client.HttpClient;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs every importer in sequence against a single {@link DataStore}, populating
 * an empty (or existing) {@code pf-data} directory from scratch.
 * <p>
 * Usage: {@code mvn exec:java -Dexec.mainClass=org.mortbay.sailing.pf.importer.FullImport -Dpf-data=/path/to/pf-data}
 * <p>
 * The data root is resolved the same way as every other importer/server entry
 * point — see {@link DataStore#resolveDataRoot(String[])} — so it may be passed
 * as the first argument, via the {@code PF_DATA} environment variable, or it
 * defaults to {@code ./pf-data}.
 * <p>
 * The SailSys API import starts at race id 1000; earlier ids are not Australian
 * elapsed-time races of interest. A failure in one importer is logged and the
 * run continues with the next, so a single bad source does not abort the lot.
 */
public class FullImport
{
    private static final Logger LOG = LoggerFactory.getLogger(FullImport.class);

    /**
     * First SailSys race id to fetch from the API.
     */
    private static final int SAILSYS_START_ID = 1000;

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);
        LOG.info("Full import into data root {}", dataRoot.toAbsolutePath());

        DataStore dataStore = new DataStore(dataRoot);
        dataStore.start();

        HttpClient client = new HttpClient();
        client.start();

        try
        {
            runImporter("SailSys", () ->
                new SailSysImporter(dataStore, client)
                    .runFromApi(SAILSYS_START_ID, id ->
                        {}, () -> false,
                        dataRoot.resolve("cache/sailsys/races")));

            runImporter("TopYacht", () ->
                new TopYachtImporter(dataStore, client).run());

            runImporter("BWPS", () ->
                new BwpsImporter(dataStore, client).run());

            runImporter("ORC", () ->
                new OrcImporter(dataStore, client).run(dataRoot.resolve("cache/orc"), 1));

            runImporter("AMS", () ->
                new AmsImporter(dataStore, client).run());
        }
        finally
        {
            dataStore.stop();
            client.stop();
        }

        LOG.info("Full import complete");
    }

    /**
     * A single importer run that may throw.
     */
    private interface ImporterRun
    {
        void run() throws Exception;
    }

    private static void runImporter(String name, ImporterRun run)
    {
        LOG.info("=== Starting {} import ===", name);
        try
        {
            run.run();
            LOG.info("=== Finished {} import ===", name);
        }
        catch (Exception e)
        {
            LOG.error("=== {} import failed — continuing with remaining importers ===", name, e);
        }
    }
}
