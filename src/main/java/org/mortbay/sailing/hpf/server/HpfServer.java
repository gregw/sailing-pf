package org.mortbay.sailing.hpf.server;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class HpfServer
{
    private static final Logger LOG = LoggerFactory.getLogger(HpfServer.class);

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);

        DataStore store = new DataStore(dataRoot);
        store.start();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        ImporterService importerService = new ImporterService(store, httpClient, dataRoot);
        importerService.start();

        AnalysisCache cache = new AnalysisCache(store);
        cache.refresh(importerService.targetIrcYear(), importerService.outlierSigma());
        importerService.setCache(cache);

        Server server = new Server(8080);
        ServletContextHandler context = new ServletContextHandler("/");
        context.addServlet(new ServletHolder(new AdminApiServlet(store, importerService, cache)), "/api/*");
        context.addServlet(new ServletHolder(new AnalysisServlet(store, cache)), "/api/analyse/*");
        context.addServlet(new ServletHolder(new StaticResourceServlet()), "/*");
        server.setHandler(context);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("Shutting down");
            try
            {
                importerService.stop();
                store.stop();
                httpClient.stop();
            }
            catch (Exception e)
            {
                LOG.error("Error during shutdown", e);
            }
        }));

        LOG.info("HPF admin server started on http://localhost:8080/");
        server.join();
    }
}
