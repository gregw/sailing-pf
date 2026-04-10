package org.mortbay.sailing.hpf.server;

import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;
import org.eclipse.jetty.security.openid.OpenIdLoginService;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.SessionHandler;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.mortbay.sailing.hpf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.EnumSet;

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

        TaskService taskService = new TaskService(store, httpClient, dataRoot);
        taskService.start();

        AnalysisCache cache = new AnalysisCache(store);
        cache.refresh(taskService.targetIrcYear(), taskService.outlierSigma(), taskService.clubCertificateWeight(), taskService.minAnalysisR2());
        taskService.setCache(cache);
        taskService.runStartupTasks();

        AuthConfig authConfig = taskService.authConfig();

        Server server = new Server();

        ServerConnector userConnector = new ServerConnector(server);
        userConnector.setName("user");
        userConnector.setPort(authConfig.userPort());
        server.addConnector(userConnector);

        ServerConnector adminConnector = new ServerConnector(server);
        adminConnector.setName("admin");
        adminConnector.setPort(authConfig.adminPort());
        server.addConnector(adminConnector);

        ServletContextHandler context = new ServletContextHandler("/");

        // Session handler (required for OpenID and for WriteAuthFilter's session check)
        SessionHandler sessionHandler = new SessionHandler();
        sessionHandler.getSessionCookieConfig().setAttribute("SameSite", "Lax");
        if (authConfig.baseUrl().startsWith("https://"))
            sessionHandler.getSessionCookieConfig().setSecure(true);
        context.setSessionHandler(sessionHandler);

        // OpenID security handler (prod only — dev mode skips OAuth entirely)
        if (!authConfig.devMode())
        {
            OpenIdConfiguration openIdConfig = new OpenIdConfiguration(
                "https://accounts.google.com",
                authConfig.clientId(),
                authConfig.clientSecret()
            );
            openIdConfig.addScopes("openid", "email");

            OpenIdLoginService loginService = new OpenIdLoginService(openIdConfig);
            OpenIdAuthenticator authenticator = new OpenIdAuthenticator(openIdConfig, "/error");

            SecurityHandler.PathMapped security = new SecurityHandler.PathMapped();
            security.setLoginService(loginService);
            security.setAuthenticator(authenticator);
            security.put("/auth/protected", Constraint.ANY_USER);
            context.setSecurityHandler(security);
        }

        context.addServlet(new ServletHolder(new AuthServlet(authConfig)), "/auth/*");
        FilterHolder waf = new FilterHolder(new WriteAuthFilter(authConfig));
        context.addFilter(waf, "/api/*", EnumSet.of(DispatcherType.REQUEST));

        context.addServlet(new ServletHolder(new AdminApiServlet(store, taskService, cache)), "/api/*");
        context.addServlet(new ServletHolder(new AnalysisServlet(store, cache)), "/api/analyse/*");
        context.addServlet(new ServletHolder(new StaticResourceServlet()), "/*");
        server.setHandler(context);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            LOG.info("Shutting down");
            try
            {
                taskService.stop();
                store.stop();
                httpClient.stop();
            }
            catch (Exception e)
            {
                LOG.error("Error during shutdown", e);
            }
        }));

        LOG.info("HPF server started — user: http://localhost:{}/ admin: http://localhost:{}/",
            authConfig.userPort(), authConfig.adminPort());
        server.join();
    }
}
