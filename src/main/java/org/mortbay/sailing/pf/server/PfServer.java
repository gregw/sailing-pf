package org.mortbay.sailing.pf.server;

import java.nio.file.Path;
import java.util.EnumSet;

import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.client.transport.HttpClientTransportDynamic;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.SessionHandler;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;
import org.eclipse.jetty.security.openid.OpenIdConfiguration;
import org.eclipse.jetty.security.openid.OpenIdLoginService;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PfServer
{
    private static final Logger LOG = LoggerFactory.getLogger(PfServer.class);

    public static void main(String[] args) throws Exception
    {
        Path dataRoot = DataStore.resolveDataRoot(args);

        DataStore store = new DataStore(dataRoot);
        store.start();

        // Some sailing result hosts serve incomplete certificate chains; accept any cert
        // so importers don't fail with PKIX path-building errors.
        SslContextFactory.Client ssl = new SslContextFactory.Client();
        ssl.setTrustAll(true);
        ssl.setEndpointIdentificationAlgorithm(null);
        ClientConnector clientConnector = new ClientConnector();
        clientConnector.setSslContextFactory(ssl);
        HttpClient httpClient = new HttpClient(new HttpClientTransportDynamic(clientConnector));
        httpClient.start();

        TaskService taskService = new TaskService(store, httpClient, dataRoot);
        taskService.start();
        // Early, so a broken auth.yaml stops startup before the long analysis runs.
        AuthConfig authConfig = taskService.authConfig();

        AnalysisCache cache = new AnalysisCache(store);
        cache.refresh(taskService.targetIrcYear(), taskService.outlierSigma(), taskService.clubCertificateWeight(),
            taskService.minAnalysisR2(), taskService.minAnalysisPairs());
        taskService.setCache(cache);
        taskService.runStartupTasks();

        Server server = new Server();

        // Only the user connector trusts X-Forwarded-* (when a reverse proxy sits in front of
        // it): behind a proxy the request's own host and scheme are the proxy's, and without
        // this the sign-in redirect_uri would be http://localhost:<port>/... which Google
        // refuses. Never on the admin connector, where a forged X-Forwarded-For could slip
        // past the NAT-gateway check in AuthConfig.isAdminConnector.
        HttpConfiguration userHttp = new HttpConfiguration();
        if (authConfig.forwardedHeaders())
            userHttp.addCustomizer(new ForwardedRequestCustomizer());
        ServerConnector userConnector = new ServerConnector(server, new HttpConnectionFactory(userHttp));
        userConnector.setName("user");
        userConnector.setPort(authConfig.userPort());
        server.addConnector(userConnector);

        ServerConnector adminConnector = new ServerConnector(server);
        adminConnector.setName("admin");
        adminConnector.setPort(authConfig.adminPort());
        server.addConnector(adminConnector);

        ServletContextHandler context = new ServletContextHandler("/");

        // Sessions hold the signed-in account; the cookie is marked Secure on HTTPS requests
        // (Jetty's default), which behind a proxy relies on forwardedHeaders.
        SessionHandler sessionHandler = new SessionHandler();
        sessionHandler.getSessionCookieConfig().setAttribute("SameSite", "Lax");
        context.setSessionHandler(sessionHandler);

        if (authConfig.enabled())
            secure(context, authConfig, server);

        context.addServlet(new ServletHolder(new AuthServlet(authConfig)), "/auth/*");
        FilterHolder waf = new FilterHolder(new WriteAuthFilter(authConfig));
        context.addFilter(waf, "/api/*", EnumSet.of(DispatcherType.REQUEST));

        context.addServlet(new ServletHolder(new AdminApiServlet(store, taskService, cache, httpClient, authConfig)), "/api/*");
        context.addServlet(new ServletHolder(new AnalysisServlet(store, cache)), "/api/analyse/*");
        context.addServlet(new ServletHolder(new StaticResourceServlet(Path.of("").toAbsolutePath())), "/*");
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

        LOG.info("PF server started — user: http://localhost:{}/ admin: http://localhost:{}/",
            authConfig.userPort(), authConfig.adminPort());
        server.join();
    }

    /**
     * Puts the context behind OpenID Connect sign-in. Only {@link AuthServlet#LOGIN_PATH} (and
     * its old name {@code /auth/protected}) requires a login — asking for it is asking to sign
     * in; every page stays readable, and what a signed-in account may change is decided per
     * request ({@link Access#isEditor}). The issuer is all that is configured: its endpoints and
     * keys are discovered from it at startup, so a server with sign-in on needs the network to
     * start. Sessions are in memory, so a restart signs everybody out.
     */
    private static void secure(ServletContextHandler context, AuthConfig auth, Server server)
    {
        OpenIdConfiguration oidc = new OpenIdConfiguration.Builder()
            .issuer(auth.issuer())
            .clientId(auth.clientId())
            .clientSecret(auth.clientSecret())
            // The address and Workspace domain come in these; "openid" is added by
            // OpenIdConfiguration itself.
            .scopes("email", "profile")
            .httpClient(tokenExchangeClient())
            .build();
        // The identity provider and its HTTP client are the server's to start and stop.
        server.addBean(oidc);

        // The third argument is the ERROR PAGE: without one Jetty answers a failed callback
        // with a bare 403, and every way sign-in can fail looks the same.
        OpenIdAuthenticator authenticator =
            new OpenIdAuthenticator(oidc, auth.redirectPath(), AuthServlet.ERROR_PATH, null);
        SecurityHandler.PathMapped security = new SecurityHandler.PathMapped();
        security.setLoginService(new OpenIdLoginService(oidc));
        security.setAuthenticator(authenticator);
        security.put(AuthServlet.LOGIN_PATH, Constraint.ANY_USER);
        security.put("/auth/protected", Constraint.ANY_USER);
        context.setSecurityHandler(security);
        LOG.info("Sign-in via {}, callback {}", auth.issuer(), auth.redirectPath());
    }

    /**
     * The client that redeems the authorisation code, without the WWW-Authenticate handler.
     * When the client id or secret is wrong, Google's token endpoint answers 401 with a JSON
     * body naming the problem ({@code invalid_client}) and no WWW-Authenticate header; that
     * handler would turn it into an opaque "protocol violation" and discard the body. This
     * client only talks to the token endpoint, which never challenges, so nothing is lost and
     * Jetty reports the provider's own error.
     */
    private static HttpClient tokenExchangeClient()
    {
        return new HttpClient()
        {
            @Override
            protected void doStart() throws Exception
            {
                super.doStart();
                // After super.doStart(): the default handlers are installed as it starts.
                getProtocolHandlers().remove(WWWAuthenticationProtocolHandler.NAME);
            }
        };
    }
}
