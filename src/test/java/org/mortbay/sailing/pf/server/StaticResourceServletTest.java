package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;

import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The static servlet's filesystem fallback (the project directory) serves only the project
 * documents pages include — never sources, build files, install scripts or a local pf-data.
 */
class StaticResourceServletTest
{
    @TempDir
    Path projectDir;

    private Server server;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() throws Exception
    {
        Files.writeString(projectDir.resolve("RELEASE_NOTES.md"), "# Notes\n\nSeries type added.\n");
        Files.writeString(projectDir.resolve("pom.xml"), "<project/>");
        Files.createDirectories(projectDir.resolve("etc"));
        Files.writeString(projectDir.resolve("etc/install.sh"), "#!/bin/sh\n");
        Files.createDirectories(projectDir.resolve("pf-data/config"));
        Files.writeString(projectDir.resolve("pf-data/config/auth.yaml"), "clientSecret: secret\n");

        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(0);
        server.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler("/");
        context.addServlet(new ServletHolder(new StaticResourceServlet(projectDir)), "/*");
        server.setHandler(context);
        server.start();
        port = connector.getLocalPort();
        client = new HttpClient();
        client.start();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        client.stop();
        server.stop();
    }

    private ContentResponse get(String path) throws Exception
    {
        return client.GET("http://localhost:" + port + path);
    }

    @Test
    void servesReleaseNotesFromTheProjectDirectory() throws Exception
    {
        ContentResponse resp = get("/RELEASE_NOTES.md");
        assertEquals(200, resp.getStatus());
        assertTrue(resp.getContentAsString().contains("Series type added."));
    }

    @Test
    void refusesEverythingElseInTheProjectDirectory() throws Exception
    {
        for (String path : new String[]{"/pom.xml", "/etc/", "/etc", "/etc/install.sh",
            "/pf-data/config/auth.yaml"})
            assertEquals(404, get(path).getStatus(), path);
    }

    @Test
    void refusesDotDotPaths() throws Exception
    {
        // Jetty may already reject these as ambiguous (400); either way nothing is served.
        for (String path : new String[]{"/%2e%2e/pom.xml", "/x/%2e%2e/pom.xml", "/..%2fpom.xml"})
        {
            int status = get(path).getStatus();
            assertNotEquals(200, status, path);
        }
    }

    @Test
    void stillServesContentFromTheClasspath() throws Exception
    {
        assertEquals(200, get("/common.js").getStatus());
    }
}
