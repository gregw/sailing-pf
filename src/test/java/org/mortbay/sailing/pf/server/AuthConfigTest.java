package org.mortbay.sailing.pf.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthConfigTest
{
    @TempDir
    Path configDir;

    private AuthConfig load(String yaml) throws Exception
    {
        Files.writeString(configDir.resolve("auth.yaml"), yaml);
        return AuthConfig.load(configDir, 8888, 8081, null);
    }

    @Test
    void noFileMeansOff() throws Exception
    {
        AuthConfig auth = AuthConfig.load(configDir, 8888, 8081, "10.0.0.1");
        assertFalse(auth.enabled());
        assertEquals(8888, auth.adminPort());
        assertEquals("10.0.0.1", auth.natGatewayIp());
    }

    @Test
    void disabledFileIsOff() throws Exception
    {
        assertFalse(load("enabled: false\nclientId: x\n").enabled());
    }

    @Test
    void enabledWithoutSecretRefusesToStart()
    {
        assertThrows(IllegalStateException.class, () -> load("enabled: true\nclientId: abc\n"));
        assertThrows(IllegalStateException.class, () -> load("enabled: true\nclientSecret: s\n"));
    }

    @Test
    void readsSettingsWithDefaults() throws Exception
    {
        AuthConfig auth = load("""
            enabled: true
            clientId: id
            clientSecret: secret
            allowedDomain: MYC.org.au
            allowedDomains: [other.org]
            editors: [" Greg@Example.COM "]
            forwardedHeaders: true
            """);
        assertTrue(auth.enabled());
        assertEquals(AuthConfig.GOOGLE, auth.issuer());
        assertEquals("/auth/callback", auth.redirectPath());
        assertEquals(List.of("myc.org.au", "other.org"), auth.allowedDomains());
        assertEquals(List.of("greg@example.com"), auth.editors());
        assertTrue(auth.forwardedHeaders());
    }

    @Test
    void editorsAreNamedAddressesOrAllowedDomains() throws Exception
    {
        AuthConfig auth = load("""
            enabled: true
            clientId: id
            clientSecret: secret
            allowedDomains: [myc.org.au]
            editors: [greg@example.com]
            """);
        assertTrue(auth.isEditor("Greg@Example.com", null), "named address");
        assertTrue(auth.isEditor("someone@myc.org.au", null), "address in domain");
        assertTrue(auth.isEditor("someone@gmail.com", "myc.org.au"), "hd claim");
        assertFalse(auth.isEditor("someone@gmail.com", null));
        assertFalse(auth.isEditor("someone@notmyc.org.au", null), "suffix is not the domain");
        assertFalse(auth.isEditor(null, "myc.org.au"), "no address");
    }
}
