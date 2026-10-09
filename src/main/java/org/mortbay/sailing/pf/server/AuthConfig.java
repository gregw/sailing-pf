package org.mortbay.sailing.pf.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Who may change data on this server.
 *
 * <p>The sign-in settings are read from {@code config/auth.yaml}, kept apart from
 * {@code admin.yaml} because they hold a client secret: {@code auth.yaml} is in
 * {@code .gitignore}; {@code auth.yaml.example} beside it shows the shape. The connector
 * ports come from {@code admin.yaml}.
 *
 * <p><b>Absent means off.</b> No file, or {@code enabled: false}: nobody can sign in, and
 * changes can only be made through the admin connector — as before sign-in existed.
 *
 * <p>With it on, any account the issuer (Google) authenticates may sign in, but only an
 * <em>editor</em> may change anything: an address in {@link #editors}, or an account in one
 * of the {@link #allowedDomains}. Everyone else is read-only. The admin connector is always
 * an editor, unless the request came through the NAT gateway (see {@link #isAdminConnector}).
 *
 * @param adminPort    port of the "admin" connector — requests on this port are editors
 *                     unless they arrive from the natGatewayIp (second line of defence)
 * @param userPort     port of the "user" connector — normal public-facing access
 * @param natGatewayIp if set, requests whose remote address matches this IP are always treated
 *                     as user-connector requests even if they arrive on the admin port, preventing
 *                     traffic routed through the public NAT gateway from gaining admin access
 */
record AuthConfig(boolean enabled, String issuer, String clientId, String clientSecret,
                  String redirectPath, List<String> allowedDomains, List<String> editors,
                  boolean forwardedHeaders, int adminPort, int userPort, String natGatewayIp)
{
    private static final Logger LOG = LoggerFactory.getLogger(AuthConfig.class);

    /** Google's OpenID Connect issuer. Everything else is discovered from it. */
    static final String GOOGLE = "https://accounts.google.com";

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    AuthConfig
    {
        if (issuer == null || issuer.isBlank())
            issuer = GOOGLE;
        if (redirectPath == null || redirectPath.isBlank())
            redirectPath = "/auth/callback";
        if (!redirectPath.startsWith("/"))
            redirectPath = "/" + redirectPath;
        allowedDomains = lowerCased(allowedDomains);
        editors = lowerCased(editors);
    }

    /** Sign-in off: changes only through the admin connector. */
    static AuthConfig disabled(int adminPort, int userPort, String natGatewayIp)
    {
        return new AuthConfig(false, null, null, null, null, List.of(), List.of(), false,
            adminPort, userPort, natGatewayIp);
    }

    /**
     * Loads {@code auth.yaml} from the config directory, or returns {@link #disabled} if it is
     * not there. A file that is present but enabled without a client ID and secret is
     * <em>not</em> treated as off — failing open on a broken security config would leave the
     * server unprotected without anyone noticing — so this throws and the server does not start.
     */
    static AuthConfig load(Path configDir, int adminPort, int userPort, String natGatewayIp)
        throws IOException
    {
        Path file = configDir.resolve("auth.yaml");
        if (!Files.isRegularFile(file))
        {
            LOG.info("No {} -- sign-in is off; changes only through the admin connector", file);
            return disabled(adminPort, userPort, natGatewayIp);
        }
        AuthFile f = YAML.readValue(file.toFile(), AuthFile.class);
        List<String> domains = new ArrayList<>();
        if (f.allowedDomain != null)
            domains.add(f.allowedDomain);
        if (f.allowedDomains != null)
            domains.addAll(f.allowedDomains);
        AuthConfig auth = new AuthConfig(f.enabled, f.issuer, f.clientId, f.clientSecret,
            f.redirectPath, domains, f.editors, f.forwardedHeaders, adminPort, userPort, natGatewayIp);
        if (!auth.enabled())
        {
            LOG.warn("{} says enabled: false -- sign-in is off", file);
            return auth;
        }
        if (auth.clientId() == null || auth.clientId().isBlank())
            throw new IllegalStateException(file + ": clientId is required when enabled");
        if (auth.clientSecret() == null || auth.clientSecret().isBlank())
            throw new IllegalStateException(file + ": clientSecret is required when enabled");
        if (auth.allowedDomains().isEmpty() && auth.editors().isEmpty())
            LOG.warn("{} has no allowedDomains and no editors -- anyone can sign in, but nobody "
                + "signed in can change anything", file);
        LOG.info("Sign-in on via {}: editors are {} and {} named address(es){}", auth.issuer(),
            auth.allowedDomains().isEmpty() ? "no domains" : "accounts in " + auth.allowedDomains(),
            auth.editors().size(), auth.forwardedHeaders() ? "; trusting X-Forwarded-* on the user port" : "");
        return auth;
    }

    /**
     * Whether a signed-in account may change data: named in {@link #editors}, or in one of the
     * {@link #allowedDomains}. The domain is checked against the {@code hd} claim — the
     * Workspace domain Google itself asserts — and then the address.
     */
    boolean isEditor(String email, String hostedDomain)
    {
        if (email == null || email.isBlank())
            return false;
        String address = email.trim().toLowerCase(Locale.ENGLISH);
        if (editors.contains(address))
            return true;
        for (String domain : allowedDomains)
        {
            if (hostedDomain != null && domain.equalsIgnoreCase(hostedDomain.trim()))
                return true;
            if (address.endsWith("@" + domain))
                return true;
        }
        return false;
    }

    /**
     * Returns true if this request arrived on the admin connector AND did not come from the
     * configured NAT gateway IP (which would indicate it was routed via the public internet).
     */
    boolean isAdminConnector(HttpServletRequest request)
    {
        if (request.getLocalPort() != adminPort)
            return false;
        if (natGatewayIp != null && !natGatewayIp.isBlank()
            && natGatewayIp.equals(request.getRemoteAddr()))
            return false;
        return true;
    }

    private static List<String> lowerCased(List<String> values)
    {
        return values == null ? List.of()
            : values.stream()
                .filter(v -> v != null && !v.isBlank())
                .map(v -> v.trim().toLowerCase(Locale.ENGLISH))
                .distinct()
                .toList();
    }

    /** The shape of {@code auth.yaml}. {@code allowedDomain}, singular, is also accepted. */
    static class AuthFile
    {
        public boolean enabled;
        public String issuer;
        public String clientId;
        public String clientSecret;
        public String redirectPath;
        public String allowedDomain;
        public List<String> allowedDomains;
        public List<String> editors;
        public boolean forwardedHeaders;
    }
}
