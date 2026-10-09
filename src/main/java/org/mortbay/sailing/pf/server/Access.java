package org.mortbay.sailing.pf.server;

import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.eclipse.jetty.security.openid.OpenIdAuthenticator;

/**
 * Who is making a request, as far as changing data is concerned.
 *
 * <p>The claims come from the session, where {@link OpenIdAuthenticator} leaves them after a
 * successful sign-in, so the id token is verified exactly once, by Jetty.
 */
final class Access
{
    private Access()
    {
    }

    /** The signed-in account's id-token claims, or null when nobody is signed in. */
    @SuppressWarnings("unchecked")
    static Map<String, Object> claims(HttpServletRequest req)
    {
        HttpSession session = req.getSession(false);
        Object claims = session == null ? null : session.getAttribute(OpenIdAuthenticator.CLAIMS);
        return claims instanceof Map ? (Map<String, Object>)claims : null;
    }

    /** The signed-in account's address, or null. */
    static String email(HttpServletRequest req)
    {
        Map<String, Object> claims = claims(req);
        Object email = claims == null ? null : claims.get("email");
        return email == null ? null : email.toString();
    }

    /**
     * Whether this request may change data: it came in on the admin connector, or sign-in is
     * on and the signed-in account is an editor ({@link AuthConfig#isEditor}).
     */
    static boolean isEditor(HttpServletRequest req, AuthConfig auth)
    {
        if (auth == null)
            return false;
        if (auth.isAdminConnector(req))
            return true;
        if (!auth.enabled())
            return false;
        Map<String, Object> claims = claims(req);
        if (claims == null)
            return false;
        // An address the provider has not verified proves nothing about who this is.
        Object verified = claims.get("email_verified");
        if (Boolean.FALSE.equals(verified) || "false".equals(String.valueOf(verified)))
            return false;
        Object email = claims.get("email");
        Object hd = claims.get("hd");
        return auth.isEditor(email == null ? null : email.toString(), hd == null ? null : hd.toString());
    }
}
