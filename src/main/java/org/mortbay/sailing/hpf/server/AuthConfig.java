package org.mortbay.sailing.hpf.server;

import jakarta.servlet.http.HttpServletRequest;

/**
 * @param adminPort    port of the "admin" connector — requests on this port are pre-authenticated
 *                     unless they arrive from the natGatewayIp (second line of defence)
 * @param userPort     port of the "user" connector — normal public-facing access
 * @param natGatewayIp if set, requests whose remote address matches this IP are always treated
 *                     as user-connector requests even if they arrive on the admin port, preventing
 *                     traffic routed through the public NAT gateway from gaining admin access
 */
record AuthConfig(String clientId, String clientSecret, String baseUrl, String allowedDomain,
                  int adminPort, int userPort, String natGatewayIp)
{
    boolean devMode()
    {
        return clientId == null || clientId.isBlank();
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
}
