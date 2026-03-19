package org.mortbay.sailing.hpf.data;

import java.time.LocalDate;

/**
 * A measurement-based handicap certificate held by a boat.
 * Only IRC, ORC and AMS certificates are stored — PHS and CBH are excluded.
 * <p>
 * The ID follows the pattern: boatId-system-year-seq, e.g. "aus1234-raging-3f9a-irc-2024-001".
 * <p>
 * Value semantics by system:
 * IRC — TCC (time correction factor), e.g. 0.987
 * ORC — TCF (time correction factor), e.g. 1.020; stored as 600/GPH, never raw GPH
 * AMS — TCF (time correction factor), same scale as IRC TCC
 * <p>
 * twoHanded is only set for AMS two-handed certificates; false for all IRC/ORC certs.
 * <p>
 * Not a Loadable — Certificate is embedded in its owning Boat file; dirty semantics
 * are inherited from the Boat.
 */
public record Certificate(
    String system,           // "IRC", "ORC", or "AMS"
    int year,                // certificate year
    double value,            // TCF for all systems (IRC TCC; ORC 600/GPH; AMS TCF)
    boolean nonSpinnaker,    // true if this is a non-spinnaker certificate
    boolean twoHanded,       // true for AMS two-handed certificates; false for all IRC/ORC certs
    String certificateNumber, // dxtID for ORC; cert number for AMS
    LocalDate expiryDate     // null for AMS
) {}
