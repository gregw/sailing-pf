package org.mortbay.sailing.pf.data;

import java.time.LocalDate;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

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
 * twoHanded is set for AMS two-handed and ORC Double Handed (DH) certificates; false for IRC.
 * <p>
 * ORC certificates come only from the ORC importers (numbered by ORC dxtID). ORC handicaps
 * read from race results are not kept: clubs score ORC with many different ratings (time on
 * time inshore/coastal/offshore, triple number, GPH), so they are not on the 600/GPH scale.
 * See {@link #isRaceDerivedOrc()}.
 * <p>
 * Not a Loadable — Certificate is embedded in its owning Boat file; dirty semantics
 * are inherited from the Boat.
 */
public record Certificate(
    String system,           // "IRC", "ORC", or "AMS"
    int year,                // certificate year
    double value,            // TCF for all systems (IRC TCC; ORC 600/GPH; AMS TCF)
    boolean nonSpinnaker,    // true if this is a non-spinnaker certificate
    boolean twoHanded,       // true for AMS two-handed and ORC Double Handed (DH) certificates; false for IRC
    boolean club,            // true for club-level certs (ORC club, potentially IRC club); carries configurable weight penalty
    boolean windwardLeeward, // true for ORC WL (windward/leeward) course-specific certs; carries 0.8× weight penalty
    String certificateNumber, // dxtID for ORC; cert number for AMS
    LocalDate expiryDate,    // null for AMS
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Integer ageYear          // ORC age date year (usually the design's series year); null if unknown
)
{
    /** A certificate without an age year (all but ORC certificates). */
    public Certificate(String system, int year, double value, boolean nonSpinnaker, boolean twoHanded,
                       boolean club, boolean windwardLeeward, String certificateNumber, LocalDate expiryDate)
    {
        this(system, year, value, nonSpinnaker, twoHanded, club, windwardLeeward, certificateNumber, expiryDate, null);
    }

    /** This certificate with the given age year. */
    public Certificate withAgeYear(Integer age)
    {
        return new Certificate(system, year, value, nonSpinnaker, twoHanded, club, windwardLeeward,
            certificateNumber, expiryDate, age);
    }

    /**
     * Jackson deserialisation factory. Reads both {@code club} and legacy {@code orcClub}
     * field names for backward compatibility with older JSON files.
     */
    @JsonCreator
    public static Certificate create(
        @JsonProperty("system")            String system,
        @JsonProperty("year")              int year,
        @JsonProperty("value")             double value,
        @JsonProperty("nonSpinnaker")      boolean nonSpinnaker,
        @JsonProperty("twoHanded")         boolean twoHanded,
        @JsonProperty("club")             Boolean club,
        @JsonProperty("orcClub")           Boolean orcClub,
        @JsonProperty("windwardLeeward")   Boolean windwardLeeward,
        @JsonProperty("certificateNumber") String certificateNumber,
        @JsonProperty("expiryDate")        LocalDate expiryDate,
        @JsonProperty("ageYear")           Integer ageYear)
    {
        // Accept either "club" or legacy "orcClub" field
        boolean isClub = (club != null && club) || (orcClub != null && orcClub);
        return new Certificate(system, year, value, nonSpinnaker, twoHanded,
            isClub, windwardLeeward != null && windwardLeeward,
            certificateNumber, expiryDate, ageYear);
    }

    /** Certificate-number prefix of ORC handicaps copied from race results by a results importer. */
    private static final Pattern RACE_DERIVED_ORC = Pattern.compile("^(orc-inferred-|ty-orc|[a-z]+-orc-)");

    /**
     * True for an ORC "certificate" inferred from a race result (SailSys {@code orc-inferred-…},
     * TopYacht {@code ty-orc…}, BWPS/RSHYR {@code <source>-orc-…}) rather than an official ORC
     * certificate. These are not on the 600/GPH scale and are dropped at load.
     */
    @JsonIgnore
    public boolean isRaceDerivedOrc()
    {
        return "ORC".equalsIgnoreCase(system) && certificateNumber != null
            && RACE_DERIVED_ORC.matcher(certificateNumber).find();
    }

    @Override
    public String toString()
    {
        return "Certificate{" +
            "system='" + system + '\'' +
            ", year=" + year +
            ", value=" + value +
            ", nonSpinnaker=" + nonSpinnaker +
            ", twoHanded=" + twoHanded +
            ", club=" + club +
            ", windwardLeeward=" + windwardLeeward +
            ", certificateNumber='" + certificateNumber + '\'' +
            ", expiryDate=" + expiryDate +
            ", ageYear=" + ageYear +
            '}';
    }
}
