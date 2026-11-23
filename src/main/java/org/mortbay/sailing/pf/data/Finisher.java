package org.mortbay.sailing.pf.data;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A boat's finishing result within a division. Raw layer — immutable, no derived fields.
 * Only finishers are imported; elapsedTime is always present.
 * Uniquely identified by boatId within its containing Division.
 * <p>
 * certificateNumber links this result to the Certificate (IRC/ORC/AMS) under which
 * the boat raced. Null for PHS finishers or when no handicap value was used.
 * <p>
 * twoHanded marks a two-handed result in a division that also holds crewed boats (e.g. a
 * CYCA offshore race, where everyone sails the same course at the same time). Elsewhere a
 * two-handed result is known by its division's name; see {@link EntryVariant#of}.
 */
public record Finisher(
    String boatId,
    Duration elapsedTime,
    boolean nonSpinnaker,
    String certificateNumber, // nullable; cert number of the Certificate used for scoring
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean twoHanded
) {
    /** A crewed (not two-handed) result. */
    public Finisher(String boatId, Duration elapsedTime, boolean nonSpinnaker, String certificateNumber)
    {
        this(boatId, elapsedTime, nonSpinnaker, certificateNumber, false);
    }

    /** This result, for another boat (e.g. after a merge or rename). */
    public Finisher withBoatId(String id)
    {
        return new Finisher(id, elapsedTime, nonSpinnaker, certificateNumber, twoHanded);
    }

    /** This result, with its spinnaker flag set. */
    public Finisher withNonSpinnaker(boolean ns)
    {
        return new Finisher(boatId, elapsedTime, ns, certificateNumber, twoHanded);
    }

    @Override
    public String toString()
    {
        return "Finisher{" +
            "boatId='" + boatId + '\'' +
            ", elapsedTime=" + elapsedTime +
            ", nonSpinnaker=" + nonSpinnaker +
            ", certificateNumber='" + certificateNumber + '\'' +
            (twoHanded ? ", twoHanded=true" : "") +
            '}';
    }
}
