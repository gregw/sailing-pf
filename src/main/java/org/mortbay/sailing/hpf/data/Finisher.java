package org.mortbay.sailing.hpf.data;

import java.time.Duration;

/**
 * A boat's finishing result within a division. Raw layer — immutable, no derived fields.
 * Only finishers are imported; elapsedTime is always present.
 * Uniquely identified by boatId within its containing Division.
 * <p>
 * certificateNumber links this result to the Certificate (IRC/ORC/AMS) under which
 * the boat raced. Null for PHS finishers or when no handicap value was used.
 */
public record Finisher(
    String boatId,
    Duration elapsedTime,
    boolean nonSpinnaker,
    String certificateNumber  // nullable; cert number of the Certificate used for scoring
) {}
