package org.mortbay.sailing.hpf.data;

import java.time.Duration;

/**
 * A {@link Duration} with an associated confidence weight.
 * <p>
 * Structurally parallel to {@link Factor}: both carry a {@code weight} in [0, 1]
 * by the same convention. Weight of zero means "discard" — callers should skip
 * zero-weight intervals rather than letting them silently distort results.
 */
public record WeightedInterval(
    Duration duration,  // elapsed time
    double weight       // scalar in [0, 1]; 0 = discard
)
{
    public WeightedInterval
    {
        if (duration == null)
            throw new IllegalArgumentException("duration must not be null");
        if (weight < 0 || weight > 1)
            throw new IllegalArgumentException("weight must be in [0, 1], got: " + weight);
    }

    @Override
    public String toString()
    {
        return "WeightedInterval{" +
            "duration=" + duration +
            ", weight=" + weight +
            '}';
    }
}
