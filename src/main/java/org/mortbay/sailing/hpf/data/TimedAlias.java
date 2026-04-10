package org.mortbay.sailing.hpf.data;

import java.time.LocalDate;

/**
 * An alternate name for a boat, optionally bounded by date range.
 * Used by the alias seed to represent names a boat has raced under over time.
 */
public record TimedAlias(
    String name,       // the alternate name
    LocalDate from,    // null = applies from the beginning of time
    LocalDate until    // null = applies until the end of time
)
{
    /**
     * Returns true if this alias is active on the given date.
     * If {@code date} is null, returns true always (no date context available).
     */
    public boolean activeOn(LocalDate date)
    {
        if (date == null)
            return true;
        if (from != null && date.isBefore(from))
            return false;
        if (until != null && date.isAfter(until))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "TimedAlias{" +
            "name='" + name + '\'' +
            ", from=" + from +
            ", until=" + until +
            '}';
    }
}
