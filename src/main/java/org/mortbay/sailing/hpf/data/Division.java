package org.mortbay.sailing.hpf.data;

import java.util.List;

/**
 * A division within a race, containing its finishers.
 * Uniquely identified by name within its containing Race.
 */
public record Division(
    String name,               // e.g. "Division 1", "Open"
    List<Finisher> finishers
) {
    @Override
    public String toString()
    {
        return "Division{" +
            "name='" + name + '\'' +
            ", finishers=" + finishers +
            '}';
    }
}
