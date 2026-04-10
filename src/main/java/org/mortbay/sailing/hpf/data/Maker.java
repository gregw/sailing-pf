package org.mortbay.sailing.hpf.data;

import java.util.List;

/**
 * A boat manufacturer.
 * The ID is the normalised maker name (lowercase, non-alphanumeric stripped), e.g. "farr", "beneteau".
 */
public record Maker(
    String id,            // normalised name, e.g. "farr"
    String canonicalName, // display name, e.g. "Farr"
    List<String> aliases  // alternate names, e.g. "Farr Yacht Design"
) {
    @Override
    public String toString()
    {
        return "Maker{" +
            "id='" + id + '\'' +
            ", canonicalName='" + canonicalName + '\'' +
            ", aliases=" + aliases +
            '}';
    }
}
