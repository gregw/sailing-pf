package org.mortbay.sailing.hpf.data;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A boat design (hull type).
 * The ID is the normalised design name, e.g. "j24", "farr40".
 * Canonical name is the authoritative display form, preferably from the ORC Class field.
 * Most designs have a single maker, but some (e.g. J/24) have been built by multiple manufacturers.
 */
public record Design(
    String id,            // normalised name, e.g. "j24", "farr40"
    String canonicalName, // display name, e.g. "J/24", "Farr 40"
    List<String> aliases,  // alternate design names, e.g. "Mumm 30" for "Farr 30"
    List<String> sources,  // short importer names that have contributed to this record, e.g. ["SailSys", "ORC"]
    Instant lastUpdated,   // when this record was last written by an importer; nullable
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Design>
{

    public Design
    {
        if (sources == null)
            sources = List.of();
    }

    @Override
    public Design withLoadedAt(Instant t)
    {
        return new Design(id, canonicalName, aliases, sources, lastUpdated, t);
    }

    // loadedAt is loading metadata, not domain data — exclude from equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Design d))
            return false;
        return Objects.equals(id, d.id) && Objects.equals(canonicalName, d.canonicalName)
            && Objects.equals(aliases, d.aliases)
            && Objects.equals(sources, d.sources) && Objects.equals(lastUpdated, d.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, canonicalName, aliases, sources, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "Design{" +
            "id='" + id + '\'' +
            ", canonicalName='" + canonicalName + '\'' +
            ", aliases=" + aliases +
            ", sources=" + sources +
            ", lastUpdated=" + lastUpdated +
            '}';
    }
}
