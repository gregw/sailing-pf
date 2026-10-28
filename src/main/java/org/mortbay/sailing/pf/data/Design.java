package org.mortbay.sailing.pf.data;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

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
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<Link> links,      // external web pages about this design, e.g. sailboatdata.com, Wikipedia
    Instant lastUpdated,   // when this record was last written by an importer; nullable
    @JsonIgnore boolean noSpinnaker, // true when this design physically cannot fly a spinnaker
    // (e.g. cat-rigged): spin and nonSpin RF/PF are treated
    // as a single combined factor. Persisted in design.yaml,
    // not in the per-design JSON file.
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Design>
{

    public Design
    {
        if (sources == null)
            sources = List.of();
        links = Link.normalise(links);
    }

    /**
     * A design with no links. For new records only: a copy of an existing record must use the
     * canonical constructor and carry its {@link #links()} across.
     */
    public Design(String id, String canonicalName, List<String> aliases, List<String> sources,
                  Instant lastUpdated, boolean noSpinnaker, Instant loadedAt)
    {
        this(id, canonicalName, aliases, sources, List.of(), lastUpdated, noSpinnaker, loadedAt);
    }

    @Override
    public Design withLoadedAt(Instant t)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, t);
    }

    /**
     * Returns a copy of this Design with the given links.
     */
    public Design withLinks(List<Link> newLinks)
    {
        return new Design(id, canonicalName, aliases, sources, newLinks, lastUpdated, noSpinnaker, loadedAt);
    }

    /**
     * Returns a copy of this Design with the given {@code noSpinnaker} flag value.
     */
    public Design withNoSpinnaker(boolean flag)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, flag, loadedAt);
    }

    // loadedAt is loading metadata, not domain data — exclude from equality.
    // noSpinnaker is catalogue-derived runtime state — exclude from equality so toggling
    // the flag never marks a Design dirty for disk persistence.
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Design d))
            return false;
        return Objects.equals(id, d.id) && Objects.equals(canonicalName, d.canonicalName)
            && Objects.equals(aliases, d.aliases)
            && Objects.equals(sources, d.sources) && Objects.equals(links, d.links)
            && Objects.equals(lastUpdated, d.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, canonicalName, aliases, sources, links, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "Design{" +
            "id='" + id + '\'' +
            ", canonicalName='" + canonicalName + '\'' +
            ", aliases=" + aliases +
            ", sources=" + sources +
            ", links=" + links +
            ", lastUpdated=" + lastUpdated +
            ", noSpinnaker=" + noSpinnaker +
            '}';
    }
}
