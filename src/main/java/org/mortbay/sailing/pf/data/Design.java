package org.mortbay.sailing.pf.data;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A boat design (hull type).
 * The ID is the normalised design name, e.g. "j24", "farr40".
 * Canonical name is the authoritative display form, preferably from the ORC Class field.
 * Most designs have a single maker, but some (e.g. J/24) have been built by multiple manufacturers.
 */
@JsonPropertyOrder({"id", "canonicalName", "aliases", "sources", "links", "lastUpdated",
    "noSpinnaker", "oneDesign", "excluded", "ignored", "year"})
public record Design(
    String id,            // normalised name, e.g. "j24", "farr40"
    String canonicalName, // display name, e.g. "J/24", "Farr 40"
    List<String> aliases,  // alternate design names, e.g. "Mumm 30" for "Farr 30"
    List<String> sources,  // short importer names that have contributed to this record, e.g. ["SailSys", "ORC"]
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<Link> links,      // external web pages about this design, e.g. sailboatdata.com, Wikipedia
    Instant lastUpdated,   // when this record was last written; nullable
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean noSpinnaker,   // cannot fly a spinnaker (e.g. cat-rigged): spin and nonSpin factors are one
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean oneDesign,     // identical boats: each boat not modified takes the design's reference factor
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean excluded,      // left out of the analysis (dinghy, off-the-beach class)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean ignored,       // not a real design (e.g. a builder's name): its boats are treated as design-less
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Integer year,          // manual design year, overriding the year from ORC certificate age dates
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Design>
{

    public Design
    {
        if (aliases == null)
            aliases = List.of();
        if (sources == null)
            sources = List.of();
        links = Link.normalise(links);
    }

    /** A design with no flags but noSpinnaker, and no year. */
    public Design(String id, String canonicalName, List<String> aliases, List<String> sources,
                  List<Link> links, Instant lastUpdated, boolean noSpinnaker, Instant loadedAt)
    {
        this(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, false, false, false, null, loadedAt);
    }

    /**
     * A design with no links. For new records only: a copy of an existing record must use the
     * withers, to carry its links and flags across.
     */
    public Design(String id, String canonicalName, List<String> aliases, List<String> sources,
                  Instant lastUpdated, boolean noSpinnaker, Instant loadedAt)
    {
        this(id, canonicalName, aliases, sources, List.of(), lastUpdated, noSpinnaker, loadedAt);
    }

    @Override
    public Design withLoadedAt(Instant t)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, t);
    }

    /** This design under another id (e.g. renamed), with everything else kept. */
    public Design withId(String newId)
    {
        return new Design(newId, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withCanonicalName(String name)
    {
        return new Design(id, name, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withAliases(List<String> newAliases)
    {
        return new Design(id, canonicalName, newAliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withSources(List<String> newSources)
    {
        return new Design(id, canonicalName, aliases, newSources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withLinks(List<Link> newLinks)
    {
        return new Design(id, canonicalName, aliases, sources, newLinks, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withLastUpdated(Instant t)
    {
        return new Design(id, canonicalName, aliases, sources, links, t, noSpinnaker, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withNoSpinnaker(boolean flag)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, flag, oneDesign, excluded, ignored, year, loadedAt);
    }

    public Design withOneDesign(boolean flag)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, flag, excluded, ignored, year, loadedAt);
    }

    public Design withExcluded(boolean flag)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, flag, ignored, year, loadedAt);
    }

    public Design withIgnored(boolean flag)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, flag, year, loadedAt);
    }

    public Design withYear(Integer newYear)
    {
        return new Design(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, newYear, loadedAt);
    }

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
            && Objects.equals(lastUpdated, d.lastUpdated) && noSpinnaker == d.noSpinnaker
            && oneDesign == d.oneDesign && excluded == d.excluded && ignored == d.ignored
            && Objects.equals(year, d.year);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, canonicalName, aliases, sources, links, lastUpdated, noSpinnaker, oneDesign, excluded, ignored, year);
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
