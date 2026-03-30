package org.mortbay.sailing.hpf.data;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A racing boat. Raw layer — immutable, no derived fields, no back-references.
 * <p>
 * The ID is a generated slug: sailnum-firstname-hex, e.g. "aus1234-raging-3f9a".
 * It is stable once assigned and never derived from any source system ID.
 * <p>
 * designId and clubId are nullable: a boat may be of unknown design or without a
 * recorded home club.
 * <p>
 * certificates are embedded in the boat file; dirty semantics on the boat file
 * cover cert changes too.
 */
public record Boat(
    String id,           // e.g. "aus1234-raging-3f9a"
    String sailNumber,   // normalised sail number, e.g. "aus1234"
    String name,         // canonical name
    String designId,     // normalised design ID, nullable
    String clubId,       // primary home club domain, nullable
    List<String> aliases,           // alternate names seen for this boat across sources
    List<String> altSailNumbers,    // alternate sail numbers (previous sail numbers after a change)
    List<Certificate> certificates, // measurement certificates held by this boat
    List<String> sources, // short importer names that have contributed to this record, e.g. ["SailSys", "ORC"]
    Instant lastUpdated,  // when this record was last written by an importer; nullable
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Boat>
{

    public Boat
    {
        if (certificates == null)
            certificates = List.of();
        if (sources == null)
            sources = List.of();
        if (altSailNumbers == null)
            altSailNumbers = List.of();
    }

    @Override
    public Boat withLoadedAt(Instant t)
    {
        return new Boat(id, sailNumber, name, designId, clubId, aliases,
            altSailNumbers, certificates, sources, lastUpdated, t);
    }

    // loadedAt is loading metadata, not domain data — exclude from equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Boat b))
            return false;
        return Objects.equals(id, b.id) && Objects.equals(sailNumber, b.sailNumber)
            && Objects.equals(name, b.name) && Objects.equals(designId, b.designId)
            && Objects.equals(clubId, b.clubId) && Objects.equals(aliases, b.aliases)
            && Objects.equals(altSailNumbers, b.altSailNumbers)
            && Objects.equals(certificates, b.certificates)
            && Objects.equals(sources, b.sources) && Objects.equals(lastUpdated, b.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, sailNumber, name, designId, clubId, aliases, altSailNumbers, certificates, sources, lastUpdated);
    }
}
