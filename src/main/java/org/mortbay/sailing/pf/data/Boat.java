package org.mortbay.sailing.pf.data;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A racing boat. Raw layer — immutable, no derived fields, no back-references.
 * <p>
 * The ID is a generated slug: sailnum-firstname-hex, e.g. "aus1234-raging-3f9a".
 * It is stable once assigned and never derived from any source system ID.
 * <p>
 * designId is nullable: a boat may be of unknown design.
 * <p>
 * A boat may belong to zero, one, or several clubs. {@code clubIds} is ordered;
 * the first entry is treated as the primary club. Empty list means the boat has
 * no recorded home club.
 * <p>
 * certificates are embedded in the boat file; dirty semantics on the boat file
 * cover cert changes too.
 */
public record Boat(
    String id,           // e.g. "aus1234-raging-3f9a"
    String sailNumber,   // normalised sail number, e.g. "AUS1234"
    String name,         // canonical name
    String designId,     // normalised design ID, nullable
    List<String> clubIds, // home club domains; first entry is primary, may be empty
    List<Certificate> certificates, // measurement certificates held by this boat
    List<String> sources, // short importer names that have contributed to this record, e.g. ["SailSys", "ORC"]
    Instant lastUpdated,  // when this record was last written by an importer; nullable
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Boat>
{

    public Boat
    {
        clubIds = normaliseClubIds(clubIds);
        if (certificates == null)
            certificates = List.of();
        if (sources == null)
            sources = List.of();
    }

    /**
     * Jackson factory accepting both the new {@code clubIds} list and the legacy
     * scalar {@code clubId} field. Older JSON records that pre-date multi-club
     * support continue to deserialise correctly.
     */
    @JsonCreator
    static Boat fromJson(
        @JsonProperty("id") String id,
        @JsonProperty("sailNumber") String sailNumber,
        @JsonProperty("name") String name,
        @JsonProperty("designId") String designId,
        @JsonProperty("clubIds") List<String> clubIds,
        @JsonProperty("clubId") String legacyClubId,
        @JsonProperty("certificates") List<Certificate> certificates,
        @JsonProperty("sources") List<String> sources,
        @JsonProperty("lastUpdated") Instant lastUpdated)
    {
        List<String> resolved;
        if (clubIds != null && !clubIds.isEmpty())
            resolved = clubIds;
        else if (legacyClubId != null && !legacyClubId.isBlank())
            resolved = List.of(legacyClubId);
        else
            resolved = List.of();
        return new Boat(id, sailNumber, name, designId, resolved,
            certificates, sources, lastUpdated, null);
    }

    /**
     * Returns the primary (first) club id, or null if the boat has no clubs.
     */
    @JsonIgnore
    public String primaryClubId()
    {
        return clubIds.isEmpty() ? null : clubIds.getFirst();
    }

    /**
     * True if the boat is a member of {@code clubId}.
     */
    public boolean hasClub(String clubId)
    {
        return clubId != null && clubIds.contains(clubId);
    }

    /**
     * Returns an immutable, deduplicated, blank-stripped club id list.
     */
    public static List<String> normaliseClubIds(List<String> raw)
    {
        if (raw == null || raw.isEmpty())
            return List.of();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        for (String s : raw)
        {
            if (s == null)
                continue;
            String t = s.trim();
            if (!t.isEmpty())
                seen.add(t);
        }
        return seen.isEmpty() ? List.of() : List.copyOf(new ArrayList<>(seen));
    }

    @Override
    public Boat withLoadedAt(Instant t)
    {
        return new Boat(id, sailNumber, name, designId, clubIds,
            certificates, sources, lastUpdated, t);
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
            && Objects.equals(clubIds, b.clubIds)
            && Objects.equals(certificates, b.certificates)
            && Objects.equals(sources, b.sources) && Objects.equals(lastUpdated, b.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, sailNumber, name, designId, clubIds, certificates, sources, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "Boat{" +
            "id='" + id + '\'' +
            ", sailNumber='" + sailNumber + '\'' +
            ", name='" + name + '\'' +
            ", designId='" + designId + '\'' +
            ", clubIds=" + clubIds +
            ", certificates=" + certificates +
            ", sources=" + sources +
            ", lastUpdated=" + lastUpdated +
            '}';
    }
}
