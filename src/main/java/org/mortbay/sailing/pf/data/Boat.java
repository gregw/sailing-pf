package org.mortbay.sailing.pf.data;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A boat: one hull of one design, through any number of {@link Identity identities} (sail
 * number + name), e.g. About Time, then Sail Exchange, then Kanreki. Raw layer: immutable.
 * <p>
 * {@code identities} maps each identity ID ({@code sail-name-design}, unique across the
 * database) to the identity, oldest first; the last is the boat's <em>current</em> identity,
 * which names its file and gives its display name and primary club. Any identity ID refers to
 * the boat (the store indexes them all).
 * <p>
 * {@code id} is the identity this record is seen through: the current one as stored, or the
 * one a lookup or an import matched ({@link #withId}). {@link #sailNumber()} and {@link #name()}
 * are that identity's; {@link #certificates()}, {@link #sources()} and {@link #clubIds()} cover
 * all identities, the seen-through one's first. The id is not stored: a boat file holds
 * designId, links, identities and lastUpdated.
 * <p>
 * A boat's design is the same through all its identities: a modified boat is a new boat.
 */
@JsonPropertyOrder({"designId", "modified", "links", "identities", "lastUpdated"})
@JsonIgnoreProperties(value = {"id"}, allowSetters = true) // not written; read from v1 files
public record Boat(
    String id,
    String designId,    // normalised design ID, nullable
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    boolean modified,   // modified from its design: keeps its own RF, not pulled toward the design's
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<Link> links,   // external web pages about this boat (ORC certificate links are derived, not stored)
    Map<String, Identity> identities, // identity ID -> identity, oldest first; the last is current
    Instant lastUpdated,  // when this record was last written; nullable
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Boat>
{
    public Boat
    {
        links = Link.normalise(links);
        identities = identities == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(identities));
        if (identities.isEmpty())
            throw new IllegalArgumentException("A boat needs at least one identity");
        if (id == null)
            id = lastKey(identities);
        else if (!identities.containsKey(id))
            throw new IllegalArgumentException("Boat seen through " + id + ", which is not one of its identities " + identities.keySet());
    }

    /**
     * A boat with one identity (and links): the v1 shape, where the boat ID is its only
     * identity's ID.
     */
    public Boat(String id, String sailNumber, String name, String designId, List<String> clubIds,
                List<Certificate> certificates, List<String> sources, List<Link> links,
                Instant lastUpdated, Instant loadedAt)
    {
        this(id, designId, false, links, singleIdentity(id, new Identity(sailNumber, name, clubIds, certificates, sources)),
            lastUpdated, loadedAt);
    }

    /** A boat that is not modified from its design. */
    public Boat(String id, String designId, List<Link> links, Map<String, Identity> identities,
                Instant lastUpdated, Instant loadedAt)
    {
        this(id, designId, false, links, identities, lastUpdated, loadedAt);
    }

    /** A boat with one identity and no links. */
    public Boat(String id, String sailNumber, String name, String designId, List<String> clubIds,
                List<Certificate> certificates, List<String> sources, Instant lastUpdated, Instant loadedAt)
    {
        this(id, sailNumber, name, designId, clubIds, certificates, sources, List.of(), lastUpdated, loadedAt);
    }

    /**
     * Reads a boat file: the v2 form ({@code identities}), or a v1 file (one sail number and
     * name, with {@code id}, {@code clubIds} or the older {@code clubId}, certificates and
     * sources), which becomes a boat with that one identity.
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    static Boat fromJson(
        @JsonProperty("designId") String designId,
        @JsonProperty("modified") boolean modified,
        @JsonProperty("links") List<Link> links,
        @JsonProperty("identities") LinkedHashMap<String, Identity> identities,
        @JsonProperty("lastUpdated") Instant lastUpdated,
        @JsonProperty("id") String v1Id,
        @JsonProperty("sailNumber") String v1SailNumber,
        @JsonProperty("name") String v1Name,
        @JsonProperty("clubIds") List<String> v1ClubIds,
        @JsonProperty("clubId") String v1ClubId,
        @JsonProperty("certificates") List<Certificate> v1Certificates,
        @JsonProperty("sources") List<String> v1Sources)
    {
        if (identities == null || identities.isEmpty())
        {
            List<String> clubs = v1ClubIds != null && !v1ClubIds.isEmpty() ? v1ClubIds
                : v1ClubId != null && !v1ClubId.isBlank() ? List.of(v1ClubId) : List.of();
            return new Boat(v1Id, v1SailNumber, v1Name, designId, clubs, v1Certificates, v1Sources, links, lastUpdated, null)
                .withModified(modified);
        }
        return new Boat(null, designId, modified, links, identities, lastUpdated, null);
    }

    private static Map<String, Identity> singleIdentity(String id, Identity identity)
    {
        Map<String, Identity> m = new LinkedHashMap<>();
        m.put(id, identity);
        return m;
    }

    private static String lastKey(Map<String, Identity> identities)
    {
        String last = null;
        for (String k : identities.keySet())
            last = k;
        return last;
    }

    // --- Identities ---

    /** The ID of the boat's current identity: the last of its identities. */
    @JsonIgnore
    public String currentId()
    {
        return lastKey(identities);
    }

    /** The boat seen through its current identity. */
    public Boat current()
    {
        return withId(currentId());
    }

    /** The identity this record is seen through. */
    @JsonIgnore
    public Identity identity()
    {
        return identities.get(id);
    }

    /** True if {@code identityId} is one of this boat's identities. */
    public boolean hasIdentity(String identityId)
    {
        return identityId != null && identities.containsKey(identityId);
    }

    /** The same boat seen through another of its identities. */
    public Boat withId(String identityId)
    {
        return Objects.equals(identityId, id) ? this : new Boat(identityId, designId, modified, links, identities, lastUpdated, loadedAt);
    }

    /** This boat with the identity it is seen through replaced. */
    public Boat withIdentity(Identity identity)
    {
        return withIdentity(id, identity);
    }

    /**
     * This boat with identity {@code identityId} replaced, or added as the newest (current)
     * identity if it has none by that ID.
     */
    public Boat withIdentity(String identityId, Identity identity)
    {
        Map<String, Identity> m = new LinkedHashMap<>(identities);
        m.put(identityId, identity);
        return new Boat(id, designId, modified, links, m, lastUpdated, loadedAt);
    }

    /** This boat with the identity it is seen through changed by {@code change}. */
    public Boat updateIdentity(UnaryOperator<Identity> change)
    {
        return withIdentity(change.apply(identity()));
    }

    /**
     * This boat with all its identities replaced (in order, oldest first), seen through the
     * same identity if it still has it, else through the new current one.
     */
    public Boat withIdentities(Map<String, Identity> newIdentities)
    {
        String through = newIdentities.containsKey(id) ? id : null;
        return new Boat(through, designId, modified, links, newIdentities, lastUpdated, loadedAt);
    }

    /** Every identity changed by {@code change} (e.g. to filter certificates). */
    public Boat mapIdentities(UnaryOperator<Identity> change)
    {
        Map<String, Identity> m = new LinkedHashMap<>();
        identities.forEach((k, v) -> m.put(k, change.apply(v)));
        return new Boat(id, designId, modified, links, m, lastUpdated, loadedAt);
    }

    // --- The identity seen through ---

    /** The sail number of the identity this record is seen through. */
    @JsonIgnore
    public String sailNumber()
    {
        return identity().sailNumber();
    }

    /** The name of the identity this record is seen through. */
    @JsonIgnore
    public String name()
    {
        return identity().name();
    }

    // --- Across all identities ---

    /**
     * The clubs the boat raced at under any identity, the seen-through identity's first (its
     * primary club first of all).
     */
    @JsonIgnore
    public List<String> clubIds()
    {
        LinkedHashSet<String> all = new LinkedHashSet<>(identity().clubIds());
        identities.values().forEach(i -> all.addAll(i.clubIds()));
        return List.copyOf(all);
    }

    /** The certificates of all the boat's identities, the seen-through identity's first. */
    @JsonIgnore
    public List<Certificate> certificates()
    {
        List<Certificate> all = new ArrayList<>(identity().certificates());
        identities.forEach((k, v) ->
        {
            if (!k.equals(id))
                all.addAll(v.certificates());
        });
        return List.copyOf(all);
    }

    /** The importers that contributed to any of the boat's identities. */
    @JsonIgnore
    public List<String> sources()
    {
        LinkedHashSet<String> all = new LinkedHashSet<>(identity().sources());
        identities.values().forEach(i -> all.addAll(i.sources()));
        return List.copyOf(all);
    }

    /** Returns the primary club id (the seen-through identity's first), or null. */
    @JsonIgnore
    public String primaryClubId()
    {
        List<String> c = clubIds();
        return c.isEmpty() ? null : c.getFirst();
    }

    /** True if the boat raced at {@code clubId} under any identity. */
    public boolean hasClub(String clubId)
    {
        return clubId != null && identities.values().stream().anyMatch(i -> i.clubIds().contains(clubId));
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

    // --- Copies ---

    @Override
    public Boat withLoadedAt(Instant t)
    {
        return new Boat(id, designId, modified, links, identities, lastUpdated, t);
    }

    public Boat withLinks(List<Link> newLinks)
    {
        return new Boat(id, designId, modified, newLinks, identities, lastUpdated, loadedAt);
    }

    public Boat withDesignId(String newDesignId)
    {
        return new Boat(id, newDesignId, modified, links, identities, lastUpdated, loadedAt);
    }

    public Boat withModified(boolean newModified)
    {
        return new Boat(id, designId, newModified, links, identities, lastUpdated, loadedAt);
    }

    public Boat withLastUpdated(Instant t)
    {
        return new Boat(id, designId, modified, links, identities, t, loadedAt);
    }

    /** This boat with only the certificates {@code keep} accepts, in every identity. */
    public Boat withCertificatesFiltered(Predicate<Certificate> keep)
    {
        return mapIdentities(i -> i.withCertificates(i.certificates().stream().filter(keep).toList()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Boat b))
            return false;
        return Objects.equals(id, b.id) && Objects.equals(designId, b.designId) && modified == b.modified
            && Objects.equals(links, b.links) && Objects.equals(identities, b.identities)
            && Objects.equals(lastUpdated, b.lastUpdated);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, designId, modified, links, identities, lastUpdated);
    }

    @Override
    public String toString()
    {
        return "Boat{id='" + id + "', designId='" + designId + "'" + (modified ? ", modified" : "") + ", identities=" + identities
            + ", links=" + links + ", lastUpdated=" + lastUpdated + '}';
    }
}
