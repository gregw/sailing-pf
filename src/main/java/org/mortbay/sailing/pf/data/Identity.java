package org.mortbay.sailing.pf.data;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * A boat under one sail number and name: one of the boat's identities. Its ID (the key in
 * {@link Boat#identities()}) is {@code sail-name-design}, unique across the database, and is
 * what a race finisher records as the boat it raced as.
 * <p>
 * {@code from}/{@code until} are optional, set by hand: by default an identity's period is the
 * span of its finishes, and dates never decide which boat a result goes to. A boat may race
 * under two identities at once.
 * <p>
 * {@code clubIds} are the clubs the boat raced at under this identity, first the primary one.
 * {@code clubsCurated} marks a list set by hand, which importers leave alone; an empty curated
 * list means no club. Certificates and sources are those issued to, or imported for, this
 * identity.
 */
@JsonPropertyOrder({"sailNumber", "name", "from", "until", "clubIds", "clubsCurated", "aliases", "certificates", "sources"})
public record Identity(
    String sailNumber,
    String name,
    @JsonInclude(JsonInclude.Include.NON_NULL) LocalDate from,
    @JsonInclude(JsonInclude.Include.NON_NULL) LocalDate until,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> clubIds,
    @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean clubsCurated,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<BoatAlias> aliases,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<Certificate> certificates,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> sources)
{
    public Identity
    {
        clubIds = Boat.normaliseClubIds(clubIds);
        aliases = aliases == null ? List.of() : List.copyOf(aliases);
        certificates = certificates == null ? List.of() : List.copyOf(certificates);
        sources = sources == null ? List.of() : List.copyOf(new LinkedHashSet<>(sources));
    }

    /** An identity with no dates, aliases or curated clubs. */
    public Identity(String sailNumber, String name, List<String> clubIds, List<Certificate> certificates, List<String> sources)
    {
        this(sailNumber, name, null, null, clubIds, false, List.of(), certificates, sources);
    }

    public Identity withSailNumberName(String newSailNumber, String newName)
    {
        return new Identity(newSailNumber, newName, from, until, clubIds, clubsCurated, aliases, certificates, sources);
    }

    public Identity withDates(LocalDate newFrom, LocalDate newUntil)
    {
        return new Identity(sailNumber, name, newFrom, newUntil, clubIds, clubsCurated, aliases, certificates, sources);
    }

    public Identity withClubIds(List<String> newClubIds)
    {
        return new Identity(sailNumber, name, from, until, newClubIds, clubsCurated, aliases, certificates, sources);
    }

    /** This identity with clubs set by hand: importers leave a curated list alone. */
    public Identity withCuratedClubIds(List<String> newClubIds)
    {
        return new Identity(sailNumber, name, from, until, newClubIds, true, aliases, certificates, sources);
    }

    /** This identity with its clubs no longer curated (importers may add to them again). */
    public Identity withUncuratedClubs()
    {
        return new Identity(sailNumber, name, from, until, clubIds, false, aliases, certificates, sources);
    }

    public Identity withAliases(List<BoatAlias> newAliases)
    {
        return new Identity(sailNumber, name, from, until, clubIds, clubsCurated, newAliases, certificates, sources);
    }

    public Identity withCertificates(List<Certificate> newCertificates)
    {
        return new Identity(sailNumber, name, from, until, clubIds, clubsCurated, aliases, newCertificates, sources);
    }

    public Identity withSources(List<String> newSources)
    {
        return new Identity(sailNumber, name, from, until, clubIds, clubsCurated, aliases, certificates, newSources);
    }

    /** This identity with {@code source} added to its sources, if not there already. */
    public Identity withSource(String source)
    {
        if (source == null || sources.contains(source))
            return this;
        List<String> s = new ArrayList<>(sources);
        s.add(source);
        return withSources(s);
    }

    /**
     * This identity with {@code alias} added, unless it already has it or it names this
     * identity itself.
     */
    public Identity withAlias(BoatAlias alias)
    {
        if (alias == null || aliases.stream().anyMatch(alias::sameAs)
            || alias.sameAs(new BoatAlias(sailNumber, name)))
            return this;
        List<BoatAlias> a = new ArrayList<>(aliases);
        a.add(alias);
        return withAliases(a);
    }

    /**
     * This identity with the club added if it raced there, unless its clubs are curated (set
     * by hand). The first club added becomes its primary club.
     */
    public Identity withRacedAtClub(String clubId)
    {
        if (clubId == null || clubId.isBlank() || clubsCurated || clubIds.contains(clubId))
            return this;
        List<String> c = new ArrayList<>(clubIds);
        c.add(clubId);
        return withClubIds(c);
    }

    /**
     * True if an entry with this normalised sail number and name is this identity: its own
     * sail number and name, a full alias, or a pairing of its sail numbers (its own and
     * sail-number-only aliases) with its names (its own and name-only aliases).
     */
    public boolean matches(String normSailNumber, String normName)
    {
        if (normSailNumber == null || normName == null)
            return false;
        java.util.Set<String> sails = new java.util.HashSet<>();
        java.util.Set<String> names = new java.util.HashSet<>();
        sails.add(org.mortbay.sailing.pf.importer.IdGenerator.normaliseSailNumber(sailNumber));
        names.add(org.mortbay.sailing.pf.importer.IdGenerator.normaliseName(name));
        for (BoatAlias a : aliases)
        {
            if (a.sailNumber() != null && a.name() != null)
            {
                if (normSailNumber.equalsIgnoreCase(a.normSailNumber()) && normName.equals(a.normName()))
                    return true;
            }
            else if (a.sailNumber() != null)
                sails.add(a.normSailNumber());
            else
                names.add(a.normName());
        }
        return sails.stream().anyMatch(normSailNumber::equalsIgnoreCase) && names.contains(normName);
    }
}
