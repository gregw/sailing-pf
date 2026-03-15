package org.mortbay.sailing.hpf.data;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;


import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A single race. Raw layer — immutable, no derived fields.
 * <p>
 * The ID is a generated slug: clubDomain-isoDate-hex, e.g. "myc.com.au-2024-11-06-4a1f".
 * A race may belong to more than one series; seriesIds holds all of them.
 * <p>
 * handicapSystem is the primary system under which results were scored.
 * offsetPursuit is true for pursuit-format races.
 */
public record Race(
    String id,                       // e.g. "myc.com.au-2024-11-06-4a1f"
    String clubId,                   // organising club website domain
    List<String> seriesIds,          // series this race contributes to (at least one)
    LocalDate date,
    int number,                      // race number within its primary series
    String name,                     // named race title, e.g. "Flinders Race"; null if unnamed
    String handicapSystem,           // primary scoring system, e.g. "PHS", "IRC", "ORC", "AMS"
    boolean offsetPursuit,           // true if pursuit-format race
    List<Division> divisions,
    @JsonIgnore Instant loadedAt     // file modification time at load; not persisted
) implements Loadable<Race>
{

    @Override
    public Race withLoadedAt(Instant t)
    {
        return new Race(id, clubId, seriesIds, date, number, name, handicapSystem, offsetPursuit, divisions, t);
    }

    // loadedAt is loading metadata, not domain data — exclude from equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Race r))
            return false;
        return number == r.number && offsetPursuit == r.offsetPursuit
            && Objects.equals(id, r.id) && Objects.equals(clubId, r.clubId)
            && Objects.equals(seriesIds, r.seriesIds) && Objects.equals(date, r.date)
            && Objects.equals(name, r.name) && Objects.equals(handicapSystem, r.handicapSystem)
            && Objects.equals(divisions, r.divisions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, clubId, seriesIds, date, number, name, handicapSystem, offsetPursuit, divisions);
    }
}
