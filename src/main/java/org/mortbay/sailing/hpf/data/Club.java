package org.mortbay.sailing.hpf.data;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A sailing club that organises races.
 * The ID is the club's website domain (e.g. "myc.com.au"), or domain/path for clubs on
 * shared platforms (e.g. "rycv.com.au/ppnyc"). Globally unique and independent of any
 * source system.
 * <p>
 * A club embeds its series directly. Season is not a separate structural level —
 * series names include the season where relevant (e.g. "Main Series 2018-19").
 * One file per club: {root}/clubs/{clubId}.json
 */
public record Club(
    String id,                  // website domain or domain/path, e.g. "myc.com.au" or "rycv.com.au/ppnyc"
    String shortName,           // e.g. "MYC"
    String longName,            // e.g. "Manly Yacht Club"
    String state,               // Australian state code, e.g. "NSW", "VIC"; null if unknown
    List<String> aliases,       // alternate short names or former domains
    List<String> topyachtUrls,  // TopYacht club index page URLs to scan for results
    List<Series> series,        // series run by this club
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Club>
{

    @Override
    public Club withLoadedAt(Instant t)
    {
        return new Club(id, shortName, longName, state, aliases, topyachtUrls, series, t);
    }

    // loadedAt is loading metadata, not domain data — exclude from equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Club c))
            return false;
        return Objects.equals(id, c.id) && Objects.equals(shortName, c.shortName)
            && Objects.equals(longName, c.longName) && Objects.equals(state, c.state)
            && Objects.equals(aliases, c.aliases) && Objects.equals(topyachtUrls, c.topyachtUrls)
            && Objects.equals(series, c.series);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, shortName, longName, state, aliases, topyachtUrls, series);
    }
}
