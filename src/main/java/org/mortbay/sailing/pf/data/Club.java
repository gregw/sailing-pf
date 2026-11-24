package org.mortbay.sailing.pf.data;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Objects;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

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
@JsonPropertyOrder({"id", "shortName", "longName", "state", "excluded", "email", "aliases",
    "topyachtGroups", "sailsysEvents", "seriesTypes", "series"})
public record Club(
    String id,                  // website domain or domain/path, e.g. "myc.com.au" or "rycv.com.au/ppnyc"
    String shortName,           // e.g. "MYC"
    @JsonInclude(JsonInclude.Include.NON_NULL) String longName,
    @JsonInclude(JsonInclude.Include.NON_NULL) String state,
    @JsonInclude(JsonInclude.Include.NON_DEFAULT) boolean excluded, // left out of the analysis
    @JsonInclude(JsonInclude.Include.NON_NULL) String email,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> aliases,       // other names results give it
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<TopYachtGroup> topyachtGroups, // its TopYacht results feeds
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<SailSysEvent> sailsysEvents,   // its SailSys results feeds
    List<Series> series,        // series run by this club
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String, SeriesType> seriesTypes, // series ID -> its spinnaker type, set by hand
    @JsonIgnore Instant loadedAt  // file modification time at load; not persisted
) implements Loadable<Club>
{
    public Club
    {
        aliases = aliases == null ? List.of() : aliases;
        topyachtGroups = topyachtGroups == null ? List.of() : topyachtGroups;
        sailsysEvents = sailsysEvents == null ? List.of() : sailsysEvents;
        series = series == null ? List.of() : series;
        seriesTypes = seriesTypes == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(seriesTypes));
    }

    /** A club with no series types set. */
    public Club(String id, String shortName, String longName, String state, boolean excluded, String email,
                List<String> aliases, List<TopYachtGroup> topyachtGroups, List<SailSysEvent> sailsysEvents,
                List<Series> series, Instant loadedAt)
    {
        this(id, shortName, longName, state, excluded, email, aliases, topyachtGroups, sailsysEvents, series,
            Map.of(), loadedAt);
    }

    @Override
    public Club withLoadedAt(Instant t)
    {
        return new Club(id, shortName, longName, state, excluded, email, aliases, topyachtGroups,
            sailsysEvents, series, seriesTypes, t);
    }

    public Club withShortName(String name)
    {
        return new Club(id, name, longName, state, excluded, email, aliases, topyachtGroups, sailsysEvents, series, seriesTypes, loadedAt);
    }

    /** This club with its long name, state and email replaced. */
    public Club withMeta(String newLongName, String newState, String newEmail)
    {
        return new Club(id, shortName, newLongName, newState, excluded, newEmail, aliases, topyachtGroups, sailsysEvents, series, seriesTypes, loadedAt);
    }

    public Club withExcluded(boolean flag)
    {
        return new Club(id, shortName, longName, state, flag, email, aliases, topyachtGroups, sailsysEvents, series, seriesTypes, loadedAt);
    }

    public Club withAliases(List<String> newAliases)
    {
        return new Club(id, shortName, longName, state, excluded, email, newAliases, topyachtGroups, sailsysEvents, series, seriesTypes, loadedAt);
    }

    public Club withTopyachtGroups(List<TopYachtGroup> groups)
    {
        return new Club(id, shortName, longName, state, excluded, email, aliases, groups, sailsysEvents, series, seriesTypes, loadedAt);
    }

    public Club withSailsysEvents(List<SailSysEvent> events)
    {
        return new Club(id, shortName, longName, state, excluded, email, aliases, topyachtGroups, events, series, seriesTypes, loadedAt);
    }

    public Club withSeries(List<Series> newSeries)
    {
        return new Club(id, shortName, longName, state, excluded, email, aliases, topyachtGroups, sailsysEvents, newSeries, seriesTypes, loadedAt);
    }

    public Club withSeriesTypes(Map<String, SeriesType> types)
    {
        return new Club(id, shortName, longName, state, excluded, email, aliases, topyachtGroups, sailsysEvents, series, types, loadedAt);
    }

    /**
     * Whether this club has a results feed that is still live in {@code year}: a SailSys club
     * entry (which always lists the club's current series), a TopYacht URL naming the year
     * (e.g. {@code /2026/}, {@code 2025-2026} or {@code 2025-26}), or a race that year imported
     * from a source the club still has configured — {@code sailsysRacedInYear} counts only with a
     * SailSys entry, {@code topyachtRacedInYear} only with a TopYacht URL (so races left by a
     * since-removed feed, or the old SailSys platform scan, do not vouch for another feed).
     * Without one, the club's data is historic only.
     */
    public boolean hasCurrentFeed(int year, boolean sailsysRacedInYear, boolean topyachtRacedInYear)
    {
        boolean hasSailsys = sailsysEvents != null && !sailsysEvents.isEmpty();
        boolean hasTopyacht = topyachtGroups != null && topyachtGroups.stream().anyMatch(g -> !g.urls().isEmpty());
        if (!hasSailsys && !hasTopyacht)
            return false;
        if ((hasSailsys && sailsysRacedInYear) || (hasTopyacht && topyachtRacedInYear))
            return true;
        if (sailsysEvents != null && sailsysEvents.stream().anyMatch(e -> e.clubId() != null))
            return true;
        String shortSeason = (year - 1) + "-" + String.format("%02d", year % 100);
        Pattern yearToken = Pattern.compile("(?<!\\d)(" + year + "|" + shortSeason + ")(?!\\d)");
        return topyachtGroups != null && topyachtGroups.stream()
            .flatMap(g -> g.urls().stream())
            .anyMatch(u -> yearToken.matcher(u).find());
    }

    //mode/ loadedAt is loading metadata, not domain data — exclude from equality
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Club c))
            return false;
        return Objects.equals(id, c.id) && Objects.equals(shortName, c.shortName)
            && Objects.equals(longName, c.longName) && Objects.equals(state, c.state)
            && excluded == c.excluded && Objects.equals(email, c.email)
            && Objects.equals(aliases, c.aliases) && Objects.equals(topyachtGroups, c.topyachtGroups)
            && Objects.equals(sailsysEvents, c.sailsysEvents)
            && Objects.equals(series, c.series) && Objects.equals(seriesTypes, c.seriesTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, shortName, longName, state, excluded, email, aliases, topyachtGroups,
            sailsysEvents, series, seriesTypes);
    }

    @Override
    public String toString()
    {
        return "Club{" +
            "id='" + id + '\'' +
            ", shortName='" + shortName + '\'' +
            ", longName='" + longName + '\'' +
            ", state='" + state + '\'' +
            ", excluded=" + excluded +
            ", email='" + email + '\'' +
            ", aliases=" + aliases +
            ", topyachtGroups=" + topyachtGroups +
            ", sailsysEvents=" + sailsysEvents +
            ", series=" + series +
            ", seriesTypes=" + seriesTypes +
            '}';
    }
}
