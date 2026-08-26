package org.mortbay.sailing.pf.data;

/**
 * One opt-in SailSys results source for a club: either a whole club whose current series are
 * discovered on every run, or one specific series.
 * <p>
 * SailSys races used to be found by walking global race IDs from 1 upwards, which fetched the
 * entire platform to reach the few clubs we care about. Clubs now opt in instead, and only
 * what they point at is fetched.
 * <p>
 * The two kinds differ in reach:
 * <ul>
 *   <li>{@code clubId} — {@code clubs/{id}/profile} lists the club's series, so new seasons
 *       are picked up with no config change. That endpoint returns only <em>current</em>
 *       series, so this kind never reaches past seasons.</li>
 *   <li>{@code seriesId} — one series, named explicitly. This is how history is added.</li>
 * </ul>
 * Exactly one of the two is set. Sourced from the {@code sailsys:} block in clubs.yaml.
 *
 * @param key      stable map key in clubs.yaml, e.g. {@code "club-41"} or {@code "series-3386"}
 * @param name     human-readable label; may be null
 * @param clubId   SailSys club ID to discover series from, or null
 * @param seriesId SailSys series ID to import, or null
 */
public record SailSysEvent(String key, String name, Integer clubId, Integer seriesId)
{
    public SailSysEvent
    {
        if (name != null && name.isBlank())
            name = null;
    }

    /** A club-kind event discovers its series on every run. */
    public static SailSysEvent ofClub(int clubId, String name)
    {
        return new SailSysEvent("club-" + clubId, name, clubId, null);
    }

    /** A series-kind event names one series explicitly, including a past season. */
    public static SailSysEvent ofSeries(int seriesId, String name)
    {
        return new SailSysEvent("series-" + seriesId, name, null, seriesId);
    }

    /** True when this entry discovers its series from a club profile. */
    public boolean isClub()
    {
        return clubId != null;
    }

    /** True when this entry names one series directly. */
    public boolean isSeries()
    {
        return seriesId != null;
    }

    /** Usable only if it identifies something to fetch. */
    public boolean isValid()
    {
        return isClub() || isSeries();
    }

    /** The name if set, otherwise the key — for display where something must be shown. */
    public String displayName()
    {
        return name != null ? name : key;
    }
}
