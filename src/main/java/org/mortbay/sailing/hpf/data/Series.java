package org.mortbay.sailing.hpf.data;

import java.util.List;

/**
 * A named series of races at a club within a season.
 * The ID follows the pattern: clubDomain/normalised-series-name,
 * e.g. "myc.com.au/wednesday-twilight".
 * Season is encoded in the series name itself (e.g. "Main Series 2018-19"),
 * not as a separate structural level.
 * <p>
 * Each club has a catch-all series (isCatchAll = true) for standalone races that
 * do not belong to any real series. Every Race belongs to at least one Series.
 * <p>
 * raceIds is the downward link (series → [raceId...]); Race.seriesIds is the upward
 * link. Both must be kept in sync by importers.
 */
public record Series(
    String id,          // e.g. "myc.com.au/wednesday-twilight"
    String name,        // human-readable name, e.g. "Wednesday Twilight"
    boolean isCatchAll, // true for the pseudo-series holding standalone races
    List<String> raceIds // IDs of races belonging to this series
) {
    @Override
    public String toString()
    {
        return "Series{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", isCatchAll=" + isCatchAll +
            ", raceIds=" + raceIds +
            '}';
    }
}
