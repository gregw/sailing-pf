package org.mortbay.sailing.pf.data;

import java.util.List;

/**
 * One TopYacht results "event" for a club: a short {@code prefix}, an optional human
 * {@code name}, the index URLs (usually one per season) that publish it, and whether its
 * series are divisions of a single event rather than events in their own right.
 * <p>
 * The prefix is what separates two events run by the same club — a regatta and the club's
 * own season can both hold "Race 1" on the same date, which would otherwise collide in the
 * generated series and race IDs. It becomes part of every series ID produced from these
 * URLs (see {@link org.mortbay.sailing.pf.importer.IdGenerator#generateSeriesId}), so it
 * must stay stable once races have been imported.
 * <p>
 * Sourced from {@code clubs.yaml}. A club whose {@code topyacht:} entry is still a plain
 * list of URLs is grouped automatically by
 * {@link org.mortbay.sailing.pf.importer.TopYachtPrefix#group}.
 *
 * @param prefix stable short identifier, lowercase kebab-case, e.g. {@code "mirw"}
 * @param name human-readable event name, e.g. {@code "Magnetic Island Race Week"}; may be null
 * @param urls TopYacht index page URLs, one per season
 * @param mergeDivisions true when this event's TopYacht series are divisions of one event
 * (a regatta publishing Spinnaker Div 1, Div 2, … separately) and
 * should collapse into a single series whose races carry one division
 * per contributing series. Leave false for a club's own season, whose
 * series (Saturday, Twilight, Passage) are genuinely distinct.
 */
public record TopYachtGroup(String prefix, String name, List<String> urls, boolean mergeDivisions)
{
    public TopYachtGroup
    {
        urls = urls == null ? List.of() : List.copyOf(urls);
        if (name != null && name.isBlank())
            name = null;
    }

    /**
     * Convenience for the common non-merging case.
     */
    public TopYachtGroup(String prefix, String name, List<String> urls)
    {
        this(prefix, name, urls, false);
    }

    /**
     * The name if set, otherwise the prefix — for display where something must be shown.
     */
    public String displayName()
    {
        return name != null ? name : prefix;
    }
}
