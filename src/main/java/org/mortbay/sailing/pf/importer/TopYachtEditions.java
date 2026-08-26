package org.mortbay.sailing.pf.importer;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Splits a merged TopYacht event into editions — the separate occasions it has been run.
 * <p>
 * An event whose divisions are merged (see {@link org.mortbay.sailing.pf.data.TopYachtGroup})
 * is normally an annual regatta, and its index URLs cover several years. Collapsing all of
 * them into one series would put 2022, 2023 and 2025 racing in a single "series" spanning
 * years, which is not what a series means anywhere else in the database.
 * <p>
 * Editions are maximal clusters of race dates in which consecutive dates are no more than
 * {@link #MAX_GAP_MONTHS} apart, so a regatta running over a week stays one edition (even
 * across New Year) while next year's running starts a new one. Each edition is labelled by
 * the year of its earliest race, giving series IDs like {@code tyc.com.au/mirw-2025}.
 * <p>
 * Race IDs deliberately do <em>not</em> include the edition — they are keyed on the event
 * prefix and the date ({@code tyc.com.au-mirw-2025-08-29-0001}), which is already unique and
 * stays stable if an edition boundary ever shifts as more races arrive.
 */
public final class TopYachtEditions
{
    /**
     * Races further apart than this belong to different editions of the event.
     */
    public static final int MAX_GAP_MONTHS = 6;

    /**
     * Matches the slug of an edition series, e.g. {@code mirw-2025} for prefix {@code mirw}.
     */
    public static Pattern editionSlugPattern(String prefix)
    {
        return Pattern.compile("^" + Pattern.quote(prefix) + "-\\d{4}$");
    }

    /**
     * True if {@code seriesId} is an edition series of this event (rather than a division series).
     */
    public static boolean isEditionSeriesId(String seriesId, String clubId, String prefix)
    {
        if (seriesId == null)
            return false;
        String expectedClub = IdGenerator.sanitizeIdForFilesystem(clubId);
        int slash = seriesId.indexOf('/');
        if (slash < 0 || !seriesId.substring(0, slash).equals(expectedClub))
            return false;
        return editionSlugPattern(prefix).matcher(seriesId.substring(slash + 1)).matches();
    }

    /**
     * The series ID for one edition, e.g. {@code tyc.com.au/mirw-2025}.
     */
    public static String editionSeriesId(String clubId, String prefix, String label)
    {
        return IdGenerator.sanitizeIdForFilesystem(clubId) + "/" + prefix + "-" + label;
    }

    /**
     * The label for an edition, taken from the year of its earliest race.
     */
    public static String label(LocalDate earliest)
    {
        return String.valueOf(earliest.getYear());
    }

    /**
     * Display name for an edition series: the event name qualified by the edition's year,
     * e.g. "Magnetic Island Race Week 2025", so the three runnings of a regatta are
     * distinguishable in a series list.
     */
    public static String editionName(String eventName, String editionSeriesId)
    {
        String slug = IdGenerator.seriesSlug(editionSeriesId);
        int dash = slug.lastIndexOf('-');
        String year = dash >= 0 ? slug.substring(dash + 1) : "";
        return year.isEmpty() ? eventName : eventName + " " + year;
    }

    /**
     * True if the two dates are close enough to belong to the same edition.
     */
    public static boolean sameEdition(LocalDate a, LocalDate b)
    {
        if (a == null || b == null)
            return false;
        LocalDate earlier = a.isBefore(b) ? a : b;
        LocalDate later = a.isBefore(b) ? b : a;
        return !later.isAfter(earlier.plusMonths(MAX_GAP_MONTHS));
    }

    /**
     * Groups dates into editions, earliest first, each cluster sorted ascending. Consecutive
     * dates more than {@link #MAX_GAP_MONTHS} apart start a new cluster. Duplicate dates are
     * preserved — callers cluster race dates, and several divisions race on the same day.
     */
    public static List<List<LocalDate>> cluster(Collection<LocalDate> dates)
    {
        List<LocalDate> sorted = new ArrayList<>();
        for (LocalDate d : dates)
        {
            if (d != null)
                sorted.add(d);
        }
        sorted.sort(java.util.Comparator.naturalOrder());

        List<List<LocalDate>> clusters = new ArrayList<>();
        List<LocalDate> current = null;
        LocalDate previous = null;
        for (LocalDate d : sorted)
        {
            if (current == null || !sameEdition(previous, d))
            {
                current = new ArrayList<>();
                clusters.add(current);
            }
            current.add(d);
            previous = d;
        }
        return clusters;
    }

    private TopYachtEditions()
    {
    }
}
