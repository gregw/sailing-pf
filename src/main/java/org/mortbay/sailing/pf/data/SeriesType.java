package org.mortbay.sailing.pf.data;

import java.util.Locale;

/**
 * The handicap variant a series is sailed under. Set per series in clubs.yaml
 * ({@code seriesTypes:}); a series with no entry takes its type from its name
 * ({@link #fromName}).
 * <p>
 * {@link #SPIN} and {@link #NON_SPIN} override the per-finisher spinnaker flag of every race
 * in the series — sources such as SailSys flag entries from the boat's certificate, not the
 * race rules. {@link #TWO_HANDED} makes spinnaker entries two-handed. {@link #MIXED} and
 * {@link #UNKNOWN} leave the source's per-finisher flags alone. See {@link EntryVariant#of}.
 */
public enum SeriesType
{
    SPIN("spin"),
    NON_SPIN("ns"),
    TWO_HANDED("2h"),
    MIXED("mixed"),
    UNKNOWN("unknown");

    private final String code;

    SeriesType(String code)
    {
        this.code = code;
    }

    /** The value written to clubs.yaml and used by the REST API. */
    public String code()
    {
        return code;
    }

    /** Parses a {@link #code()} (or enum name), case-insensitively; null if unrecognised. */
    public static SeriesType parse(String value)
    {
        if (value == null)
            return null;
        String v = value.trim().toLowerCase(Locale.ENGLISH);
        for (SeriesType t : values())
        {
            if (t.code.equals(v) || t.name().toLowerCase(Locale.ENGLISH).equals(v))
                return t;
        }
        return null;
    }

    /** The default type for a series with no clubs.yaml entry, derived from its name. */
    public static SeriesType fromName(String seriesName)
    {
        if (containsNonSpinKeyword(seriesName))
            return NON_SPIN;
        return containsTwoHandedKeyword(seriesName) ? TWO_HANDED : UNKNOWN;
    }

    /** True if the text (a series or division name) names two-handed racing. */
    public static boolean containsTwoHandedKeyword(String text)
    {
        if (text == null)
            return false;
        String t = text.toLowerCase(Locale.ENGLISH);
        return t.contains("2hd") || t.contains("two-handed") || t.contains("two handed")
            || t.contains("twohanded") || t.contains("2-handed") || t.contains("2 handed")
            || t.contains("double-handed") || t.contains("double handed")
            || t.contains("shorthanded") || t.contains("short-handed") || t.contains("short handed");
    }

    /** True if the text (a series or division name) names non-spinnaker racing. */
    public static boolean containsNonSpinKeyword(String text)
    {
        if (text == null)
            return false;
        String t = text.toLowerCase(Locale.ENGLISH);
        return t.contains("non-spinnaker") || t.contains("non spinnaker")
            || t.contains("nonspinnaker") || t.contains("non-spin") || t.contains("non spin");
    }
}
