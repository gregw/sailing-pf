package org.mortbay.sailing.pf.importer;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mortbay.sailing.pf.data.Design;

/**
 * Normalisation and ID slug utilities for importers.
 * All methods are pure functions with no side effects.
 */
public class IdGenerator
{
    /**
     * Decorative suffixes appearing after the last {@code -} in a boat name that carry no
     * identity information and should be stripped during normalisation. The comparison is
     * done in lowercase-and-non-alnum-removed form, so the raw suffix can use any case and
     * any internal whitespace — {@code -GM}, {@code -gm}, {@code "- Under 17"}, and
     * {@code "-UNDER  17"} all strip cleanly.
     * <ul>
     *   <li>Grand-Master class markers: {@code l}, {@code m}, {@code gm}, {@code ggm}, {@code gggm}</li>
     *   <li>Youth division markers: {@code u16}, {@code u17}, ... , {@code under21}</li>
     * </ul>
     * Add new tokens here when a new convention appears in the data.
     */
    private static final Set<String> STANDARD_SUFFIXES = Set.of(
        "l", "m", "gm", "ggm", "gggm",
        "u16", "u17", "u18", "u19", "u20", "u21",
        "under16", "under17", "under18", "under19", "under20", "under21");

    /**
     * Pattern used to peel a trailing whitespace-separated numeral token off a cleaned
     * raw name. Roman numerals are accepted in either case. The whitespace before the
     * numeral is required so that "Tivoli" (ending in "li") and "Anna" (ending in "a")
     * are left intact — the numeral handling is for names like "Sticky II" / "Sticky 2",
     * not embedded letter runs that happen to look like Roman digits.
     */
    private static final Pattern TRAILING_NUMERAL = Pattern.compile(
        "^(.*?)\\s+(\\d+|[IVXLCDMivxlcdm]+)$");

    /**
     * Strip decorative suffixes from a raw boat name (see {@link #STANDARD_SUFFIXES}).
     * Returns the input unchanged when no suffix matches. Iterates so chains like
     * {@code "Boat - GM - U18"} collapse fully. Case and internal spacing of the surviving
     * portion are preserved so display names stay readable.
     * <p>
     * The suffix check is done in fully-normalised space (lowercase, non-alphanumerics
     * dropped) on the portion after the last {@code -}, so the raw form can carry any
     * mixture of case and internal whitespace ({@code "- Under 17"}, {@code "-UNDER 18"},
     * {@code " - gm"}) and still match.
     */
    public static String stripStandardSuffixes(String raw)
    {
        if (raw == null)
            return null;
        while (true)
        {
            int dash = raw.lastIndexOf('-');
            if (dash < 0)
                return raw;
            String afterDash = raw.substring(dash + 1)
                .toLowerCase(Locale.ENGLISH)
                .replaceAll("[^a-z0-9]", "");
            if (!STANDARD_SUFFIXES.contains(afterDash))
                return raw;
            raw = raw.substring(0, dash).stripTrailing();
        }
    }

    /**
     * Lowercase, strip ALL non-[a-z0-9] characters (including spaces and punctuation).
     * "Raging Bull" → "ragingbull"
     * <p>
     * Decorative suffixes ({@link #stripStandardSuffixes}) are removed first so that
     * "Foobar - GM" and "Foobar" both normalise to "foobar" and end up sharing a boat
     * ID. This is the single canonical normalisation used everywhere a name becomes
     * part of an identifier.
     */
    public static String normaliseName(String raw)
    {
        if (raw == null)
            return "";
        String cleaned = stripStandardSuffixes(raw);
        return cleaned.toLowerCase(Locale.ENGLISH).replaceAll("[^a-z0-9]", "");
    }

    /**
     * Lowercase, strip ALL non-[a-z0-9] characters (including spaces and punctuation).
     * Collapses common variants: "J/24", "J 24", "J24" all → "j24".
     */
    public static String normaliseDesignName(String raw)
    {
        if (raw == null)
            return "";
        return raw.toLowerCase(Locale.ENGLISH).replaceAll("[^a-z0-9]", "");
    }

    /**
     * Uppercase, strip ALL non-[A-Z0-9] characters.
     * "AUS-1234" → "AUS1234", "myc 7" → "MYC7"
     */
    public static String normaliseSailNumber(String raw)
    {
        if (raw == null)
            return "";
        return raw.toUpperCase(Locale.ENGLISH).replaceAll("[^A-Z0-9]", "");
    }

    /**
     * Generate a boat ID from sail number, name, and optional design.
     * <p>
     * Examples:
     * "AUS1234", "Raging Bull", null      → "AUS1234-ragingbull"
     * "AUS1234", "Raging Bull", j24design → "AUS1234-ragingbull-j24"
     */
    public static String generateBoatId(String rawSail, String rawName, Design design)
    {
        String normSail = normaliseSailNumber(rawSail);
        if (normSail.isEmpty())
            normSail = "nosail";
        String normName = normaliseName(rawName);
        String base = normSail + "-" + normName;
        return design == null ? base : base + "-" + design.id();
    }

    /**
     * Equivalence-class key for boat names that share the same sail number and design.
     * Two raw names with the same non-empty match key represent the same boat. Returns
     * an empty string when the input collapses to nothing (e.g. {@code null}, blank, or
     * "The") so callers can skip the match-key path.
     * <p>
     * Algorithm (operates on the raw form so word boundaries survive the lowercase
     * pass — embedded letter runs like "Tivoli" / "Pinta" are not mistaken for Roman
     * numerals because no whitespace precedes them):
     * <ol>
     *   <li>{@link #stripStandardSuffixes} (drops "- GM" etc.).</li>
     *   <li>Lowercase.</li>
     *   <li>If it starts with "the " (article + space), drop those four characters.</li>
     *   <li>Repeatedly trim a trailing whitespace-separated numeric or Roman-numeral
     *       token, so "Sticky 2", "Sticky II", and "Sticky II 2" all collapse to "sticky".</li>
     *   <li>Strip non-alphanumerics, matching {@link #normaliseName}.</li>
     * </ol>
     * <p>
     * Worked examples:
     * "Goat"               → "goat"
     * "The Goat"           → "goat"
     * "Sticky 2"           → "sticky"
     * "Sticky II"          → "sticky"
     * "The Sticky 2 - GM"  → "sticky"
     * "Tivoli"             → "tivoli"  (no whitespace before "li")
     * "Thelma"             → "thelma"  ("the" not followed by space)
     */
    public static String nameMatchKey(String raw)
    {
        if (raw == null || raw.isBlank())
            return "";
        String cleaned = stripStandardSuffixes(raw);
        String lower = cleaned.toLowerCase(Locale.ENGLISH);
        if (lower.startsWith("the "))
            lower = lower.substring(4);
        while (true)
        {
            String stripped = lower.replaceFirst("\\s+(\\d+|[ivxlcdm]+)\\s*$", "");
            if (stripped.equals(lower))
                break;
            lower = stripped;
        }
        return lower.replaceAll("[^a-z0-9]", "");
    }

    /**
     * Choose the canonical display name from a set of raw names that all share the same
     * {@link #nameMatchKey}. The selection rule:
     * <ol>
     *   <li>Each candidate is stripped of decorative suffixes ({@link #stripStandardSuffixes}).</li>
     *   <li>Each cleaned candidate is split into {@code (body, numeral)} where {@code numeral}
     *       is a trailing whitespace-separated Arabic or Roman number, or {@code null}.</li>
     *   <li>The candidate with the longest {@code body} wins as the body template;
     *       ties are broken in input order (so the caller's preferred ordering — typically
     *       "incoming first, stored second" — decides ties).</li>
     *   <li>An Arabic numeral observed anywhere in the group wins over a Roman one;
     *       no numeral observed anywhere → none appended.</li>
     * </ol>
     * <p>
     * Worked examples:
     * ["Goat", "The Goat"]                  → "The Goat"
     * ["Sticky", "Sticky II"]                → "Sticky II"
     * ["Sticky 2", "Sticky II"]              → "Sticky 2"   (Arabic preferred)
     * ["Foobar - GM", "Foobar"]              → "Foobar"
     * ["The Sticky 2 - GM", "Sticky"]        → "The Sticky 2"
     * ["Pompus - GGM"]                       → "Pompus"
     */
    public static String preferredDisplayName(Iterable<String> rawNames)
    {
        record Split(String body, String numeral) {}
        List<Split> splits = new ArrayList<>();
        for (String raw : rawNames)
        {
            if (raw == null)
                continue;
            String cleaned = stripStandardSuffixes(raw);
            if (cleaned == null || cleaned.isBlank())
                continue;
            Matcher m = TRAILING_NUMERAL.matcher(cleaned);
            if (m.matches())
                splits.add(new Split(m.group(1), m.group(2)));
            else
                splits.add(new Split(cleaned, null));
        }
        if (splits.isEmpty())
            return "";

        Split longest = splits.getFirst();
        for (Split s : splits)
        {
            if (s.body.length() > longest.body.length())
                longest = s;
        }

        String chosenNumeral = null;
        for (Split s : splits)
        {
            if (s.numeral != null && s.numeral.matches("\\d+"))
            {
                chosenNumeral = s.numeral;
                break;
            }
        }
        if (chosenNumeral == null)
        {
            for (Split s : splits)
            {
                if (s.numeral != null)
                {
                    chosenNumeral = s.numeral;
                    break;
                }
            }
        }
        return chosenNumeral == null ? longest.body : longest.body + " " + chosenNumeral;
    }

    /**
     * Lowercase, replace runs of non-alphanumeric characters with a single hyphen, trim
     * leading/trailing hyphens.
     * "Main Series 2018-19" → "main-series-2018-19"
     */
    public static String normaliseSeriesName(String raw)
    {
        if (raw == null)
            return "";
        return raw.toLowerCase(Locale.ENGLISH).replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
    }

    /**
     * Replaces '/' with '--' so a club ID can be used safely as a filename or directory name.
     * "rycv.com.au/ppnyc" → "rycv.com.au--ppnyc"
     * "myc.com.au"        → "myc.com.au"  (no-op)
     */
    public static String sanitizeIdForFilesystem(String id)
    {
        return id == null ? "" : id.replace("/", "--");
    }

    /**
     * Generate a series ID from the club ID and series name.
     * "myc.com.au", "Main Series 2018-19" → "myc.com.au/main-series-2018-19"
     */
    public static String generateSeriesId(String clubId, String seriesName)
    {
        return sanitizeIdForFilesystem(clubId) + "/" + normaliseSeriesName(seriesName);
    }

    /**
     * Generate a race ID from the club ID, date, and race number.
     * "myc.com.au", 2020-09-13, 1 → "myc.com.au-2020-09-13-0001"
     */
    public static String generateRaceId(String clubId, LocalDate date, int number)
    {
        return sanitizeIdForFilesystem(clubId) + "-" + date + String.format("-%04d", number);
    }

    private IdGenerator()
    {
    }
}
