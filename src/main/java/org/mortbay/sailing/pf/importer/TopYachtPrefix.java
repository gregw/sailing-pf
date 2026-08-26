package org.mortbay.sailing.pf.importer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.mortbay.sailing.pf.data.TopYachtGroup;

/**
 * Derives a stable, human-recognisable prefix for a TopYacht results URL.
 * <p>
 * TopYacht publishes results under a path that carries the event code and the season in
 * some order, with several layouts in the wild:
 * <pre>
 *   .../results/2025/mirw/index.htm            → mirw
 *   .../results/cycsa/2020/club/index.htm      → cycsa-club
 *   .../results/plyc/lincolnweek/2024/         → plyc-lincolnweek
 *   .../results/fsc/2023/kb23/                 → fsc-kb
 *   .../results/2019/bwps_1920/index.htm       → bwps
 *   .../results/2022-23/                       → (no code — falls back to short name)
 * </pre>
 * The season must be stripped wherever it appears — as its own path segment, as a range,
 * or embedded in the code itself — or the same event yields a different prefix each year
 * and its races stop grouping together.
 * <p>
 * Prefixes only ever need to be unique <em>within</em> a club, since every ID they feed
 * already starts with the club ID.
 */
public final class TopYachtPrefix
{
    /**
     * Path segments that are site plumbing rather than event identity.
     */
    private static final Set<String> PATH_NOISE =
        Set.of("results", "files", "handicapper", "raceresults");

    private static final Pattern YEAR_SEGMENT = Pattern.compile("^(19|20)\\d{2}$");
    private static final Pattern YEAR_RANGE =
        Pattern.compile("^(19|20)?\\d{2}[-_](19|20)?\\d{2}$");

    /**
     * Season markers embedded inside an otherwise meaningful segment. Applied in order:
     * a range anywhere ({@code bwps_1920}, {@code 2020-2021_keel_boat}), a leading year
     * ({@code 2020_club_series}), a trailing year ({@code winter2020}), and finally a
     * trailing two-digit season ({@code kb23}, {@code bruny20}) — restricted to 15-30 so
     * class names like {@code j24} keep their number.
     */
    private static final Pattern[] INNER_SEASON = {
        Pattern.compile("[-_]?(19|20)\\d{2}[-_](19|20)?\\d{2}"),
        Pattern.compile("^(19|20)\\d{2}[-_]"),
        Pattern.compile("[-_]?(19|20)\\d{2}$"),
        Pattern.compile("(?<=[a-z])(1[5-9]|2\\d|30)$")
    };

    /**
     * Derives the prefix for one URL.
     *
     * @param url the TopYacht index URL
     * @param clubShortName used as the fallback when the path carries no event code at
     * all (some clubs host results on their own domain with only a
     * season in the path); may be null, in which case "club" is used
     */
    public static String derive(String url, String clubShortName)
    {
        List<String> parts = new ArrayList<>();
        for (String segment : pathSegments(url))
        {
            String s = segment.toLowerCase(Locale.ENGLISH);
            if (s.endsWith(".htm") || s.endsWith(".html"))
                continue;
            if (PATH_NOISE.contains(s))
                continue;
            if (YEAR_SEGMENT.matcher(s).matches() || YEAR_RANGE.matcher(s).matches())
                continue;
            for (Pattern p : INNER_SEASON)
            {
                s = p.matcher(s).replaceAll("");
            }
            s = slug(s);
            if (s.isEmpty() || PATH_NOISE.contains(s) || YEAR_SEGMENT.matcher(s).matches())
                continue;
            parts.add(s);
        }
        if (parts.isEmpty())
            return fallbackPrefix(clubShortName);
        return String.join("-", parts);
    }

    /**
     * Lowercase kebab-case form of the club short name, or "club" when there is none.
     */
    public static String fallbackPrefix(String clubShortName)
    {
        String slug = slug(clubShortName == null ? "" : clubShortName.toLowerCase(Locale.ENGLISH));
        return slug.isEmpty() ? "club" : slug;
    }

    /**
     * Groups a plain list of URLs into {@link TopYachtGroup}s by derived prefix, preserving
     * first-seen order both of the groups and of the URLs within each. Used to read the
     * legacy {@code topyacht:} list form in clubs.yaml, and to seed the admin editor.
     */
    public static List<TopYachtGroup> group(List<String> urls, String clubShortName)
    {
        Map<String, List<String>> byPrefix = new LinkedHashMap<>();
        if (urls != null)
        {
            for (String url : urls)
            {
                if (url == null || url.isBlank())
                    continue;
                String trimmed = url.trim();
                byPrefix.computeIfAbsent(derive(trimmed, clubShortName), k -> new ArrayList<>())
                    .add(trimmed);
            }
        }
        List<TopYachtGroup> groups = new ArrayList<>(byPrefix.size());
        byPrefix.forEach((prefix, groupUrls) -> groups.add(new TopYachtGroup(prefix, null, groupUrls)));
        return List.copyOf(groups);
    }

    /**
     * Normalises to lowercase kebab-case, collapsing runs of other characters to one dash.
     */
    public static String slug(String raw)
    {
        if (raw == null)
            return "";
        return raw.toLowerCase(Locale.ENGLISH)
            .replaceAll("[^a-z0-9]+", "-")
            .replaceAll("^-+|-+$", "");
    }

    /**
     * Path segments of a URL, excluding the scheme and host. Tolerates malformed input.
     */
    private static List<String> pathSegments(String url)
    {
        if (url == null)
            return List.of();
        String p = url.trim().replaceFirst("^[a-zA-Z][a-zA-Z0-9+.-]*://", "");
        int query = p.indexOf('?');
        if (query >= 0)
            p = p.substring(0, query);
        List<String> segments = new ArrayList<>();
        for (String s : p.split("/"))
        {
            if (!s.isEmpty())
                segments.add(s);
        }
        // First segment is the host
        return segments.isEmpty() ? List.of() : segments.subList(1, segments.size());
    }

    private TopYachtPrefix()
    {
    }
}
