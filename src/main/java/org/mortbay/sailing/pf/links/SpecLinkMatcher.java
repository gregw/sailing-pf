package org.mortbay.sailing.pf.links;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.links.WikipediaDesignHarvest.Article;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mortbay.sailing.pf.links.DesignLinkMatcher.csv;
import static org.mortbay.sailing.pf.links.DesignLinkMatcher.isDistinctive;
import static org.mortbay.sailing.pf.links.DesignLinkMatcher.key;

/**
 * Matches designs directly to spec-site pages (sailboatdata.com and boat-specs.com) by the
 * names in the pages' URLs, so designs with no Wikipedia article can still get spec links.
 * Neither site is fetched: their terms forbid harvesting. The pages are the spec links found in
 * the Wikipedia harvest ({@link WikipediaDesignHarvest}), plus the URLs in any files given (for
 * example web search results, or a sitemap the site owner has agreed we may use).
 * <p>
 * A page matches a design when one of the page's name keys equals one of the design's keys
 * ({@link DesignLinkMatcher#designKeys}). The kinds of match, strongest first:
 * <ol>
 *   <li>{@code exact} — the page's whole name, e.g. "hunter-30", or with a trailing builder
 *       moved to the front ("first-407-beneteau" → "beneteau first 40.7"); for boat-specs.com
 *       the model, with or without the builder, e.g. "grand-surprise" or "archambault" +
 *       "grand-surprise"</li>
 *   <li>{@code stripped} — the page's name less leading builder words or a trailing one
 *       ("beneteau-first-40" → "first-40", "surprise-25-archambault" → "surprise-25"), or the
 *       design's name less one or two leading words ("J Boats J/70" → "J/70"), but not
 *       both; only builder-like words (no numbers) are
 *       stripped, and what is left must still have a number in it</li>
 *   <li>{@code fuzzy} — one letter different, with the same numbers ("grand-surpise")</li>
 * </ol>
 * For each site a design gets the pages at its strongest kind. A single exact page on every site
 * matched, with a distinctive name, is proposed for approval ({@code approve=y}); anything else
 * is left for review.
 * <p>
 * Designs whose approved row in {@code design-link-reviewed.csv} (else
 * {@code design-link-candidates.csv}) already has a link to a site are not matched to that site
 * again. Writes {@code <pf-data>/cache/links/design-speclink-candidates.csv}, in the format
 * {@link ApplyDesignLinks} reads: apply it with {@code --source=speclinks} so that its links are
 * kept apart from the Wikipedia ones.
 * <p>
 * Reads only: nothing in the data store is changed.
 * <p>
 * Usage: {@code SpecLinkMatcher [pf-data] [url-file ...]}
 */
public class SpecLinkMatcher
{
    private static final Logger LOG = LoggerFactory.getLogger(SpecLinkMatcher.class);

    enum Kind
    {
        exact, stripped, fuzzy
    }

    enum Site
    {
        sailboatdata("https://sailboatdata.com/sailboat/"),
        boatspecs("https://www.boat-specs.com/sailing/sailboats/");

        final String prefix;

        Site(String prefix)
        {
            this.prefix = prefix;
        }
    }

    /** A spec page: its site, canonical URL and the words of its name. */
    record Page(Site site, String url, List<String> words, int builderWords)
    {
    }

    record Match(Page page, Kind kind)
    {
    }

    /** The matches for one design: for each site, its best pages (all of one kind). */
    record Proposal(Design design, int boats, boolean excluded, Map<Site, List<Match>> bySite)
    {
        boolean approvable()
        {
            return bySite.values().stream().allMatch(ms -> ms.size() == 1 && ms.getFirst().kind() == Kind.exact
                && isApprovableName(String.join("", ms.getFirst().page().words())));
        }

        List<String> links()
        {
            List<String> links = new ArrayList<>();
            bySite.values().forEach(ms -> ms.forEach(m -> links.add(m.page().url())));
            return links;
        }
    }

    private static final Pattern URL = Pattern.compile("https?://[^\\s<>\"']+");
    private static final Pattern SAILBOATDATA = Pattern.compile("^https?://(?:www\\.)?sailboatdata\\.com/sailboat/([^/?#]+)/?$");
    private static final Pattern BOAT_SPECS = Pattern.compile("^https?://(?:www\\.)?boat-specs\\.com/sailing/sailboats/([^/?#]+)/([^/?#]+)/?$");
    private static final Pattern DIGITS = Pattern.compile("\\d+");

    private static final ObjectMapper JSON = JsonMapper.builder()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    private final Map<String, List<Match>> byKey = new HashMap<>();
    private final int pages;

    public SpecLinkMatcher(Iterable<String> urls)
    {
        Map<String, Page> unique = new LinkedHashMap<>();
        for (String url : urls)
        {
            Page p = page(url);
            if (p != null)
                unique.putIfAbsent(p.url(), p);
        }
        pages = unique.size();
        for (Page p : unique.values())
            for (Map.Entry<String, Kind> e : pageKeys(p).entrySet())
                byKey.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(new Match(p, e.getValue()));
    }

    public static void main(String[] args) throws Exception
    {
        Path root = DataStore.resolveDataRoot(args);
        Path dir = WikipediaDesignHarvest.outputFile(root).getParent();

        List<String> urls = new ArrayList<>();
        List<Article> articles = JSON.readValue(WikipediaDesignHarvest.outputFile(root).toFile(),
            new TypeReference<List<Article>>() {});
        articles.forEach(a -> urls.addAll(a.extlinks()));
        for (int i = 1; i < args.length; i++)
        {
            Matcher m = URL.matcher(Files.readString(Path.of(args[i]), StandardCharsets.UTF_8));
            while (m.find())
                urls.add(m.group());
        }

        List<Design> designs = DesignLinkMatcher.readDesigns(root.resolve("imported").resolve("designs"));
        Map<String, Integer> boats = DesignLinkMatcher.boatCounts(root.resolve("imported").resolve("boats"));
        Set<String> excluded = DesignLinkMatcher.excludedDesigns(root.resolve("config").resolve("design.yaml"));
        Path reviewed = dir.resolve("design-link-reviewed.csv");
        if (!Files.exists(reviewed))
            reviewed = dir.resolve("design-link-candidates.csv");
        Map<String, Set<Site>> covered = covered(reviewed);

        SpecLinkMatcher matcher = new SpecLinkMatcher(urls);
        LOG.info("{} spec pages, {} designs, {} already linked by {}", matcher.pages, designs.size(), covered.size(),
            reviewed.getFileName());
        List<Proposal> proposals = new ArrayList<>();
        for (Design d : designs)
        {
            Proposal p = matcher.propose(d, boats.getOrDefault(d.id(), 0), excluded.contains(d.id()),
                covered.getOrDefault(d.id(), Set.of()));
            if (p != null)
                proposals.add(p);
        }
        proposals.sort(Comparator.comparingInt(Proposal::boats).reversed().thenComparing(p -> p.design().id()));

        Path out = dir.resolve("design-speclink-candidates.csv");
        write(out, proposals);
        long approvable = proposals.stream().filter(Proposal::approvable).count();
        LOG.info("Wrote {}: {} designs matched ({} proposed for approval, {} to review)", out, proposals.size(),
            approvable, proposals.size() - approvable);
    }

    /** The page a spec-site URL is for, with a canonical URL, or null if it is not one. */
    static Page page(String url)
    {
        Matcher m = SAILBOATDATA.matcher(url.trim());
        if (m.find())
        {
            String slug = m.group(1).toLowerCase();
            return new Page(Site.sailboatdata, Site.sailboatdata.prefix + slug, words(slug), 0);
        }
        m = BOAT_SPECS.matcher(url.trim());
        if (m.find())
        {
            String builder = m.group(1).toLowerCase();
            String model = m.group(2).toLowerCase();
            List<String> builderWords = words(builder);
            List<String> words = new ArrayList<>(builderWords);
            words.addAll(words(model));
            return new Page(Site.boatspecs, Site.boatspecs.prefix + builder + "/" + model, List.copyOf(words),
                builderWords.size());
        }
        return null;
    }

    private static List<String> words(String slug)
    {
        return Arrays.stream(slug.split("[-_]+")).map(DesignLinkMatcher::key).filter(w -> !w.isEmpty()).toList();
    }

    /** The page's name keys, each with its strongest kind. */
    static Map<String, Kind> pageKeys(Page p)
    {
        Map<String, Kind> keys = new LinkedHashMap<>();
        List<String> w = p.words();
        put(keys, String.join("", w), Kind.exact);
        if (p.builderWords() > 0)
            put(keys, String.join("", w.subList(p.builderWords(), w.size())), Kind.exact);
        // "first-407-beneteau" is "Beneteau First 40.7"
        if (w.size() > 1 && !hasDigit(w.getLast()))
            put(keys, w.getLast() + String.join("", w.subList(0, w.size() - 1)), Kind.exact);
        // Only builder-like words are stripped: "columbia-24-contender" is not a "contender"
        for (int n = 1; n <= 2 && n < w.size() && !hasDigit(w.get(n - 1)); n++)
            putStripped(keys, String.join("", w.subList(n, w.size())));
        if (w.size() > 1 && !hasDigit(w.getLast()))
            putStripped(keys, String.join("", w.subList(0, w.size() - 1)));
        return keys;
    }

    /**
     * The proposal for a design, or null if no page matches it on a site it is not already
     * linked to.
     */
    Proposal propose(Design design, int boats, boolean excluded, Set<Site> covered)
    {
        Set<String> designKeys = DesignLinkMatcher.designKeys(design);
        Map<String, Kind> keys = new LinkedHashMap<>();
        designKeys.forEach(k -> put(keys, k, Kind.exact));
        // "Beneteau First 40" → "First 40"
        for (String name : names(design))
        {
            // "J Boats J/70" → "J/70"
            String[] words = name.trim().split("\\s+");
            for (int n = 1; n <= 2 && n < words.length && !hasDigit(words[n - 1]); n++)
                putStripped(keys, key(String.join(" ", Arrays.copyOfRange(words, n, words.length))));
        }

        Map<Page, Kind> best = new LinkedHashMap<>();
        for (Map.Entry<String, Kind> k : keys.entrySet())
            for (Match m : byKey.getOrDefault(k.getKey(), List.of()))
            {
                // Stripped on both sides is too loose: "Jeanneau 45DS" is not "hunter-45-ds"
                if (k.getValue() == Kind.stripped && m.kind() == Kind.stripped)
                    continue;
                best.merge(m.page(), weaker(k.getValue(), m.kind()), SpecLinkMatcher::stronger);
            }

        Map<Site, List<Match>> bySite = new LinkedHashMap<>();
        for (Site site : Site.values())
        {
            if (covered.contains(site))
                continue;
            List<Match> matches = strongest(best, site);
            if (matches.isEmpty())
                matches = fuzzy(designKeys, site);
            if (!matches.isEmpty())
                bySite.put(site, matches);
        }
        return bySite.isEmpty() ? null : new Proposal(design, boats, excluded, bySite);
    }

    private static List<Match> strongest(Map<Page, Kind> best, Site site)
    {
        Kind strongest = best.entrySet().stream().filter(e -> e.getKey().site() == site)
            .map(Map.Entry::getValue).min(Comparator.naturalOrder()).orElse(null);
        if (strongest == null)
            return List.of();
        return best.entrySet().stream()
            .filter(e -> e.getKey().site() == site && e.getValue() == strongest)
            .map(e -> new Match(e.getKey(), e.getValue()))
            .toList();
    }

    /** Pages of the site whose exact key is one letter different from a design key, with the same numbers. */
    private List<Match> fuzzy(Set<String> designKeys, Site site)
    {
        Set<Page> pages = new LinkedHashSet<>();
        for (String dk : designKeys)
        {
            if (dk.length() < 6)
                continue;
            for (Map.Entry<String, List<Match>> e : byKey.entrySet())
            {
                String pk = e.getKey();
                if (Math.abs(pk.length() - dk.length()) > 1 || !numbers(pk).equals(numbers(dk)) || !oneEditApart(pk, dk))
                    continue;
                for (Match m : e.getValue())
                    if (m.page().site() == site && m.kind() == Kind.exact)
                        pages.add(m.page());
            }
        }
        return pages.stream().map(p -> new Match(p, Kind.fuzzy)).toList();
    }

    private static List<String> numbers(String key)
    {
        return DIGITS.matcher(key).results().map(java.util.regex.MatchResult::group).toList();
    }

    /** True if the strings differ by exactly one inserted, deleted or replaced character. */
    static boolean oneEditApart(String a, String b)
    {
        if (a.equals(b))
            return false;
        if (a.length() > b.length())
            return oneEditApart(b, a);
        int i = 0;
        while (i < a.length() && a.charAt(i) == b.charAt(i))
            i++;
        if (a.length() == b.length())
            return a.substring(i + 1).equals(b.substring(i + 1));
        return b.length() == a.length() + 1 && a.substring(i).equals(b.substring(i + 1));
    }

    private static List<String> names(Design d)
    {
        List<String> names = new ArrayList<>();
        names.add(d.canonicalName());
        if (d.aliases() != null)
            names.addAll(d.aliases());
        return names;
    }

    /** A name safe to approve on an exact match: long, or with a number in it ("j24", not "ross"). */
    static boolean isApprovableName(String key)
    {
        return key.length() >= 6 || (key.length() >= 3 && key.chars().anyMatch(Character::isDigit)
            && key.chars().anyMatch(Character::isLetter));
    }

    private static Kind weaker(Kind a, Kind b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static Kind stronger(Kind a, Kind b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static void put(Map<String, Kind> keys, String key, Kind kind)
    {
        if (key != null && key.length() >= 2)
            keys.merge(key, kind, SpecLinkMatcher::stronger);
    }

    /**
     * Puts a name with words stripped, if it is still specific: letters and a number, e.g.
     * "first40" or "j70", but not "dinghy" from "OK Dinghy" or "classic" from "Devoti Classic".
     */
    private static void putStripped(Map<String, Kind> keys, String key)
    {
        if (key.length() >= 3 && hasDigit(key) && key.chars().anyMatch(Character::isLetter))
            put(keys, key, Kind.stripped);
    }

    private static boolean hasDigit(String s)
    {
        return s.chars().anyMatch(Character::isDigit);
    }

    /** The spec sites each design's approved row in a reviewed candidates CSV already links to. */
    static Map<String, Set<Site>> covered(Path csvFile) throws IOException
    {
        Map<String, Set<Site>> covered = new HashMap<>();
        if (!Files.exists(csvFile))
            return covered;
        List<List<String>> rows = ApplyDesignLinks.parseCsv(Files.readString(csvFile, StandardCharsets.UTF_8));
        List<String> header = rows.getFirst();
        int approveCol = header.indexOf("approve");
        int idCol = header.indexOf("designId");
        int linksCol = header.indexOf("links");
        for (List<String> row : rows.subList(1, rows.size()))
        {
            if (row.size() <= linksCol || !"y".equalsIgnoreCase(row.get(approveCol).trim()))
                continue;
            for (String url : row.get(linksCol).trim().split("\\s+"))
            {
                Page p = page(url);
                if (p != null)
                    covered.computeIfAbsent(row.get(idCol).trim(), k -> new HashSet<>()).add(p.site());
            }
        }
        return covered;
    }

    private static void write(Path file, List<Proposal> proposals) throws IOException
    {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8)))
        {
            out.println(csv("approve", "designId", "canonicalName", "boats", "excluded", "match", "links"));
            for (Proposal p : proposals)
            {
                List<String> match = new ArrayList<>();
                p.bySite().forEach((site, ms) -> match.add(site + " " + ms.getFirst().kind()
                    + (ms.size() > 1 ? " (" + ms.size() + " pages)" : "")));
                out.println(csv(p.approvable() ? "y" : "?", p.design().id(), p.design().canonicalName(),
                    Integer.toString(p.boats()), p.excluded() ? "excluded" : "", String.join("; ", match),
                    String.join(" ", p.links())));
            }
        }
    }
}
