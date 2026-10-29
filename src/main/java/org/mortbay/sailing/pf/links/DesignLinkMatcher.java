package org.mortbay.sailing.pf.links;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.mortbay.sailing.pf.links.WikipediaDesignHarvest.Article;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage 2 of finding web pages about designs: matches the Wikipedia articles collected by
 * {@link WikipediaDesignHarvest} to our designs by normalised name, and writes the proposed
 * links for review to {@code <pf-data>/cache/links/design-link-candidates.csv}, most-raced
 * designs first. Designs with no match go to {@code design-link-unmatched.csv}, and articles
 * matched by more than one design (likely the same design under different names: merge
 * candidates) to {@code design-merge-candidates.csv}.
 * <p>
 * An article matches a design when one of the article's name keys equals one of the design's
 * (its id, canonical name or an alias, normalised). The article's keys, strongest first:
 * <ol>
 *   <li>{@code title} — its title, less any "(…)" qualifier</li>
 *   <li>{@code redirect} — a title that redirects to it (an alternate name)</li>
 *   <li>{@code builder} — its title with the builder or designer added or removed, e.g.
 *       "Archambault Grand Surprise" ↔ "Grand Surprise", using the article's
 *       "built by" / "designs by" categories</li>
 *   <li>{@code slug} — the name in a sailboatdata.com link of the article; weak, since articles
 *       also link related designs</li>
 * </ol>
 * A design gets the articles at its strongest matching kind. One article at title or redirect
 * strength that is about a particular design (see {@link #isDesignArticle}) is proposed for
 * approval ({@code approve=y}); anything else is left for review ({@code approve=?}).
 * <p>
 * The proposed links for a match are the article itself plus the article's sailboatdata.com
 * and boat-specs.com links whose page name matches the design or article. When there is no such
 * link and the article has just one link to the site, that one is proposed.
 * <p>
 * Reads only: nothing in the data store is changed.
 * <p>
 * Usage: {@code DesignLinkMatcher [pf-data]}
 */
public class DesignLinkMatcher
{
    private static final Logger LOG = LoggerFactory.getLogger(DesignLinkMatcher.class);

    /** Kinds of match, strongest first. */
    enum Kind
    {
        title, redirect, builder, slug
    }

    record Candidate(Article article, Kind kind)
    {
    }

    /** The proposal for one design: its best candidates (all of one kind) and their links. */
    record Proposal(Design design, int boats, boolean excluded, List<Candidate> candidates, List<String> links)
    {
        boolean approvable()
        {
            return candidates.size() == 1 && candidates.getFirst().kind().compareTo(Kind.redirect) <= 0
                && isDesignArticle(candidates.getFirst().article());
        }
    }

    private static final Pattern QUALIFIER = Pattern.compile("\\s*\\([^)]*\\)\\s*$");
    private static final Pattern BUILT_BY = Pattern.compile("^Category:Sailboat types built by (.+)$");
    private static final Pattern DESIGN_DECADE = Pattern.compile("^Category:\\d{4}s sailboat type designs$");
    private static final Pattern DESIGNS_BY = Pattern.compile("^Category:Sailboat type designs by (.+)$");
    private static final Pattern SAILBOATDATA = Pattern.compile("^https?://(?:www\\.)?sailboatdata\\.com/sailboat/([^/?#]+)");
    private static final Pattern BOAT_SPECS = Pattern.compile("^https?://(?:www\\.)?boat-specs\\.com/sailing/sailboats/([^/?#]+)/([^/?#]+)");
    // Words in builder names that are not part of how the boats are named
    private static final Set<String> BUILDER_NOISE = Set.of("boats", "boat", "yachts", "yacht", "marine",
        "boatworks", "boatbuilders", "company", "inc", "ltd", "sa", "the", "and");

    private static final ObjectMapper JSON = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    private final List<Article> articles;
    private final Map<String, List<Candidate>> byKey = new HashMap<>();

    public DesignLinkMatcher(List<Article> articles)
    {
        this.articles = articles;
        for (Article a : articles)
        {
            for (Map.Entry<String, Kind> e : articleKeys(a).entrySet())
                byKey.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(new Candidate(a, e.getValue()));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Path root = DataStore.resolveDataRoot(args);
        Path dir = WikipediaDesignHarvest.outputFile(root).getParent();
        List<Article> articles = JSON.readValue(WikipediaDesignHarvest.outputFile(root).toFile(),
            new TypeReference<List<Article>>() {});
        List<Design> designs = readDesigns(root.resolve("imported").resolve("designs"));
        Map<String, Integer> boats = boatCounts(root.resolve("imported").resolve("boats"));
        Set<String> excluded = excludedDesigns(root.resolve("config").resolve("design.yaml"));
        LOG.info("{} articles, {} designs", articles.size(), designs.size());

        DesignLinkMatcher matcher = new DesignLinkMatcher(articles);
        List<Proposal> proposals = new ArrayList<>();
        List<Design> unmatched = new ArrayList<>();
        for (Design d : designs)
        {
            Proposal p = matcher.propose(d, boats.getOrDefault(d.id(), 0), excluded.contains(d.id()));
            if (p == null)
                unmatched.add(d);
            else
                proposals.add(p);
        }
        proposals.sort(Comparator.comparingInt(Proposal::boats).reversed().thenComparing(p -> p.design().id()));
        unmatched.sort(Comparator.comparingInt((Design d) -> boats.getOrDefault(d.id(), 0)).reversed()
            .thenComparing(Design::id));

        writeCandidates(dir.resolve("design-link-candidates.csv"), proposals);
        writeUnmatched(dir.resolve("design-link-unmatched.csv"), unmatched, boats, excluded);
        writeMergeCandidates(dir.resolve("design-merge-candidates.csv"), proposals);
        long approvable = proposals.stream().filter(Proposal::approvable).count();
        LOG.info("{} designs matched ({} proposed for approval, {} to review), {} unmatched",
            proposals.size(), approvable, proposals.size() - approvable, unmatched.size());
        for (Kind k : Kind.values())
            LOG.info("  best match {}: {}", k,
                proposals.stream().filter(p -> p.candidates().getFirst().kind() == k).count());
    }

    /**
     * The proposal for a design, or null if no article matches it.
     */
    Proposal propose(Design design, int boats, boolean excluded)
    {
        Set<String> designKeys = designKeys(design);
        Map<Article, Kind> best = new LinkedHashMap<>();
        for (String key : designKeys)
        {
            for (Candidate c : byKey.getOrDefault(key, List.of()))
                best.merge(c.article(), c.kind(), (a, b) -> a.compareTo(b) <= 0 ? a : b);
        }
        if (best.isEmpty())
            return null;
        Kind strongest = best.values().stream().min(Comparator.naturalOrder()).orElseThrow();
        List<Candidate> candidates = best.entrySet().stream()
            .filter(e -> e.getValue() == strongest)
            .map(e -> new Candidate(e.getKey(), e.getValue()))
            .toList();
        List<String> links = new ArrayList<>();
        for (Candidate c : candidates)
            links.addAll(articleLinks(c.article(), designKeys));
        return new Proposal(design, boats, excluded, candidates, List.copyOf(new LinkedHashSet<>(links)));
    }

    /** The design's name keys: its id, canonical name and aliases, normalised. */
    static Set<String> designKeys(Design d)
    {
        Set<String> keys = new LinkedHashSet<>();
        keys.add(d.id());
        keys.add(key(d.canonicalName()));
        if (d.aliases() != null)
            d.aliases().forEach(a -> keys.add(key(a)));
        keys.removeIf(k -> k == null || k.length() < 2);
        return keys;
    }

    /** The article's name keys, each with its strongest kind. */
    static Map<String, Kind> articleKeys(Article a)
    {
        Map<String, Kind> keys = new LinkedHashMap<>();
        List<String> names = new ArrayList<>();
        names.add(a.title());
        names.addAll(a.redirects());
        put(keys, key(a.title()), Kind.title);
        for (String r : a.redirects())
            put(keys, key(r), Kind.redirect);

        List<String> makers = makers(a);
        for (String name : names)
        {
            String bare = QUALIFIER.matcher(name).replaceAll("");
            String nameKey = key(bare);
            for (String maker : makers)
            {
                // "Archambault Grand Surprise" → "Grand Surprise"
                String[] words = bare.split("\\s+");
                for (int n = 1; n < words.length && n <= 2; n++)
                {
                    String lead = key(String.join(" ", List.of(words).subList(0, n)));
                    if (!lead.isEmpty() && maker.startsWith(lead))
                    {
                        String rest = key(String.join(" ", List.of(words).subList(n, words.length)));
                        if (isDistinctive(rest))
                            put(keys, rest, Kind.builder);
                    }
                }
                // "Grand Surprise" → "Archambault Grand Surprise"
                if (!nameKey.startsWith(maker))
                    put(keys, maker + nameKey, Kind.builder);
            }
        }
        for (String url : a.extlinks())
        {
            Matcher m = SAILBOATDATA.matcher(url);
            if (m.find())
                put(keys, key(m.group(1)), Kind.slug);
        }
        keys.keySet().removeIf(k -> k.length() < 2);
        return keys;
    }

    /**
     * True if the article is about a particular design, not a kind of boat such as "Trailer
     * sailer": it is in a designer, builder or decade category of sailboat types.
     */
    static boolean isDesignArticle(Article a)
    {
        return a.categories().stream().anyMatch(c -> BUILT_BY.matcher(c).find() || DESIGNS_BY.matcher(c).find()
            || DESIGN_DECADE.matcher(c).find());
    }

    /**
     * The normalised names of the article's builders and designers (from its categories), with
     * company words such as "Boats" or "Marine" dropped: "Archambault Boats" → "archambault".
     */
    static List<String> makers(Article a)
    {
        List<String> makers = new ArrayList<>();
        for (String c : a.categories())
        {
            Matcher m = BUILT_BY.matcher(c);
            if (!m.find())
            {
                m = DESIGNS_BY.matcher(c);
                if (!m.find())
                    continue;
            }
            String name = QUALIFIER.matcher(m.group(1)).replaceAll("");
            List<String> words = new ArrayList<>();
            for (String w : name.split("\\s+"))
                if (!BUILDER_NOISE.contains(w.toLowerCase(Locale.ENGLISH)))
                    words.add(w);
            if (words.isEmpty())
                continue;
            // The whole name, and the last word alone: designs carry the designer's surname,
            // e.g. "Farr 40" in "Sailboat type designs by Bruce Farr"
            for (String maker : List.of(key(String.join(" ", words)), key(words.getLast())))
                if (maker.length() >= 3 && !makers.contains(maker))
                    makers.add(maker);
        }
        return makers;
    }

    /**
     * The links proposed for a design matched to an article: the article, and for each spec site
     * the article's link whose page name matches the design, else the article (else the site's
     * only link).
     */
    static List<String> articleLinks(Article a, Set<String> designKeys)
    {
        Set<String> articleNames = new HashSet<>();
        articleNames.add(key(a.title()));
        a.redirects().forEach(r -> articleNames.add(key(r)));

        List<String> links = new ArrayList<>();
        links.add(wikipediaUrl(a.title()));
        Map<String, Set<String>> sailboatdata = new LinkedHashMap<>();  // url → page name keys
        Map<String, Set<String>> boatSpecs = new LinkedHashMap<>();
        for (String url : a.extlinks())
        {
            Matcher m = SAILBOATDATA.matcher(url);
            if (m.find())
            {
                sailboatdata.put(url, Set.of(key(m.group(1))));
                continue;
            }
            m = BOAT_SPECS.matcher(url);
            if (m.find())
                boatSpecs.put(url, Set.of(key(m.group(2)), key(m.group(1)) + key(m.group(2))));
        }
        for (Map<String, Set<String>> site : List.of(sailboatdata, boatSpecs))
        {
            String chosen = firstNamed(site, designKeys);
            if (chosen == null)
                chosen = firstNamed(site, articleNames);
            if (chosen == null && site.size() == 1)
                chosen = site.keySet().iterator().next();
            if (chosen != null)
                links.add(chosen);
        }
        return links;
    }

    private static String firstNamed(Map<String, Set<String>> pages, Set<String> names)
    {
        for (Map.Entry<String, Set<String>> e : pages.entrySet())
            for (String n : e.getValue())
                if (names.contains(n))
                    return e.getKey();
        return null;
    }

    static String wikipediaUrl(String title)
    {
        try
        {
            return new URI("https", "en.wikipedia.org", "/wiki/" + title.replace(' ', '_'), null).toASCIIString();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(title, e);
        }
    }

    /** A name normalised for matching: no "(…)" qualifier, lower case letters and digits only. */
    static String key(String name)
    {
        if (name == null)
            return "";
        return IdGenerator.normaliseDesignName(QUALIFIER.matcher(name).replaceAll(""));
    }

    /** A builder-stripped name worth matching on: not just a number, e.g. "30" from "Hunter 30". */
    static boolean isDistinctive(String key)
    {
        return key.length() >= 4 && key.chars().filter(Character::isLetter).count() >= 3;
    }

    private static void put(Map<String, Kind> keys, String key, Kind kind)
    {
        if (key != null && !key.isEmpty())
            keys.merge(key, kind, (a, b) -> a.compareTo(b) <= 0 ? a : b);
    }

    // --- Reading and writing ---

    static List<Design> readDesigns(Path dir) throws IOException
    {
        List<Design> designs = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir))
        {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).sorted().toList())
                designs.add(JSON.readValue(f.toFile(), Design.class));
        }
        return designs;
    }

    static Map<String, Integer> boatCounts(Path dir) throws IOException
    {
        Map<String, Integer> counts = new HashMap<>();
        try (Stream<Path> files = Files.list(dir))
        {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
            {
                JsonNode designId = JSON.readTree(f.toFile()).path("designId");
                if (designId.isTextual())
                    counts.merge(designId.asText(), 1, Integer::sum);
            }
        }
        return counts;
    }

    /** The excluded and ignored design ids of {@code design.yaml}. */
    static Set<String> excludedDesigns(Path designYaml) throws IOException
    {
        Set<String> ids = new HashSet<>();
        if (!Files.exists(designYaml))
            return ids;
        JsonNode yaml = new ObjectMapper(new YAMLFactory()).readTree(designYaml.toFile());
        for (String section : List.of("excluded", "ignored"))
            for (JsonNode n : yaml.path(section))
                ids.add(IdGenerator.normaliseDesignName(n.asText()));
        return ids;
    }

    private static void writeCandidates(Path file, List<Proposal> proposals) throws IOException
    {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8)))
        {
            out.println(csv("approve", "designId", "canonicalName", "boats", "excluded", "match",
                "article", "description", "links"));
            for (Proposal p : proposals)
            {
                out.println(csv(p.approvable() ? "y" : "?", p.design().id(), p.design().canonicalName(),
                    Integer.toString(p.boats()), p.excluded() ? "excluded" : "",
                    p.candidates().getFirst().kind().name() + (p.candidates().size() > 1
                        ? " (" + p.candidates().size() + " articles)" : ""),
                    String.join(" | ", p.candidates().stream().map(c -> c.article().title()).toList()),
                    String.join(" | ", p.candidates().stream()
                        .map(c -> c.article().description() == null ? "" : c.article().description()).toList()),
                    String.join(" ", p.links())));
            }
        }
        LOG.info("Wrote {}", file);
    }

    private static void writeUnmatched(Path file, List<Design> designs, Map<String, Integer> boats,
                                       Set<String> excluded) throws IOException
    {
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8)))
        {
            out.println(csv("designId", "canonicalName", "boats", "excluded", "aliases"));
            for (Design d : designs)
                out.println(csv(d.id(), d.canonicalName(), Integer.toString(boats.getOrDefault(d.id(), 0)),
                    excluded.contains(d.id()) ? "excluded" : "",
                    d.aliases() == null ? "" : String.join(" | ", d.aliases())));
        }
        LOG.info("Wrote {}", file);
    }

    /** Articles that are the single match of two or more designs. */
    private static void writeMergeCandidates(Path file, List<Proposal> proposals) throws IOException
    {
        Map<String, List<Proposal>> byArticle = new LinkedHashMap<>();
        for (Proposal p : proposals)
            if (p.candidates().size() == 1 && p.candidates().getFirst().kind() != Kind.slug)
                byArticle.computeIfAbsent(p.candidates().getFirst().article().title(), t -> new ArrayList<>()).add(p);
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file, StandardCharsets.UTF_8)))
        {
            out.println(csv("article", "boats", "designs"));
            byArticle.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .sorted(Comparator.comparingInt((Map.Entry<String, List<Proposal>> e) ->
                    e.getValue().stream().mapToInt(Proposal::boats).sum()).reversed())
                .forEach(e -> out.println(csv(e.getKey(),
                    Integer.toString(e.getValue().stream().mapToInt(Proposal::boats).sum()),
                    String.join(" | ", e.getValue().stream()
                        .map(p -> p.design().id() + " (" + p.design().canonicalName() + ", " + p.boats() + " boats)")
                        .toList()))));
        }
        LOG.info("Wrote {}", file);
    }

    static String csv(String... fields)
    {
        StringBuilder sb = new StringBuilder();
        for (String f : fields)
        {
            if (!sb.isEmpty())
                sb.append(',');
            String v = f == null ? "" : f;
            if (v.contains(",") || v.contains("\"") || v.contains("\n"))
                sb.append('"').append(v.replace("\"", "\"\"")).append('"');
            else
                sb.append(v);
        }
        return sb.toString();
    }
}
