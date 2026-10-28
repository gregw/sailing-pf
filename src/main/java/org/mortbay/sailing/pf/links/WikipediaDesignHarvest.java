package org.mortbay.sailing.pf.links;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage 1 of finding web pages about designs: collects the English Wikipedia articles about
 * sailboat types, with their alternate names (redirects), categories and external links, into
 * {@code <pf-data>/cache/links/wikipedia-designs.json}. {@link DesignLinkMatcher} then matches
 * them to our designs.
 * <p>
 * Articles come from the sailboat type categories (by designer, decade and manufacturer, the
 * keelboat size categories and Sailing yachts), from every article that links to a
 * sailboatdata.com or boat-specs.com boat page, and from looking up our design names as titles
 * (keeping only articles about boats, see {@link #isAboutBoats}). Only the Wikipedia API is read: the spec sites'
 * terms forbid harvesting them, so their pages are found only through Wikipedia's links.
 * <p>
 * Usage: {@code WikipediaDesignHarvest [pf-data]}
 */
public class WikipediaDesignHarvest
{
    private static final Logger LOG = LoggerFactory.getLogger(WikipediaDesignHarvest.class);

    static final String API = "https://en.wikipedia.org/w/api.php";
    // Wikimedia asks API clients to identify themselves with a contact URL
    static final String USER_AGENT = "sailing-pf/1.0 (https://github.com/gregw/sailing-pf)";
    // Pause between API requests, to stay well inside Wikimedia's rate limits
    private static final long PAUSE_MS = 100;

    /** Categories whose subcategories hold sailboat type articles. */
    static final List<String> PARENT_CATEGORIES = List.of(
        "Category:Sailboat type designs by designer",
        "Category:Sailboat type designs by decade",
        "Category:Sailboat types by manufacturer",
        "Category:Keelboats");

    /** Categories that hold sailboat type articles directly. */
    static final List<String> PAGE_CATEGORIES = List.of(
        "Category:Sailing yachts",
        "Category:Keelboats");

    /** External link prefixes (without protocol) of spec-site boat pages. */
    static final List<String> SPEC_SITE_PREFIXES = List.of(
        "sailboatdata.com/sailboat/",
        "www.sailboatdata.com/sailboat/",
        "boat-specs.com/sailing/sailboats/",
        "www.boat-specs.com/sailing/sailboats/");

    /** One Wikipedia article. */
    public record Article(
        String title,
        long pageId,
        String wikidata,          // Wikidata item id, e.g. "Q104543351"; nullable
        String description,       // Wikipedia short description, e.g. "Sailboat class"; nullable
        List<String> redirects,   // titles that redirect here: alternate names
        List<String> categories,
        List<String> extlinks)
    {
    }

    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final HttpClient client;

    public WikipediaDesignHarvest(HttpClient client)
    {
        this.client = client;
    }

    public static Path outputFile(Path dataRoot)
    {
        return dataRoot.resolve("cache").resolve("links").resolve("wikipedia-designs.json");
    }

    public static void main(String[] args) throws Exception
    {
        Path root = DataStore.resolveDataRoot(args);
        Path out = outputFile(root);
        HttpClient client = new HttpClient();
        client.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, USER_AGENT));
        client.start();
        try
        {
            List<Article> articles = new WikipediaDesignHarvest(client)
                .harvest(designNames(root.resolve("imported").resolve("designs")));
            Files.createDirectories(out.getParent());
            MAPPER.writeValue(out.toFile(), articles);
            LOG.info("Wrote {} articles to {}", articles.size(), out);
        }
        finally
        {
            client.stop();
        }
    }

    public List<Article> harvest(Collection<String> designNames) throws Exception
    {
        Set<String> titles = new TreeSet<>();
        for (String parent : PARENT_CATEGORIES)
        {
            List<String> subcats = categoryMembers(parent, "subcat");
            LOG.info("{}: {} subcategories", parent, subcats.size());
            for (String sub : subcats)
                titles.addAll(categoryMembers(sub, "page"));
        }
        for (String category : PAGE_CATEGORIES)
            titles.addAll(categoryMembers(category, "page"));
        LOG.info("{} articles from categories", titles.size());
        for (String prefix : SPEC_SITE_PREFIXES)
        {
            for (String protocol : List.of("https", "http"))
                titles.addAll(linkingArticles(prefix, protocol));
        }
        LOG.info("{} articles from categories and links", titles.size());
        List<Article> articles = new ArrayList<>(articles(new ArrayList<>(titles)));

        Set<Long> have = new HashSet<>();
        articles.forEach(a -> have.add(a.pageId()));
        Set<String> guesses = new TreeSet<>();
        for (String name : designNames)
            guesses.addAll(titleGuesses(name));
        guesses.removeAll(titles);
        LOG.info("Looking up {} design names as titles", guesses.size());
        int found = 0;
        for (Article a : articles(new ArrayList<>(guesses)))
        {
            if (have.add(a.pageId()) && isAboutBoats(a))
            {
                articles.add(a);
                found++;
            }
        }
        LOG.info("{} more articles from design names, {} in all", found, articles.size());
        return articles;
    }

    /** Words in a description or category that mark an article about boats. */
    private static final Pattern BOATS = Pattern.compile(
        "(?i)\\b(sail\\w*|boat\\w*|yacht\\w*|keelboat\\w*|dinghy|dinghies|catamaran\\w*|trimaran\\w*|skiff\\w*|sloop|cutter|ketch)\\b");

    /**
     * True if the article is about boats, judged by its short description and categories.
     * Disambiguation pages are not.
     */
    static boolean isAboutBoats(Article a)
    {
        if (a.categories().stream().anyMatch(c -> c.toLowerCase(Locale.ENGLISH).contains("disambiguation")))
            return false;
        if (a.description() != null && BOATS.matcher(a.description()).find())
            return true;
        return a.categories().stream().anyMatch(c -> BOATS.matcher(c).find());
    }

    /**
     * Titles a design might have on Wikipedia: the name as is and in title case ("NORTHSHORE 38"
     * → "Northshore 38"), and without its first one or two words, usually the builder
     * ("J Boats J/70" → "J/70").
     */
    static Set<String> titleGuesses(String name)
    {
        Set<String> guesses = new LinkedHashSet<>();
        if (name == null || name.isBlank())
            return guesses;
        String[] words = name.trim().split("\\s+");
        for (int drop = 0; drop <= 2 && drop < words.length; drop++)
        {
            String rest = String.join(" ", Arrays.asList(words).subList(drop, words.length));
            if (drop > 0 && rest.replaceAll("[^A-Za-z]", "").isEmpty())
                continue;   // "Hunter 30" → "30" is not a name
            guesses.add(rest);
            guesses.add(titleCase(rest));
        }
        guesses.removeIf(g -> g.length() < 2 || g.contains("|") || g.contains("#"));
        return guesses;
    }

    private static String titleCase(String s)
    {
        StringBuilder sb = new StringBuilder();
        for (String w : s.split(" "))
        {
            if (!sb.isEmpty())
                sb.append(' ');
            sb.append(w.isEmpty() ? w : w.substring(0, 1).toUpperCase(Locale.ENGLISH)
                + w.substring(1).toLowerCase(Locale.ENGLISH));
        }
        return sb.toString();
    }

    /** The canonical names and aliases of the designs in {@code dir}. */
    static List<String> designNames(Path dir) throws IOException
    {
        List<String> names = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir))
        {
            for (Path f : files.filter(p -> p.toString().endsWith(".json")).toList())
            {
                JsonNode d = MAPPER.readTree(f.toFile());
                names.add(d.path("canonicalName").asText());
                for (JsonNode a : d.path("aliases"))
                    names.add(a.asText());
            }
        }
        return names;
    }

    /** Titles of the members of {@code category} of the given type ("page" or "subcat"). */
    List<String> categoryMembers(String category, String type) throws Exception
    {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("list", "categorymembers");
        params.put("cmtitle", category);
        params.put("cmtype", type);
        params.put("cmlimit", "max");
        if ("page".equals(type))
            params.put("cmnamespace", "0");
        List<String> titles = new ArrayList<>();
        for (JsonNode response : query(params))
            for (JsonNode m : response.path("query").path("categorymembers"))
                titles.add(m.path("title").asText());
        return titles;
    }

    /** Titles of the articles with an external link starting with {@code protocol://prefix}. */
    List<String> linkingArticles(String prefix, String protocol) throws Exception
    {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("list", "exturlusage");
        params.put("euquery", prefix);
        params.put("euprotocol", protocol);
        params.put("eunamespace", "0");
        params.put("eulimit", "max");
        params.put("euprop", "title");
        Set<String> titles = new LinkedHashSet<>();
        for (JsonNode response : query(params))
            for (JsonNode u : response.path("query").path("exturlusage"))
                titles.add(u.path("title").asText());
        LOG.info("{}://{}: {} articles", protocol, prefix, titles.size());
        return new ArrayList<>(titles);
    }

    /** The details of the given articles, fetched 50 titles at a time. */
    List<Article> articles(List<String> titles) throws Exception
    {
        Map<Long, ArticleBuilder> byPageId = new LinkedHashMap<>();
        for (int i = 0; i < titles.size(); i += 50)
        {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("prop", "pageprops|redirects|categories|extlinks");
            params.put("titles", String.join("|", titles.subList(i, Math.min(i + 50, titles.size()))));
            params.put("redirects", "1");
            params.put("ppprop", "wikibase_item|wikibase-shortdesc");
            params.put("rdlimit", "max");
            params.put("clshow", "!hidden");
            params.put("cllimit", "max");
            params.put("ellimit", "max");
            for (JsonNode response : query(params))
            {
                for (JsonNode page : response.path("query").path("pages"))
                {
                    if (page.has("missing") || page.has("invalid") || !page.has("pageid") || page.path("ns").asInt() != 0)
                        continue;
                    ArticleBuilder b = byPageId.computeIfAbsent(page.path("pageid").asLong(),
                        id -> new ArticleBuilder(page.path("title").asText(), id));
                    JsonNode props = page.path("pageprops");
                    if (props.has("wikibase_item"))
                        b.wikidata = props.path("wikibase_item").asText();
                    if (props.has("wikibase-shortdesc"))
                        b.description = props.path("wikibase-shortdesc").asText();
                    for (JsonNode r : page.path("redirects"))
                        b.redirects.add(r.path("title").asText());
                    for (JsonNode c : page.path("categories"))
                        b.categories.add(c.path("title").asText());
                    for (JsonNode e : page.path("extlinks"))
                        b.extlinks.add(e.path("url").asText());
                }
            }
            LOG.info("Fetched {} of {} articles", Math.min(i + 50, titles.size()), titles.size());
        }
        return byPageId.values().stream().map(ArticleBuilder::build).toList();
    }

    private static class ArticleBuilder
    {
        final String title;
        final long pageId;
        String wikidata;
        String description;
        final Set<String> redirects = new LinkedHashSet<>();
        final Set<String> categories = new LinkedHashSet<>();
        final Set<String> extlinks = new LinkedHashSet<>();

        ArticleBuilder(String title, long pageId)
        {
            this.title = title;
            this.pageId = pageId;
        }

        Article build()
        {
            return new Article(title, pageId, wikidata, description, List.copyOf(redirects),
                List.copyOf(categories), List.copyOf(extlinks));
        }
    }

    /**
     * Runs an API query, following "continue" until done, and returns every response.
     */
    private List<JsonNode> query(Map<String, String> params) throws Exception
    {
        Map<String, String> p = new LinkedHashMap<>(params);
        p.put("action", "query");
        p.put("format", "json");
        p.put("formatversion", "2");
        p.put("maxlag", "5");
        List<JsonNode> responses = new ArrayList<>();
        while (true)
        {
            JsonNode response = get(p);
            responses.add(response);
            JsonNode cont = response.path("continue");
            if (cont.isMissingNode() || cont.isEmpty())
                return responses;
            cont.properties().forEach(e -> p.put(e.getKey(), e.getValue().asText()));
        }
    }

    private JsonNode get(Map<String, String> params) throws Exception
    {
        StringBuilder url = new StringBuilder(API).append('?');
        params.forEach((k, v) -> url.append(k).append('=')
            .append(URLEncoder.encode(v, StandardCharsets.UTF_8)).append('&'));
        for (int attempt = 1; ; attempt++)
        {
            Thread.sleep(PAUSE_MS);
            ContentResponse response;
            try
            {
                response = client.newRequest(url.toString()).timeout(30, TimeUnit.SECONDS).send();
            }
            catch (TimeoutException | ExecutionException e)
            {
                if (attempt >= 5)
                    throw e;
                LOG.warn("Wikipedia API retry {} ({})", attempt, e.toString());
                Thread.sleep(5000L * attempt);
                continue;
            }
            JsonNode json = response.getStatus() == 200 ? MAPPER.readTree(response.getContentAsString()) : null;
            boolean lagged = json != null && "maxlag".equals(json.path("error").path("code").asText());
            if (json != null && !lagged && !json.has("error"))
                return json;
            if (attempt >= 5)
                throw new IllegalStateException("Wikipedia API failed: HTTP " + response.getStatus() + " "
                    + (json != null ? json.path("error") : response.getContentAsString()));
            LOG.warn("Wikipedia API retry {} (HTTP {}{})", attempt, response.getStatus(), lagged ? ", maxlag" : "");
            Thread.sleep(5000L * attempt);
        }
    }
}
