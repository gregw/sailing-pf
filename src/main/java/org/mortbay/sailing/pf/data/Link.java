package org.mortbay.sailing.pf.data;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * An external web page about a design or boat, e.g. a sailboatdata.com spec page, a Wikipedia
 * article or a class association site. Embedded in its owning {@link Design} or {@link Boat} file.
 * <p>
 * The URL is the link's identity: lists never hold two links with the same URL.
 * The kind of site (Wikipedia, sailboatdata, …) is not stored; consumers derive it from the host.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Link(
    String url,    // absolute http(s) URL
    String label,  // optional display text, e.g. "Class association"; null to use the host
    String source  // how the link was added: "manual", or a harvester name, e.g. "wikipedia"
)
{
    public static final String MANUAL = "manual";

    public Link
    {
        url = url == null ? null : url.trim();
        label = label == null || label.isBlank() ? null : label.trim();
        source = source == null || source.isBlank() ? MANUAL : source.trim();
    }

    /**
     * True if {@code url} is an absolute http or https URL with a host.
     */
    public static boolean isValidUrl(String url)
    {
        if (url == null || url.isBlank())
            return false;
        try
        {
            URI uri = new URI(url.trim());
            String scheme = uri.getScheme();
            return ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                && uri.getHost() != null && !uri.getHost().isBlank();
        }
        catch (Exception e)
        {
            return false;
        }
    }

    /**
     * Returns an immutable list of the valid links in {@code raw}, in order, with one link per URL
     * (the first wins).
     */
    public static List<Link> normalise(List<Link> raw)
    {
        if (raw == null || raw.isEmpty())
            return List.of();
        Map<String, Link> byUrl = new LinkedHashMap<>();
        for (Link l : raw)
            if (l != null && isValidUrl(l.url()))
                byUrl.putIfAbsent(l.url(), l);
        return List.copyOf(byUrl.values());
    }

    /**
     * Returns the union of the given link lists, in order, with one link per URL (the first wins).
     * Used when records are merged so no link is lost.
     */
    public static List<Link> merge(Collection<List<Link>> lists)
    {
        List<Link> all = new ArrayList<>();
        for (List<Link> l : lists)
            if (l != null)
                all.addAll(l);
        return normalise(all);
    }

    /**
     * Returns the union of two link lists; see {@link #merge(Collection)}.
     */
    public static List<Link> merge(List<Link> a, List<Link> b)
    {
        return merge(List.of(a == null ? List.of() : a, b == null ? List.of() : b));
    }
}
