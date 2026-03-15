package org.mortbay.sailing.hpf.importer;

import java.util.Locale;

import org.mortbay.sailing.hpf.data.Design;

/**
 * Normalisation and ID slug utilities for importers.
 * All methods are pure functions with no side effects.
 */
public class IdGenerator
{

    /**
     * Lowercase, strip non-alphanumeric except spaces (replaced with underscore).
     * "Raging Bull" → "raging_bull"
     */
    public static String normaliseName(String raw)
    {
        if (raw == null)
            return "";
        return raw.toLowerCase().replaceAll("[^a-z0-9 ]", "").replaceAll(" ", "_");
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
     * Same normalisation as normaliseName; separate method for clarity at call sites.
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
     * "AUS1234", "Raging Bull", null      → "AUS1234-raging_bull"
     * "AUS1234", "Raging Bull", j24design → "AUS1234-raging_bull-j24"
     */
    public static String generateBoatId(String rawSail, String rawName, Design design)
    {
        String normSail = normaliseSailNumber(rawSail);
        String normName = normaliseName(rawName);
        String base = normSail + "-" + normName;
        return design == null ? base : base + "-" + design.id();
    }

    private IdGenerator()
    {
    }
}
