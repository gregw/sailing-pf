package org.mortbay.sailing.hpf.importer;

import java.time.LocalDate;
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
        if (normSail.isEmpty())
            normSail = "nosail";
        String normName = normaliseName(rawName);
        String base = normSail + "-" + normName;
        return design == null ? base : base + "-" + design.id();
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
