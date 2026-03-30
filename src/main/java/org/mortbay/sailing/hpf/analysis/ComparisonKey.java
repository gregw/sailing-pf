package org.mortbay.sailing.hpf.analysis;

/**
 * Identifies a specific handicap comparison:
 * system A (variant A) vs system B (variant B), year A → year B.
 * <p>
 * For same-year comparisons, yearA == yearB.
 * For year-transition comparisons, systemA == systemB and yearB == yearA + 1.
 * <p>
 * Non-spinnaker and two-handed flags describe which certificate variant was selected
 * on the A and B sides. Typically both sides share the same variant (both spinnaker,
 * both non-spinnaker, etc.) except for the spin-vs-nonspin and twohanded-vs-normal
 * comparisons.
 */
public record ComparisonKey(
    String systemA, boolean nonSpinA, boolean twoHandedA,
    String systemB, boolean nonSpinB, boolean twoHandedB,
    int yearA, int yearB
)
{
    // --- Factory methods for each comparison type ---

    /** ORC spinnaker vs IRC spinnaker, same year. */
    public static ComparisonKey orcVsIrc(int year)
    {
        return new ComparisonKey("ORC", false, false, "IRC", false, false, year, year);
    }

    /** ORC non-spinnaker vs IRC non-spinnaker, same year. */
    public static ComparisonKey orcNsVsIrcNs(int year)
    {
        return new ComparisonKey("ORC", true, false, "IRC", true, false, year, year);
    }

    /** ORC two-handed vs IRC two-handed, same year. */
    public static ComparisonKey orcTwoHandedVsIrcTwoHanded(int year)
    {
        return new ComparisonKey("ORC", false, true, "IRC", false, true, year, year);
    }

    /** AMS spinnaker vs IRC spinnaker, same year. */
    public static ComparisonKey amsVsIrc(int year)
    {
        return new ComparisonKey("AMS", false, false, "IRC", false, false, year, year);
    }

    /** AMS spinnaker vs ORC spinnaker, same year. */
    public static ComparisonKey amsVsOrc(int year)
    {
        return new ComparisonKey("AMS", false, false, "ORC", false, false, year, year);
    }

    /** AMS non-spinnaker vs ORC non-spinnaker, same year. */
    public static ComparisonKey amsNsVsOrcNs(int year)
    {
        return new ComparisonKey("AMS", true, false, "ORC", true, false, year, year);
    }

    /** AMS two-handed vs ORC two-handed, same year. */
    public static ComparisonKey amsTwoHandedVsOrcTwoHanded(int year)
    {
        return new ComparisonKey("AMS", false, true, "ORC", false, true, year, year);
    }

    /** Non-spinnaker vs spinnaker, pooled across all handicap systems, same year. */
    public static ComparisonKey allNsVsSpin(int year)
    {
        return new ComparisonKey("ALL", true, false, "ALL", false, false, year, year);
    }

    /** Two-handed vs spinnaker, pooled across all handicap systems, same year. */
    public static ComparisonKey allTwoHandedVsSpin(int year)
    {
        return new ComparisonKey("ALL", false, true, "ALL", false, false, year, year);
    }

    /** Year-to-year transition for IRC spinnaker certs. */
    public static ComparisonKey ircYearTransition(int yearFrom)
    {
        return new ComparisonKey("IRC", false, false, "IRC", false, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for ORC spinnaker certs. */
    public static ComparisonKey orcYearTransition(int yearFrom)
    {
        return new ComparisonKey("ORC", false, false, "ORC", false, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for AMS spinnaker certs. */
    public static ComparisonKey amsYearTransition(int yearFrom)
    {
        return new ComparisonKey("AMS", false, false, "AMS", false, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for IRC non-spinnaker certs. */
    public static ComparisonKey ircNsYearTransition(int yearFrom)
    {
        return new ComparisonKey("IRC", true, false, "IRC", true, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for ORC non-spinnaker certs. */
    public static ComparisonKey orcNsYearTransition(int yearFrom)
    {
        return new ComparisonKey("ORC", true, false, "ORC", true, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for AMS non-spinnaker certs. */
    public static ComparisonKey amsNsYearTransition(int yearFrom)
    {
        return new ComparisonKey("AMS", true, false, "AMS", true, false, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for IRC two-handed certs. */
    public static ComparisonKey ircTwoHandedYearTransition(int yearFrom)
    {
        return new ComparisonKey("IRC", false, true, "IRC", false, true, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for ORC two-handed certs. */
    public static ComparisonKey orcTwoHandedYearTransition(int yearFrom)
    {
        return new ComparisonKey("ORC", false, true, "ORC", false, true, yearFrom, yearFrom + 1);
    }

    /** Year-to-year transition for AMS two-handed certs. */
    public static ComparisonKey amsTwoHandedYearTransition(int yearFrom)
    {
        return new ComparisonKey("AMS", false, true, "AMS", false, true, yearFrom, yearFrom + 1);
    }

    // --- String id encoding ---

    /**
     * Returns a URL-safe id string, e.g. {@code orc-vs-irc-2024},
     * {@code irc-spin-vs-nonspin-2024}, {@code irc-2023-to-2024}.
     */
    public String toId()
    {
        String sA = systemA.toLowerCase();
        String sB = systemB.toLowerCase();
        String vA = variantSuffix(nonSpinA, twoHandedA);
        String vB = variantSuffix(nonSpinB, twoHandedB);

        if (yearA != yearB)
        {
            // Year transition: systemA == systemB, same variant
            // Include variant suffix for non-spin to avoid ID collisions with spin transitions
            String v = variantSuffix(nonSpinA, twoHandedA);
            if ("spin".equals(v))
                return sA + "-" + yearA + "-to-" + yearB;
            else
                return sA + "-" + v + "-" + yearA + "-to-" + yearB;
        }

        if (systemA.equals(systemB))
        {
            // Variant comparison within same system
            return sA + "-" + vA + "-vs-" + vB + "-" + yearA;
        }

        // Cross-system: include variant suffix when non-default (vA == vB by convention)
        String variant = vA.equals("spin") ? "" : "-" + vA;
        return sA + "-vs-" + sB + variant + "-" + yearA;
    }

    private static String variantSuffix(boolean nonSpin, boolean twoHanded)
    {
        if (nonSpin)
            return "nonspin";
        if (twoHanded)
            return "twohanded";
        return "spin";
    }

    /**
     * Parses an id produced by {@link #toId()} back into a {@code ComparisonKey}.
     * Returns {@code null} if the id is not recognised.
     */
    public static ComparisonKey fromId(String id)
    {
        if (id == null)
            return null;

        // NS/2H year transition: {sys}-{variant}-{yearA}-to-{yearB}
        // e.g. irc-nonspin-2021-to-2022, ams-twohanded-2025-to-2026
        if (id.matches("[a-z]+-[a-z]+-\\d{4}-to-\\d{4}"))
        {
            String[] parts = id.split("-");
            String sys     = parts[0].toUpperCase();
            String variant = parts[1];
            int ya         = Integer.parseInt(parts[2]);
            int yb         = Integer.parseInt(parts[4]);
            boolean nonSpin   = "nonspin".equals(variant);
            boolean twoHanded = "twohanded".equals(variant);
            return new ComparisonKey(sys, nonSpin, twoHanded, sys, nonSpin, twoHanded, ya, yb);
        }

        // Spin year transition: {sys}-{yearA}-to-{yearB}
        // e.g. irc-2023-to-2024
        if (id.matches("[a-z]+-\\d{4}-to-\\d{4}"))
        {
            String[] parts = id.split("-");
            String sys = parts[0].toUpperCase();
            int ya = Integer.parseInt(parts[1]);
            int yb = Integer.parseInt(parts[3]);
            return new ComparisonKey(sys, false, false, sys, false, false, ya, yb);
        }

        // Cross-system with variant: {sysA}-vs-{sysB}-{variant}-{year}
        // e.g. orc-vs-irc-nonspin-2024, orc-vs-irc-twohanded-2024
        if (id.matches("[a-z]+-vs-[a-z]+-[a-z]+-\\d{4}"))
        {
            String[] parts = id.split("-");
            String sA = parts[0].toUpperCase();
            String sB = parts[2].toUpperCase();
            String variant = parts[3];
            int year = Integer.parseInt(parts[4]);
            boolean nonSpin = "nonspin".equals(variant);
            boolean twoHanded = "twohanded".equals(variant);
            return new ComparisonKey(sA, nonSpin, twoHanded, sB, nonSpin, twoHanded, year, year);
        }

        // Cross-system spin: {sysA}-vs-{sysB}-{year}
        // e.g. orc-vs-irc-2024
        if (id.matches("[a-z]+-vs-[a-z]+-\\d{4}"))
        {
            String[] parts = id.split("-");
            String sA = parts[0].toUpperCase();
            String sB = parts[2].toUpperCase();
            int year = Integer.parseInt(parts[3]);
            return new ComparisonKey(sA, false, false, sB, false, false, year, year);
        }

        // Variant comparison within same system: {sys}-{vA}-vs-{vB}-{year}
        // e.g. irc-spin-vs-nonspin-2024
        if (id.matches("[a-z]+-[a-z]+-vs-[a-z]+-\\d{4}"))
        {
            String[] parts = id.split("-");
            String sys = parts[0].toUpperCase();
            String vA = parts[1];
            String vB = parts[3];
            int year = Integer.parseInt(parts[4]);
            return new ComparisonKey(
                sys, "nonspin".equals(vA), "twohanded".equals(vA),
                sys, "nonspin".equals(vB), "twohanded".equals(vB),
                year, year);
        }

        return null;
    }
}
