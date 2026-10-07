package org.mortbay.sailing.pf.data;

/**
 * The handicap variant a finisher raced under: spinnaker, non-spinnaker or two-handed.
 * Two-handed is a variant of a spinnaker handicap — a two-handed boat racing without a
 * spinnaker takes a non-spinnaker handicap.
 * <p>
 * {@link #of} is the single place a finisher's variant is decided, for the optimiser and
 * the REST API alike.
 */
public enum EntryVariant
{
    SPIN("spin"),
    NON_SPIN("nonSpin"),
    TWO_HANDED("twoHanded");

    private final String code;

    EntryVariant(String code)
    {
        this.code = code;
    }

    /** The name used by the REST API and the frontend. */
    public String code()
    {
        return code;
    }

    /**
     * Decides a finisher's variant, in order:
     * <ol>
     *   <li>a boat whose design has no spinnaker (e.g. cat-rigged) is never upgraded by a
     *       {@link SeriesType#SPIN} or {@link SeriesType#TWO_HANDED} series — its spin and
     *       non-spin handicaps are the same;</li>
     *   <li>a {@link SeriesType#NON_SPIN} series makes everyone non-spinnaker;</li>
     *   <li>a two-handed division name makes its finishers two-handed — even in a
     *       {@link SeriesType#SPIN} series, since two-handed is a spinnaker handicap;</li>
     *   <li>a {@link SeriesType#SPIN} series makes everyone else spinnaker;</li>
     *   <li>a non-spinnaker division name makes its finishers non-spinnaker;</li>
     *   <li>in a {@link SeriesType#TWO_HANDED} series spinnaker entries are two-handed and
     *       non-spinnaker entries stay non-spinnaker;</li>
     *   <li>otherwise the finisher's own flag.</li>
     * </ol>
     *
     * @param raceType     the race's combined series type ({@code DataStore.raceSeriesType}),
     *                     or null when it is mixed or unknown
     * @param noSpinDesign true if the finisher's boat is of a no-spinnaker design
     *                     ({@code DataStore.isBoatNoSpinnakerDesign})
     */
    public static EntryVariant of(SeriesType raceType, Division div, Finisher f, boolean noSpinDesign)
    {
        if (noSpinDesign && (raceType == SeriesType.SPIN || raceType == SeriesType.TWO_HANDED))
            raceType = null;
        if (raceType == SeriesType.NON_SPIN)
            return NON_SPIN;
        String divName = div.name();
        if (SeriesType.containsTwoHandedKeyword(divName))
            return TWO_HANDED;
        if (raceType == SeriesType.SPIN)
            return SPIN;
        if (SeriesType.containsNonSpinKeyword(divName))
            return NON_SPIN;
        if (f.nonSpinnaker())
            return NON_SPIN;
        return raceType == SeriesType.TWO_HANDED ? TWO_HANDED : SPIN;
    }
}
