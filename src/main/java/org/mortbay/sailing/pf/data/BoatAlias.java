package org.mortbay.sailing.pf.data;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.mortbay.sailing.pf.importer.IdGenerator;

/**
 * Another sail number and/or name an identity has been entered as: a misspelling, a sponsor's
 * name, a sail number with a country prefix. Either may be null (a sail-number-only or a
 * name-only alias), but not both. An alias belongs to one identity of one boat, so it is
 * scoped by that boat's design.
 * <p>
 * {@code designId}, if set, says entries name another design: results entered as this sail
 * number and name with that design (or with any design, for {@value #ANY_DESIGN}) are this
 * identity's, though the boat is of its own design. E.g. a misspelling merged in from a record
 * of another design, or a boat whose results always give the wrong design.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BoatAlias(String sailNumber, String name, String designId)
{
    /** The designId of an alias entered under any design. */
    public static final String ANY_DESIGN = "*";

    public BoatAlias(String sailNumber, String name)
    {
        this(sailNumber, name, null);
    }

    public BoatAlias
    {
        designId = designId == null || designId.isBlank() ? null : designId.trim();
        sailNumber = sailNumber == null || sailNumber.isBlank() ? null : sailNumber.trim();
        name = name == null || name.isBlank() ? null : name.trim();
        if (sailNumber == null && name == null)
            throw new IllegalArgumentException("An alias needs a sail number or a name");
    }

    /** The normalised sail number, or null. */
    public String normSailNumber()
    {
        return sailNumber == null ? null : IdGenerator.normaliseSailNumber(sailNumber);
    }

    /** The normalised name, or null. */
    public String normName()
    {
        return name == null ? null : IdGenerator.normaliseName(name);
    }

    /**
     * True if this alias names the same sail number, name and entered design as {@code other},
     * once normalised.
     */
    public boolean sameAs(BoatAlias other)
    {
        return other != null && Objects.equals(normSailNumber(), other.normSailNumber())
            && Objects.equals(normName(), other.normName()) && Objects.equals(designId, other.designId);
    }

    /** True if an entry giving {@code entryDesignId} is covered by this alias's entered design. */
    public boolean coversDesign(String entryDesignId)
    {
        return designId != null && (ANY_DESIGN.equals(designId) || designId.equals(entryDesignId));
    }
}
