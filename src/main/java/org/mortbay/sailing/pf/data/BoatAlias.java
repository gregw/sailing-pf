package org.mortbay.sailing.pf.data;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.mortbay.sailing.pf.importer.IdGenerator;

/**
 * Another sail number and/or name an identity has been entered as: a misspelling, a sponsor's
 * name, a sail number with a country prefix. Either may be null (a sail-number-only or a
 * name-only alias), but not both. An alias belongs to one identity of one boat, so it is
 * scoped by that boat's design.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record BoatAlias(String sailNumber, String name)
{
    public BoatAlias
    {
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

    /** True if this alias names the same sail number and name as {@code other}, once normalised. */
    public boolean sameAs(BoatAlias other)
    {
        return other != null && Objects.equals(normSailNumber(), other.normSailNumber())
            && Objects.equals(normName(), other.normName());
    }
}
