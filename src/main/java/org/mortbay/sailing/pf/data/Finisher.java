package org.mortbay.sailing.pf.data;

import java.time.Duration;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A boat's finishing result within a division. Raw layer — immutable, no derived fields.
 * Only finishers are imported; elapsedTime is always present.
 * <p>
 * {@code identityId} is the boat identity the result was entered as (e.g. the boat's former
 * name); it is what the race file records, as {@code boatId}. In memory, {@code boatId} is the
 * boat's canonical (current) identity ID, so that everything keyed by boat sees all its
 * results together; the store keeps it in step as boats are renamed and linked.
 * <p>
 * certificateNumber links this result to the Certificate (IRC/ORC/AMS) under which
 * the boat raced. Null for PHS finishers or when no handicap value was used.
 * <p>
 * twoHanded marks a two-handed result in a division that also holds crewed boats (e.g. a
 * CYCA offshore race, where everyone sails the same course at the same time). Elsewhere a
 * two-handed result is known by its division's name; see {@link EntryVariant#of}.
 */
@JsonSerialize(using = Finisher.Json.Writer.class)
@JsonDeserialize(using = Finisher.Json.Reader.class)
public record Finisher(
    String boatId,
    Duration elapsedTime,
    boolean nonSpinnaker,
    String certificateNumber, // nullable; cert number of the Certificate used for scoring
    boolean twoHanded,
    String identityId
) {
    public Finisher
    {
        if (identityId == null)
            identityId = boatId;
        if (boatId == null)
            boatId = identityId;
    }

    /** A result entered as, and counted for, boat identity {@code boatId}. */
    public Finisher(String boatId, Duration elapsedTime, boolean nonSpinnaker, String certificateNumber, boolean twoHanded)
    {
        this(boatId, elapsedTime, nonSpinnaker, certificateNumber, twoHanded, boatId);
    }

    /** A crewed (not two-handed) result. */
    public Finisher(String boatId, Duration elapsedTime, boolean nonSpinnaker, String certificateNumber)
    {
        this(boatId, elapsedTime, nonSpinnaker, certificateNumber, false);
    }

    /** The race-file form: {@code boatId} is the identity entered as; twoHanded only when set. */
    static final class Json
    {
        static final class Writer extends JsonSerializer<Finisher>
        {
            @Override
            public void serialize(Finisher f, JsonGenerator g, SerializerProvider p) throws IOException
            {
                g.writeStartObject();
                g.writeStringField("boatId", f.identityId());
                g.writeFieldName("elapsedTime");
                p.defaultSerializeValue(f.elapsedTime(), g);
                g.writeBooleanField("nonSpinnaker", f.nonSpinnaker());
                g.writeStringField("certificateNumber", f.certificateNumber());
                if (f.twoHanded())
                    g.writeBooleanField("twoHanded", true);
                g.writeEndObject();
            }
        }

        static final class Reader extends JsonDeserializer<Finisher>
        {
            @Override
            public Finisher deserialize(JsonParser parser, DeserializationContext ctx) throws IOException
            {
                JsonNode n = parser.readValueAsTree();
                String id = n.hasNonNull("boatId") ? n.get("boatId").asText() : null;
                Duration elapsed = n.hasNonNull("elapsedTime")
                    ? ctx.readTreeAsValue(n.get("elapsedTime"), Duration.class) : null;
                String cert = n.hasNonNull("certificateNumber") ? n.get("certificateNumber").asText() : null;
                return new Finisher(id, elapsed, n.path("nonSpinnaker").asBoolean(false), cert,
                    n.path("twoHanded").asBoolean(false), id);
            }
        }
    }

    /**
     * This result, entered as another identity (e.g. merging a misspelling into the right
     * name, or an identity renamed): both the identity and the boat change.
     */
    public Finisher withBoatId(String id)
    {
        return new Finisher(id, elapsedTime, nonSpinnaker, certificateNumber, twoHanded, id);
    }

    /** This result, still entered as the same identity, counted for boat {@code canonicalId}. */
    public Finisher withCanonicalBoatId(String canonicalId)
    {
        return new Finisher(canonicalId, elapsedTime, nonSpinnaker, certificateNumber, twoHanded, identityId);
    }

    /** This result, with its spinnaker flag set. */
    public Finisher withNonSpinnaker(boolean ns)
    {
        return new Finisher(boatId, elapsedTime, ns, certificateNumber, twoHanded, identityId);
    }

    @Override
    public String toString()
    {
        return "Finisher{" +
            "boatId='" + boatId + '\'' +
            (identityId.equals(boatId) ? "" : ", identityId='" + identityId + '\'') +
            ", elapsedTime=" + elapsedTime +
            ", nonSpinnaker=" + nonSpinnaker +
            ", certificateNumber='" + certificateNumber + '\'' +
            (twoHanded ? ", twoHanded=true" : "") +
            '}';
    }
}
