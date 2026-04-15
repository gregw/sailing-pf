package org.mortbay.sailing.hpf.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Certificate;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Design;
import org.mortbay.sailing.hpf.data.Division;
import org.mortbay.sailing.hpf.data.Finisher;
import org.mortbay.sailing.hpf.data.Loadable;
import org.mortbay.sailing.hpf.data.Maker;
import org.mortbay.sailing.hpf.data.Race;
import org.mortbay.sailing.hpf.importer.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.util.StringUtil.isBlank;
import static org.eclipse.jetty.util.StringUtil.isNotBlank;

/**
 * Reads and writes the data store.
 * <p>
 * Layout:
 * {root}/races/{clubId}/{seriesSlug}/{raceId}.json  — one file per Race, in subdirectories
 * {root}/boats/{boatId}.json       — one file per Boat (embeds certificates)
 * {root}/designs/{designId}.json   — one file per Design
 * {root}/clubs/{clubId}.json       — one file per Club (embeds series)
 * {root}/catalogue/makers.json     — all Makers (small stable collection)
 * <p>
 * Call {@link #start()} to load all data into memory, {@link #save()} to flush dirty
 * entities to disk, and {@link #stop()} to flush and clear the in-memory maps.
 */
public class DataStore
{
    private static final Logger LOG = LoggerFactory.getLogger(DataStore.class);
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    private final Path root;
    private final Path configDir;
    private final Path racesDir;
    private final Path boatsDir;
    private final Path designsDir;
    private final Path clubsDir;
    private final Path catalogueDir;

    // In-memory maps — null before start()
    private Map<String, Race> races;
    private Map<String, Boat> boats;
    private Map<String, Design> designs;
    private Map<String, Club> clubs;      // persisted entities (from disk / putClub)
    private Map<String, Club> clubSeed;   // lookup-only stubs from clubs.yaml; never written to disk
    private Aliases.Loaded aliases; // lookup-only alias data from aliases.yaml; never written to disk
    private Designs.DesignCatalogue designCatalogue; // lookup-only exclusion list from design.yaml
    private ClubLoader.ClubCatalogue clubCatalogue;  // lookup-only club overrides from clubs.yaml
    private List<Maker> makers;
    private boolean makersDirty;

    // Mutable exclusion sets — persisted to config/exclusions.json, managed via admin UI
    private final Set<String> excludedBoatIds          = new LinkedHashSet<>();
    private final Set<String> excludedDesignOverrideIds = new LinkedHashSet<>();
    private final Set<String> excludedRaceIds           = new LinkedHashSet<>();

    // Invalidation listener for derived data caches
    private volatile InvalidationListener invalidationListener;

    /**
     * Listener notified when raw entities are added, updated, or removed.
     * Used by AnalysisCache to invalidate per-entity derived data.
     */
    public interface InvalidationListener
    {
        void onBoatChanged(String boatId);
        void onDesignChanged(String designId);
        void onRaceChanged(String raceId);
        void onClubChanged(String clubId);
        void onAllChanged();
    }

    public void setInvalidationListener(InvalidationListener listener)
    {
        this.invalidationListener = listener;
    }

    public DataStore(Path root)
    {
        this.root = root;
        this.configDir = root.resolve("config");
        this.racesDir = root.resolve("imported/races");
        this.boatsDir = root.resolve("imported/boats");
        this.designsDir = root.resolve("imported/designs");
        this.clubsDir = root.resolve("imported/clubs");
        this.catalogueDir = root.resolve("catalogue");
    }

    private boolean designNameMatches(Design candidate, String normIncoming)
    {
        if (candidate.id().equals(normIncoming))
            return true;
        return candidate.aliases().stream().anyMatch(a -> IdGenerator.normaliseDesignName(a).equals(normIncoming));
    }

    // --- Lifecycle ---

    /**
     * Resolves the data root directory using the standard lookup chain:
     * <ol>
     *   <li>First element of {@code args}, if provided</li>
     *   <li>{@code HPF_DATA} environment variable</li>
     *   <li>{@code ./hpf-data} in the current working directory, if it exists</li>
     *   <li>{@code $HOME/.hpf-data} as the default fallback</li>
     * </ol>
     */
    public Path dataRoot()
    {
        return root;
    }

    public Path configDir()
    {
        return configDir;
    }

    public static Path resolveDataRoot(String[] args)
    {
        if (args.length > 0)
            return Path.of(args[0]);

        String env = System.getenv("HPF_DATA");
        if (env != null && !env.isBlank())
            return Path.of(env);

        Path local = Path.of("hpf-data");
        if (Files.isDirectory(local))
            return local;

        return Path.of(System.getProperty("user.home"), ".hpf-data");
    }

    // --- Read accessors (require started) ---

    public Map<String, Boat> boats()
    {
        requireStarted();
        return Collections.unmodifiableMap(boats);
    }

    public Map<String, Club> clubs()
    {
        requireStarted();
        return Collections.unmodifiableMap(clubs);
    }

    public Map<String, Club> clubSeed()
    {
        requireStarted();
        return Collections.unmodifiableMap(clubSeed);
    }

    public Map<String, Design> designs()
    {
        requireStarted();
        return Collections.unmodifiableMap(designs);
    }

    /** Convenience overload for tests — no date, no source. */
    public Boat findOrCreateBoat(String sailNo, String name, String rawDesign)
    {
        return findOrCreateBoat(sailNo, name, rawDesign, null, "test");
    }

    /**
     * Finds or creates a boat, resolving the design internally from the raw design string.
     * <p>
     * Algorithm:
     * <ol>
     *   <li>Search for an existing boat by sail number and name (ignoring design)</li>
     *   <li>If found and rawDesign is null or fuzzy/alias-matches the boat's design → use it</li>
     *   <li>If found and the boat has no design → upgrade with the resolved design</li>
     *   <li>If found but design mismatches → skip and keep looking; if a design override is
     *       active, migrate the boat to the override design</li>
     *   <li>If no existing boat found → resolve design (fuzzy/alias match or create), create boat</li>
     * </ol>
     * Sources are recorded as {@code "source:rawDesign"} for traceability, or just
     * {@code "source"} when rawDesign is null/blank.
     *
     * @param rawSailNo raw sail number
     * @param rawName   yacht name
     * @param rawDesign raw design/class name string (nullable); resolved internally
     * @param date      the date of the event this boat is in (or null if not known).
     * @param source    importer source tag, e.g. "SailSys"
     * @return the found or created Boat
     */
    public Boat findOrCreateBoat(String rawSailNo, String rawName, String rawDesign, LocalDate date, String source)
    {
        // Build enriched source entry for traceability
        String sourceDesign = buildSourceEntry(source, rawDesign);

        String sailNo = IdGenerator.normaliseSailNumber(rawSailNo);
        String name = IdGenerator.normaliseName(rawName);

        Aliases.BoatMatch aliased = aliases.lookupBoat(sailNo, name).orElse(null);
        if (aliased != null)
        {
            sourceDesign += ":" + sailNo + " " + name + "=>" + aliased;
            sailNo = aliased.normSailNumber() != null ? aliased.normSailNumber() : sailNo;
            name = aliased.normName() != null ? aliased.normName() : name;
            rawName = aliased.canonicalDisplayName() != null ? aliased.canonicalDisplayName() : rawName;
        }

        String designId = aliases.resolveDesignAlias(IdGenerator.normaliseName(rawDesign));
        String overrideDesignId = designCatalogue.resolveDesignOverride(sailNo, name, date);
        if (overrideDesignId != null && !designId.equalsIgnoreCase(overrideDesignId))
        {
            LOG.info("Boat {}/{}: design overridden {} → {}", sailNo, name,
                rawDesign == null ? "null" : rawDesign, overrideDesignId);
            sourceDesign += "->" + designId;
            designId = overrideDesignId;
            rawDesign = designs.containsKey(designId) ? designs.get(designId).canonicalName() : overrideDesignId;
        }

        final String normSailNo = sailNo;
        final String normName = name;

        // Phase 1: Search for existing boat by sail+name, checking design compatibility.
        List<Boat> matches = new ArrayList<>();
        for (Boat candidate : boats.values())
        {
            if (!normSailNo.equalsIgnoreCase(candidate.sailNumber()))
                continue;
            if (!normName.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                continue;

            // Found a sail+name match, let's consider the designs.

            if (isBlank(designId))
            {
                // We don't have a design, so if the candidate does, so it is a potential match...
                // but may be one of many.
                if (isNotBlank(candidate.designId()))
                    matches.add(candidate);
                else
                    // A matching boat without a design must be the only one.
                    return candidate;
            }
            else if (isNotBlank(candidate.designId()))
            {
                // They are different designs, so they are different boats.
                if (!Objects.equals(designId, candidate.designId()))
                    continue;

                // they are the same design, so this is the boat.
                return candidate;
            }
            else
            {
                // We have a design, but the candidate does not, so we need to merge the boats
                Design design = isNotBlank(rawDesign) ? findOrCreateDesign(rawDesign) : findOrCreateDesign(designId);
                String resolvedDesignId = design != null ? design.id() : designId;
                String boatId = IdGenerator.generateBoatId(normSailNo, normName, design);

                // Preserve the existing display name to avoid case-variant flip-flopping
                Boat upgraded = new Boat(
                    boatId,
                    normSailNo,
                    candidate.name(),
                    resolvedDesignId,
                    candidate.clubId(),
                    candidate.certificates(),
                    addSource(candidate.sources(), sourceDesign), Instant.now(), candidate.loadedAt());
                String oldId = candidate.id();
                removeBoat(oldId);
                putBoat(upgraded);
                rewriteFinisherBoatId(oldId, boatId);
                LOG.info("Upgraded boat {} → {} (updated finisher references)", oldId, boatId);

                // return the merged boat because there cannot be two candidates without a design.
                return upgraded;
            }
        }

        // If we have no matches, then create the new boat
        if (matches.isEmpty())
        {
            Design design = isNotBlank(rawDesign) ? findOrCreateDesign(rawDesign) : findOrCreateDesign(designId);
            String newBoatId = IdGenerator.generateBoatId(normSailNo, normName, design);

            // Defensive: if a boat with this ID already exists (e.g. due to name case
            // variants that normalise to the same ID), return it instead of overwriting.
            Boat existingById = boats.get(newBoatId);
            if (existingById != null)
            {
                LOG.debug("Boat ID {} already exists (existing name='{}', incoming name='{}'), returning existing",
                    newBoatId, existingById.name(), rawName);
                return existingById;
            }

            String newClubId = clubCatalogue.resolveClubOverride(normSailNo, rawName);
            if (newClubId != null)
                LOG.info("Boat {}/{}: club override → {}", normSailNo, rawName, newClubId);
            Boat newBoat = new Boat(
                newBoatId,
                normSailNo,
                rawName,
                design != null ? design.id() : null,
                newClubId,
                List.of(),
                List.of(sourceDesign),
                Instant.now(),
                null);
            putBoat(newBoat);
            return newBoat;
        }

        // If we only have 1 match, then let's assume we are of the same design
        if (matches.size() == 1)
            return matches.getFirst();

        // We have multiple boats with the same sailNo, name but different designs, so we don't know which one this is?
        LOG.warn("Ambiguous boat match: sailNo={} name={} design={} — {} candidates with different designs",
            normSailNo, normName, designId, matches.size());
        return null;
    }

    // --- findOrCreateBoat helpers ---

    private static String buildSourceEntry(String source, String rawDesign)
    {
        if (source == null) return ":" + rawDesign;
        return rawDesign != null && !rawDesign.isBlank() ? source + ":" + rawDesign : source;
    }

    /** Finds or creates a design by class name — used internally by findOrCreateBoat. */
    private Design findOrCreateDesign(String className)
    {
        if (className == null || className.isBlank())
            return null;
        String designId = IdGenerator.normaliseDesignName(className);
        if (designCatalogue.isIgnored(designId))
            return null;
        Design design = designs.get(designId);
        if (design != null)
            return design;
        for (Design d : designs.values())
        {
            if (designNameMatches(d, designId))
                return d;
        }

        // Check the alias seed for a known equivalence
        String canonicalId = aliases.resolveDesignAlias(designId);
        if (canonicalId != null)
        {
            Design existing = designs.get(canonicalId);
            if (existing != null)
                return existing;
            // Canonical design not yet in store — create it using the seed's canonical name
            String seedName = aliases.designCanonicalName(canonicalId);
            design = new Design(canonicalId, seedName != null ? seedName : className.trim(), List.of(), List.of(), null, null);
            putDesign(design);
            if (designCatalogue.isExcluded(canonicalId))
                LOG.info("Design {} is excluded (dinghy/OTB class)", canonicalId);
            return design;
        }

        design = new Design(designId, className.trim(), List.of(), List.of(), null, null);
        putDesign(design);
        if (designCatalogue.isExcluded(designId))
            LOG.info("Design {} is excluded (dinghy/OTB class)", designId);
        return design;
    }

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }

    /**
     * Finds a club by short name, long name, or alias, ignoring state.
     * First tries an exact short name match; if that finds nothing, falls back to matching
     * against long name or aliases (handles full-name club fields from BWPS etc.).
     * If {@code longName} is provided and the result is still ambiguous, narrows to clubs
     * whose long name matches (case-insensitive) as a tiebreaker.
     * Returns the club if there is exactly one match; null with a log if none or still ambiguous.
     */
    public Club findUniqueClubByShortName(String shortName, String longName, String context)
    {
        requireStarted();
        List<Club> allClubs = Stream.concat(
                clubs.values().stream(),
                clubSeed.values().stream().filter(c -> !clubs.containsKey(c.id())))
            .filter(c -> !isClubExcluded(c.id()))
            .toList();

        // Primary: exact short name match
        List<Club> matches = allClubs.stream()
            .filter(c -> shortName.equalsIgnoreCase(c.shortName()))
            .toList();

        // Fallback: long name or alias match (handles full-name club fields from BWPS etc.)
        if (matches.isEmpty())
        {
            matches = allClubs.stream()
                .filter(c -> shortName.equalsIgnoreCase(c.longName())
                          || c.aliases().stream().anyMatch(shortName::equalsIgnoreCase))
                .toList();
        }

        // Fallback: compound name (e.g. "CYCA/RPEYC") — try each slash-separated token in order
        if (matches.isEmpty() && shortName.contains("/"))
        {
            for (String token : shortName.split("/"))
            {
                String t = token.trim();
                if (t.isBlank())
                    continue;
                matches = allClubs.stream()
                    .filter(c -> t.equalsIgnoreCase(c.shortName())
                              || t.equalsIgnoreCase(c.longName())
                              || c.aliases().stream().anyMatch(t::equalsIgnoreCase))
                    .toList();
                if (!matches.isEmpty())
                    break;
            }
        }

        if (matches.isEmpty())
        {
            LOG.info("No club found for name={} ({})", shortName, context);
            return null;
        }
        if (matches.size() > 1 && longName != null && !longName.isBlank())
        {
            List<Club> narrowed = matches.stream()
                .filter(c -> longName.equalsIgnoreCase(c.longName()))
                .toList();
            if (narrowed.size() == 1)
                return narrowed.getFirst();
            // Narrowing didn't resolve it — fall through to ambiguity log below
            matches = narrowed.isEmpty() ? matches : narrowed;
        }
        if (matches.size() > 1)
        {
            LOG.warn("Ambiguous club name={} — {} matches ({}); clubId not set ({})",
                shortName, matches.size(),
                matches.stream().map(c -> c.id() + "/" + c.state()).toList(),
                context);
            return null;
        }
        return matches.getFirst();
    }

    /**
     * Finds a club by its short name and state code (e.g. "MYC", "NSW").
     * Returns null and logs an error if no match is found or if the match is ambiguous.
     * State matching is exact (case-insensitive); a blank state matches only clubs
     * whose state is also blank or null.
     */
    public Club findClubByShortName(String shortName, String state, String context)
    {
        requireStarted();
        boolean blankState = state == null || state.isBlank();
        // Search persisted clubs first; fall back to seed stubs
        List<Club> matches = Stream.concat(
                clubs.values().stream(),
                clubSeed.values().stream().filter(c -> !clubs.containsKey(c.id())))
            .filter(c -> shortName.equalsIgnoreCase(c.shortName()))
            .filter(c -> blankState
                ? (c.state() == null || c.state().isBlank())
                : state.equalsIgnoreCase(c.state()))
            .toList();
        if (matches.isEmpty())
        {
            LOG.error("Unknown club shortName={} state={} ({})", shortName, state, context);
            return null;
        }
        if (matches.size() > 1)
        {
            LOG.error("Ambiguous club shortName={} state={} — {} matches ({})",
                shortName, state, matches.size(), context);
            return null;
        }
        return matches.getFirst();
    }

    // --- Write mutators (require started; loadedAt = null → always written by save()) ---

    public void putBoat(Boat boat)
    {
        requireStarted();
        boats.put(boat.id(), boat);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onBoatChanged(boat.id());
    }

    public void putClub(Club club)
    {
        requireStarted();
        clubs.put(club.id(), club);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onClubChanged(club.id());
    }

    public void putDesign(Design design)
    {
        requireStarted();
        designs.put(design.id(), design);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onDesignChanged(design.id());
    }

    public void putRace(Race race)
    {
        requireStarted();
        races.put(race.id(), race);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onRaceChanged(race.id());
    }

    public Map<String, Race> races()
    {
        requireStarted();
        return Collections.unmodifiableMap(races);
    }

    public void removeBoat(String id)
    {
        requireStarted();
        Boat existing = boats.remove(id);
        if (existing != null)
        {
            try
            {
                Files.deleteIfExists(boatsDir.resolve(id + ".json"));
            }
            catch (IOException e)
            {
                LOG.warn("Could not delete boat file {}: {}", id, e.getMessage());
            }
            InvalidationListener l = invalidationListener;
            if (l != null) l.onBoatChanged(id);
        }
    }

    public void removeDesign(String id)
    {
        requireStarted();
        Design existing = designs.remove(id);
        if (existing != null)
        {
            try
            {
                Files.deleteIfExists(designsDir.resolve(id + ".json"));
            }
            catch (IOException e)
            {
                LOG.warn("Could not delete design file {}: {}", id, e.getMessage());
            }
            InvalidationListener l = invalidationListener;
            if (l != null) l.onDesignChanged(id);
        }
    }

    /**
     * Merges a set of duplicate designs into one canonical design.
     * <ul>
     *   <li>All canonical names and aliases from the merged-away designs are added to the
     *       keep design's aliases list.</li>
     *   <li>Maker IDs are merged (duplicates dropped, order preserved).</li>
     *   <li>All {@link Boat} records whose designId references a merged-away design ID
     *       are updated to use the keep design ID.  Boat IDs are left unchanged.</li>
     *   <li>The merged-away design files are deleted from disk.</li>
     * </ul>
     * Callers must call {@link #save()} after this method to persist the changes.
     *
     * @param keepId    ID of the canonical design to keep
     * @param mergeIds  IDs of the designs to merge into keepId (must not include keepId)
     * @return summary of the number of boat records updated
     */
    public DesignMergeResult mergeDesigns(String keepId, List<String> mergeIds)
    {
        requireStarted();
        Design keepDesign = designs.get(keepId);
        if (keepDesign == null)
            throw new IllegalArgumentException("Keep design not found: " + keepId);

        List<Design> toMerge = new ArrayList<>();
        for (String id : mergeIds)
        {
            Design d = designs.get(id);
            if (d == null)
                throw new IllegalArgumentException("Merge design not found: " + id);
            toMerge.add(d);
        }

        // Build merged aliases — add canonical names and existing aliases from merged-away designs
        Set<String> allAliases = new LinkedHashSet<>(keepDesign.aliases());
        for (Design md : toMerge)
        {
            if (!md.canonicalName().equalsIgnoreCase(keepDesign.canonicalName()))
                allAliases.add(md.canonicalName());
            allAliases.addAll(md.aliases());
        }
        allAliases.removeIf(a -> a.equalsIgnoreCase(keepDesign.canonicalName()));

        Set<String> allSources = new LinkedHashSet<>(keepDesign.sources());
        for (Design md : toMerge)
            allSources.addAll(md.sources());
        putDesign(new Design(keepDesign.id(), keepDesign.canonicalName(),
            List.copyOf(allAliases), List.copyOf(allSources), Instant.now(), null));

        // Repoint all boats whose designId references a merged-away design; fix boat IDs too
        Set<String> mergeIdSet = new HashSet<>(mergeIds);
        Map<String, String> boatIdRemap = new LinkedHashMap<>();
        int updatedBoats = 0;
        for (Boat boat : List.copyOf(boats.values()))
        {
            if (!mergeIdSet.contains(boat.designId()))
                continue;
            String newId = IdGenerator.generateBoatId(boat.sailNumber(), boat.name(), keepDesign);
            Boat toWrite;
            if (!newId.equals(boat.id()))
            {
                removeBoat(boat.id());
                boatIdRemap.put(boat.id(), newId);

                Boat existingAtNewId = boats.get(newId);
                if (existingAtNewId != null)
                {
                    // Collision: merge the renamed boat into the existing one
                    Map<String, Certificate> certMap = new LinkedHashMap<>();
                    for (Certificate c : existingAtNewId.certificates()) certMap.put(certKey(c), c);
                    for (Certificate c : boat.certificates()) certMap.putIfAbsent(certKey(c), c);

                    Set<String> mergedSources = new LinkedHashSet<>(existingAtNewId.sources());
                    mergedSources.addAll(boat.sources());

                    String clubId = existingAtNewId.clubId() != null ? existingAtNewId.clubId() : boat.clubId();
                    toWrite = new Boat(newId, existingAtNewId.sailNumber(), existingAtNewId.name(), keepId,
                        clubId, List.copyOf(certMap.values()), List.copyOf(mergedSources), Instant.now(), null);
                }
                else
                {
                    toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                        boat.clubId(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
                }
            }
            else
            {
                toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                    boat.clubId(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
            }
            putBoat(toWrite);
            updatedBoats++;
        }

        // Repoint race finishers for remapped boat IDs
        int updatedRaces = 0;
        int updatedFinishers = 0;
        if (!boatIdRemap.isEmpty())
        {
            for (Race race : List.copyOf(races.values()))
            {
                boolean changed = false;
                List<Division> newDivisions = new ArrayList<>();
                for (Division div : race.divisions())
                {
                    List<Finisher> newFinishers = new ArrayList<>();
                    for (Finisher f : div.finishers())
                    {
                        String remapped = boatIdRemap.get(f.boatId());
                        if (remapped != null)
                        {
                            newFinishers.add(new Finisher(remapped, f.elapsedTime(), f.nonSpinnaker(), f.certificateNumber()));
                            changed = true;
                            updatedFinishers++;
                        }
                        else
                            newFinishers.add(f);
                    }
                    newDivisions.add(new Division(div.name(), newFinishers));
                }
                if (changed)
                {
                    putRace(new Race(race.id(), race.clubId(), race.seriesIds(), race.date(),
                        race.number(), race.name(),
                        newDivisions, race.source(), race.lastUpdated(), null));
                    updatedRaces++;
                }
            }
        }

        // Delete merged-away design files
        for (Design md : toMerge)
            removeDesign(md.id());

        LOG.info("mergeDesigns: kept={} merged={} updatedBoats={} updatedRaces={} updatedFinishers={}",
            keepId, mergeIds, updatedBoats, updatedRaces, updatedFinishers);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return new DesignMergeResult(updatedBoats, updatedRaces, updatedFinishers);
    }

    /**
     * Returns true if the given design ID is configured as excluded (dinghy/OTB class).
     * The HPF optimiser uses this to skip excluded designs during calculation.
     * Raw records are still created — exclusion is a configuration concern, not a data concern.
     * Checks both the static design.yaml catalogue and any UI-driven overrides.
     */
    public boolean isDesignExcluded(String designId)
    {
        requireStarted();
        return designCatalogue.isExcluded(designId) || excludedDesignOverrideIds.contains(designId);
    }

    /**
     * Returns true if the club is excluded from analysis (e.g. multihull club, non-Australian).
     * Checks the persisted club record first, then falls back to the seed.
     */
    public boolean isClubExcluded(String clubId)
    {
        requireStarted();
        Club club = clubs.get(clubId);
        if (club != null)
            return club.excluded();
        Club seed = clubSeed.get(clubId);
        return seed != null && seed.excluded();
    }

    /**
     * Returns true if every club matching the given short name (or long name / alias) is
     * excluded. Used by importers to decide whether to skip races from an excluded club
     * when {@link #findUniqueClubByShortName} returns null because all candidates are excluded.
     */
    public boolean isClubNameExcluded(String shortName)
    {
        requireStarted();
        if (shortName == null || shortName.isBlank())
            return false;
        String lower = shortName.toLowerCase();
        List<Club> allIncludingExcluded = Stream.concat(
                clubs.values().stream(),
                clubSeed.values().stream().filter(c -> !clubs.containsKey(c.id())))
            .filter(c -> lower.equalsIgnoreCase(c.shortName())
                      || lower.equalsIgnoreCase(c.longName())
                      || c.aliases().stream().anyMatch(lower::equalsIgnoreCase))
            .toList();
        return !allIncludingExcluded.isEmpty()
            && allIncludingExcluded.stream().allMatch(c -> isClubExcluded(c.id()));
    }

    /**
     * Toggles the excluded flag on a club. If the club doesn't yet have a persisted record,
     * one is created from the seed.
     */
    public void setClubExcluded(String clubId, boolean excluded)
    {
        requireStarted();
        Club club = clubs.get(clubId);
        if (club == null)
        {
            Club seed = clubSeed.get(clubId);
            if (seed == null)
                throw new IllegalArgumentException("Unknown club: " + clubId);
            club = seed;
        }
        putClub(club.withExcluded(excluded));
        save();
    }

    /** Returns true if the boat has been manually excluded from analysis via the admin UI. */
    public boolean isBoatExcluded(String boatId)
    {
        requireStarted();
        return excludedBoatIds.contains(boatId);
    }

    /** Returns true if the race has been manually excluded from analysis via the admin UI. */
    public boolean isRaceExcluded(String raceId)
    {
        requireStarted();
        return excludedRaceIds.contains(raceId);
    }

    public void setBoatExcluded(String id, boolean excluded)
    {
        requireStarted();
        if (excluded) excludedBoatIds.add(id);
        else excludedBoatIds.remove(id);
        saveExclusions();
    }

    public void setDesignExcluded(String id, boolean excluded)
    {
        requireStarted();
        if (excluded) excludedDesignOverrideIds.add(id);
        else excludedDesignOverrideIds.remove(id);
        saveExclusions();
    }

    public void setRaceExcluded(String id, boolean excluded)
    {
        requireStarted();
        if (excluded) excludedRaceIds.add(id);
        else excludedRaceIds.remove(id);
        saveExclusions();
    }

    private static class ExclusionsFile
    {
        public Set<String> boats   = new LinkedHashSet<>();
        public Set<String> designs = new LinkedHashSet<>();
        public Set<String> races   = new LinkedHashSet<>();
    }

    private void loadExclusions()
    {
        Path file = configDir.resolve("exclusions.json");
        if (!Files.exists(file))
            return;
        try
        {
            ExclusionsFile ef = MAPPER.readValue(file.toFile(), ExclusionsFile.class);
            if (ef.boats   != null) excludedBoatIds.addAll(ef.boats);
            if (ef.designs != null) excludedDesignOverrideIds.addAll(ef.designs);
            if (ef.races   != null) excludedRaceIds.addAll(ef.races);
            LOG.info("Loaded exclusions: {} boats, {} designs, {} races",
                excludedBoatIds.size(), excludedDesignOverrideIds.size(), excludedRaceIds.size());
        }
        catch (Exception e)
        {
            LOG.warn("Failed to load exclusions.json: {}", e.getMessage());
        }
    }

    private void saveExclusions()
    {
        Path file = configDir.resolve("exclusions.json");
        ExclusionsFile ef = new ExclusionsFile();
        ef.boats   = new LinkedHashSet<>(excludedBoatIds);
        ef.designs = new LinkedHashSet<>(excludedDesignOverrideIds);
        ef.races   = new LinkedHashSet<>(excludedRaceIds);
        try
        {
            Files.createDirectories(configDir);
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), ef);
        }
        catch (Exception e)
        {
            LOG.warn("Failed to save exclusions.json: {}", e.getMessage());
        }
    }

    /**
     * Reloads the alias seed from disk.  Called after aliases.yaml has been updated
     * (e.g. following a merge operation) so that subsequent imports honour the new entries.
     */
    public void reloadAliases()
    {
        requireStarted();
        aliases = Aliases.load(configDir);
    }

    /**
     * Writes a boat design override to design.yaml and reloads the design catalogue.
     */
    public void addDesignOverride(String sailNumber, String name, String designId, String canonicalName)
    {
        requireStarted();
        Designs.addDesignOverride(configDir, sailNumber, name, designId, canonicalName);
        reloadDesignCatalogue();
    }

    /**
     * Reloads the design catalogue from disk.
     */
    public void reloadDesignCatalogue()
    {
        requireStarted();
        designCatalogue = Designs.load(configDir);
    }

    /**
     * Writes a boat club override to clubs.yaml and reloads the club catalogue.
     */
    public void addClubOverride(String sailNumber, String name, String clubId)
    {
        requireStarted();
        ClubLoader.addClubOverride(configDir, sailNumber, name, clubId);
        reloadClubCatalogue();
    }

    /**
     * Reloads the club catalogue from disk.
     */
    public void reloadClubCatalogue()
    {
        requireStarted();
        clubCatalogue = ClubLoader.loadCatalogue(configDir);
    }

    /**
     * Returns the alias list for a boat from the alias seed.
     */
    public List<Aliases.SailNumberName> boatAliases(String normSailNumber, String normName)
    {
        requireStarted();
        return aliases.boatAliases(normSailNumber, normName);
    }

    /**
     * Result of a {@link #mergeBoats} operation.
     */
    public record MergeResult(int updatedRaces, int updatedFinishers) {}

    /**
     * Result of a {@link #mergeDesigns} operation.
     */
    public record DesignMergeResult(int updatedBoats, int updatedRaces, int updatedFinishers) {}

    /**
     * Merges a set of duplicate boats into one canonical boat.
     * <ul>
     *   <li>All names and aliases from the merged-away boats are added to the keep boat's
     *       aliases list.</li>
     *   <li>Certificates are merged (duplicates by system+year+variant are dropped).</li>
     *   <li>All {@link Finisher} records in all races that reference a merged-away boat ID
     *       are repointed to the keep boat ID.</li>
     *   <li>The merged-away boat files are deleted from disk.</li>
     * </ul>
     * Callers must call {@link #save()} after this method to persist the changes.
     *
     * @param keepId    ID of the canonical boat to keep
     * @param mergeIds  IDs of the boats to merge into keepId (must not include keepId)
     * @return summary of the number of races and finisher records updated
     */
    public MergeResult mergeBoats(String keepId, List<String> mergeIds)
    {
        requireStarted();
        Boat keepBoat = boats.get(keepId);
        if (keepBoat == null)
            throw new IllegalArgumentException("Keep boat not found: " + keepId);

        List<Boat> toMerge = new ArrayList<>();
        for (String id : mergeIds)
        {
            Boat b = boats.get(id);
            if (b == null)
                throw new IllegalArgumentException("Merge boat not found: " + id);
            toMerge.add(b);
        }

        // Merge certificates — deduplicate by system+year+variant; keep boat's certs take priority
        Map<String, Certificate> certMap = new LinkedHashMap<>();
        for (Certificate c : keepBoat.certificates())
            certMap.put(certKey(c), c);
        for (Boat mb : toMerge)
            for (Certificate c : mb.certificates())
                certMap.putIfAbsent(certKey(c), c);

        // Prefer a non-null designId and clubId from any of the boats
        String designId = keepBoat.designId() != null ? keepBoat.designId()
            : toMerge.stream().map(Boat::designId).filter(Objects::nonNull).findFirst().orElse(null);
        String clubId = keepBoat.clubId() != null ? keepBoat.clubId()
            : toMerge.stream().map(Boat::clubId).filter(Objects::nonNull).findFirst().orElse(null);

        Set<String> mergedSources = new LinkedHashSet<>(keepBoat.sources());
        for (Boat mb : toMerge)
            mergedSources.addAll(mb.sources());
        Boat mergedBoat = new Boat(keepBoat.id(), keepBoat.sailNumber(), keepBoat.name(),
            designId, clubId,
            List.copyOf(certMap.values()), List.copyOf(mergedSources), Instant.now(), null);
        putBoat(mergedBoat);

        // Repoint all finisher records that reference a merged-away boat ID
        Set<String> mergeIdSet = new HashSet<>(mergeIds);
        int updatedRaces = 0;
        int updatedFinishers = 0;
        for (Race race : List.copyOf(races.values()))
        {
            boolean changed = false;
            List<Division> newDivisions = new ArrayList<>();
            for (Division div : race.divisions())
            {
                List<Finisher> newFinishers = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    if (mergeIdSet.contains(f.boatId()))
                    {
                        newFinishers.add(new Finisher(keepId, f.elapsedTime(), f.nonSpinnaker(), f.certificateNumber()));
                        changed = true;
                        updatedFinishers++;
                    }
                    else
                    {
                        newFinishers.add(f);
                    }
                }
                newDivisions.add(new Division(div.name(), newFinishers));
            }
            if (changed)
            {
                putRace(new Race(race.id(), race.clubId(), race.seriesIds(), race.date(),
                    race.number(), race.name(),
                    newDivisions, race.source(), race.lastUpdated(), null));
                updatedRaces++;
            }
        }

        // Delete merged-away boat files
        for (Boat mb : toMerge)
            removeBoat(mb.id());

        LOG.info("mergeBoats: kept={} merged={} updatedRaces={} updatedFinishers={}",
            keepId, mergeIds, updatedRaces, updatedFinishers);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return new MergeResult(updatedRaces, updatedFinishers);
    }

    private static String certKey(Certificate c)
    {
        return c.system() + "|" + c.year() + "|" + c.nonSpinnaker() + "|" + c.twoHanded();
    }

    /**
     * Rewrite all finisher references from oldBoatId to newBoatId across all races.
     * Called when a boat is upgraded (e.g. design added to ID) so that existing race
     * finisher records continue to point to the correct boat.
     */
    private void rewriteFinisherBoatId(String oldBoatId, String newBoatId)
    {
        int updatedRaces = 0;
        int updatedFinishers = 0;
        for (Race race : List.copyOf(races.values()))
        {
            if (race.divisions() == null) continue;
            boolean changed = false;
            List<Division> newDivisions = new ArrayList<>();
            for (Division div : race.divisions())
            {
                List<Finisher> newFinishers = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    if (oldBoatId.equals(f.boatId()))
                    {
                        newFinishers.add(new Finisher(newBoatId, f.elapsedTime(), f.nonSpinnaker(), f.certificateNumber()));
                        changed = true;
                        updatedFinishers++;
                    }
                    else
                    {
                        newFinishers.add(f);
                    }
                }
                newDivisions.add(new Division(div.name(), newFinishers));
            }
            if (changed)
            {
                putRace(new Race(race.id(), race.clubId(), race.seriesIds(), race.date(),
                    race.number(), race.name(),
                    newDivisions, race.source(), race.lastUpdated(), null));
                updatedRaces++;
            }
        }
        if (updatedFinishers > 0)
            LOG.info("Rewritten {} finisher reference(s) in {} race(s): {} → {}",
                updatedFinishers, updatedRaces, oldBoatId, newBoatId);
    }

    /**
     * Write all dirty entities to disk (dirty-check via loadedAt). Keeps maps loaded.
     */
    public void save()
    {
        requireStarted();
        boats.values().forEach(b -> write(boatsDir.resolve(b.id() + ".json"), b));
        designs.values().forEach(d -> write(designsDir.resolve(d.id() + ".json"), d));
        clubs.values().forEach(c -> write(clubsDir.resolve(IdGenerator.sanitizeIdForFilesystem(c.id()) + ".json"), c));
        races.values().forEach(r -> write(raceFilePath(r), r));
        if (makersDirty)
        {
            write(catalogueDir.resolve("makers.json"), makers);
            makersDirty = false;
        }
    }

    /**
     * Load all raw data from disk into in-memory maps.
     */
    public void start()
    {
        LOG.info("Start DataStore root={}", root.toAbsolutePath());

        boats = new LinkedHashMap<>();
        loadDir(boatsDir, Boat.class).forEach(b ->
        {
            boats.put(b.id(), b);
            if (b.sources().isEmpty())
                LOG.warn("Boat {} has no sources — likely a stale entry, consider deleting {}", b.id(), b.id() + ".json");
        });
        designs = new LinkedHashMap<>();
        loadDir(designsDir, Design.class).forEach(d -> designs.put(d.id(), d));
        clubSeed = ClubLoader.load(configDir);
        clubCatalogue = ClubLoader.loadCatalogue(configDir);
        aliases = Aliases.load(configDir);
        designCatalogue = Designs.load(configDir);
        designCatalogue.overrideDesigns().forEach((normId, canonicalName) ->
        {
            Design existing = designs.get(normId);
            if (existing == null)
            {
                Design d = new Design(normId, canonicalName, List.of(),
                    List.of("DesignOverride"), Instant.now(), null);
                putDesign(d);
                LOG.info("Created design {} ('{}') from boatDesignOverrides in design.yaml", normId, canonicalName);
            }
            else if (!existing.sources().contains("DesignOverride"))
            {
                putDesign(new Design(existing.id(), existing.canonicalName(),
                    existing.aliases(), addSource(existing.sources(), "DesignOverride"),
                    existing.lastUpdated(), null));
            }
        });
        loadExclusions();
        clubs = new LinkedHashMap<>();
        loadDir(clubsDir, Club.class).forEach(c -> clubs.put(c.id(), c));
        races = new LinkedHashMap<>();
        loadDirRecursive(racesDir, Race.class).forEach(r -> races.put(r.id(), r));

        // Auto-fix stale boats: if the alias seed maps a boat's name to a different canonical
        // name and the canonical boat already exists, merge the stale boat into it.
        // This repairs boats that were created before the alias entry was added and prevents
        // them from persisting across imports via the direct boats.get(boatId) fast path.
        {
            List<Map.Entry<String, String>> staleBoatPairs = new ArrayList<>();
            for (Boat b : new ArrayList<>(boats.values()))
            {
                String normName = IdGenerator.normaliseName(b.name());
                String ndk = normName + (b.designId() != null ? "-" + b.designId() : "");
                var match = aliases.lookupBoat(b.sailNumber(), ndk);
                if (match.isPresent() && match.get().normName() != null)
                {
                    String canonNorm = match.get().normName();
                    if (!canonNorm.equals(normName))
                    {
                        String canonSail = match.get().normSailNumber() != null
                            ? match.get().normSailNumber() : b.sailNumber();
                        String displayName = match.get().canonicalDisplayName() != null
                            ? match.get().canonicalDisplayName() : b.name();
                        Design d = b.designId() != null ? designs.get(b.designId()) : null;
                        String canonId = IdGenerator.generateBoatId(canonSail, displayName, d);
                        if (boats.containsKey(canonId))
                            staleBoatPairs.add(Map.entry(b.id(), canonId));
                        else
                            LOG.warn("Stale boat {} (name '{}') should map to canonical '{}' per alias seed but canonical boat {} not found; will be renamed on next import",
                                b.id(), b.name(), displayName, canonId);
                    }
                }
            }
            if (!staleBoatPairs.isEmpty())
            {
                LOG.info("Auto-fixing {} stale boat(s) at startup", staleBoatPairs.size());
                for (Map.Entry<String, String> pair : staleBoatPairs)
                {
                    LOG.info("Auto-merging stale boat {} into canonical {}", pair.getKey(), pair.getValue());
                    mergeBoats(pair.getValue(), List.of(pair.getKey()));
                }
            }
        }

        // Design upgrade: merge design-less boats into a design-bearing boat with the same
        // sail number and name.  These arise when one importer (e.g. TopYacht) creates a boat
        // without design information and another (e.g. SailSys) later creates the same boat
        // with a design, resulting in two separate records on disk.
        {
            List<Map.Entry<String, String>> designUpgradePairs = new ArrayList<>();
            for (Boat b : new ArrayList<>(boats.values()))
            {
                if (b.designId() != null) continue;  // already has design
                String normSail = b.sailNumber();    // already normalised in stored form
                String normName = IdGenerator.normaliseName(b.name());
                for (Boat other : boats.values())
                {
                    if (other == b || other.designId() == null) continue;
                    if (!other.sailNumber().equals(normSail)) continue;
                    if (!IdGenerator.normaliseName(other.name()).equals(normName)) continue;
                    designUpgradePairs.add(Map.entry(other.id(), b.id()));
                    break;
                }
            }
            if (!designUpgradePairs.isEmpty())
            {
                LOG.info("Auto-merging {} design-less boat(s) into design-bearing counterpart(s) at startup",
                    designUpgradePairs.size());
                for (Map.Entry<String, String> pair : designUpgradePairs)
                {
                    LOG.info("Design upgrade: merging {} into {}", pair.getValue(), pair.getKey());
                    mergeBoats(pair.getKey(), List.of(pair.getValue()));
                }
            }
        }

        makers = new ArrayList<>(loadList(catalogueDir.resolve("makers.json"), Maker.class));
        makersDirty = false;
    }

    // --- Internal helpers ---

    /**
     * save() then clear in-memory maps.
     */
    public void stop()
    {
        save();
        races = null;
        boats = null;
        designs = null;
        clubs = null;
        clubSeed = null;
        clubCatalogue = null;
        aliases = null;
        designCatalogue = null;
        makers = null;
        makersDirty = false;
        excludedBoatIds.clear();
        excludedDesignOverrideIds.clear();
        excludedRaceIds.clear();
    }

    private <T> List<T> loadDir(Path dir, Class<T> type)
    {
        List<T> loaded;
        if (!Files.exists(dir))
            loaded = Collections.emptyList();
        else
        {
            try (var stream = Files.list(dir))
            {
                loaded = stream.filter(p -> p.toString().endsWith(".json")).map(p ->
                {
                    try
                    {
                        T entity = MAPPER.readValue(p.toFile(), type);
                        if (entity instanceof Loadable<?>)
                        {
                            Instant modified = Files.getLastModifiedTime(p).toInstant();
                            @SuppressWarnings("unchecked") T stamped = ((Loadable<T>)entity).withLoadedAt(modified);
                            entity = stamped;
                        }
                        return entity;
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException(e);
                    }
                }).toList();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        LOG.info("Loaded {} {}(s)", loaded.size(), type.getSimpleName());
        return loaded;
    }

    /**
     * Returns the file path for a race: races/{clubId}/{seriesSlug}/{raceId}.json
     * seriesSlug is the portion of the first seriesId after the clubId prefix.
     */
    private Path raceFilePath(Race race)
    {
        String clubSlug = race.clubId() != null ? IdGenerator.sanitizeIdForFilesystem(race.clubId()) : "unknown";
        String seriesSlug;
        if (race.seriesIds() == null || race.seriesIds().isEmpty())
        {
            seriesSlug = "uncategorised";
        }
        else
        {
            String firstSeries = race.seriesIds().getFirst();
            int slashIdx = firstSeries.indexOf('/');
            seriesSlug = slashIdx >= 0 ? firstSeries.substring(slashIdx + 1) : "uncategorised";
        }
        return racesDir.resolve(clubSlug).resolve(seriesSlug).resolve(race.id() + ".json");
    }

    /**
     * Like loadDir but walks all subdirectories recursively. Used for races.
     */
    private <T> List<T> loadDirRecursive(Path dir, Class<T> type)
    {
        if (!Files.exists(dir))
        {
            LOG.info("Loaded 0 {}(s) (directory absent)", type.getSimpleName());
            return Collections.emptyList();
        }
        List<T> loaded;
        try (var stream = Files.walk(dir))
        {
            loaded = stream
                .filter(Files::isRegularFile)
                .filter(p -> p.toString().endsWith(".json"))
                .map(p ->
                {
                    try
                    {
                        T entity = MAPPER.readValue(p.toFile(), type);
                        if (entity instanceof Loadable<?>)
                        {
                            Instant modified = Files.getLastModifiedTime(p).toInstant();
                            @SuppressWarnings("unchecked") T stamped = ((Loadable<T>)entity).withLoadedAt(modified);
                            entity = stamped;
                        }
                        return entity;
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException(e);
                    }
                })
                .toList();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        LOG.info("Loaded {} {}(s)", loaded.size(), type.getSimpleName());
        return loaded;
    }

    private <T> List<T> loadList(Path path, Class<T> type)
    {
        if (!Files.exists(path))
            return Collections.emptyList();
        try
        {
            return MAPPER.readValue(path.toFile(), MAPPER.getTypeFactory().constructCollectionType(List.class, type));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private void requireStarted()
    {
        if (boats == null)
            throw new IllegalStateException("DataStore not started — call start() first");
    }

    private void write(Path path, Object value)
    {
        // Dirty check: skip if file exists and modification time matches loadedAt
        if (value instanceof Loadable<?> l && l.loadedAt() != null)
        {
            try
            {
                if (Files.exists(path) && Files.getLastModifiedTime(path).toInstant().equals(l.loadedAt()))
                {
                    LOG.debug("Skipping unchanged {}", path.getFileName());
                    return;
                }
            }
            catch (IOException ignored)
            { /* fall through and write */ }
        }

        boolean isNew = !Files.exists(path);
        try
        {
            Files.createDirectories(path.getParent());
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
            if (isNew)
                LOG.info("Created {}", path.getFileName());
            else
                LOG.info("Updated {}", path.getFileName());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
