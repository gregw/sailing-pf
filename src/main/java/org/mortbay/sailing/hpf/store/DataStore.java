package org.mortbay.sailing.hpf.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
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
    private static final JaroWinklerSimilarity JARO_WINKLER = new JaroWinklerSimilarity();
    private double fuzzyThreshold = 0.90;
    private static final JsonMapper MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
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
    private AliasSeedLoader.AliasSeed aliasSeed; // lookup-only alias data from aliases.yaml; never written to disk
    private DesignCatalogueLoader.DesignCatalogue designCatalogue; // lookup-only exclusion list from design.yaml
    private List<Maker> makers;
    private boolean makersDirty;

    // Mutable exclusion sets — persisted to config/exclusions.json, managed via admin UI
    private final Set<String> excludedBoatIds          = new LinkedHashSet<>();
    private final Set<String> excludedDesignOverrideIds = new LinkedHashSet<>();
    private final Set<String> excludedRaceIds           = new LinkedHashSet<>();

    public DataStore(Path root)
    {
        this.root = root;
        this.configDir = root.resolve("config");
        this.racesDir = root.resolve("races");
        this.boatsDir = root.resolve("boats");
        this.designsDir = root.resolve("designs");
        this.clubsDir = root.resolve("clubs");
        this.catalogueDir = root.resolve("catalogue");
    }

    public void setFuzzyThreshold(double threshold)
    {
        this.fuzzyThreshold = threshold;
    }

    private boolean boatNameMatches(Boat candidate, String incomingName, String normIncoming)
    {
        if (IdGenerator.normaliseName(candidate.name()).equals(normIncoming))
            return true;
        if (candidate.aliases().stream().anyMatch(a -> a.equalsIgnoreCase(incomingName)))
            return true;
        if (candidate.aliases().stream().anyMatch(a -> IdGenerator.normaliseName(a).equals(normIncoming)))
            return true;

        if (JARO_WINKLER.apply(IdGenerator.normaliseName(candidate.name()), normIncoming) >= fuzzyThreshold)
            return true;
        return candidate.aliases().stream().anyMatch(a -> JARO_WINKLER.apply(IdGenerator.normaliseName(a), normIncoming) >= fuzzyThreshold);
    }

    private boolean designNameMatches(Design candidate, String normIncoming)
    {
        if (IdGenerator.normaliseDesignName(candidate.canonicalName()).equals(normIncoming))
            return true;
        if (candidate.aliases().stream().anyMatch(a -> IdGenerator.normaliseDesignName(a).equals(normIncoming)))
            return true;

        // Fuzzy matching — only for names long enough to avoid short-string false positives.
        // Digits must match exactly so that "sydney38" never conflates with "sydney39".
        if (normIncoming.length() >= 6)
        {
            String incomingDigits = extractDigits(normIncoming);
            String candidateNorm = IdGenerator.normaliseDesignName(candidate.canonicalName());
            if (extractDigits(candidateNorm).equals(incomingDigits) && JARO_WINKLER.apply(candidateNorm, normIncoming) >= fuzzyThreshold)
                return true;
            return candidate.aliases().stream().anyMatch(a ->
            {
                String aNorm = IdGenerator.normaliseDesignName(a);
                return extractDigits(aNorm).equals(incomingDigits) && JARO_WINKLER.apply(aNorm, normIncoming) >= fuzzyThreshold;
            });
        }
        return false;
    }

    // --- Lifecycle ---

    private static String extractDigits(String s)
    {
        return s.replaceAll("[^0-9]", "");
    }

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

    public Boat findOrCreateBoat(String sailNo, String name, Design design)
    {
        // Apply design override from design.yaml config, if any
        String overrideDesignId = designCatalogue.resolveDesignOverride(sailNo, name);
        if (overrideDesignId != null)
        {
            Design overrideDesign = designs.get(overrideDesignId);
            if (overrideDesign != null)
            {
                if (design == null || !overrideDesignId.equals(design.id()))
                    LOG.info("Boat {}/{}: design overridden {} → {}", sailNo, name,
                        design == null ? "null" : design.id(), overrideDesignId);
                design = overrideDesign;
            }
            else
            {
                LOG.warn("Boat {}/{}: design override designId='{}' not found in store", sailNo, name, overrideDesignId);
            }
        }

        String boatId = IdGenerator.generateBoatId(sailNo, name, design);

        Boat boat = boats.get(boatId);
        if (boat != null)
            return boat;

        String normSail = IdGenerator.normaliseSailNumber(sailNo);
        String normName = IdGenerator.normaliseName(name);

        for (Boat candidate : boats.values())
        {
            if (!candidate.sailNumber().equals(normSail) && !candidate.altSailNumbers().contains(normSail))
                continue;
            if (design != null && candidate.designId() != null && !candidate.designId().equals(design.id()))
                continue;

            if (boatNameMatches(candidate, name, normName))
            {
                if (candidate.designId() == null && design != null)
                {
                    removeBoat(candidate.id());
                    Boat upgraded = new Boat(boatId, normSail, name, design.id(), candidate.clubId(), candidate.aliases(), candidate.altSailNumbers(), candidate.certificates(), candidate.sources(), candidate.lastUpdated(), null);
                    putBoat(upgraded);
                    LOG.info("Upgraded boat {} → {}", candidate.id(), boatId);
                    return upgraded;
                }
                return candidate;
            }
        }

        // Check the alias seed using the new lookupBoat method (sail number + name-design key).
        String nameDesignKey = normName + (design != null ? "-" + design.id() : "");
        Optional<AliasSeedLoader.AliasSeed.BoatSeedMatch> seedMatch =
            aliasSeed.lookupBoat(normSail, nameDesignKey);
        if (seedMatch.isPresent())
        {
            String seedCanonicalName = seedMatch.get().canonicalName();
            String seedCanonicalSail = seedMatch.get().canonicalSailNumber();
            // If canonical sail differs from incoming, redirect to the canonical sail number
            if (seedCanonicalSail != null && !seedCanonicalSail.equalsIgnoreCase(normSail))
            {
                LOG.info("Sail number {} redirected to {} via alias seed (altSailNumbers)", normSail, seedCanonicalSail);
                normSail = seedCanonicalSail;
            }
            if (seedCanonicalName != null)
            {
                String normCanonical = IdGenerator.normaliseName(seedCanonicalName);
                for (Boat candidate : boats.values())
                {
                    if (!candidate.sailNumber().equals(normSail) && !candidate.altSailNumbers().contains(normSail))
                        continue;
                    if (design != null && candidate.designId() != null && !candidate.designId().equals(design.id()))
                        continue;
                    if (boatNameMatches(candidate, seedCanonicalName, normCanonical))
                    {
                        if (candidate.designId() == null && design != null)
                        {
                            String canonicalBoatId = IdGenerator.generateBoatId(normSail, seedCanonicalName, design);
                            removeBoat(candidate.id());
                            Boat upgraded = new Boat(canonicalBoatId, normSail, seedCanonicalName, design.id(), candidate.clubId(), candidate.aliases(), candidate.altSailNumbers(), candidate.certificates(), candidate.sources(), candidate.lastUpdated(), null);
                            putBoat(upgraded);
                            LOG.info("Upgraded boat (via alias seed) {} → {}", candidate.id(), canonicalBoatId);
                            return upgraded;
                        }
                        return candidate;
                    }
                }
                // No existing boat found — create with the canonical name, recording the incoming name as an alias
                String canonicalBoatId = IdGenerator.generateBoatId(normSail, seedCanonicalName, design);
                List<String> aliases = normName.equals(normCanonical) ? List.of() : List.of(name);
                Boat newBoat = new Boat(canonicalBoatId, normSail, seedCanonicalName, design == null ? null : design.id(), null, aliases, List.of(), List.of(), List.of(), null, null);
                putBoat(newBoat);
                LOG.info("Created new boat (via alias seed) {}", newBoat);
                return newBoat;
            }
        }

        // Fallback: check old boatAliases/boatCanonicalName for legacy compatibility
        List<org.mortbay.sailing.hpf.data.TimedAlias> seedAliases = aliasSeed.boatAliases(normSail);
        String seedCanonicalName = aliasSeed.boatCanonicalName(normSail);
        boolean nameMatchesSeedAlias = !seedAliases.isEmpty() && seedAliases.stream()
            .anyMatch(a -> IdGenerator.normaliseName(a.name()).equals(normName)
                || a.name().equalsIgnoreCase(name));
        if (nameMatchesSeedAlias && seedCanonicalName != null)
        {
            String normCanonical = IdGenerator.normaliseName(seedCanonicalName);
            for (Boat candidate : boats.values())
            {
                if (!candidate.sailNumber().equals(normSail))
                    continue;
                if (design != null && candidate.designId() != null && !candidate.designId().equals(design.id()))
                    continue;
                if (boatNameMatches(candidate, seedCanonicalName, normCanonical))
                {
                    if (candidate.designId() == null && design != null)
                    {
                        String canonicalBoatId = IdGenerator.generateBoatId(sailNo, seedCanonicalName, design);
                        removeBoat(candidate.id());
                        Boat upgraded = new Boat(canonicalBoatId, normSail, seedCanonicalName, design.id(), candidate.clubId(), candidate.aliases(), candidate.altSailNumbers(), candidate.certificates(), candidate.sources(), candidate.lastUpdated(), null);
                        putBoat(upgraded);
                        LOG.info("Upgraded boat (via alias seed legacy) {} → {}", candidate.id(), canonicalBoatId);
                        return upgraded;
                    }
                    return candidate;
                }
            }
            // No existing boat found — create with the canonical name, recording the incoming name as an alias
            String canonicalBoatId = IdGenerator.generateBoatId(sailNo, seedCanonicalName, design);
            List<String> aliases = normName.equals(normCanonical) ? List.of() : List.of(name);
            Boat newBoat = new Boat(canonicalBoatId, normSail, seedCanonicalName, design == null ? null : design.id(), null, aliases, List.of(), List.of(), List.of(), null, null);
            putBoat(newBoat);
            LOG.info("Created new boat (via alias seed legacy) {}", newBoat);
            return newBoat;
        }

        // Check sail number redirect: if this sail number is a known typo/alias for another,
        // retry with the canonical sail number so the boats are not re-duplicated.
        String redirectSail = aliasSeed.sailNumberRedirect(normSail);
        if (redirectSail != null && !redirectSail.equals(normSail))
        {
            LOG.info("Sail number {} redirected to {} via alias seed", normSail, redirectSail);
            return findOrCreateBoat(redirectSail, name, design);
        }

        Boat newBoat = new Boat(boatId, normSail, name, design == null ? null : design.id(), null, List.of(), List.of(), List.of(), List.of(), null, null);
        putBoat(newBoat);
        LOG.info("Created new boat {}", newBoat);
        return newBoat;
    }

    public Design findOrCreateDesign(String className)
    {
        if (className == null || className.isBlank())
            return null;
        String designId = IdGenerator.normaliseDesignName(className);
        Design design = designs.get(designId);
        if (design != null)
            return design;
        for (Design d : designs.values())
        {
            if (designNameMatches(d, designId))
                return d;
        }

        // Check the alias seed for a known equivalence
        String canonicalId = aliasSeed.resolveDesignAlias(designId);
        if (canonicalId != null)
        {
            Design existing = designs.get(canonicalId);
            if (existing != null)
                return existing;
            // Canonical design not yet in store — create it using the seed's canonical name
            String seedName = aliasSeed.designCanonicalName(canonicalId);
            design = new Design(canonicalId, seedName != null ? seedName : className.trim(), List.of(), List.of(), List.of(), null, null);
            putDesign(design);
            if (designCatalogue.isExcluded(canonicalId))
                LOG.info("Design {} is excluded (dinghy/OTB class)", canonicalId);
            return design;
        }

        design = new Design(designId, className.trim(), List.of(), List.of(), List.of(), null, null);
        putDesign(design);
        if (designCatalogue.isExcluded(designId))
            LOG.info("Design {} is excluded (dinghy/OTB class)", designId);
        return design;
    }

    /**
     * Like {@link #findOrCreateDesign(String)} but also records {@code source} in the
     * design's sources list if it is not already present, and updates {@code lastUpdated}.
     * Pass {@code null} for {@code source} to behave identically to the no-source overload.
     */
    public Design findOrCreateDesign(String className, String source)
    {
        Design design = findOrCreateDesign(className);
        if (design == null || source == null || design.sources().contains(source))
            return design;
        Design updated = new Design(design.id(), design.canonicalName(), design.makerIds(),
            design.aliases(), addSource(design.sources(), source), Instant.now(), null);
        putDesign(updated);
        return updated;
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
    public Club findUniqueClubByShortName(String shortName, String longName)
    {
        requireStarted();
        List<Club> allClubs = Stream.concat(
                clubs.values().stream(),
                clubSeed.values().stream().filter(c -> !clubs.containsKey(c.id())))
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
                          || c.aliases().stream().anyMatch(a -> shortName.equalsIgnoreCase(a)))
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
                              || c.aliases().stream().anyMatch(a -> t.equalsIgnoreCase(a)))
                    .toList();
                if (!matches.isEmpty())
                    break;
            }
        }

        if (matches.isEmpty())
        {
            LOG.info("No club found for name={}", shortName);
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
            LOG.warn("Ambiguous name={} — {} matches ({}); clubId not set",
                shortName, matches.size(),
                matches.stream().map(c -> c.id() + "/" + c.state()).toList());
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
    public Club findClubByShortName(String shortName, String state)
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
            LOG.error("Unknown club shortName={} state={}", shortName, state);
            return null;
        }
        if (matches.size() > 1)
        {
            LOG.error("Ambiguous club shortName={} state={} — {} matches", shortName, state, matches.size());
            return null;
        }
        return matches.getFirst();
    }

    public List<Maker> makers()
    {
        requireStarted();
        return Collections.unmodifiableList(makers);
    }

    // --- Write mutators (require started; loadedAt = null → always written by save()) ---

    public void putBoat(Boat boat)
    {
        requireStarted();
        boats.put(boat.id(), boat);
    }

    public void putClub(Club club)
    {
        requireStarted();
        clubs.put(club.id(), club);
    }

    public void putDesign(Design design)
    {
        requireStarted();
        designs.put(design.id(), design);
    }

    public void putMakers(List<Maker> makers)
    {
        requireStarted();
        this.makers = new ArrayList<>(makers);
        makersDirty = true;
    }

    public void putRace(Race race)
    {
        requireStarted();
        races.put(race.id(), race);
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

        // Merge maker IDs — deduplicate preserving keep design's order first
        Set<String> allMakers = new LinkedHashSet<>(keepDesign.makerIds());
        for (Design md : toMerge)
            allMakers.addAll(md.makerIds());

        Set<String> allSources = new LinkedHashSet<>(keepDesign.sources());
        for (Design md : toMerge)
            allSources.addAll(md.sources());
        putDesign(new Design(keepDesign.id(), keepDesign.canonicalName(),
            List.copyOf(allMakers), List.copyOf(allAliases), List.copyOf(allSources), Instant.now(), null));

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
                    Set<String> mergedAliases = new LinkedHashSet<>(existingAtNewId.aliases());
                    if (!boat.name().equalsIgnoreCase(existingAtNewId.name()))
                        mergedAliases.add(boat.name());
                    mergedAliases.addAll(boat.aliases());
                    mergedAliases.removeIf(a -> a.equalsIgnoreCase(existingAtNewId.name()));

                    Map<String, Certificate> certMap = new LinkedHashMap<>();
                    for (Certificate c : existingAtNewId.certificates()) certMap.put(certKey(c), c);
                    for (Certificate c : boat.certificates()) certMap.putIfAbsent(certKey(c), c);

                    Set<String> mergedSources = new LinkedHashSet<>(existingAtNewId.sources());
                    mergedSources.addAll(boat.sources());

                    String clubId = existingAtNewId.clubId() != null ? existingAtNewId.clubId() : boat.clubId();
                    // Merge altSailNumbers from both boats
                    LinkedHashSet<String> mergedAltSails = new LinkedHashSet<>(existingAtNewId.altSailNumbers());
                    mergedAltSails.addAll(boat.altSailNumbers());
                    toWrite = new Boat(newId, existingAtNewId.sailNumber(), existingAtNewId.name(), keepId,
                        clubId, List.copyOf(mergedAliases), List.copyOf(mergedAltSails),
                        List.copyOf(certMap.values()), List.copyOf(mergedSources), Instant.now(), null);
                }
                else
                {
                    toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                        boat.clubId(), boat.aliases(), boat.altSailNumbers(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
                }
            }
            else
            {
                toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                    boat.clubId(), boat.aliases(), boat.altSailNumbers(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
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
                        race.number(), race.name(), race.handicapSystem(), race.offsetPursuit(),
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
    public void reloadAliasSeed()
    {
        requireStarted();
        aliasSeed = AliasSeedLoader.load(configDir);
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

        // Build merged aliases — collect all names and existing aliases from merged-away boats (name aliases only)
        Set<String> allAliases = new LinkedHashSet<>(keepBoat.aliases());
        for (Boat mb : toMerge)
        {
            if (!mb.name().equalsIgnoreCase(keepBoat.name()))
                allAliases.add(mb.name());
            allAliases.addAll(mb.aliases());
        }
        allAliases.removeIf(a -> a.equalsIgnoreCase(keepBoat.name()));

        // Build merged alternate sail numbers — collect from merged-away boats' canonical and alt sail numbers
        LinkedHashSet<String> allAltSails = new LinkedHashSet<>(keepBoat.altSailNumbers());
        for (Boat mb : toMerge)
        {
            // Add the merged-away boat's canonical sail number if different from keep boat's
            if (!mb.sailNumber().equalsIgnoreCase(keepBoat.sailNumber()))
                allAltSails.add(mb.sailNumber());
            allAltSails.addAll(mb.altSailNumbers());
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
            designId, clubId, List.copyOf(allAliases), List.copyOf(allAltSails),
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
                    race.number(), race.name(), race.handicapSystem(), race.offsetPursuit(),
                    newDivisions, race.source(), race.lastUpdated(), null));
                updatedRaces++;
            }
        }

        // Delete merged-away boat files
        for (Boat mb : toMerge)
            removeBoat(mb.id());

        LOG.info("mergeBoats: kept={} merged={} updatedRaces={} updatedFinishers={}",
            keepId, mergeIds, updatedRaces, updatedFinishers);
        return new MergeResult(updatedRaces, updatedFinishers);
    }

    private static String certKey(Certificate c)
    {
        return c.system() + "|" + c.year() + "|" + c.nonSpinnaker() + "|" + c.twoHanded();
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
        loadDir(boatsDir, Boat.class).forEach(b -> boats.put(b.id(), b));
        designs = new LinkedHashMap<>();
        loadDir(designsDir, Design.class).forEach(d -> designs.put(d.id(), d));
        clubSeed = ClubSeedLoader.load(configDir);
        aliasSeed = AliasSeedLoader.load(configDir);
        designCatalogue = DesignCatalogueLoader.load(configDir);
        loadExclusions();
        clubs = new LinkedHashMap<>();
        loadDir(clubsDir, Club.class).forEach(c -> clubs.put(c.id(), c));
        races = new LinkedHashMap<>();
        loadDirRecursive(racesDir, Race.class).forEach(r -> races.put(r.id(), r));
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
        aliasSeed = null;
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
                            @SuppressWarnings("unchecked") T stamped = (T)((Loadable<T>)entity).withLoadedAt(modified);
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
                            @SuppressWarnings("unchecked") T stamped = (T)((Loadable<T>)entity).withLoadedAt(modified);
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
