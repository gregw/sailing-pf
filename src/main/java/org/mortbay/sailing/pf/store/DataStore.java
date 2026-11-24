package org.mortbay.sailing.pf.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Identity;
import org.mortbay.sailing.pf.data.BoatAlias;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Link;
import org.mortbay.sailing.pf.data.Loadable;
import org.mortbay.sailing.pf.data.SailSysEvent;
import org.mortbay.sailing.pf.data.Maker;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.Series;
import org.mortbay.sailing.pf.data.SeriesType;
import org.mortbay.sailing.pf.data.TopYachtGroup;
import org.mortbay.sailing.pf.importer.IdGenerator;
import org.mortbay.sailing.pf.importer.TopYachtEditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.eclipse.jetty.util.StringUtil.isBlank;
import static org.eclipse.jetty.util.StringUtil.isNotBlank;

/**
 * Reads and writes the data store.
 * <p>
 * Layout:
 * {root}/races/{clubId}/{seriesSlug}/{raceId}.json  -- one file per Race, in subdirectories
 * {root}/boats/{boatId}.json       -- one file per Boat (embeds certificates)
 * {root}/designs/{designId}.json   -- one file per Design
 * {root}/clubs/{clubId}.json       -- one file per Club (embeds series)
 * {root}/catalogue/makers.json     -- all Makers (small stable collection)
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
    private static final JsonMapper YAML_MAPPER = JsonMapper.builder(
            new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();
    private static final String EXCLUSIONS_HEADER = """
            # IMPORTANT: This file is managed by the server.
            # It is overwritten whenever exclusions are changed via the admin UI.
            # Only edit manually when the server is NOT running.
            #
            # This file holds boat, race, and series exclusions. Design-level flags
            # (excluded designs and ignored designs) live in design.yaml under the
            # `excluded:` and `ignored:` fields, which are the single source of truth
            # for long-lived design catalogue state.
            """;


    private final Path root;
    private final Path configDir;
    private final Path racesDir;
    private final Path boatsDir;
    private final Path designsDir;
    private final Path clubsDir;
    private final Path catalogueDir;

    // In-memory maps -- null before start()
    private Map<String, Race> races;
    private Map<String, Boat> boats;      // keyed by each boat's current identity ID
    private Map<String, String> identityIndex; // every identity ID -> the key of its boat in boats
    private Map<String, Design> designs;
    private Map<String, Club> clubs;      // persisted entities (from disk / putClub)
    private Map<String, Club> clubSeed;   // lookup-only stubs from clubs.yaml; never written to disk
    private Aliases.Loaded aliases; // lookup-only alias data from aliases.yaml; never written to disk
    private Designs.DesignCatalogue designCatalogue; // lookup-only exclusion list from design.yaml
    private ClubLoader.ClubCatalogue clubCatalogue;  // lookup-only club overrides from clubs.yaml
    private List<Maker> makers;
    private boolean makersDirty;

    // Set by computeStaleBoatViolations() at the end of start(). Empty when the dataset is
    // clean; non-empty when the startup repair passes left residual stale boats. Read via
    // findStaleBoatViolations() and surfaced through the /api/health endpoint.
    private volatile List<String> staleBoatViolations = List.of();

    // Mutable exclusion sets -- persisted to config/exclusions.yaml, managed via admin UI.
    // Map values are the operator-supplied (or auto-supplied) reason for exclusion;
    // empty string when no reason was given. LinkedHashMap preserves insertion order
    // so the YAML stays diff-friendly across saves.
    private final Map<String, String> excludedBoats = new LinkedHashMap<>();
    private final Map<String, String> excludedRaces = new LinkedHashMap<>();
    private final Map<String, String> excludedSeries = new LinkedHashMap<>(); // pattern -> reason
    private volatile List<Pattern> compiledSeriesPatterns = List.of();

    // When true, putRace runs RaceSanityChecker on freshly inserted races and auto-excludes
    // any that look like placeholder / synthetic data. Tests using synthetic fixtures set
    // this to false; production importers leave it at the default.
    private volatile boolean autoSanityCheck = true;

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
        this.racesDir = root.resolve("races");
        this.boatsDir = root.resolve("boats");
        this.designsDir = root.resolve("designs");
        this.clubsDir = root.resolve("clubs");
        this.catalogueDir = root.resolve("catalogue");
    }

    private boolean designNameMatches(Design candidate, String normIncoming)
    {
        if (candidate.id().equals(normIncoming))
            return true;
        return candidate.aliases().stream().anyMatch(a -> IdGenerator.normaliseDesignName(a).equals(normIncoming));
    }

    // --- Lifecycle ---

    /** The file in the data root naming the database format. */
    public static final String FORMAT_FILE = "FORMAT";
    /** The database format this code reads and writes: boats with identities, no imported/ tier. */
    public static final String FORMAT = "2";

    /**
     * Checks the data root holds a format {@value #FORMAT} database, marking a new (empty) one.
     * A v1 database (an {@code imported/} directory and no FORMAT file) must be converted first.
     */
    private void checkFormat()
    {
        Path formatFile = root.resolve(FORMAT_FILE);
        try
        {
            if (Files.exists(formatFile))
            {
                String format = Files.readString(formatFile).trim();
                if (!FORMAT.equals(format))
                    throw new IllegalStateException("Database " + root + " is format " + format
                        + ", but this version reads format " + FORMAT);
                return;
            }
            if (Files.isDirectory(root.resolve("imported")))
                throw new IllegalStateException("Database " + root + " is a v1 database (imported/ and no "
                    + FORMAT_FILE + " file): convert it with the v2 migration first");
            Files.createDirectories(root);
            Files.writeString(formatFile, FORMAT + "\n");
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Resolves the data root directory using the standard lookup chain:
     * <ol>
     *   <li>First element of {@code args}, if provided</li>
     *   <li>{@code PF_DATA} environment variable</li>
     *   <li>{@code ./pf-data} in the current working directory, if it exists</li>
     *   <li>{@code $HOME/.pf-data} as the default fallback</li>
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

        String env = System.getenv("PF_DATA");
        if (env != null && !env.isBlank())
            return Path.of(env);

        Path local = Path.of("pf-data");
        if (Files.isDirectory(local))
            return local;

        return Path.of(System.getProperty("user.home"), ".pf-data");
    }

    // --- Read accessors (require started) ---

    public Map<String, Boat> boats()
    {
        requireStarted();
        return Collections.unmodifiableMap(boats);
    }

    /**
     * The boat with identity {@code identityId}, seen through that identity (its sail number
     * and name are that identity's); null if no boat has it. Any identity ID refers to its boat.
     */
    public Boat boat(String identityId)
    {
        requireStarted();
        if (identityId == null)
            return null;
        Boat b = boats.get(identityIndex.getOrDefault(identityId, identityId));
        return b == null ? null : b.withId(b.hasIdentity(identityId) ? identityId : b.id());
    }

    /**
     * The canonical ID of the boat with identity {@code identityId} (its current identity's
     * ID, the key in {@link #boats()}), or {@code identityId} itself if no boat has it.
     */
    public String canonicalBoatId(String identityId)
    {
        requireStarted();
        return identityId == null ? null : identityIndex.getOrDefault(identityId, identityId);
    }

    private void indexIdentities(Boat b)
    {
        for (String identityId : b.identities().keySet())
        {
            String other = identityIndex.put(identityId, b.id());
            if (other != null && !other.equals(b.id()))
                LOG.warn("Identity {} is in two boats, {} and {}: keeping it in {}", identityId, other, b.id(), b.id());
        }
    }

    /** The race with each finisher counted for its boat's canonical ID. */
    private Race canonicalise(Race race)
    {
        if (race.divisions() == null)
            return race;
        boolean changed = false;
        List<Division> divisions = new ArrayList<>(race.divisions().size());
        for (Division div : race.divisions())
        {
            List<Finisher> finishers = new ArrayList<>(div.finishers().size());
            for (Finisher f : div.finishers())
            {
                String key = identityIndex.getOrDefault(f.identityId(), f.identityId());
                if (!key.equals(f.boatId()))
                {
                    f = f.withCanonicalBoatId(key);
                    changed = true;
                }
                finishers.add(f);
            }
            divisions.add(new Division(div.name(), finishers));
        }
        return changed ? new Race(race.id(), race.clubId(), race.seriesIds(), race.date(), race.number(),
            race.name(), divisions, race.source(), race.lastUpdated(), race.loadedAt()) : race;
    }

    /** Recounts, in memory, the finishers of these identities for their boats' canonical IDs. */
    private void recanonicalise(java.util.Collection<String> identityIds)
    {
        if (identityIds.isEmpty() || races == null)
            return;
        Set<String> ids = new HashSet<>(identityIds);
        races.replaceAll((id, race) -> race.divisions() != null && race.divisions().stream()
            .anyMatch(d -> d.finishers().stream().anyMatch(f -> ids.contains(f.identityId())))
            ? canonicalise(race) : race);
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

    /**
     * True if the boat with identity {@code boatId} has been set, by hand, to have no club under
     * that identity (an empty curated club list): importers then leave its clubs alone.
     */
    public boolean isExplicitlyNoClub(String boatId)
    {
        Boat b = boatId == null ? null : boat(boatId);
        return b != null && b.identity().clubsCurated() && b.identity().clubIds().isEmpty();
    }

    /**
     * Reads -- never creates -- an existing Boat by sail number and/or name, honouring
     * the alias system ({@code aliases.yaml}) and AUS-prefix collapsing. Used by
     * read-only server paths where creating a phantom boat would be wrong.
     * <p>
     * Either {@code rawSailNo} or {@code rawName} may be null/blank; the lookup uses
     * whichever key(s) are present and still requires a unique match.
     * <p>
     * Returns {@code Optional.empty()} when there is no match <em>or</em> when more
     * than one candidate would match (ambiguous -- treated the same as no match by
     * callers, who must not silently create a phantom).
     */
    public Optional<Boat> findBoat(String rawSailNo, String rawName)
    {
        requireStarted();
        String sailNo = IdGenerator.normaliseSailNumber(rawSailNo);
        String name = IdGenerator.normaliseName(rawName);
        boolean haveSail = !sailNo.isEmpty();
        boolean haveName = !name.isEmpty();
        if (!haveSail && !haveName)
            return Optional.empty();

        String key = IdGenerator.sailKey(sailNo);
        List<Boat> matches = new ArrayList<>();
        for (Boat candidate : boats.values())
        {
            for (Map.Entry<String, Identity> e : candidate.identities().entrySet())
            {
                Identity i = e.getValue();
                boolean hit = haveSail && haveName ? i.matches(sailNo, name)
                    : haveSail ? key.equals(IdGenerator.sailKey(i.sailNumber()))
                    : name.equals(IdGenerator.normaliseName(i.name()))
                        || i.aliases().stream().anyMatch(al -> name.equals(al.normName()));
                if (hit)
                {
                    matches.add(candidate.withId(e.getKey()));
                    break;
                }
            }
        }

        // Name-equivalence fallback: only consulted when the exact-name pass finds nothing,
        // so we never widen a unique exact hit into ambiguity. Requires sail to be supplied
        // (matchKey alone is too coarse) and a non-empty matchKey to avoid trivial collisions.
        if (matches.isEmpty() && haveSail)
        {
            String matchKey = IdGenerator.nameMatchKey(rawName);
            if (!matchKey.isEmpty())
            {
                for (Boat candidate : boats.values())
                {
                    for (Map.Entry<String, Identity> e : candidate.identities().entrySet())
                    {
                        Identity i = e.getValue();
                        if (key.equals(IdGenerator.sailKey(i.sailNumber()))
                            && matchKey.equals(IdGenerator.nameMatchKey(i.name())))
                        {
                            matches.add(candidate.withId(e.getKey()));
                            break;
                        }
                    }
                }
            }
        }

        if (matches.size() == 1)
            return Optional.of(matches.getFirst());
        if (matches.size() > 1)
            LOG.debug("findBoat ambiguous: sailNo='{}' name='{}' → {} candidates: {}",
                sailNo, name, matches.size(), matches.stream().map(Boat::id).toList());
        return Optional.empty();
    }

    /**
     * Read-only lookup by name + club, used by importers whose results pages omit the
     * sail number column. Returns the boat only when there is exactly one match
     * (case-insensitive on normalised name) among boats belonging to {@code clubId}.
     * Candidates whose stored design is currently ignored are skipped, mirroring the
     * orphan-tolerance in {@link #findOrCreateBoat}.
     */
    public Optional<Boat> findBoatByNameAndClub(String rawName, String clubId)
    {
        requireStarted();
        if (clubId == null || clubId.isBlank())
            return Optional.empty();
        String normName = IdGenerator.normaliseName(rawName);
        if (normName.isEmpty())
            return Optional.empty();
        List<Boat> matches = new ArrayList<>();
        for (Boat candidate : boats.values())
        {
            if (!normName.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                continue;
            if (!candidate.hasClub(clubId))
                continue;
            if (isDesignIgnored(candidate.designId()))
                continue;
            matches.add(candidate);
        }
        // Name-equivalence fallback: only when exact-name pass found nothing in this club.
        // Same logic as findBoat -- protects unique exact hits from being widened.
        if (matches.isEmpty())
        {
            String matchKey = IdGenerator.nameMatchKey(rawName);
            if (!matchKey.isEmpty())
            {
                for (Boat candidate : boats.values())
                {
                    if (normName.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                        continue;
                    if (!candidate.hasClub(clubId))
                        continue;
                    if (isDesignIgnored(candidate.designId()))
                        continue;
                    if (!matchKey.equals(IdGenerator.nameMatchKey(candidate.name())))
                        continue;
                    matches.add(candidate);
                }
            }
        }
        return matches.size() == 1 ? Optional.of(matches.getFirst()) : Optional.empty();
    }

    /**
     * Convenience overload for tests -- no date, no source.
     */
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
     * A boat that resolves to a "… Modified" design is folded into the base design and ticked
     * modified (see {@link #baseOfModifiedDesign}).
     *
     * @return the found or created Boat
     */
    public Boat findOrCreateBoat(String rawSailNo, String rawName, String rawDesign, LocalDate date, String source)
    {
        // A "… Modified" design name resolves to its base design, so the boat is found or
        // created there (rather than created under the modified design and merged back)
        boolean modified = false;
        String resolved = isNotBlank(rawDesign) ? resolveDesignId(rawDesign) : null;
        if (isModifiedDesignId(resolved))
        {
            Design d = designs.get(resolved);
            String seedName = aliases.designCanonicalName(resolved);
            String baseName = withoutModified(d != null ? d.canonicalName() : seedName != null ? seedName : rawDesign);
            if (baseName != null && !isModifiedDesignId(resolveDesignId(baseName)))
            {
                rawDesign = baseName;
                modified = true;
            }
        }
        Boat boat = findOrCreateBoatUnfolded(rawSailNo, rawName, rawDesign, date, source);
        if (modified && boat != null && markModified(boat))
            boat = boat(boat.id());
        return foldModifiedDesign(boat);
    }

    private Boat findOrCreateBoatUnfolded(String rawSailNo, String rawName, String rawDesign, LocalDate date, String source)
    {
        // Build enriched source entry for traceability
        String sourceDesign = buildSourceEntry(source, rawDesign);

        // Strip decorative suffixes ("- GM", "- U18", etc.) up front so the persisted
        // display name and any match-key comparisons use the cleaned form. normaliseName
        // strips the same suffixes internally for IDs; doing it here as well keeps the
        // visible Boat.name field consistent with the normalised ID.
        rawName = IdGenerator.stripStandardSuffixes(rawName);

        // The sail number without an AUS country/fleet prefix or leading zeros: "AUS1234" and
        // "1234" are the same boat, recorded as 1234
        String sailNo = IdGenerator.sailKey(rawSailNo);
        String name = IdGenerator.normaliseName(rawName);

        final String normSailNo = sailNo;
        final String normName = name;

        String designId = aliases.resolveDesignAlias(IdGenerator.normaliseName(rawDesign));
        // If the resolved design is ignored -- via the curated {@code ignored:} list in
        // design.yaml or via the runtime user-toggled set -- treat the incoming boat as
        // design-less for matching purposes, otherwise a new designless record would get
        // created alongside an existing properly-designed one.
        if (isDesignIgnored(designId))
        {
            designId = "";
            rawDesign = null;
        }


        // Identities whose sail number and name (or aliases) match the entry: each boat seen
        // through its matching identity. Design decides between boats sharing a name.
        List<Boat> designed = new ArrayList<>();
        List<Boat> otherDesign = new ArrayList<>();
        for (Boat candidate : identitiesMatching(normSailNo, normName))
        {
            // A boat whose design is now ignored is a stale orphan: invisible to matching
            if (isDesignIgnored(candidate.designId()))
                continue;
            if (isBlank(designId))
            {
                // No design on the entry: a design-less boat is the one; designed ones are
                // candidates (one is the boat, several are ambiguous)
                if (isNotBlank(candidate.designId()))
                    designed.add(candidate);
                else
                    return candidate;
            }
            else if (isNotBlank(candidate.designId()))
            {
                if (Objects.equals(designId, candidate.designId()))
                    return candidate;
                // Entered as another design, which an alias says is this boat's
                if (candidate.identity().enteredAs(normSailNo, normName, designId))
                    return candidate;
                // Another design: another boat, unless the entry matched one of its aliases
                // (a spelling someone merged into it), which says it is this boat
                if (!candidate.identity().isNamed(normSailNo, normName))
                    otherDesign.add(candidate);
            }
            else
            {
                // The entry has a design the matching boat lacks: the boat takes it
                Design design = isNotBlank(rawDesign) ? findOrCreateDesign(rawDesign) : findOrCreateDesign(designId);
                Boat upgraded = redesignBoat(candidate, design != null ? design.id() : designId, "Design upgrade");
                LOG.info("Upgraded boat {} → {} with design {}", candidate.id(), upgraded.id(), upgraded.designId());
                return upgraded;
            }
        }
        if (designed.size() == 1)
            return designed.getFirst();
        // Design-free fallback: no boat of the entry's design has this sail number and name, but
        // exactly one boat of another design has it as an alias: the entry's design is taken to
        // be misreported (logged). Several such boats: a new boat, below.
        if (designed.isEmpty() && otherDesign.size() == 1)
        {
            Boat only = otherDesign.getFirst();
            LOG.info("Boat {}/{} entered as design {} matches {} of design {}: taken as that boat",
                normSailNo, normName, designId, only.id(), only.designId());
            return only;
        }

        // Name-equivalence fallback ("Sticky II" = "Sticky 2", "The Goat" = "Goat"): an
        // identity with the same sail number whose name collapses to the same key, of a
        // compatible design. A unique hit takes the entry's name as an alias.
        if (designed.isEmpty() && otherDesign.isEmpty())
        {
            String matchKey = IdGenerator.nameMatchKey(rawName);
            if (!matchKey.isEmpty())
            {
                List<Boat> keyHits = new ArrayList<>();
                String key = IdGenerator.sailKey(normSailNo);
                for (Boat b : boats.values())
                {
                    if (isDesignIgnored(b.designId()))
                        continue;
                    if (isNotBlank(designId) && isNotBlank(b.designId()) && !Objects.equals(designId, b.designId()))
                        continue;
                    for (Map.Entry<String, Identity> e : b.identities().entrySet())
                    {
                        Identity i = e.getValue();
                        if (key.equals(IdGenerator.sailKey(i.sailNumber()))
                            && matchKey.equals(IdGenerator.nameMatchKey(i.name())))
                        {
                            keyHits.add(b.withId(e.getKey()));
                            break;
                        }
                    }
                }
                if (keyHits.size() == 1)
                {
                    Boat hit = keyHits.getFirst();
                    BoatAlias entryAlias = new BoatAlias(rawSailNo, rawName);
                    Boat aliased = hit.updateIdentity(i -> i.withAlias(entryAlias))
                        .withLoadedAt(null);
                    if (!aliased.equals(hit))
                    {
                        putBoat(aliased);
                        LOG.info("Name-equivalence: '{}' is {} ('{}'), added as an alias", rawName, hit.id(), hit.name());
                    }
                    if (isBlank(aliased.designId()) && isNotBlank(designId))
                        return redesignBoat(aliased, designId, "Design upgrade");
                    return aliased;
                }
                if (keyHits.size() > 1)
                {
                    LOG.warn("Ambiguous name-equivalence match: sailNo={} matchKey={} → {} candidates: {}",
                        normSailNo, matchKey, keyHits.size(), keyHits.stream().map(Boat::id).toList());
                    logAmbiguousMatch(source, normSailNo, normName, designId, keyHits);
                }
            }
        }
        else
        {
            // Several boats of other designs share the sail number and name, and the entry
            // does not say which: a new boat, to be merged or linked by hand
            List<Boat> candidates = designed.isEmpty() ? otherDesign : designed;
            LOG.warn("Ambiguous boat match: sailNo={} name={} design={} -- {} candidates with different designs; new boat",
                normSailNo, normName, designId, candidates.size());
            logAmbiguousMatch(source, normSailNo, normName, designId, candidates);
        }

        // No match (or an ambiguous one): a new boat with one identity
        {
            Design design = isNotBlank(rawDesign) ? findOrCreateDesign(rawDesign) : findOrCreateDesign(designId);
            String newBoatId = IdGenerator.generateBoatId(normSailNo, normName, design);

            // Defensive: if a boat with this ID already exists (e.g. due to name case
            // variants that normalise to the same ID), return it instead of overwriting.
            Boat existingById = boat(newBoatId);
            if (existingById != null)
            {
                LOG.debug("Boat ID {} already exists (existing name='{}', incoming name='{}'), returning existing",
                    newBoatId, existingById.name(), rawName);
                return existingById;
            }

            // A new boat has no club yet: importers add the clubs it races at
            List<String> newClubIds = List.of();
            Boat newBoat = new Boat(
                newBoatId,
                normSailNo,
                rawName,
                design != null ? design.id() : null,
                newClubIds,
                List.of(),
                List.of(sourceDesign),
                Instant.now(),
                null);
            putBoat(newBoat);
            return newBoat;
        }

    }

    /** Each boat with an identity matching this sail number and name, seen through that identity. */
    private List<Boat> identitiesMatching(String sailNo, String normName)
    {
        List<Boat> out = new ArrayList<>();
        if (isBlank(sailNo) || isBlank(normName))
            return out;
        for (Boat b : boats.values())
        {
            for (Map.Entry<String, Identity> e : b.identities().entrySet())
            {
                if (e.getValue().matches(sailNo, normName))
                {
                    out.add(b.withId(e.getKey()));
                    break;
                }
            }
        }
        return out;
    }

    /**
     * Append a single tab-separated record describing an ambiguous boat match to
     * {@code <dataRoot>/log/ambiguous-boats.log}. Each record names the source importer,
     * the incoming sail/name/design, and the list of existing candidate boats (boatId:designId).
     * Failures are reported via {@code LOG.warn} and never propagate -- logging must not
     * break an import.
     */
    private void logAmbiguousMatch(String source, String normSailNo, String normName,
                                   String designId, List<Boat> candidates)
    {
        try
        {
            Path logDir = root.resolve("log");
            Files.createDirectories(logDir);
            Path file = logDir.resolve("ambiguous-boats.log");
            StringBuilder sb = new StringBuilder();
            sb.append(Instant.now())
                .append('\t').append(source == null ? "" : source)
                .append('\t').append(normSailNo)
                .append('\t').append(normName)
                .append('\t').append(isBlank(designId) ? "(none)" : designId)
                .append('\t');
            for (int i = 0; i < candidates.size(); i++)
            {
                if (i > 0)
                    sb.append(',');
                Boat c = candidates.get(i);
                sb.append(c.id()).append(':').append(c.designId() == null ? "(none)" : c.designId());
            }
            sb.append('\n');
            Files.writeString(file, sb.toString(),
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);
        }
        catch (IOException e)
        {
            LOG.warn("Failed to append to ambiguous-boats.log: {}", e.getMessage());
        }
    }

    // --- findOrCreateBoat helpers ---

    private static String buildSourceEntry(String source, String rawDesign)
    {
        if (source == null) return ":" + rawDesign;
        return rawDesign != null && !rawDesign.isBlank() ? source + ":" + rawDesign : source;
    }

    /** Finds or creates a design by class name -- used internally by findOrCreateBoat. */
    /**
     * The design ID a class name resolves to — the same lookup as {@link #findOrCreateDesign}
     * (exact ID, a held design's name or alias, then the alias seed) — without creating
     * anything. Null for a blank name.
     */
    public String resolveDesignId(String className)
    {
        requireStarted();
        if (className == null || className.isBlank())
            return null;
        String designId = IdGenerator.normaliseDesignName(className);
        if (designs.containsKey(designId))
            return designId;
        for (Design d : designs.values())
        {
            if (designNameMatches(d, designId))
                return d.id();
        }
        String canonicalId = aliases.resolveDesignAlias(designId);
        return canonicalId != null ? canonicalId : designId;
    }

    public String resolveDesignId(String className, Boat boat, LocalDate date)
    {
        requireStarted();
        String resolved = resolveDesignId(className);
        // A boat whose results are entered as another design (an alias says so) is its own
        if (boat != null && boat.designId() != null && !boat.designId().equals(resolved)
            && boat.identity().enteredAs(boat.sailNumber(), IdGenerator.normaliseName(boat.name()), resolved))
            return boat.designId();
        return resolved;
    }

    /** The word "modified" (optionally followed by digits, as in "Modified2") in a design name. */
    private static final Pattern MODIFIED_WORD = Pattern.compile("(?i)\\bmodified\\d*\\b");

    /** True for the id of a "… Modified" design (one whose id contains "modified"). */
    static boolean isModifiedDesignId(String designId)
    {
        return designId != null && designId.contains("modified");
    }

    /**
     * For a "… Modified" design, the id of the base design its boats are modified examples of:
     * the design named by its canonical name without "Modified" ("Farr 40 Modified" → Farr 40,
     * "Magic Modified 25" → Magic 25), found via the usual name/alias lookup or created.
     * Null when the design is not a modified one or yields no usable base design.
     */
    public String baseOfModifiedDesign(String designId)
    {
        requireStarted();
        if (!isModifiedDesignId(designId))
            return null;
        Design d = designs.get(designId);
        String name = withoutModified(d != null ? d.canonicalName() : designId);
        if (name == null)
            return null;
        Design base = findOrCreateDesign(name);
        if (base == null)
            return null;
        // A held design that is itself an alias (e.g. s80 → swarbricks80) resolves to its canonical
        String canonical = aliases.resolveDesignAlias(base.id());
        String baseId = canonical != null && designs.containsKey(canonical) ? canonical : base.id();
        return isModifiedDesignId(baseId) ? null : baseId;
    }

    /**
     * A design name without the word "Modified" ("Mount Gay (Modified) 30" → "Mount Gay 30"),
     * or null if nothing is left.
     */
    private static String withoutModified(String designName)
    {
        if (designName == null)
            return null;
        String name = MODIFIED_WORD.matcher(designName).replaceAll(" ").replace("()", " ");
        if (name.chars().filter(c -> c == '(').count() != name.chars().filter(c -> c == ')').count())
            name = name.replace("(", " ").replace(")", " ");
        name = name.replaceAll("\\s+", " ").trim();
        return name.isEmpty() ? null : name;
    }

    private boolean markModified(Boat b)
    {
        if (b.modified())
            return false;
        setBoatModified(b.id(), true);
        return true;
    }

    /**
     * Folds a boat of a "… Modified" design into the base design (see
     * {@link #baseOfModifiedDesign}), ticking it modified, moves design overrides targeting
     * the modified design to the base, and removes the modified design once no boat uses it. Returns the boat as it now is (possibly re-keyed or
     * merged), or the boat unchanged when it is not of a modified design.
     */
    private Boat foldModifiedDesign(Boat b)
    {
        if (b == null || !isModifiedDesignId(b.designId()))
            return b;
        String modifiedDesignId = b.designId();
        String base = baseOfModifiedDesign(modifiedDesignId);
        if (base == null)
            return b;
        if (markModified(b))
            b = boat(b.id());
        Boat folded = redesignBoat(b, base, "Modified-design fold");
        if (boats.values().stream().noneMatch(o -> modifiedDesignId.equals(o.designId())))
            removeDesign(modifiedDesignId);
        return folded;
    }

    /**
     * Sets the design of the boat with identity {@code identityId}: every identity ID changes
     * (it carries the design), with its results, or the boat merges into one already recorded
     * under that design. Callers must call {@link #save()}.
     *
     * @return the boat, seen through the same identity (re-keyed)
     */
    public Boat setBoatDesign(String identityId, String designId)
    {
        requireStarted();
        Boat b = boat(identityId);
        if (b == null)
            throw new IllegalArgumentException("Boat not found: " + identityId);
        if (Objects.equals(b.designId(), designId))
            return b;
        Boat moved = redesignBoat(b, designId, "Edit");
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return moved;
    }

    private Boat redesignBoat(Boat b, String designId, String why)
    {
        Design design = designs.get(designId);
        Map<String, Identity> rekeyed = new LinkedHashMap<>();
        Map<String, String> renames = new LinkedHashMap<>();
        for (Map.Entry<String, Identity> e : b.identities().entrySet())
        {
            Identity i = e.getValue();
            String newId = design != null ? IdGenerator.generateBoatId(i.sailNumber(), i.name(), design)
                : renameBoatId(i.sailNumber(), i.name(), designId);
            rekeyed.put(newId, i);
            if (!newId.equals(e.getKey()))
                renames.put(e.getKey(), newId);
        }
        // Another boat already at a new ID (the same boat recorded under the design): merge into it
        for (String newId : renames.values())
        {
            String other = identityIndex.get(newId);
            if (other != null && !b.hasIdentity(newId))
            {
                LOG.info("{}: merging {} into {} (designId {} → {})", why, b.id(), newId, b.designId(), designId);
                mergeBoats(newId, List.of(b.id()));
                Boat merged = boat(newId);
                if (!Objects.equals(merged.designId(), designId))
                {
                    merged = merged.withDesignId(designId).withLoadedAt(null);
                    putBoat(merged);
                }
                return merged;
            }
        }
        LOG.info("{}: updating designId {} → {} for boat {}", why, b.designId(), designId, b.id());
        String through = renames.getOrDefault(b.id(), b.id());
        Boat updated = new Boat(through, designId, b.modified(), b.links(), rekeyed, Instant.now(), null);
        removeBoat(b.id());
        putBoat(updated);
        renames.forEach(this::rewriteFinisherBoatId);
        return updated;
    }

    private Design findOrCreateDesign(String className)
    {
        if (className == null || className.isBlank())
            return null;
        String designId = IdGenerator.normaliseDesignName(className);
        if (isDesignIgnored(designId))
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
            // Canonical design not yet in store -- create it using the seed's canonical name
            String seedName = aliases.designCanonicalName(canonicalId);
            design = new Design(canonicalId, seedName != null ? seedName : className.trim(),
                List.of(), List.of(), null, false, null);
            putDesign(design);
            if (designCatalogue.isExcluded(canonicalId))
                LOG.info("Design {} is excluded (dinghy/OTB class)", canonicalId);
            return design;
        }

        design = new Design(designId, className.trim(), List.of(), List.of(), null, false, null);
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
     * Merge two ordered club id lists, preserving order, dropping duplicates.
     */
    private static List<String> mergeClubIds(List<String> a, List<String> b)
    {
        if (a == null || a.isEmpty())
            return b == null ? List.of() : List.copyOf(b);
        if (b == null || b.isEmpty())
            return List.copyOf(a);
        LinkedHashSet<String> merged = new LinkedHashSet<>(a);
        merged.addAll(b);
        return List.copyOf(merged);
    }

    /**
     * Gives a boat a home club from a results page's club code (e.g. "MYC"), when the boat
     * has no club yet and has not been explicitly marked as having none. A code equal to
     * the short name of {@code organisingClubId} (nullable) means that club, since short
     * names such as "MYC" are shared by clubs in different states; any other code is
     * resolved with {@link #findUniqueClubByShortName}. An unknown or ambiguous code
     * leaves the boat unchanged.
     *
     * @return the boat as now stored (the input boat if nothing changed)
     */
    public Boat assignClubFromCode(Boat boat, String clubCode, String organisingClubId,
                                   String source, String context)
    {
        requireStarted();
        if (clubCode == null || clubCode.isBlank() || !boat.clubIds().isEmpty()
            || isExplicitlyNoClub(boat.id()))
            return boat;
        Club organising = organisingClubId == null ? null
            : Optional.ofNullable(clubs().get(organisingClubId)).orElse(clubSeed().get(organisingClubId));
        Club club = organising != null && clubCode.trim().equalsIgnoreCase(organising.shortName())
            ? organising
            : findUniqueClubByShortName(clubCode, null, context);
        if (club == null)
            return boat;
        Boat updated = boat.updateIdentity(i -> i.withClubIds(List.of(club.id())).withSource(source))
            .withLastUpdated(Instant.now()).withLoadedAt(null);
        putBoat(updated);
        return updated;
    }

    /**
     * Records that {@code raceId} belongs to the series {@code seriesId} of club
     * {@code clubId}, creating the series (named {@code seriesName}) if the club does not
     * yet have it. A club present only in the seed (clubs.yaml) is persisted first. Does
     * nothing if the club is unknown.
     */
    public void addRaceToClubSeries(String clubId, String seriesId, String seriesName, String raceId)
    {
        requireStarted();
        Club club = clubs().get(clubId);
        if (club == null)
        {
            Club seed = clubSeed().get(clubId);
            if (seed == null)
                return;
            club = new Club(seed.id(), seed.shortName(), seed.longName(), seed.state(), seed.excluded(),
                seed.email(),
                seed.aliases() != null ? seed.aliases() : List.of(),
                seed.topyachtGroups() != null ? seed.topyachtGroups() : List.of(),
                seed.sailsysEvents() != null ? seed.sailsysEvents() : List.of(),
                List.of(), null);
            putClub(club);
        }

        List<Series> series = new ArrayList<>(club.series() != null ? club.series() : List.of());
        int idx = -1;
        for (int i = 0; i < series.size(); i++)
        {
            if (seriesId.equals(series.get(i).id()))
            {
                idx = i;
                break;
            }
        }
        if (idx >= 0)
        {
            Series existing = series.get(idx);
            if (existing.raceIds().contains(raceId))
                return;
            List<String> raceIds = new ArrayList<>(existing.raceIds());
            raceIds.add(raceId);
            series.set(idx, new Series(existing.id(), existing.name(), existing.isCatchAll(),
                List.copyOf(raceIds)));
        }
        else
        {
            series.add(new Series(seriesId, seriesName, false, List.of(raceId)));
        }
        putClub(new Club(club.id(), club.shortName(), club.longName(), club.state(), club.excluded(),
            club.email(), club.aliases(), club.topyachtGroups(), club.sailsysEvents(),
            List.copyOf(series), null));
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
            .toList();
        List<Club> nonExcluded = allClubs.stream().filter(c -> !isClubExcluded(c.id())).toList();

        // Primary: exact short name match -- prefer non-excluded, fall back to all if needed
        List<Club> matches = nonExcluded.stream()
            .filter(c -> shortName.equalsIgnoreCase(c.shortName()))
            .toList();
        if (matches.isEmpty())
            matches = allClubs.stream().filter(c -> shortName.equalsIgnoreCase(c.shortName())).toList();

        // Fallback: long name or alias match (handles full-name club fields from BWPS etc.)
        if (matches.isEmpty())
        {
            matches = nonExcluded.stream()
                .filter(c -> shortName.equalsIgnoreCase(c.longName())
                          || c.aliases().stream().anyMatch(shortName::equalsIgnoreCase))
                .toList();
            if (matches.isEmpty())
                matches = allClubs.stream()
                    .filter(c -> shortName.equalsIgnoreCase(c.longName())
                              || c.aliases().stream().anyMatch(shortName::equalsIgnoreCase))
                    .toList();
        }

        // Fallback: compound name (e.g. "CYCA/RPEYC") -- try each slash-separated token in order
        if (matches.isEmpty() && shortName.contains("/"))
        {
            for (String token : shortName.split("/"))
            {
                String t = token.trim();
                if (t.isBlank())
                    continue;
                matches = nonExcluded.stream()
                    .filter(c -> t.equalsIgnoreCase(c.shortName())
                              || t.equalsIgnoreCase(c.longName())
                              || c.aliases().stream().anyMatch(t::equalsIgnoreCase))
                    .toList();
                if (matches.isEmpty())
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
            // Narrowing didn't resolve it -- fall through to ambiguity log below
            matches = narrowed.isEmpty() ? matches : narrowed;
        }
        if (matches.size() > 1)
        {
            // If ambiguous across excluded/non-excluded, prefer non-excluded
            List<Club> preferNonExcluded = matches.stream().filter(c -> !isClubExcluded(c.id())).toList();
            if (preferNonExcluded.size() == 1)
                return preferNonExcluded.getFirst();
            LOG.warn("Ambiguous club name={} -- {} matches ({}); clubId not set ({})",
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
            LOG.error("Ambiguous club shortName={} state={} -- {} matches ({})",
                shortName, state, matches.size(), context);
            return null;
        }
        return matches.getFirst();
    }

    // --- Write mutators (require started; loadedAt = null → always written by save()) ---

    /**
     * Stores a boat, under its current identity's ID, whichever identity it is seen through.
     * If its current identity changed, the record (and its file) moves to the new ID, and its
     * finishers are recounted for it. Identities it no longer has stop referring to it.
     */
    public void putBoat(Boat boat)
    {
        requireStarted();
        Boat stored = boat.current();
        String key = stored.id();
        String oldKey = null;
        for (String identityId : boat.identities().keySet())
        {
            oldKey = identityIndex.get(identityId);
            if (oldKey != null)
                break;
        }
        Boat previous = oldKey == null ? boats.get(key) : boats.get(oldKey);
        if (previous != null)
            previous.identities().keySet().forEach(i -> identityIndex.remove(i));
        if (oldKey != null && !oldKey.equals(key))
        {
            boats.remove(oldKey);
            deleteBoatFile(oldKey);
        }
        boats.put(key, stored);
        indexIdentities(stored);
        boolean keyChanged = oldKey != null && !oldKey.equals(key);
        boolean identitiesChanged = previous == null ? stored.identities().size() > 1
            : !previous.identities().keySet().equals(stored.identities().keySet());
        if (keyChanged || identitiesChanged)
        {
            Set<String> touched = new HashSet<>(stored.identities().keySet());
            if (previous != null)
                touched.addAll(previous.identities().keySet());
            recanonicalise(touched);
        }
        InvalidationListener l = invalidationListener;
        if (l != null)
        {
            l.onBoatChanged(key);
            if (oldKey != null && !oldKey.equals(key))
                l.onBoatChanged(oldKey);
        }
    }

    private void deleteBoatFile(String key)
    {
        try
        {
            Files.deleteIfExists(boatsDir.resolve(key + ".json"));
        }
        catch (IOException e)
        {
            LOG.warn("Could not delete boat file {}: {}", key, e.getMessage());
        }
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
        // Always reflect the catalogue's current noSpinnaker state on the in-memory record.
        boolean catNoSpin = designCatalogue.isNoSpinnaker(design.id());
        Design stamped = design.noSpinnaker() == catNoSpin ? design : design.withNoSpinnaker(catNoSpin);
        designs.put(stamped.id(), stamped);
        InvalidationListener l = invalidationListener;
        if (l != null)
            l.onDesignChanged(stamped.id());
    }

    public void putRace(Race imported)
    {
        requireStarted();
        Race race = canonicalise(imported);
        boolean isNew = !races.containsKey(race.id());
        races.put(race.id(), race);
        if (autoSanityCheck && isNew && !excludedRaces.containsKey(race.id()))
        {
            RaceSanityChecker.check(race).ifPresent(issue ->
            {
                String reason = "sanity-check: " + issue.checkName() + " -- " + issue.description();
                excludedRaces.put(race.id(), reason);
                LOG.info("Race sanity check '{}' auto-excluded race {}: {}",
                    issue.checkName(), race.id(), issue.description());
                saveExclusions();
            });
        }
        InvalidationListener l = invalidationListener;
        if (l != null) l.onRaceChanged(race.id());
    }

    /**
     * Enable or disable the import-time race sanity checker. When enabled (the default),
     * {@link #putRace} runs {@link RaceSanityChecker} on first insert and auto-adds any
     * suspicious race to the exclusion list. Tests using synthetic fixtures with identical
     * or round elapsed times should disable this.
     */
    public void setAutoSanityCheck(boolean enabled)
    {
        this.autoSanityCheck = enabled;
    }

    public Map<String, Race> races()
    {
        requireStarted();
        return Collections.unmodifiableMap(races);
    }

    /**
     * A read-only view of this store as at {@code cutoff}: races before that date, and
     * certificates for years up to its year; everything else as now. For rerunning the
     * analysis as at a past date. The view supports only the reads the analysis makes.
     */
    public DataStore asOf(java.time.LocalDate cutoff)
    {
        requireStarted();
        return new DatedDataStore(this, cutoff);
    }

    /** Removes the boat with identity {@code id} (any of its identities), and its file. */
    public void removeBoat(String id)
    {
        requireStarted();
        String key = identityIndex.getOrDefault(id, id);
        Boat existing = boats.remove(key);
        if (existing != null)
        {
            existing.identities().keySet().forEach(i -> identityIndex.remove(i, key));
            deleteBoatFile(key);
            InvalidationListener l = invalidationListener;
            if (l != null) l.onBoatChanged(key);
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

        // Build merged aliases -- add canonical names and existing aliases from merged-away designs
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
        List<List<Link>> allLinks = new ArrayList<>();
        allLinks.add(keepDesign.links());
        for (Design md : toMerge)
            allLinks.add(md.links());
        List<Link> mergedLinks = Link.merge(allLinks);
        putDesign(new Design(keepDesign.id(), keepDesign.canonicalName(),
            List.copyOf(allAliases), List.copyOf(allSources), mergedLinks, Instant.now(),
            keepDesign.noSpinnaker(), null));

        // Move all boats of a merged-away design to the kept one: their identity IDs change
        // (or they merge into a boat already recorded under the kept design), results follow
        Set<String> mergeIdSet = new HashSet<>(mergeIds);
        List<Boat> affected = boats.values().stream().filter(b -> mergeIdSet.contains(b.designId())).toList();
        Set<String> movedIdentities = new HashSet<>();
        for (Boat boat : affected)
            for (Map.Entry<String, Identity> e : boat.identities().entrySet())
                if (!e.getKey().equals(IdGenerator.generateBoatId(e.getValue().sailNumber(), e.getValue().name(), keepDesign)))
                    movedIdentities.add(e.getKey());
        int updatedRaces = 0;
        int updatedFinishers = 0;
        for (Race race : races.values())
        {
            if (race.divisions() == null)
                continue;
            long n = race.divisions().stream().flatMap(d -> d.finishers().stream())
                .filter(f -> movedIdentities.contains(f.identityId())).count();
            if (n > 0)
            {
                updatedRaces++;
                updatedFinishers += (int)n;
            }
        }
        for (Boat boat : affected)
            if (boat(boat.id()) != null)
                redesignBoat(boat(boat.id()), keepId, "Design merge");
        int updatedBoats = affected.size();

        // Delete merged-away design files
        for (Design md : toMerge)
            removeDesign(md.id());

        if (!movedIdentities.isEmpty())
            reloadClubCatalogue();

        LOG.info("mergeDesigns: kept={} merged={} updatedBoats={} updatedRaces={} updatedFinishers={}",
            keepId, mergeIds, updatedBoats, updatedRaces, updatedFinishers);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return new DesignMergeResult(updatedBoats, updatedRaces, updatedFinishers);
    }

    /**
     * Returns true if the given design ID is configured as excluded (dinghy/OTB class).
     * The PF optimiser uses this to skip excluded designs during calculation.
     * Raw records are still created -- exclusion is a configuration concern, not a data concern.
     * Checks both the static design.yaml catalogue and any UI-driven overrides.
     */
    // Design excluded/ignored state is persisted in design.yaml (curated catalogue) and
    // loaded into the DesignCatalogue at start / reload. The runtime helpers just delegate.
    public boolean isDesignExcluded(String designId)
    {
        requireStarted();
        return designCatalogue.isExcluded(designId);
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
     * Toggles the excluded flag on a club. The flag is YAML-owned: this writes
     * {@code clubs.yaml} (auto-creating the entry if needed) and refreshes the
     * in-memory seed and the corresponding entry in the persisted clubs map.
     */
    public void setClubExcluded(String clubId, boolean excluded)
    {
        requireStarted();
        Club existing = clubs.get(clubId);
        Club seed = clubSeed.get(clubId);
        if (existing == null && seed == null)
            throw new IllegalArgumentException("Unknown club: " + clubId);

        String shortNameIfNew = existing != null ? existing.shortName()
            : seed.shortName();
        boolean changed = ClubLoader.setClubExcluded(configDir, clubId, shortNameIfNew, excluded);
        if (!changed)
            return;

        clubSeed = ClubLoader.load(configDir);
        if (existing != null)
            clubs.put(clubId, enrichWithSeed(existing));
    }

    /**
     * Updates the YAML-owned metadata fields ({@code longName}, {@code state}, {@code email})
     * for a club. Each value is written verbatim -- passing {@code null} clears the field.
     * Auto-creates the YAML entry if missing. Refreshes the in-memory seed and the
     * corresponding entry in the persisted clubs map.
     */
    public void updateClubMeta(String clubId, String longName, String state, String email)
    {
        requireStarted();
        Club existing = clubs.get(clubId);
        Club seed = clubSeed.get(clubId);
        if (existing == null && seed == null)
            throw new IllegalArgumentException("Unknown club: " + clubId);

        String shortNameIfNew = existing != null ? existing.shortName()
            : seed.shortName();
        boolean changed = ClubLoader.updateClubMeta(configDir, clubId, shortNameIfNew,
            longName, state, email);
        if (!changed)
            return;

        clubSeed = ClubLoader.load(configDir);
        if (existing != null)
            clubs.put(clubId, enrichWithSeed(existing));
    }

    /**
     * Normalises TopYacht groups exactly as persisting them to clubs.yaml would: prefixes
     * slugged, blank names dropped, URLs de-duplicated within a group, groups sharing a
     * prefix merged, and groups with no prefix or no URLs discarded. Exposed so the admin
     * API can validate an incoming edit against what would actually be stored.
     */
    public static List<TopYachtGroup> cleanTopyachtGroups(List<TopYachtGroup> groups)
    {
        return ClubLoader.cleanGroups(groups);
    }

    /** Normalises SailSys events exactly as persisting them to clubs.yaml would. */
    public static List<SailSysEvent> cleanSailsysEvents(List<SailSysEvent> events)
    {
        return ClubLoader.cleanSailsysEvents(events);
    }

    /**
     * Updates the YAML-owned {@code sailsys} events for a club. A null or empty list clears
     * the field. Auto-creates the YAML entry if missing.
     */
    public void updateClubSailsysEvents(String clubId, List<SailSysEvent> sailsysEvents)
    {
        requireStarted();
        Club existing = clubs.get(clubId);
        Club seed = clubSeed.get(clubId);
        if (existing == null && seed == null)
            throw new IllegalArgumentException("Unknown club: " + clubId);

        String shortNameIfNew = existing != null ? existing.shortName() : seed.shortName();
        if (!ClubLoader.updateClubSailsysEvents(configDir, clubId, shortNameIfNew, sailsysEvents))
            return;

        clubSeed = ClubLoader.load(configDir);
        if (existing != null)
            clubs.put(clubId, enrichWithSeed(existing));
    }

    /**
     * Updates the YAML-owned {@code topyacht} groups for a club. A null or empty
     * list clears the field. Auto-creates the YAML entry if missing.
     */
    public void updateClubTopyachtGroups(String clubId, List<TopYachtGroup> topyachtGroups)
    {
        requireStarted();
        Club existing = clubs.get(clubId);
        Club seed = clubSeed.get(clubId);
        if (existing == null && seed == null)
            throw new IllegalArgumentException("Unknown club: " + clubId);

        String shortNameIfNew = existing != null ? existing.shortName()
            : seed.shortName();
        boolean changed = ClubLoader.updateClubTopyachtGroups(configDir, clubId,
            shortNameIfNew, topyachtGroups);
        if (!changed)
            return;

        clubSeed = ClubLoader.load(configDir);
        if (existing != null)
            clubs.put(clubId, enrichWithSeed(existing));
    }

    /**
     * Returns a copy of {@code json} with the YAML-owned fields populated from the matching
     * entry in {@link #clubSeed}. If no seed entry exists, leaves the YAML fields empty and
     * logs a warning -- the club exists in JSON but has no clubs.yaml entry.
     */
    private Club enrichWithSeed(Club json)
    {
        Club seed = clubSeed.get(json.id());
        if (seed == null)
        {
            LOG.warn("Club {} loaded from JSON has no entry in clubs.yaml -- YAML-owned fields will be empty",
                json.id());
            return new Club(json.id(), json.shortName(),
                null, null, false, null, List.of(), List.of(), List.of(),
                json.series(), json.loadedAt());
        }
        return new Club(json.id(), json.shortName(),
            seed.longName(), seed.state(), seed.excluded(), seed.email(),
            seed.aliases(), seed.topyachtGroups(), seed.sailsysEvents(),
            json.series(), json.loadedAt());
    }

    /** Returns true if the boat has been manually excluded from analysis via the admin UI. */
    public boolean isBoatExcluded(String boatId)
    {
        requireStarted();
        return excludedBoats.containsKey(boatId);
    }

    /**
     * Returns the operator-supplied reason a boat was excluded, or null if it isn't excluded
     * (or was excluded without a reason).
     */
    public String boatExclusionReason(String boatId)
    {
        requireStarted();
        String r = excludedBoats.get(boatId);
        return (r == null || r.isEmpty()) ? null : r;
    }

    /** Returns true if the race has been manually excluded from analysis via the admin UI. */
    public boolean isRaceExcluded(String raceId)
    {
        requireStarted();
        return excludedRaces.containsKey(raceId);
    }

    /**
     * Returns the operator-supplied reason a race was excluded, or null if it isn't excluded
     * (or was excluded without a reason).
     */
    public String raceExclusionReason(String raceId)
    {
        requireStarted();
        String r = excludedRaces.get(raceId);
        return (r == null || r.isEmpty()) ? null : r;
    }

    public void setBoatExcluded(String id, boolean excluded)
    {
        setBoatExcluded(id, excluded, null);
    }

    public void setBoatExcluded(String id, boolean excluded, String reason)
    {
        requireStarted();
        if (excluded)
            excludedBoats.put(id, reason == null ? "" : reason);
        else
            excludedBoats.remove(id);
        saveExclusions();
    }

    public void setDesignExcluded(String id, boolean excluded)
    {
        requireStarted();
        Designs.setFlag(configDir, id, Designs.Flag.EXCLUDED, excluded);
        reloadDesignCatalogue();
    }

    /**
     * Returns true if the given design is flagged as physically unable to fly a spinnaker
     * (cat-rigged, gaff cutter, etc.). When true, the reference-factor and PF analyses
     * collapse a boat's spin and non-spin factors into a single aggregated value, since
     * any "spin entry" in the source data does not actually imply a spinnaker was flown.
     * Toggled via {@link #setDesignNoSpinnaker(String, boolean)}.
     */
    /** True if the boat's design is flagged no-spinnaker (see {@link #isDesignNoSpinnaker}). */
    public boolean isBoatNoSpinnakerDesign(String boatId)
    {
        Boat boat = boatId == null ? null : boats.get(boatId);
        return boat != null && isDesignNoSpinnaker(boat.designId());
    }

    public boolean isDesignNoSpinnaker(String designId)
    {
        if (designId == null || designId.isBlank())
            return false;
        return designCatalogue.isNoSpinnaker(designId);
    }

    public void setDesignNoSpinnaker(String id, boolean flag)
    {
        requireStarted();
        Designs.setFlag(configDir, id, Designs.Flag.NO_SPINNAKER, flag);
        reloadDesignCatalogue();
        InvalidationListener l = invalidationListener;
        if (l != null)
            l.onDesignChanged(id);
    }

    /**
     * True if the design is flagged one-design ({@code oneDesign:} in {@code design.yaml}):
     * its boats are identical, so each boat not flagged {@link #isBoatModified modified} takes
     * the design's reference factor. Toggled via {@link #setDesignOneDesign(String, boolean)}.
     */
    public boolean isDesignOneDesign(String designId)
    {
        if (designId == null || designId.isBlank())
            return false;
        return designCatalogue.isOneDesign(designId);
    }

    public void setDesignOneDesign(String id, boolean flag)
    {
        requireStarted();
        Designs.setFlag(configDir, id, Designs.Flag.ONE_DESIGN, flag);
        reloadDesignCatalogue();
        InvalidationListener l = invalidationListener;
        if (l != null)
            l.onDesignChanged(id);
    }

    /**
     * The design's manual year ({@code designYears:} in {@code design.yaml}), or null if none is
     * set. Overrides the year taken from ORC certificate age dates for the design profile.
     */
    public Integer designYearOverride(String designId)
    {
        if (designId == null || designId.isBlank())
            return null;
        return designCatalogue.designYear(designId);
    }

    /** Sets (or with {@code year} null, clears) the design's manual year. */
    public void setDesignYear(String id, Integer year)
    {
        requireStarted();
        Designs.setDesignYear(configDir, id, year);
        reloadDesignCatalogue();
        InvalidationListener l = invalidationListener;
        if (l != null)
            l.onDesignChanged(id);
    }

    /** True if the boat with identity {@code boatId} is modified from its design. */
    public boolean isBoatModified(String boatId)
    {
        Boat boat = boatId == null ? null : boat(boatId);
        return boat != null && boat.modified();
    }

    /** Marks the boat with identity {@code boatId} modified from its design, or not. */
    public void setBoatModified(String boatId, boolean flag)
    {
        requireStarted();
        Boat boat = boat(boatId);
        if (boat == null || boat.modified() == flag)
            return;
        putBoat(boat.withModified(flag).withLastUpdated(Instant.now()).withLoadedAt(null));
    }

    /**
     * True when the given design is ignored for import/analysis, per the {@code ignored:}
     * list in {@code design.yaml}. Toggled via {@link #setDesignIgnored(String, boolean)}.
     */
    public boolean isDesignIgnored(String designId)
    {
        if (designId == null || designId.isBlank()) return false;
        return designCatalogue.isIgnored(designId);
    }

    /**
     * Toggles the "ignored" flag for a design. When setting to true, cascades over all
     * boats with that designId: each is de-designed (designId cleared, boat id reverts
     * to {@code sailNo-name}). If another boat already sits at the target id, the two
     * are merged (certificates deduplicated, sources unioned, clubId preserved from
     * the target) and race finisher references rewritten from the old id to the target.
     * A {@code "Ignored:<designId>"} entry is added to the affected boat's sources.
     * <p>
     * When setting to false, only the flag changes -- previously de-designed boats are
     * not restored (their original design is unrecoverable). Callers must call
     * {@link #save()} to persist the entity-level changes.
     */
    public void setDesignIgnored(String id, boolean ignored)
    {
        requireStarted();
        boolean wasIgnored = isDesignIgnored(id);
        Designs.setFlag(configDir, id, Designs.Flag.IGNORED, ignored);
        reloadDesignCatalogue();
        if (ignored && !wasIgnored)
            cascadeIgnoreDesign(id);
    }

    /**
     * Rewrites every boat whose designId equals {@code ignoredId} so it has no design.
     * When a target boat id (sailNo-name) is already occupied, merges instead of renaming.
     */
    private void cascadeIgnoreDesign(String ignoredId)
    {
        String note = "Ignored:" + ignoredId;
        boolean clubConfigChanged = false;
        for (Boat boat : List.copyOf(boats.values()))
        {
            if (!ignoredId.equals(boat.designId()))
                continue;
            // The boat loses the design (re-keying its identities, or merging into the boat
            // already recorded without it); its identities note why
            Boat noted = boat.mapIdentities(i -> i.withSource(note)).withLoadedAt(null);
            putBoat(noted);
            Boat moved = redesignBoat(boat(noted.id()), null, "Ignore cascade");
            clubConfigChanged |= !moved.identities().keySet().equals(noted.identities().keySet());
        }
        if (clubConfigChanged)
            reloadClubCatalogue();
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
    }

    public void setRaceExcluded(String id, boolean excluded)
    {
        setRaceExcluded(id, excluded, null);
    }

    public void setRaceExcluded(String id, boolean excluded, String reason)
    {
        requireStarted();
        if (excluded)
            excludedRaces.put(id, reason == null ? "" : reason);
        else
            excludedRaces.remove(id);
        saveExclusions();
    }

    /**
     * Returns true if either the race name or series name matches any of the
     * configured series exclusion regex patterns (case-insensitive).
     */
    public boolean matchesSeriesExclusion(String raceName, String seriesName)
    {
        List<Pattern> patterns = compiledSeriesPatterns;
        if (patterns.isEmpty()) return false;
        for (Pattern p : patterns)
        {
            if (raceName != null && p.matcher(raceName).find()) return true;
            if (seriesName != null && p.matcher(seriesName).find()) return true;
        }
        return false;
    }

    /**
     * Returns true if the given series name matches any series exclusion pattern.
     */
    public boolean isSeriesExcluded(String seriesName)
    {
        return matchesSeriesExclusion(null, seriesName);
    }

    /**
     * Returns the reason associated with the first series-exclusion pattern that matches the
     * given series name, or null if no pattern matches or the matching pattern has no reason.
     */
    public String seriesExclusionReason(String seriesName)
    {
        if (seriesName == null)
            return null;
        for (Map.Entry<String, String> e : excludedSeries.entrySet())
        {
            try
            {
                if (Pattern.compile(e.getKey(), Pattern.CASE_INSENSITIVE).matcher(seriesName).find())
                {
                    String r = e.getValue();
                    return (r == null || r.isEmpty()) ? null : r;
                }
            }
            catch (Exception ignored)
            { /* skip malformed */ }
        }
        return null;
    }

    public void setSeriesExcluded(String seriesName, boolean excluded)
    {
        setSeriesExcluded(seriesName, excluded, null);
    }

    /**
     * Adds or removes a series exclusion pattern. When adding, the series name
     * is wrapped as a regex that matches the full string (case-insensitive).
     * When removing, any pattern whose regex matches the exact name is removed.
     * On add, {@code reason} is stored alongside the pattern.
     */
    public void setSeriesExcluded(String seriesName, boolean excluded, String reason)
    {
        requireStarted();
        String escaped = "^" + Pattern.quote(seriesName) + "$";
        if (excluded)
        {
            if (!isSeriesExcluded(seriesName))
            {
                excludedSeries.put(escaped, reason == null ? "" : reason);
                compileSeriesPatterns();
                saveExclusions();
            }
        }
        else
        {
            // Remove any pattern that fully matches this series name
            boolean changed = excludedSeries.keySet().removeIf(p ->
            {
                try { return Pattern.compile(p, Pattern.CASE_INSENSITIVE).matcher(seriesName).find(); }
                catch (Exception e) { return false; }
            });
            if (changed)
            {
                compileSeriesPatterns();
                saveExclusions();
            }
        }
    }

    /**
     * On-disk shape for exclusions.yaml. Each list element is an object with
     * {@code id}/{@code pattern} and {@code reason}. The loader also accepts the
     * legacy flat-string form for each list (bare ID/pattern, no reason) so
     * pre-existing files upgrade cleanly on next save.
     */
    private static class ExclusionsFile
    {
        public List<Entry> boats = new ArrayList<>();
        public List<Entry> races = new ArrayList<>();
        public List<SeriesEntry> series = new ArrayList<>();

        public static class Entry
        {
            public String id;
            public String reason;

            public Entry()
            {
            }

            public Entry(String id, String reason)
            {
                this.id = id;
                this.reason = reason;
            }
        }

        public static class SeriesEntry
        {
            public String pattern;
            public String reason;

            public SeriesEntry()
            {
            }

            public SeriesEntry(String pattern, String reason)
            {
                this.pattern = pattern;
                this.reason = reason; }
        }
    }

    private void loadExclusions()
    {
        Path yamlFile = configDir.resolve("exclusions.yaml");
        Path jsonFile = configDir.resolve("exclusions.json");
        Path file = Files.exists(yamlFile) ? yamlFile : jsonFile;
        if (!Files.exists(file))
            return;
        try
        {
            boolean isYaml = file.equals(yamlFile);
            // Parse as a tree so we can accept either the legacy bare-string list shape
            // or the new object-list shape transparently.
            com.fasterxml.jackson.databind.JsonNode root =
                (isYaml ? YAML_MAPPER : MAPPER).readTree(file.toFile());
            loadEntriesInto(root.path("boats"), excludedBoats);
            loadEntriesInto(root.path("races"), excludedRaces);
            // Series entries use 'pattern' instead of 'id'.
            com.fasterxml.jackson.databind.JsonNode seriesNode = root.path("series");
            if (seriesNode.isArray())
            {
                for (com.fasterxml.jackson.databind.JsonNode n : seriesNode)
                {
                    if (n.isTextual())
                        excludedSeries.put(n.asText(), "");
                    else if (n.isObject())
                    {
                        String pat = n.path("pattern").asText(null);
                        if (pat == null || pat.isEmpty())
                            pat = n.path("id").asText(null);
                        if (pat == null || pat.isEmpty())
                            continue;
                        String reason = n.path("reason").asText("");
                        excludedSeries.put(pat, reason);
                    }
                }
                compileSeriesPatterns();
            }
            LOG.info("Loaded exclusions from {}: {} boats, {} races, {} series patterns",
                file.getFileName(), excludedBoats.size(), excludedRaces.size(),
                excludedSeries.size());
            // Migrate: if loaded from JSON, save as YAML and delete the JSON file
            if (!isYaml)
            {
                saveExclusions();
                Files.deleteIfExists(jsonFile);
                LOG.info("Migrated exclusions.json → exclusions.yaml");
            }
        }
        catch (Exception e)
        {
            LOG.warn("Failed to load {}: {}", file.getFileName(), e.getMessage());
        }
    }

    /**
     * Helper: populate an exclusion map from a JSON array of either bare strings or {id, reason} objects.
     */
    private static void loadEntriesInto(com.fasterxml.jackson.databind.JsonNode arr, Map<String, String> out)
    {
        if (!arr.isArray())
            return;
        for (com.fasterxml.jackson.databind.JsonNode n : arr)
        {
            if (n.isTextual())
                out.put(n.asText(), "");
            else if (n.isObject())
            {
                String id = n.path("id").asText(null);
                if (id == null || id.isEmpty())
                    continue;
                String reason = n.path("reason").asText("");
                out.put(id, reason);
            }
        }
    }

    private void compileSeriesPatterns()
    {
        compiledSeriesPatterns = excludedSeries.keySet().stream()
            .map(p ->
            {
                try { return Pattern.compile(p, Pattern.CASE_INSENSITIVE); }
                catch (Exception e) { LOG.warn("Invalid series exclusion regex '{}': {}", p, e.getMessage()); return null; }
            })
            .filter(Objects::nonNull)
            .toList();
    }

    private void saveExclusions()
    {
        Path file = configDir.resolve("exclusions.yaml");
        ExclusionsFile ef = new ExclusionsFile();
        for (Map.Entry<String, String> e : excludedBoats.entrySet())
        {
            ef.boats.add(new ExclusionsFile.Entry(e.getKey(), e.getValue()));
        }
        for (Map.Entry<String, String> e : excludedRaces.entrySet())
        {
            ef.races.add(new ExclusionsFile.Entry(e.getKey(), e.getValue()));
        }
        for (Map.Entry<String, String> e : excludedSeries.entrySet())
        {
            ef.series.add(new ExclusionsFile.SeriesEntry(e.getKey(), e.getValue()));
        }
        try
        {
            Files.createDirectories(configDir);
            String yaml = YAML_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(ef);
            Files.writeString(file, EXCLUSIONS_HEADER + yaml);
        }
        catch (Exception e)
        {
            LOG.warn("Failed to save exclusions.yaml: {}", e.getMessage());
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
     * Reloads the design catalogue from disk.
     */
    public void reloadDesignCatalogue()
    {
        requireStarted();
        designCatalogue = Designs.load(configDir);
        // Re-stamp catalogue-derived flags on every in-memory Design (e.g. noSpinnaker).
        designs.replaceAll((id, d) -> d.noSpinnaker() == designCatalogue.isNoSpinnaker(id)
            ? d : d.withNoSpinnaker(designCatalogue.isNoSpinnaker(id)));
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
     * The type of a series: as set in clubs.yaml {@code seriesTypes}, otherwise derived from
     * the series name ({@link SeriesType#fromName}). {@link SeriesType#UNKNOWN} if the series
     * is not held.
     */
    public SeriesType seriesType(String seriesId)
    {
        requireStarted();
        return seriesType(seriesId, null);
    }

    /** As {@link #seriesType(String)}, looking in {@code clubIdHint}'s series first. */
    private SeriesType seriesType(String seriesId, String clubIdHint)
    {
        SeriesType explicit = clubCatalogue.seriesType(seriesId);
        if (explicit != null)
            return explicit;
        Series series = findSeries(seriesId, clubIdHint);
        return series == null ? SeriesType.UNKNOWN : SeriesType.fromName(series.name());
    }

    /** True if the series type comes from clubs.yaml rather than the series name. */
    public boolean isSeriesTypeSet(String seriesId)
    {
        requireStarted();
        return clubCatalogue.seriesType(seriesId) != null;
    }

    private Series findSeries(String seriesId, String clubIdHint)
    {
        if (seriesId == null)
            return null;
        Club hinted = clubIdHint == null ? null : clubs.get(clubIdHint);
        Series found = findSeriesIn(hinted, seriesId);
        if (found != null)
            return found;
        for (Club club : clubs.values())
        {
            if (club != hinted && (found = findSeriesIn(club, seriesId)) != null)
                return found;
        }
        return null;
    }

    private static Series findSeriesIn(Club club, String seriesId)
    {
        if (club == null || club.series() == null)
            return null;
        for (Series s : club.series())
        {
            if (seriesId.equals(s.id()))
                return s;
        }
        return null;
    }

    /**
     * A finisher's spinnaker flag as raced: forced by the race's series type
     * ({@link #raceSeriesType}) -- non-spinnaker in an NS race, spinnaker in a spin race
     * except for a boat whose design has no spinnaker -- otherwise its own flag as imported.
     * The stored flag is never changed, so clearing a series type restores it. (The PF
     * optimiser decides a finisher's full variant with
     * {@link org.mortbay.sailing.pf.data.EntryVariant#of}.)
     */
    public boolean isNonSpinnakerAsRaced(Race race, Finisher f)
    {
        SeriesType type = raceSeriesType(race);
        if (type == SeriesType.NON_SPIN)
            return true;
        if (type == SeriesType.SPIN)
            return f.nonSpinnaker() && isBoatNoSpinnakerDesign(f.boatId());
        return f.nonSpinnaker();
    }

    /**
     * Sets a series' type in clubs.yaml ({@code null} removes it, reverting to the name-derived
     * default). The races themselves are not changed: their finishers keep their spinnaker
     * flags as imported, and the series type overrides them when the analysis reads them
     * ({@link org.mortbay.sailing.pf.data.EntryVariant#of}), so clearing the type restores
     * them. Returns the IDs of the series' races, whose derived data is invalidated.
     */
    public List<String> setSeriesType(String seriesId, SeriesType type)
    {
        requireStarted();
        Series series = findSeries(seriesId, null);
        if (series == null)
            throw new IllegalArgumentException("Unknown series: " + seriesId);
        String clubId = clubs.values().stream()
            .filter(c -> c.series() != null && c.series().contains(series))
            .map(Club::id).findFirst().orElseThrow();
        Club seed = clubSeed.get(clubId);
        String shortNameIfNew = seed != null ? seed.shortName() : clubs.get(clubId).shortName();
        if (!ClubLoader.setSeriesType(configDir, clubId, shortNameIfNew, seriesId, type))
            return List.of();
        clubCatalogue = ClubLoader.loadCatalogue(configDir);
        List<String> affected = races.values().stream()
            .filter(r -> r.seriesIds() != null && r.seriesIds().contains(seriesId))
            .map(Race::id).toList();
        InvalidationListener l = invalidationListener;
        if (l != null)
            affected.forEach(l::onRaceChanged);
        return affected;
    }

    /**
     * The combined type of the series a race belongs to, for deciding its finishers' variants
     * ({@link org.mortbay.sailing.pf.data.EntryVariant#of}): {@link SeriesType#NON_SPIN} if
     * any series is NS, else {@link SeriesType#TWO_HANDED} if any is 2H, else
     * {@link SeriesType#SPIN} if any is spin; null when all are mixed or unknown.
     */
    public SeriesType raceSeriesType(Race race)
    {
        requireStarted();
        if (race.seriesIds() == null)
            return null;
        boolean twoHanded = false;
        boolean spin = false;
        for (String seriesId : race.seriesIds())
        {
            SeriesType type = seriesType(seriesId, race.clubId());
            if (type == SeriesType.NON_SPIN)
                return SeriesType.NON_SPIN;
            twoHanded |= type == SeriesType.TWO_HANDED;
            spin |= type == SeriesType.SPIN;
        }
        return twoHanded ? SeriesType.TWO_HANDED : spin ? SeriesType.SPIN : null;
    }

    /**
     * Returns a list of stale-boat violations detected after the startup repair passes have
     * run. Each violation is a human-readable string identifying a boat whose stored identity
     * disagrees with the current configuration (aliases.yaml, design.yaml boatDesignOverrides,
     * or clubs.yaml noclub). An empty list means the dataset is clean.
     *
     * <p>This is the canonical health-check surface for data cleanliness: if any of the four
     * known violation patterns (alias-stale name, alias-stale sail, design-override drift,
     * stale noclub assignment) remains after startup, this method reports them so they can
     * be surfaced via {@code GET /api/health} and as ERROR-level log entries.</p>
     *
     * <p>Computed by {@link #computeStaleBoatViolations()} at the end of {@link #start()}
     * and cached in {@link #staleBoatViolations}. Subsequent edits / merges do not refresh
     * this list -- it reflects the state right after startup repair finished.</p>
     */
    public List<String> findStaleBoatViolations()
    {
        requireStarted();
        return staleBoatViolations;
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
     * Merge as spelling: the boats {@code mergeIds} are misspellings of identity {@code keepId}.
     * Every identity of a merged boat becomes an alias of that identity (with its aliases),
     * its certificates, sources and clubs join it, its results are moved to it, and the merged
     * boats are removed. Links join the kept boat; a design-less kept boat takes the first
     * design found. Callers must call {@link #save()} after this method.
     *
     * @param keepId   any identity ID of the boat to keep: the identity the others are spellings of
     * @param mergeIds identity IDs of the boats to merge into it (not of the kept boat)
     * @return the number of races and finisher records updated
     */
    public MergeResult mergeBoats(String keepId, List<String> mergeIds)
    {
        requireStarted();
        Boat keepBoat = boat(keepId);
        if (keepBoat == null)
            throw new IllegalArgumentException("Keep boat not found: " + keepId);
        List<Boat> toMerge = mergeBoatsOf(keepBoat, mergeIds);

        Identity keep = keepBoat.identity();
        Map<String, Certificate> certMap = new LinkedHashMap<>();
        for (Certificate c : keepBoat.certificates())
            certMap.put(certKey(c), c);
        List<Certificate> addCerts = new ArrayList<>(keep.certificates());
        Set<String> mergedIdentityIds = new HashSet<>();
        String designId = keepBoat.designId();
        List<List<Link>> allLinks = new ArrayList<>();
        allLinks.add(keepBoat.links());
        for (Boat mb : toMerge)
        {
            for (Map.Entry<String, Identity> e : mb.identities().entrySet())
            {
                Identity m = e.getValue();
                mergedIdentityIds.add(e.getKey());
                // Entered as another design: the alias says so, so those results still come here
                String entered = mb.designId() != null && !Objects.equals(mb.designId(), keepBoat.designId())
                    ? mb.designId() : null;
                keep = keep.withAlias(new BoatAlias(m.sailNumber(), m.name(), entered));
                for (BoatAlias a : m.aliases())
                    keep = keep.withAlias(a.designId() == null && entered != null
                        ? new BoatAlias(a.sailNumber(), a.name(), entered) : a);
                for (Certificate c : m.certificates())
                    if (certMap.putIfAbsent(certKey(c), c) == null)
                        addCerts.add(c);
                for (String src : m.sources())
                    keep = keep.withSource(src);
                if (!keep.clubsCurated())
                    keep = keep.withClubIds(mergeClubIds(keep.clubIds(), m.clubIds()));
            }
            if (designId == null)
                designId = mb.designId();
            allLinks.add(mb.links());
        }
        keep = keep.withCertificates(addCerts);
        Boat merged = keepBoat.withIdentity(keep).withDesignId(designId).withLinks(Link.merge(allLinks))
            .withLastUpdated(Instant.now()).withLoadedAt(null);

        for (Boat mb : toMerge)
            removeBoat(mb.id());
        putBoat(merged);
        int[] counts = moveFinishers(mergedIdentityIds, keepId);

        LOG.info("mergeBoats: kept={} merged={} updatedRaces={} updatedFinishers={}",
            keepId, mergeIds, counts[0], counts[1]);
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return new MergeResult(counts[0], counts[1]);
    }

    /**
     * Link identities (a rename): the boats {@code linkIds} are the boat {@code keepId} under
     * other names. Their identities join it unchanged (results stay with the identity they were
     * entered as), ordered by when each last raced, so the most recently raced becomes current.
     * Their certificates go with their identities; links join. The boats must be of the same
     * design (a design-less boat takes the other's). Callers must call {@link #save()}.
     */
    public Boat linkBoats(String keepId, List<String> linkIds)
    {
        requireStarted();
        Boat keepBoat = boat(keepId);
        if (keepBoat == null)
            throw new IllegalArgumentException("Keep boat not found: " + keepId);
        List<Boat> toLink = mergeBoatsOf(keepBoat, linkIds);
        String designId = keepBoat.designId();
        for (Boat lb : toLink)
        {
            if (designId == null)
                designId = lb.designId();
            else if (lb.designId() != null && !designId.equals(lb.designId()))
                throw new IllegalArgumentException("Cannot link " + lb.id() + " (" + lb.designId() + ") to "
                    + keepBoat.id() + " (" + designId + "): a boat has one design; merge or redesign first");
        }
        // A design-less boat joining a designed one is re-keyed to the design first
        List<Boat> linked = new ArrayList<>();
        if (keepBoat.designId() == null && designId != null)
            keepBoat = redesignBoat(keepBoat, designId, "Link");
        for (Boat lb : toLink)
            linked.add(lb.designId() == null && designId != null ? redesignBoat(lb, designId, "Link") : lb);

        Map<String, Identity> all = new LinkedHashMap<>(keepBoat.identities());
        List<List<Link>> allLinks = new ArrayList<>();
        allLinks.add(keepBoat.links());
        for (Boat lb : linked)
        {
            all.putAll(lb.identities());
            allLinks.add(lb.links());
        }
        boolean modified = keepBoat.modified() || linked.stream().anyMatch(Boat::modified);
        Boat result = new Boat(null, designId, modified, Link.merge(allLinks), orderByLastRace(all), Instant.now(), null);
        for (Boat lb : linked)
            removeBoat(lb.id());
        removeBoat(keepBoat.id());
        putBoat(result);
        LOG.info("linkBoats: {} now has identities {}", result.id(), result.identities().keySet());
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return result;
    }

    /**
     * Unlink an identity: it leaves its boat and becomes a boat of its own (same design, its
     * own certificates, sources and clubs; no links), with its results. Callers must call
     * {@link #save()}.
     */
    public Boat unlinkIdentity(String identityId)
    {
        requireStarted();
        Boat b = boat(identityId);
        if (b == null)
            throw new IllegalArgumentException("Boat not found: " + identityId);
        if (b.identities().size() < 2)
            throw new IllegalArgumentException(identityId + " is the boat's only identity");
        Map<String, Identity> rest = new LinkedHashMap<>(b.identities());
        Identity leaving = rest.remove(identityId);
        Boat remaining = b.withIdentities(rest).withLastUpdated(Instant.now()).withLoadedAt(null);
        Boat alone = new Boat(identityId, b.designId(), b.modified(), List.of(), Map.of(identityId, leaving), Instant.now(), null);
        removeBoat(b.id());
        putBoat(remaining);
        putBoat(alone);
        recanonicalise(b.identities().keySet());
        LOG.info("unlinkIdentity: {} left boat {}", identityId, remaining.id());
        InvalidationListener l = invalidationListener;
        if (l != null) l.onAllChanged();
        return alone;
    }

    /**
     * Changes the sail number and/or name of one identity: its ID changes, the old sail number
     * and name become an alias of it (unless another boat still uses them), and its results
     * move to the new ID. Fails if another boat already has the new ID (merge first).
     *
     * @return the boat, seen through the renamed identity
     */
    public Boat renameIdentity(String identityId, String newSailNumber, String newName)
    {
        requireStarted();
        Boat b = boat(identityId);
        if (b == null)
            throw new IllegalArgumentException("Boat not found: " + identityId);
        Identity old = b.identity();
        String newId = IdGenerator.generateBoatId(newSailNumber, newName, designs.get(b.designId()));
        if (b.designId() != null && !designs.containsKey(b.designId()))
            newId = renameBoatId(newSailNumber, newName, b.designId());
        if (newId.equals(identityId))
        {
            Boat updated = b.withIdentity(old.withSailNumberName(newSailNumber, newName)).withLoadedAt(null);
            putBoat(updated);
            return updated;
        }
        String other = identityIndex.get(newId);
        if (other != null && !b.hasIdentity(newId))
            throw new IllegalArgumentException("A boat with ID " + newId + " already exists. Merge first, then edit.");
        String oldSail = old.sailNumber();
        String oldName = old.name();
        boolean oldInUse = boats.values().stream().anyMatch(o -> !o.hasIdentity(identityId)
            && o.identities().values().stream().anyMatch(i -> i.matches(oldSail, IdGenerator.normaliseName(oldName))));
        Identity renamed = old.withSailNumberName(newSailNumber, newName);
        if (!oldInUse)
            renamed = renamed.withAlias(new BoatAlias(oldSail, oldName));
        Map<String, Identity> m = new LinkedHashMap<>();
        for (Map.Entry<String, Identity> e : b.identities().entrySet())
            m.put(e.getKey().equals(identityId) ? newId : e.getKey(), e.getKey().equals(identityId) ? renamed : e.getValue());
        Boat updated = new Boat(newId, b.designId(), b.modified(), b.links(), m, Instant.now(), null);
        removeBoat(b.id());
        putBoat(updated);
        rewriteFinisherBoatId(identityId, newId);
        return updated;
    }

    /** The boats of {@code ids} (not {@code keep} itself), each once. */
    private List<Boat> mergeBoatsOf(Boat keep, List<String> ids)
    {
        Map<String, Boat> out = new LinkedHashMap<>();
        for (String id : ids)
        {
            Boat b = boat(id);
            if (b == null)
                throw new IllegalArgumentException("Boat not found: " + id);
            String key = canonicalBoatId(id);
            if (key.equals(canonicalBoatId(keep.id())))
                throw new IllegalArgumentException(id + " is already an identity of " + keep.id());
            out.putIfAbsent(key, boats.get(key));
        }
        return List.copyOf(out.values());
    }

    /** Moves the results entered as any of {@code fromIdentityIds} to identity {@code toId}. */
    private int[] moveFinishers(Set<String> fromIdentityIds, String toId)
    {
        int updatedRaces = 0;
        int updatedFinishers = 0;
        for (Race race : List.copyOf(races.values()))
        {
            if (race.divisions() == null)
                continue;
            boolean changed = false;
            List<Division> newDivisions = new ArrayList<>();
            for (Division div : race.divisions())
            {
                List<Finisher> newFinishers = new ArrayList<>();
                for (Finisher f : div.finishers())
                {
                    if (fromIdentityIds.contains(f.identityId()))
                    {
                        newFinishers.add(f.withBoatId(toId));
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
                    race.number(), race.name(), newDivisions, race.source(), race.lastUpdated(), null));
                updatedRaces++;
            }
        }
        return new int[]{updatedRaces, updatedFinishers};
    }

    /**
     * The identities ordered by the date each last raced, oldest first, so the most recently
     * raced is last (current). Identities with no races keep their order, before those raced.
     */
    private Map<String, Identity> orderByLastRace(Map<String, Identity> identities)
    {
        Map<String, LocalDate> last = new java.util.HashMap<>();
        for (Race race : races.values())
        {
            if (race.date() == null || race.divisions() == null)
                continue;
            for (Division d : race.divisions())
                for (Finisher f : d.finishers())
                    if (identities.containsKey(f.identityId()))
                        last.merge(f.identityId(), race.date(), (x, y) -> x.isAfter(y) ? x : y);
        }
        List<String> order = new ArrayList<>(identities.keySet());
        order.sort(Comparator.comparing((String id) -> last.get(id), Comparator.nullsFirst(Comparator.naturalOrder())));
        Map<String, Identity> out = new LinkedHashMap<>();
        order.forEach(id -> out.put(id, identities.get(id)));
        return out;
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
                    if (oldBoatId.equals(f.identityId()))
                    {
                        newFinishers.add(f.withBoatId(newBoatId));
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
     * One-off, idempotent migration of TopYacht series and race IDs to the event-scoped form.
     * <p>
     * Race IDs used to be {@code clubId-date-number}, which assumed (club, date, number) was
     * unique. TopYacht regattas break that: each division is published as its own series, all
     * with a "Race 1" on the same day, so every division after the first was silently dropped
     * as "already imported" and the survivors were claimed by every sibling series. IDs are
     * now scoped by the series slug, itself scoped by the club's TopYacht event prefix.
     * <p>
     * Each TopYacht race's event prefix is resolved, in order, from:
     * <ol>
     *   <li>its own {@code source} URL, matched by path against the club's configured index URLs;</li>
     *   <li>a sibling race in the same old series that did resolve (covers races whose source
     *       predates URL-bearing source strings);</li>
     *   <li>the club's only configured group, when it has exactly one.</li>
     * </ol>
     * Races that resolve to none of these are left untouched and logged.
     * <p>
     * Series records of affected clubs are rebuilt from the migrated races rather than renamed,
     * because one polluted old series can split into several correct ones.
     */
    private void migrateTopYachtIds()
    {
        // clubId → (path base → prefix), longest base first so nested groups match precisely
        Map<String, List<Map.Entry<String, String>>> basesByClub = new LinkedHashMap<>();
        Map<String, List<TopYachtGroup>> groupsByClub = new LinkedHashMap<>();
        for (Club club : clubSeed.values())
        {
            List<TopYachtGroup> groups = club.topyachtGroups();
            if (groups == null || groups.isEmpty())
                continue;
            groupsByClub.put(club.id(), groups);
            List<Map.Entry<String, String>> bases = new ArrayList<>();
            for (TopYachtGroup g : groups)
            {
                for (String url : g.urls())
                {
                    String base = urlPathBase(url);
                    if (!base.isEmpty())
                        bases.add(Map.entry(base, g.prefix()));
                }
            }
            bases.sort((a, b) -> Integer.compare(b.getKey().length(), a.getKey().length()));
            basesByClub.put(club.id(), bases);
        }

        // Pass 1: resolve a prefix per race, and remember what each old series resolved to
        Map<String, String> prefixByRaceId = new LinkedHashMap<>();
        Map<String, String> prefixByOldSeriesId = new LinkedHashMap<>();
        List<Race> candidates = new ArrayList<>();
        for (Race race : races.values())
        {
            if (race.source() == null || !race.source().startsWith(TOPYACHT_SOURCE))
                continue;
            candidates.add(race);
            String prefix = resolvePrefixFromSource(race, basesByClub.get(race.clubId()));
            if (prefix != null)
            {
                prefixByRaceId.put(race.id(), prefix);
                String oldSeries = firstSeriesId(race);
                if (oldSeries != null)
                    prefixByOldSeriesId.putIfAbsent(oldSeries, prefix);
            }
        }
        if (candidates.isEmpty())
            return;

        // Pass 2: fall back to a sibling in the same old series, then to a club's sole group
        List<Race> unresolved = new ArrayList<>();
        for (Race race : candidates)
        {
            if (prefixByRaceId.containsKey(race.id()))
                continue;
            String oldSeries = firstSeriesId(race);
            String prefix = oldSeries != null ? prefixByOldSeriesId.get(oldSeries) : null;
            if (prefix == null)
            {
                List<TopYachtGroup> groups = groupsByClub.get(race.clubId());
                if (groups != null && groups.size() == 1)
                    prefix = groups.getFirst().prefix();
            }
            if (prefix != null)
                prefixByRaceId.put(race.id(), prefix);
            else
                unresolved.add(race);
        }

        // Pass 3: apply. Collect renames first so the races map is not mutated while iterating.
        record Rename(Race race, String newId, String oldSeriesId, String newSeriesId) {}
        List<Rename> renames = new ArrayList<>();
        for (Race race : candidates)
        {
            String prefix = prefixByRaceId.get(race.id());
            if (prefix == null)
                continue;
            String oldSeriesId = firstSeriesId(race);
            if (oldSeriesId == null)
                continue;
            String oldSlug = IdGenerator.seriesSlug(oldSeriesId);
            // Already migrated (or a series genuinely named after the prefix) — leave alone,
            // so a second start() is a no-op rather than double-prefixing.
            if (oldSlug.isEmpty() || oldSlug.equals(prefix) || oldSlug.startsWith(prefix + "-"))
                continue;
            String newSeriesId = IdGenerator.sanitizeIdForFilesystem(race.clubId())
                + "/" + prefix + "-" + oldSlug;
            String newId = IdGenerator.generateRaceId(race.clubId(), newSeriesId,
                race.date(), race.number());
            if (newId.equals(race.id()))
                continue;
            if (races.containsKey(newId))
            {
                LOG.warn("TopYacht ID migration: target {} already exists, leaving {} unchanged",
                    newId, race.id());
                continue;
            }
            renames.add(new Rename(race, newId, oldSeriesId, newSeriesId));
        }

        if (renames.isEmpty())
        {
            if (!unresolved.isEmpty())
                LOG.warn("TopYacht ID migration: {} race(s) could not be attributed to an event "
                    + "prefix and were left unchanged", unresolved.size());
            return;
        }

        Set<String> touchedClubs = new java.util.LinkedHashSet<>();
        Map<String, String> newSeriesIdByRaceId = new LinkedHashMap<>();
        Set<String> retiredSeriesIds = new java.util.LinkedHashSet<>();
        Set<String> migratedOldRaceIds = new java.util.LinkedHashSet<>();
        for (Rename r : renames)
        {
            Path oldFile = raceFilePath(r.race());
            races.remove(r.race().id());
            migratedOldRaceIds.add(r.race().id());
            retiredSeriesIds.add(r.oldSeriesId());
            Race moved = new Race(r.newId(), r.race().clubId(), List.of(r.newSeriesId()),
                r.race().date(), r.race().number(), r.race().name(), r.race().divisions(),
                r.race().source(), Instant.now(), null);
            races.put(moved.id(), moved);
            newSeriesIdByRaceId.put(moved.id(), r.newSeriesId());
            touchedClubs.add(r.race().clubId());
            // Exclusions are keyed by race ID; carry any across so an admin decision survives.
            String reason = excludedRaces.remove(r.race().id());
            if (reason != null)
                excludedRaces.put(moved.id(), reason);
            try
            {
                Files.deleteIfExists(oldFile);
            }
            catch (IOException e)
            {
                LOG.warn("TopYacht ID migration: could not delete stale race file {}: {}",
                    oldFile, e.getMessage());
            }
        }

        rebuildClubSeriesAfterMigration(touchedClubs, retiredSeriesIds, migratedOldRaceIds,
            newSeriesIdByRaceId);

        save();
        saveExclusions();
        LOG.info("TopYacht ID migration: rewrote {} race ID(s) across {} club(s){}",
            renames.size(), touchedClubs.size(),
            unresolved.isEmpty() ? ""
                : " (" + unresolved.size() + " race(s) left unchanged — no event prefix could be resolved)");
        for (Race race : unresolved)
        {
            LOG.warn("TopYacht ID migration: no event prefix for race {} (source={})",
                race.id(), race.source());
        }
    }

    /**
     * One-off, idempotent migration that folds a merged event's per-division races into one
     * race per sailing slot, carrying one division per contributing series.
     * <p>
     * Runs only for {@link TopYachtGroup}s flagged {@code merge}. Races are keyed by
     * {@code (date, ordinal-within-day)} rather than by TopYacht's race number: a division
     * that sails twice on one day runs a number ahead of its siblings for the rest of the
     * regatta, so the numbers stop lining up while "first race of the day" still does.
     * <p>
     * A contributing series that is itself excluded is left alone — see
     * {@code TopYachtImporter.mergingSeries} for why folding an excluded series into a
     * non-excluded event race would silently re-admit boats an admin had excluded.
     */
    private void migrateTopYachtDivisionMerge()
    {
        for (Club seed : clubSeed.values())
        {
            if (seed.topyachtGroups() == null)
                continue;
            for (TopYachtGroup group : seed.topyachtGroups())
            {
                if (group.mergeDivisions())
                    mergeEventDivisions(seed.id(), group);
            }
        }
    }

    /**
     * Merges one event's per-division races at one club.
     */
    private void mergeEventDivisions(String clubId, TopYachtGroup group)
    {
        Club club = clubs.get(clubId);
        if (club == null)
            return;
        String targetSeriesId = IdGenerator.generateEventSeriesId(clubId, group.prefix());
        String perDivisionPrefix = targetSeriesId + "-";

        Map<String, String> nameBySeriesId = new LinkedHashMap<>();
        for (Series s : club.series())
        {
            nameBySeriesId.put(s.id(), s.name());
        }

        // Gather this event's still-unmerged races, grouped by their contributing series
        Map<String, List<Race>> byContributingSeries = new LinkedHashMap<>();
        for (Race race : races.values())
        {
            if (!clubId.equals(race.clubId()))
                continue;
            String seriesId = firstSeriesId(race);
            if (seriesId == null || !seriesId.startsWith(perDivisionPrefix))
                continue;
            // An edition series (mirw-2025) is a merge result, not a division to merge
            if (TopYachtEditions.isEditionSeriesId(seriesId, clubId, group.prefix()))
                continue;
            String seriesName = nameBySeriesId.getOrDefault(seriesId,
                IdGenerator.seriesSlug(seriesId));
            if (isSeriesExcluded(seriesName))
                continue;                       // held out of the merge, keeps its own series
            byContributingSeries.computeIfAbsent(seriesId, k -> new ArrayList<>()).add(race);
        }
        if (byContributingSeries.isEmpty())
            return;

        // (date, ordinal-within-day) -> the per-division races that share that slot
        Map<String, List<Race>> slots = new LinkedHashMap<>();
        Map<String, Integer> ordinalOf = new LinkedHashMap<>();
        Map<String, LocalDate> dateOf = new LinkedHashMap<>();
        for (List<Race> seriesRaces : byContributingSeries.values())
        {
            Map<LocalDate, Integer> seenPerDate = new LinkedHashMap<>();
            List<Race> ordered = new ArrayList<>(seriesRaces);
            ordered.sort(java.util.Comparator.comparingInt(Race::number));
            for (Race race : ordered)
            {
                int ordinal = seenPerDate.merge(race.date(), 1, Integer::sum);
                String slot = race.date() + "#" + ordinal;
                slots.computeIfAbsent(slot, k -> new ArrayList<>()).add(race);
                ordinalOf.put(slot, ordinal);
                dateOf.put(slot, race.date());
            }
        }

        int mergedCount = 0;
        int consumed = 0;
        Set<String> supersededRaceIds = new java.util.LinkedHashSet<>();
        Map<String, String> newSeriesIdByRaceId = new LinkedHashMap<>();
        // A merged event is normally an annual regatta whose index URLs span years; splitting
        // its races into editions keeps 2022, 2023 and 2025 racing in separate series instead
        // of one series spanning years.
        Map<LocalDate, String> editionByDate = editionSeriesIdsByDate(clubId, group.prefix(),
            dateOf.values());

        for (Map.Entry<String, List<Race>> slot : slots.entrySet())
        {
            LocalDate date = dateOf.get(slot.getKey());
            int ordinal = ordinalOf.get(slot.getKey());
            String editionSeriesId = editionByDate.get(date);
            String newId = IdGenerator.generateRaceId(clubId, targetSeriesId, date, ordinal);

            List<Division> divisions = new ArrayList<>();
            List<String> sourceUrls = new ArrayList<>();
            String raceName = null;
            boolean anyExcluded = false;
            for (Race race : slot.getValue())
            {
                String seriesName = nameBySeriesId.getOrDefault(firstSeriesId(race),
                    IdGenerator.seriesSlug(firstSeriesId(race)));
                String divisionBase = stripEventPrefix(seriesName, group.name());
                List<Division> old = race.divisions() == null ? List.of() : race.divisions();
                for (int i = 0; i < old.size(); i++)
                {
                    // Division identity is its name downstream, so names must stay distinct
                    // within the merged race; a multi-division source page is qualified.
                    String name = old.size() <= 1 ? divisionBase
                        : divisionBase + " - " + (old.get(i).name() != null && !old.get(i).name().isBlank()
                        ? old.get(i).name() : String.valueOf(i + 1));
                    divisions.add(new Division(name, old.get(i).finishers()));
                }
                String url = sourceUrl(race.source());
                if (url != null && !sourceUrls.contains(url))
                    sourceUrls.add(url);
                if (raceName == null)
                    raceName = race.name();
                if (excludedRaces.containsKey(race.id()))
                    anyExcluded = true;
                supersededRaceIds.add(race.id());
                consumed++;
            }

            Race existing = races.get(newId);
            if (existing != null && !supersededRaceIds.contains(newId))
            {
                LOG.warn("TopYacht division merge: target {} already exists, skipping slot", newId);
                continue;
            }

            Race merged = new Race(newId, clubId, List.of(editionSeriesId), date, ordinal,
                raceName, List.copyOf(divisions),
                sourceUrls.isEmpty() ? "TopYacht" : "TopYacht - " + String.join(", ", sourceUrls),
                Instant.now(), null);
            for (Race race : slot.getValue())
            {
                if (!race.id().equals(newId))
                    deleteRaceFile(race);
            }
            slot.getValue().forEach(r -> races.remove(r.id()));
            races.put(newId, merged);
            newSeriesIdByRaceId.put(newId, editionSeriesId);
            if (anyExcluded)
            {
                excludedRaces.put(newId, "carried over from a merged per-division race");
                LOG.warn("TopYacht division merge: merged race {} inherits an exclusion from a "
                    + "contributing race", newId);
            }
            mergedCount++;
        }

        if (mergedCount == 0)
            return;

        for (String oldId : supersededRaceIds)
        {
            excludedRaces.remove(oldId);
        }

        rebuildClubSeriesAfterMigration(Set.of(clubId), byContributingSeries.keySet(),
            supersededRaceIds, newSeriesIdByRaceId);

        // The merged series is named for the event, not for any one division
        Club updated = clubs.get(clubId);
        if (updated != null)
        {
            List<Series> renamed = new ArrayList<>();
            for (Series s : updated.series())
            {
                renamed.add(TopYachtEditions.isEditionSeriesId(s.id(), clubId, group.prefix())
                    ? new Series(s.id(), TopYachtEditions.editionName(group.displayName(), s.id()), s.isCatchAll(), s.raceIds())
                    : s);
            }
            clubs.put(clubId, new Club(updated.id(), updated.shortName(), updated.longName(),
                updated.state(), updated.excluded(), updated.email(), updated.aliases(),
                updated.topyachtGroups(), updated.sailsysEvents(), List.copyOf(renamed), null));
        }

        save();
        saveExclusions();
        LOG.info("TopYacht division merge: club {} event '{}' — {} per-division race(s) merged "
            + "into {} race(s)", clubId, group.displayName(), consumed, mergedCount);
    }

    /**
     * Maps each race date of a merged event onto its edition's series ID. Dates are clustered
     * by {@link TopYachtEditions}; an edition already recorded at the club claims a cluster
     * whose races fall within the gap of one of its own, so labels stay stable across runs.
     */
    private Map<LocalDate, String> editionSeriesIdsByDate(String clubId, String prefix,
                                                          java.util.Collection<LocalDate> dates)
    {
        Map<LocalDate, String> out = new LinkedHashMap<>();
        Club club = clubs.get(clubId);
        for (List<LocalDate> edition : TopYachtEditions.cluster(dates))
        {
            if (edition.isEmpty())
                continue;
            String seriesId = existingEditionSeriesId(club, clubId, prefix, edition);
            if (seriesId == null)
                seriesId = TopYachtEditions.editionSeriesId(clubId, prefix,
                    TopYachtEditions.label(edition.getFirst()));
            for (LocalDate d : edition)
            {
                out.put(d, seriesId);
            }
        }
        return out;
    }

    /**
     * An edition series already at this club whose races fall inside {@code edition}, or null.
     */
    private String existingEditionSeriesId(Club club, String clubId, String prefix,
                                           List<LocalDate> edition)
    {
        if (club == null || club.series() == null)
            return null;
        for (Series s : club.series())
        {
            if (!TopYachtEditions.isEditionSeriesId(s.id(), clubId, prefix))
                continue;
            for (String raceId : s.raceIds())
            {
                Race race = races.get(raceId);
                if (race == null)
                    continue;
                for (LocalDate d : edition)
                {
                    if (TopYachtEditions.sameEdition(race.date(), d))
                        return s.id();
                }
            }
        }
        return null;
    }

    /**
     * Drops a leading {@code "<event name> - "} from a series name, so a division inside a
     * merged race reads "SeaLink Spinnaker Division 1" rather than repeating the event name
     * its race already carries.
     */
    private static String stripEventPrefix(String seriesName, String eventName)
    {
        if (seriesName == null || eventName == null || eventName.isBlank())
            return seriesName;
        String lead = eventName + " - ";
        return seriesName.startsWith(lead) ? seriesName.substring(lead.length()) : seriesName;
    }

    /**
     * The URL part of a {@code "TopYacht - <url>"} source string, or null if there is none.
     */
    private static String sourceUrl(String source)
    {
        if (source == null)
            return null;
        int dash = source.indexOf(" - ");
        return dash < 0 ? null : source.substring(dash + 3);
    }

    private void deleteRaceFile(Race race)
    {
        try
        {
            Files.deleteIfExists(raceFilePath(race));
        }
        catch (IOException e)
        {
            LOG.warn("TopYacht division merge: could not delete stale race file for {}: {}",
                race.id(), e.getMessage());
        }
    }

    /**
     * Rebuilds the series list of every club touched by the ID migration. Series that the
     * migration retired — either because a migrated race named them, or because they listed a
     * migrated race — are dropped and replaced by series built from where the races actually
     * landed; their display names are carried over. Series untouched by the migration (e.g.
     * every SailSys series at the same club) are left exactly as they were.
     */
    private void rebuildClubSeriesAfterMigration(Set<String> touchedClubs,
                                                 Set<String> retiredSeriesIds,
                                                 Set<String> migratedOldRaceIds,
                                                 Map<String, String> newSeriesIdByRaceId)
    {
        for (String clubId : touchedClubs)
        {
            Club club = clubs.get(clubId);
            if (club == null)
                continue;
            Map<String, String> nameByOldSeriesId = new LinkedHashMap<>();
            List<Series> kept = new ArrayList<>();
            for (Series s : club.series())
            {
                nameByOldSeriesId.put(s.id(), s.name());
                boolean retired = retiredSeriesIds.contains(s.id())
                    || s.raceIds().stream().anyMatch(migratedOldRaceIds::contains);
                if (!retired)
                    kept.add(s);
            }

            // Group the club's migrated races by their new series ID, preserving race order
            Map<String, List<String>> raceIdsByNewSeries = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : newSeriesIdByRaceId.entrySet())
            {
                Race race = races.get(e.getKey());
                if (race == null || !clubId.equals(race.clubId()))
                    continue;
                raceIdsByNewSeries.computeIfAbsent(e.getValue(), k -> new ArrayList<>())
                    .add(e.getKey());
            }

            List<Series> rebuilt = new ArrayList<>(kept);
            for (Map.Entry<String, List<String>> e : raceIdsByNewSeries.entrySet())
            {
                String newSeriesId = e.getKey();
                // The old series ID is the new one minus the event prefix; recover the display
                // name from whichever old series carried it.
                String name = null;
                for (Map.Entry<String, String> old : nameByOldSeriesId.entrySet())
                {
                    String oldSlug = IdGenerator.seriesSlug(old.getKey());
                    if (!oldSlug.isEmpty() && IdGenerator.seriesSlug(newSeriesId).endsWith("-" + oldSlug))
                    {
                        name = old.getValue();
                        break;
                    }
                }
                if (name == null)
                    name = IdGenerator.seriesSlug(newSeriesId);
                rebuilt.add(new Series(newSeriesId, name, false, List.copyOf(e.getValue())));
            }
            clubs.put(clubId, new Club(club.id(), club.shortName(), club.longName(), club.state(),
                club.excluded(), club.email(), club.aliases(), club.topyachtGroups(), club.sailsysEvents(),
                List.copyOf(rebuilt), null));
        }
    }

    /**
     * Source-string marker for races imported from TopYacht (bare, or "TopYacht - <url>").
     */
    private static final String TOPYACHT_SOURCE = "TopYacht";

    /**
     * The race's first series ID, or null when it has none.
     */
    private static String firstSeriesId(Race race)
    {
        return (race.seriesIds() == null || race.seriesIds().isEmpty())
            ? null : race.seriesIds().getFirst();
    }

    /**
     * Matches a race's {@code source} URL against a club's configured TopYacht index URLs and
     * returns the matching group's prefix, or null. Comparison is on the URL path only: the
     * same results tree is served from several hosts (www./tes./club domains), and a race's
     * stored source may name a different one than the current config.
     */
    private static String resolvePrefixFromSource(Race race, List<Map.Entry<String, String>> bases)
    {
        if (bases == null || bases.isEmpty())
            return null;
        String source = race.source();
        int dash = source.indexOf(" - ");
        if (dash < 0)
            return null;                        // bare "TopYacht" — no URL to match
        for (String url : source.substring(dash + 3).split(",\\s*"))
        {
            String path = urlPath(url);
            if (path.isEmpty())
                continue;
            for (Map.Entry<String, String> base : bases)
            {
                if (path.startsWith(base.getKey()))
                    return base.getValue();
            }
        }
        return null;
    }

    /**
     * Path portion of a URL (no scheme, no host, no query), always starting with '/'.
     */
    private static String urlPath(String url)
    {
        if (url == null)
            return "";
        String p = url.trim().replaceFirst("^[a-zA-Z][a-zA-Z0-9+.-]*://", "");
        int query = p.indexOf('?');
        if (query >= 0)
            p = p.substring(0, query);
        int slash = p.indexOf('/');
        return slash < 0 ? "" : p.substring(slash);
    }

    /**
     * Directory portion of a URL's path, e.g. ".../mirw/index.htm" → "/results/2025/mirw/".
     */
    private static String urlPathBase(String url)
    {
        String path = urlPath(url);
        if (path.isEmpty())
            return "";
        int lastSlash = path.lastIndexOf('/');
        return lastSlash < 0 ? "" : path.substring(0, lastSlash + 1);
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
        checkFormat();

        boats = new LinkedHashMap<>();
        identityIndex = new java.util.HashMap<>();
        loadDir(boatsDir, Boat.class).forEach(b ->
        {
            boats.put(b.id(), b);
            indexIdentities(b);
            if (b.sources().isEmpty())
                LOG.warn("Boat {} has no sources -- likely a stale entry, consider deleting {}", b.id(), b.id() + ".json");
        });
        designs = new LinkedHashMap<>();
        loadDir(designsDir, Design.class).forEach(d -> designs.put(d.id(), d));
        clubSeed = ClubLoader.load(configDir);
        clubCatalogue = ClubLoader.loadCatalogue(configDir);
        aliases = Aliases.load(configDir);
        designCatalogue = Designs.load(configDir);
        // Stamp the catalogue-derived noSpinnaker flag onto every Design now that both the
        // designs map and the catalogue are loaded. Subsequent toggles re-stamp via reloadDesignCatalogue().
        designs.replaceAll((id, d) -> d.noSpinnaker() == designCatalogue.isNoSpinnaker(id)
            ? d : d.withNoSpinnaker(designCatalogue.isNoSpinnaker(id)));

        loadExclusions();
        clubs = new LinkedHashMap<>();
        loadDir(clubsDir, Club.class).forEach(c -> clubs.put(c.id(), c));
        // YAML is the source of truth for longName/state/excluded/email/aliases/topyacht;
        // populate those fields on the in-memory Club records from clubSeed.
        clubs.replaceAll((id, c) -> enrichWithSeed(c));
        races = new LinkedHashMap<>();
        loadDirRecursive(racesDir, Race.class).forEach(r -> races.put(r.id(), canonicalise(r)));

        migrateTopYachtIds();
        migrateTopYachtDivisionMerge();

        // Race-derived ORC removal: ORC handicaps copied from race results by earlier importer
        // versions are on many different ORC ratings, not 600/GPH; only official ORC certs are kept.
        {
            List<Boat> withRaceOrc = boats.values().stream()
                .filter(b -> b.certificates().stream().anyMatch(Certificate::isRaceDerivedOrc))
                .toList();
            if (!withRaceOrc.isEmpty())
            {
                LOG.info("Removing race-derived ORC certificates from {} boat(s)", withRaceOrc.size());
                for (Boat b : withRaceOrc)
                    putBoat(b.withCertificatesFiltered(c -> !c.isRaceDerivedOrc())
                        .withLastUpdated(Instant.now()).withLoadedAt(null));
            }
        }

        // Design alias correction: boats whose stored designId is an alias name (not the
        // canonical ID) are updated. This fixes boats created before a design alias was added.
        {
            List<Map.Entry<String, String>> designAliasUpdates = new ArrayList<>();
            for (Boat b : boats.values())
            {
                if (b.designId() == null)
                    continue;
                String canonical = aliases.resolveDesignAlias(b.designId());
                if (!canonical.equals(b.designId()) && designs.containsKey(canonical))
                    designAliasUpdates.add(Map.entry(b.id(), canonical));
            }
            if (!designAliasUpdates.isEmpty())
            {
                LOG.info("Correcting {} boat(s) with aliased designId at startup", designAliasUpdates.size());
                for (Map.Entry<String, String> e : designAliasUpdates)
                {
                    Boat b = boats.get(e.getKey());
                    if (b != null)
                        redesignBoat(b, e.getValue(), "Design alias correction");
                }
            }
        }

        // Modified-design fold: a boat of a "… Modified" design becomes a boat of the base design
        // ticked modified (modifiedBoats in design.yaml); design overrides targeting the modified
        // design move to the base; the then-empty modified designs are removed. Runs after the
        // override correction so a boat overridden to a modified design is folded too.
        {
            List<Boat> ofModified = boats.values().stream()
                .filter(b -> isModifiedDesignId(b.designId()))
                .toList();
            if (!ofModified.isEmpty())
                LOG.info("Folding {} boat(s) of modified designs into their base designs", ofModified.size());
            for (Boat b : ofModified)
                foldModifiedDesign(b);
            Set<String> inUse = new HashSet<>();
            for (Boat b : boats.values())
                inUse.add(b.designId());
            List<String> emptyModified = designs.keySet().stream()
                .filter(id -> isModifiedDesignId(id) && !inUse.contains(id))
                .toList();
            if (!emptyModified.isEmpty())
                LOG.info("Removing {} modified design(s) with no boats: {}", emptyModified.size(), emptyModified);
            emptyModified.forEach(this::removeDesign);
        }

        // Cascade ignore for every design already in the ignored list. setDesignIgnored
        // fires cascadeIgnoreDesign only when a user toggles a design ignored at runtime;
        // designs born on the ignored list in design.yaml (e.g. TopYacht's "D1"/"D2"
        // division placeholders) leave orphan boats behind on disk: their designId still
        // holds the now-ignored id even though their boatId already lacks the design
        // suffix. Re-running the cascade on every start-up clears those designId fields
        // (and annotates sources with "Ignored:<id>"). This MUST run before the design
        // upgrade pass below, which keys off designId==null to find merge candidates.
        {
            for (String ignoredId : designCatalogue.ignoredDesignIds())
            {
                boolean hasAffected = boats.values().stream()
                    .anyMatch(b -> ignoredId.equals(b.designId()));
                if (hasAffected)
                {
                    LOG.info("Startup cascade: design {} is ignored, sweeping boats with that designId", ignoredId);
                    cascadeIgnoreDesign(ignoredId);
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

        // Post-repair sanity scan: re-check every boat after all repair passes have run
        // and remember any residual violations. These get logged as ERROR and surfaced via
        // findStaleBoatViolations() (and ultimately /api/health). An empty list means the
        // dataset is clean. The known violation patterns are:
        //   (2) a boat whose designId disagrees with an undated boatDesignOverrides entry;
        //   (3) a boat with non-empty clubIds that is listed in noclub;
        //   (4) a boat whose stored designId is currently in the design-alias map
        //       (should have been canonicalised by the design alias correction pass).
        staleBoatViolations = computeStaleBoatViolations();
        for (String violation : staleBoatViolations)
        {
            LOG.error("Stale-boat violation after startup repair: {}", violation);
        }

        makers = new ArrayList<>(loadList(catalogueDir.resolve("makers.json"), Maker.class));
        makersDirty = false;
    }

    /**
     * Variant of {@link IdGenerator#generateBoatId} that uses a designId string directly
     * instead of looking up the Design object. Used by the startup rename paths so that a
     * stored designId is preserved on the renamed boatId even when the Design object isn't
     * present in the in-memory designs map (e.g. design file missing on disk, design newly
     * marked excluded, or test fixtures that don't load design files).
     */
    private static String renameBoatId(String rawSail, String rawName, String designId)
    {
        String normSail = IdGenerator.normaliseSailNumber(rawSail);
        if (normSail.isEmpty())
            normSail = "nosail";
        String base = normSail + "-" + IdGenerator.normaliseName(rawName);
        return designId == null || designId.isBlank() ? base : base + "-" + designId;
    }

    /**
     * Scans every boat for the four known data-cleanliness violations and returns one
     * human-readable string per violation. Idempotent and side-effect-free. Called once at
     * the end of {@link #start()} and cached in {@link #staleBoatViolations}; the cached
     * value is what {@link #findStaleBoatViolations()} returns.
     */
    private List<String> computeStaleBoatViolations()
    {
        List<String> violations = new ArrayList<>();
        for (Boat b : boats.values())
        {
            String normName = IdGenerator.normaliseName(b.name());
            // (4) design alias drift: stored designId is itself an alias.
            if (b.designId() != null)
            {
                String canonical = aliases.resolveDesignAlias(b.designId());
                if (!canonical.equals(b.designId()) && designs.containsKey(canonical))
                    violations.add(String.format(
                        "design-alias drift on %s: stored designId=%s but canonical is %s",
                        b.id(), b.designId(), canonical));
            }
        }
        return List.copyOf(violations);
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
        excludedBoats.clear();
        excludedRaces.clear();
        excludedSeries.clear();
        compiledSeriesPatterns = List.of();
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
            throw new IllegalStateException("DataStore not started -- call start() first");
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
