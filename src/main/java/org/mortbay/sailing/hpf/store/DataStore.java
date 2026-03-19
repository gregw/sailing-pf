package org.mortbay.sailing.hpf.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.mortbay.sailing.hpf.data.Boat;
import org.mortbay.sailing.hpf.data.Club;
import org.mortbay.sailing.hpf.data.Design;
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
    private static final double FUZZY_THRESHOLD = 0.90;
    private static final JsonMapper MAPPER = JsonMapper.builder().addModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS).build();

    private final Path root;
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
    private List<Maker> makers;
    private boolean makersDirty;

    public DataStore(Path root)
    {
        this.root = root;
        this.racesDir = root.resolve("races");
        this.boatsDir = root.resolve("boats");
        this.designsDir = root.resolve("designs");
        this.clubsDir = root.resolve("clubs");
        this.catalogueDir = root.resolve("catalogue");
    }

    private static boolean boatNameMatches(Boat candidate, String incomingName, String normIncoming)
    {
        if (IdGenerator.normaliseName(candidate.name()).equals(normIncoming))
            return true;
        if (candidate.aliases().stream().anyMatch(a -> a.equalsIgnoreCase(incomingName)))
            return true;
        if (candidate.aliases().stream().anyMatch(a -> IdGenerator.normaliseName(a).equals(normIncoming)))
            return true;

        if (JARO_WINKLER.apply(IdGenerator.normaliseName(candidate.name()), normIncoming) >= FUZZY_THRESHOLD)
            return true;
        return candidate.aliases().stream().anyMatch(a -> JARO_WINKLER.apply(IdGenerator.normaliseName(a), normIncoming) >= FUZZY_THRESHOLD);
    }

    private static boolean designNameMatches(Design candidate, String normIncoming)
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
            if (extractDigits(candidateNorm).equals(incomingDigits) && JARO_WINKLER.apply(candidateNorm, normIncoming) >= FUZZY_THRESHOLD)
                return true;
            return candidate.aliases().stream().anyMatch(a ->
            {
                String aNorm = IdGenerator.normaliseDesignName(a);
                return extractDigits(aNorm).equals(incomingDigits) && JARO_WINKLER.apply(aNorm, normIncoming) >= FUZZY_THRESHOLD;
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

    public Map<String, Design> designs()
    {
        requireStarted();
        return Collections.unmodifiableMap(designs);
    }

    public Boat findOrCreateBoat(String sailNo, String name, Design design)
    {
        String boatId = IdGenerator.generateBoatId(sailNo, name, design);

        Boat boat = boats.get(boatId);
        if (boat != null)
            return boat;

        String normSail = IdGenerator.normaliseSailNumber(sailNo);
        String normName = IdGenerator.normaliseName(name);

        for (Boat candidate : boats.values())
        {
            if (!candidate.sailNumber().equals(normSail))
                continue;
            if (design != null && candidate.designId() != null && !candidate.designId().equals(design.id()))
                continue;

            if (boatNameMatches(candidate, name, normName))
            {
                if (candidate.designId() == null && design != null)
                {
                    removeBoat(candidate.id());
                    Boat upgraded = new Boat(boatId, normSail, name, design.id(), candidate.clubId(), candidate.aliases(), candidate.certificates(), null);
                    putBoat(upgraded);
                    LOG.info("Upgraded boat {} → {}", candidate.id(), boatId);
                    return upgraded;
                }
                return candidate;
            }
        }

        // Check the alias seed: if the incoming name matches a known alias for this sail number,
        // redirect to the canonical name — both for finding an existing boat and for creation.
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
                        Boat upgraded = new Boat(canonicalBoatId, normSail, seedCanonicalName, design.id(), candidate.clubId(), candidate.aliases(), candidate.certificates(), null);
                        putBoat(upgraded);
                        LOG.info("Upgraded boat (via alias seed) {} → {}", candidate.id(), canonicalBoatId);
                        return upgraded;
                    }
                    return candidate;
                }
            }
            // No existing boat found — create with the canonical name, recording the incoming name as an alias
            String canonicalBoatId = IdGenerator.generateBoatId(sailNo, seedCanonicalName, design);
            List<String> aliases = normName.equals(normCanonical) ? List.of() : List.of(name);
            Boat newBoat = new Boat(canonicalBoatId, normSail, seedCanonicalName, design == null ? null : design.id(), null, aliases, List.of(), null);
            putBoat(newBoat);
            LOG.info("Created new boat (via alias seed) {}", newBoat);
            return newBoat;
        }

        Boat newBoat = new Boat(boatId, normSail, name, design == null ? null : design.id(), null, List.of(), List.of(), null);
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
            design = new Design(canonicalId, seedName != null ? seedName : className.trim(), List.of(), List.of(), null);
            putDesign(design);
            return design;
        }

        design = new Design(designId, className.trim(), List.of(), List.of(), null);
        putDesign(design);
        return design;
    }

    /**
     * Finds a club by short name alone, ignoring state.
     * If {@code longName} is provided and the short name is ambiguous, narrows to clubs
     * whose long name matches (case-insensitive) as a tiebreaker.
     * Returns the club if there is exactly one match; null with a log if none or still ambiguous.
     */
    public Club findUniqueClubByShortName(String shortName, String longName)
    {
        requireStarted();
        List<Club> matches = Stream.concat(
                clubs.values().stream(),
                clubSeed.values().stream().filter(c -> !clubs.containsKey(c.id())))
            .filter(c -> shortName.equalsIgnoreCase(c.shortName()))
            .toList();
        if (matches.isEmpty())
        {
            LOG.info("No club found for shortName={}", shortName);
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
            LOG.warn("Ambiguous shortName={} — {} matches ({}); clubId not set",
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

    /**
     * Write all dirty entities to disk (dirty-check via loadedAt). Keeps maps loaded.
     */
    public void save()
    {
        requireStarted();
        boats.values().forEach(b -> write(boatsDir.resolve(b.id() + ".json"), b));
        designs.values().forEach(d -> write(designsDir.resolve(d.id() + ".json"), d));
        clubs.values().forEach(c -> write(clubsDir.resolve(c.id() + ".json"), c));
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
        clubSeed = ClubSeedLoader.load();
        aliasSeed = AliasSeedLoader.load();
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
        makers = null;
        makersDirty = false;
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
        String clubSlug = race.clubId() != null ? race.clubId() : "unknown";
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
