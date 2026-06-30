package org.mortbay.sailing.pf.store;

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
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Loadable;
import org.mortbay.sailing.pf.data.Maker;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.importer.IdGenerator;
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
    private Map<String, Boat> boats;
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

    public boolean isExplicitlyNoClub(String boatId)
    {
        List<String> override = clubCatalogue.resolveBoatIdOverride(boatId);
        return override != null && override.isEmpty();
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
        String sailNo = IdGenerator.normaliseSailNumber(rawSailNo);
        String name = IdGenerator.normaliseName(rawName);

        Aliases.BoatMatch aliased = aliases.lookupBoat(sailNo, name).orElse(null);
        if (aliased != null)
        {
            if (aliased.normSailNumber() != null)
                sailNo = aliased.normSailNumber();
            if (aliased.normName() != null)
                name = aliased.normName();
        }

        boolean haveSail = !sailNo.isEmpty();
        boolean haveName = !name.isEmpty();
        if (!haveSail && !haveName)
            return Optional.empty();

        List<Boat> matches = new ArrayList<>();
        for (Boat candidate : boats.values())
        {
            if (haveSail && !sailNo.equalsIgnoreCase(candidate.sailNumber()))
                continue;
            if (haveName && !name.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                continue;
            matches.add(candidate);
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
                    if (!sailNo.equalsIgnoreCase(candidate.sailNumber()))
                        continue;
                    if (haveName && name.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                        continue;  // already in the exact pass
                    if (!matchKey.equals(IdGenerator.nameMatchKey(candidate.name())))
                        continue;
                    matches.add(candidate);
                }
            }
        }

        if (matches.size() == 1)
            return Optional.of(matches.getFirst());
        if (matches.size() > 1)
            LOG.debug("findBoat ambiguous: sailNo='{}' name='{}' → {} candidates: {}",
                sailNo, name, matches.size(),
                matches.stream().map(Boat::id).toList());
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
     * @return the found or created Boat
     */
    public Boat findOrCreateBoat(String rawSailNo, String rawName, String rawDesign, LocalDate date, String source)
    {
        // Build enriched source entry for traceability
        String sourceDesign = buildSourceEntry(source, rawDesign);

        // Strip decorative suffixes ("- GM", "- U18", etc.) up front so the persisted
        // display name and any match-key comparisons use the cleaned form. normaliseName
        // strips the same suffixes internally for IDs; doing it here as well keeps the
        // visible Boat.name field consistent with the normalised ID.
        rawName = IdGenerator.stripStandardSuffixes(rawName);

        String sailNo = IdGenerator.normaliseSailNumber(rawSailNo);
        String name = IdGenerator.normaliseName(rawName);

        Aliases.BoatMatch aliased = aliases.lookupBoat(sailNo, name).orElse(null);
        if (aliased != null)
        {
            sourceDesign += ":" + sailNo + " " + name + "=>" + aliased;
            String canonSail = aliased.normSailNumber() != null ? aliased.normSailNumber() : sailNo;
            String canonName = aliased.normName() != null ? aliased.normName() : name;
            // Active stale-record scan: if the alias maps (sailNo, name) to a different
            // canonical, look for any orphan boat already in the store under the pre-alias
            // identity and merge / rename it into the canonical now. Without this, an
            // aliases.yaml edit only heals stale records at the next restart, which leaves
            // a window where an importer can run beforehand and observe stale duplicates.
            if (!canonSail.equalsIgnoreCase(sailNo) || !canonName.equalsIgnoreCase(name))
            {
                List<Boat> orphans = new ArrayList<>();
                for (Boat b : boats.values())
                {
                    if (sailNo.equalsIgnoreCase(b.sailNumber())
                        && name.equalsIgnoreCase(IdGenerator.normaliseName(b.name())))
                        orphans.add(b);
                }
                for (Boat orphan : orphans)
                {
                    Design d = orphan.designId() != null ? designs.get(orphan.designId()) : null;
                    String canonDisplay = aliased.canonicalDisplayName() != null
                        ? aliased.canonicalDisplayName() : orphan.name();
                    String targetId = IdGenerator.generateBoatId(canonSail, canonDisplay, d);
                    if (targetId.equals(orphan.id()))
                        continue;
                    if (boats.containsKey(targetId))
                    {
                        LOG.info("Active stale-boat scan: merging orphan {} into canonical {} during findOrCreateBoat",
                            orphan.id(), targetId);
                        mergeBoats(targetId, List.of(orphan.id()));
                    }
                    else
                    {
                        LOG.info("Active stale-boat scan: renaming orphan {} → {} during findOrCreateBoat",
                            orphan.id(), targetId);
                        Boat renamed = new Boat(targetId, canonSail, canonDisplay,
                            orphan.designId(), orphan.clubIds(), orphan.certificates(),
                            orphan.sources(), Instant.now(), null);
                        removeBoat(orphan.id());
                        putBoat(renamed);
                        rewriteFinisherBoatId(orphan.id(), targetId);
                        ClubLoader.remapBoatId(configDir, orphan.id(), targetId);
                    }
                }
            }
            sailNo = canonSail;
            name = canonName;
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
        // If the resolved design is ignored -- via the curated {@code ignored:} list in
        // design.yaml or via the runtime user-toggled set -- treat the incoming boat as
        // design-less for matching purposes, otherwise a new designless record would get
        // created alongside an existing properly-designed one.
        if (isDesignIgnored(designId))
        {
            designId = "";
            rawDesign = null;
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
            // Candidate's stored design is now marked ignored -- the boat is effectively
            // a stale orphan that should not influence matching. Treat it as invisible
            // so an unambiguous design-bearing match can win.
            if (isDesignIgnored(candidate.designId()))
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
                    candidate.clubIds(),
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

        // Phase 1.5: name-equivalence fallback. If no exact-name match was found above,
        // try to find a boat sharing the same sail + design whose name collapses to the
        // same equivalence class under nameMatchKey (e.g. "Sticky II" ≡ "Sticky 2",
        // "The Goat" ≡ "Goat"). The match key requires whitespace before any trailing
        // numeral and an article + space prefix, so embedded letter runs ("Tivoli",
        // "Thelma") are safe. When a unique candidate is found, possibly rename it to
        // the longer / Arabic-preferred canonical and persist the equivalence as an
        // alias so subsequent imports stay sticky.
        if (matches.isEmpty())
        {
            String matchKey = IdGenerator.nameMatchKey(rawName);
            if (!matchKey.isEmpty())
            {
                List<Boat> keyHits = new ArrayList<>();
                for (Boat candidate : boats.values())
                {
                    if (!normSailNo.equalsIgnoreCase(candidate.sailNumber()))
                        continue;
                    if (isDesignIgnored(candidate.designId()))
                        continue;
                    if (normName.equalsIgnoreCase(IdGenerator.normaliseName(candidate.name())))
                        continue;  // exact-name candidates were already considered above
                    if (!matchKey.equals(IdGenerator.nameMatchKey(candidate.name())))
                        continue;
                    // Same design-compatibility rule as Phase 1: same design, or at least
                    // one side has no design. Conflicting designs → different boats.
                    if (isNotBlank(designId) && isNotBlank(candidate.designId())
                        && !Objects.equals(designId, candidate.designId()))
                        continue;
                    keyHits.add(candidate);
                }
                if (keyHits.size() == 1)
                    return applyMatchKeyResult(keyHits.getFirst(), rawName, designId, rawDesign, sourceDesign);
                if (keyHits.size() > 1)
                {
                    LOG.warn("Ambiguous name-equivalence match: sailNo={} matchKey={} → {} candidates: {}",
                        normSailNo, matchKey, keyHits.size(),
                        keyHits.stream().map(Boat::id).toList());
                    logAmbiguousMatch(source, normSailNo, normName, designId, keyHits);
                    return null;
                }
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

            // BoatId-based override takes priority over legacy sail+name override
            List<String> boatIdOverride = clubCatalogue.resolveBoatIdOverride(newBoatId);
            List<String> newClubIds;
            if (boatIdOverride != null)
            {
                newClubIds = boatIdOverride;
                LOG.info("Boat {}: boatId club override → {}", newBoatId,
                    boatIdOverride.isEmpty() ? "no-club" : boatIdOverride);
            }
            else
            {
                String sailNameOverride = clubCatalogue.resolveClubOverride(normSailNo, rawName);
                if (sailNameOverride != null)
                {
                    newClubIds = List.of(sailNameOverride);
                    LOG.info("Boat {}/{}: sail+name club override → {}", normSailNo, rawName, sailNameOverride);
                }
                else
                {
                    newClubIds = List.of();
                }
            }
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

        // If we only have 1 match, then let's assume we are of the same design
        if (matches.size() == 1)
            return matches.getFirst();

        // We have multiple boats with the same sailNo, name but different designs, so we don't know which one this is?
        LOG.warn("Ambiguous boat match: sailNo={} name={} design={} -- {} candidates with different designs",
            normSailNo, normName, designId, matches.size());
        logAmbiguousMatch(source, normSailNo, normName, designId, matches);
        return null;
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

    /**
     * Apply a Phase-1.5 name-equivalence hit: pick the canonical display name from the
     * incoming raw name and the candidate's stored name (see {@link IdGenerator#preferredDisplayName}),
     * possibly upgrade the candidate's design when the incoming side carries one and the
     * candidate didn't, and rename the candidate in place if the canonical name (or the
     * upgraded design) changes the boat id. The supplanted names are persisted to
     * {@code aliases.yaml} so subsequent imports of the old variants still resolve here.
     * <p>
     * Returns the boat (possibly renamed/upgraded) that the caller should treat as the
     * found match.
     */
    private Boat applyMatchKeyResult(Boat candidate, String incomingRaw,
                                     String designId, String rawDesign, String sourceDesign)
    {
        // Incoming name first so it wins on body-length ties (fresher data preferred when
        // length is equal -- e.g. case-only variants).
        String canonicalName = IdGenerator.preferredDisplayName(
            java.util.Arrays.asList(incomingRaw, candidate.name()));

        // Decide the final design: keep candidate's if present; otherwise adopt incoming.
        String finalDesignId = candidate.designId();
        Design finalDesign = null;
        boolean designUpgraded = false;
        if (isBlank(finalDesignId) && isNotBlank(designId))
        {
            finalDesign = isNotBlank(rawDesign) ? findOrCreateDesign(rawDesign)
                : findOrCreateDesign(designId);
            finalDesignId = finalDesign != null ? finalDesign.id() : designId;
            designUpgraded = true;
        }
        else if (isNotBlank(finalDesignId))
        {
            finalDesign = designs.get(finalDesignId);
        }

        String newBoatId = IdGenerator.generateBoatId(candidate.sailNumber(), canonicalName, finalDesign);
        String canonNormName = IdGenerator.normaliseName(canonicalName);
        String candidateNormName = IdGenerator.normaliseName(candidate.name());
        String incomingNormName = IdGenerator.normaliseName(incomingRaw);

        List<Aliases.SailNumberName> aliasesToAdd = new ArrayList<>();
        if (!candidateNormName.equals(canonNormName))
            aliasesToAdd.add(new Aliases.SailNumberName(candidate.sailNumber(), candidateNormName));
        if (!incomingNormName.equals(canonNormName) && !incomingNormName.equals(candidateNormName))
            aliasesToAdd.add(new Aliases.SailNumberName(candidate.sailNumber(), incomingNormName));

        List<String> mergedSources = addSource(candidate.sources(), sourceDesign);
        boolean displayChanged = !canonicalName.equals(candidate.name());
        boolean idChanged = !newBoatId.equals(candidate.id());

        Boat result;
        if (!idChanged && !displayChanged && !designUpgraded && mergedSources.equals(candidate.sources()))
        {
            // Nothing to update on the boat record; still persist aliases below if any.
            result = candidate;
        }
        else
        {
            Boat updated = new Boat(newBoatId, candidate.sailNumber(), canonicalName, finalDesignId,
                candidate.clubIds(), candidate.certificates(), mergedSources, Instant.now(), null);
            if (idChanged)
            {
                String oldId = candidate.id();
                removeBoat(oldId);
                putBoat(updated);
                rewriteFinisherBoatId(oldId, newBoatId);
                ClubLoader.remapBoatId(configDir, oldId, newBoatId);
                LOG.info("Name-equivalence rename: {} ('{}') → {} ('{}') after incoming '{}'",
                    oldId, candidate.name(), newBoatId, canonicalName, incomingRaw);
            }
            else
            {
                putBoat(updated);
                if (displayChanged)
                    LOG.info("Name-equivalence display rename: {} '{}' → '{}'",
                        candidate.id(), candidate.name(), canonicalName);
            }
            result = updated;
        }

        if (!aliasesToAdd.isEmpty())
            Aliases.addAliases(configDir, candidate.sailNumber(), canonicalName, aliasesToAdd);
        return result;
    }

    /** Finds or creates a design by class name -- used internally by findOrCreateBoat. */
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
        // Always reflect the catalogue's current noSpinnaker state on the in-memory record.
        boolean catNoSpin = designCatalogue.isNoSpinnaker(design.id());
        Design stamped = design.noSpinnaker() == catNoSpin ? design : design.withNoSpinnaker(catNoSpin);
        designs.put(stamped.id(), stamped);
        InvalidationListener l = invalidationListener;
        if (l != null)
            l.onDesignChanged(stamped.id());
    }

    public void putRace(Race race)
    {
        requireStarted();
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
        putDesign(new Design(keepDesign.id(), keepDesign.canonicalName(),
            List.copyOf(allAliases), List.copyOf(allSources), Instant.now(),
            keepDesign.noSpinnaker(), null));

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

                    List<String> mergedClubIds = mergeClubIds(existingAtNewId.clubIds(), boat.clubIds());
                    toWrite = new Boat(newId, existingAtNewId.sailNumber(), existingAtNewId.name(), keepId,
                        mergedClubIds, List.copyOf(certMap.values()), List.copyOf(mergedSources), Instant.now(), null);
                }
                else
                {
                    toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                        boat.clubIds(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
                }
            }
            else
            {
                toWrite = new Boat(newId, boat.sailNumber(), boat.name(), keepId,
                    boat.clubIds(), boat.certificates(), boat.sources(), boat.lastUpdated(), null);
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

        // Remap boatId-based club config for any boats that got a new ID
        if (!boatIdRemap.isEmpty())
        {
            for (Map.Entry<String, String> remap : boatIdRemap.entrySet())
            {
                ClubLoader.remapBoatId(configDir, remap.getKey(), remap.getValue());
            }
            reloadClubCatalogue();
        }

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
     * Updates the YAML-owned {@code topyacht} URL list for a club. A null or empty
     * list clears the field. Auto-creates the YAML entry if missing.
     */
    public void updateClubTopyachtUrls(String clubId, List<String> topyachtUrls)
    {
        requireStarted();
        Club existing = clubs.get(clubId);
        Club seed = clubSeed.get(clubId);
        if (existing == null && seed == null)
            throw new IllegalArgumentException("Unknown club: " + clubId);

        String shortNameIfNew = existing != null ? existing.shortName()
            : seed.shortName();
        boolean changed = ClubLoader.updateClubTopyachtUrls(configDir, clubId,
            shortNameIfNew, topyachtUrls);
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
                null, null, false, null, List.of(), List.of(),
                json.series(), json.loadedAt());
        }
        return new Club(json.id(), json.shortName(),
            seed.longName(), seed.state(), seed.excluded(), seed.email(),
            seed.aliases(), seed.topyachtUrls(),
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
            if (!ignoredId.equals(boat.designId())) continue;
            String newId = IdGenerator.generateBoatId(boat.sailNumber(), boat.name(), null);
            Boat target = boats.get(newId);
            if (target != null && !target.id().equals(boat.id()))
            {
                // Collision: merge boat INTO the existing target (prefer target's clubId
                // and existing certificates; union sources).
                Map<String, Certificate> certMap = new LinkedHashMap<>();
                for (Certificate c : target.certificates()) certMap.put(certKey(c), c);
                for (Certificate c : boat.certificates())   certMap.putIfAbsent(certKey(c), c);
                Set<String> mergedSources = new LinkedHashSet<>(target.sources());
                mergedSources.addAll(boat.sources());
                mergedSources.add(note);
                List<String> mergedClubIds = mergeClubIds(target.clubIds(), boat.clubIds());
                Boat merged = new Boat(newId, target.sailNumber(), target.name(), null,
                    mergedClubIds, List.copyOf(certMap.values()), List.copyOf(mergedSources),
                    Instant.now(), null);
                removeBoat(boat.id());
                putBoat(merged);
                rewriteFinisherBoatId(boat.id(), newId);
                // Merged-away boat: remove its club config entry (target keeps its own)
                ClubLoader.removeBoatId(configDir, boat.id());
                clubConfigChanged = true;
                LOG.info("Ignore cascade: merged {} into {} (design {} ignored)",
                    boat.id(), newId, ignoredId);
            }
            else
            {
                // Simple rename: strip the design suffix in place.
                Boat updated = new Boat(newId, boat.sailNumber(), boat.name(), null,
                    boat.clubIds(), boat.certificates(),
                    addSource(boat.sources(), note), Instant.now(), null);
                if (!newId.equals(boat.id()))
                {
                    removeBoat(boat.id());
                    putBoat(updated);
                    rewriteFinisherBoatId(boat.id(), newId);
                    ClubLoader.remapBoatId(configDir, boat.id(), newId);
                    clubConfigChanged = true;
                    LOG.info("Ignore cascade: renamed {} to {} (design {} ignored)",
                        boat.id(), newId, ignoredId);
                }
                else
                {
                    // Boat id already has no suffix (shouldn't really happen since the
                    // match was on designId) -- just annotate the sources.
                    putBoat(updated);
                }
            }
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
        // Re-stamp catalogue-derived flags on every in-memory Design (e.g. noSpinnaker).
        designs.replaceAll((id, d) -> d.noSpinnaker() == designCatalogue.isNoSpinnaker(id)
            ? d : d.withNoSpinnaker(designCatalogue.isNoSpinnaker(id)));
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
     * Assigns a boatId to a specific club in clubs.yaml and reloads the catalogue.
     */
    public void setBoatClubOverrideById(String boatId, String clubId)
    {
        requireStarted();
        ClubLoader.setBoatClub(configDir, boatId, clubId);
        reloadClubCatalogue();
    }

    /**
     * Assigns a boatId to a list of clubs in clubs.yaml and reloads the catalogue.
     * An empty list moves the boatId to the {@code noclub} list.
     */
    public void setBoatClubsOverrideById(String boatId, List<String> clubIds)
    {
        requireStarted();
        ClubLoader.setBoatClubs(configDir, boatId, clubIds);
        reloadClubCatalogue();
    }

    /**
     * Marks a boatId as explicitly having no club in clubs.yaml and reloads the catalogue.
     */
    public void setBoatNoClubById(String boatId)
    {
        requireStarted();
        ClubLoader.setBoatNoClub(configDir, boatId);
        reloadClubCatalogue();
    }

    /**
     * Renames a boatId in clubs.yaml noclub/boats lists and reloads the catalogue.
     */
    public void remapBoatIdInClubConfig(String oldBoatId, String newBoatId)
    {
        requireStarted();
        ClubLoader.remapBoatId(configDir, oldBoatId, newBoatId);
        reloadClubCatalogue();
    }

    /**
     * Removes a boatId from clubs.yaml noclub/boats lists and reloads the catalogue.
     */
    public void removeBoatFromClubConfig(String boatId)
    {
        requireStarted();
        ClubLoader.removeBoatId(configDir, boatId);
        reloadClubCatalogue();
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

        // Merge certificates -- deduplicate by system+year+variant; keep boat's certs take priority
        Map<String, Certificate> certMap = new LinkedHashMap<>();
        for (Certificate c : keepBoat.certificates())
            certMap.put(certKey(c), c);
        for (Boat mb : toMerge)
            for (Certificate c : mb.certificates())
                certMap.putIfAbsent(certKey(c), c);

        // Prefer a non-null designId from any of the boats; union all club ids
        String designId = keepBoat.designId() != null ? keepBoat.designId()
            : toMerge.stream().map(Boat::designId).filter(Objects::nonNull).findFirst().orElse(null);
        List<String> mergedClubIds = keepBoat.clubIds();
        for (Boat mb : toMerge)
        {
            mergedClubIds = mergeClubIds(mergedClubIds, mb.clubIds());
        }

        Set<String> mergedSources = new LinkedHashSet<>(keepBoat.sources());
        for (Boat mb : toMerge)
            mergedSources.addAll(mb.sources());
        Boat mergedBoat = new Boat(keepBoat.id(), keepBoat.sailNumber(), keepBoat.name(),
            designId, mergedClubIds,
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
                LOG.warn("Boat {} has no sources -- likely a stale entry, consider deleting {}", b.id(), b.id() + ".json");
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
                    List.of("DesignOverride"), Instant.now(), false, null);
                putDesign(d);
                LOG.info("Created design {} ('{}') from boatDesignOverrides in design.yaml", normId, canonicalName);
            }
            else if (!existing.sources().contains("DesignOverride"))
            {
                putDesign(new Design(existing.id(), existing.canonicalName(),
                    existing.aliases(), addSource(existing.sources(), "DesignOverride"),
                    existing.lastUpdated(), existing.noSpinnaker(), null));
            }
        });

        // Stamp the catalogue-derived noSpinnaker flag onto every Design now that both the
        // designs map and the catalogue are loaded. Subsequent toggles re-stamp via reloadDesignCatalogue().
        designs.replaceAll((id, d) -> d.noSpinnaker() == designCatalogue.isNoSpinnaker(id)
            ? d : d.withNoSpinnaker(designCatalogue.isNoSpinnaker(id)));

        loadExclusions();
        clubs = new LinkedHashMap<>();
        loadDir(clubsDir, Club.class).forEach(c -> clubs.put(c.id(), c));
        // YAML is the source of truth for longName/state/excluded/email/aliases/topyachtUrls;
        // populate those fields on the in-memory Club records from clubSeed.
        clubs.replaceAll((id, c) -> enrichWithSeed(c));
        races = new LinkedHashMap<>();
        loadDirRecursive(racesDir, Race.class).forEach(r -> races.put(r.id(), r));

        // noclub correction: boats that have a persisted clubId but are listed in the noclub
        // config get their clubs cleared. This fixes boats that were assigned a club before the
        // noclub entry was added, or before the importer guard was in place.
        {
            List<Boat> noclubViolations = boats.values().stream()
                .filter(b -> !b.clubIds().isEmpty() && isExplicitlyNoClub(b.id()))
                .toList();
            if (!noclubViolations.isEmpty())
            {
                LOG.warn("Correcting {} boat(s) with a club that are listed in noclub", noclubViolations.size());
                for (Boat b : noclubViolations)
                {
                    LOG.warn("noclub correction: clearing clubIds {} from boat {}", b.clubIds(), b.id());
                    putBoat(new Boat(b.id(), b.sailNumber(), b.name(), b.designId(),
                        List.of(), b.certificates(), b.sources(), Instant.now(), null));
                }
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
                    if (b == null)
                        continue;
                    String canonDesignId = e.getValue();
                    String canonBoatId = IdGenerator.generateBoatId(b.sailNumber(), b.name(), designs.get(canonDesignId));
                    if (boats.containsKey(canonBoatId))
                    {
                        LOG.info("Design alias correction: merging {} into {} (designId {} → {})",
                            b.id(), canonBoatId, b.designId(), canonDesignId);
                        mergeBoats(canonBoatId, List.of(b.id()));
                    }
                    else
                    {
                        LOG.info("Design alias correction: updating designId {} → {} for boat {}",
                            b.designId(), canonDesignId, b.id());
                        Boat updated = new Boat(canonBoatId, b.sailNumber(), b.name(), canonDesignId,
                            b.clubIds(), b.certificates(), b.sources(), Instant.now(), null);
                        removeBoat(b.id());
                        putBoat(updated);
                        rewriteFinisherBoatId(b.id(), canonBoatId);
                        ClubLoader.remapBoatId(configDir, b.id(), canonBoatId);
                    }
                }
            }
        }

        // Design override correction: boats whose stored designId disagrees with an active
        // boatDesignOverrides entry in design.yaml are migrated to the override design. This
        // repairs boats created before the override was added (e.g. MYC12 / San Toy /
        // radford12 → radford12catrig; 1088 / Corum / farrmumm36 → farr36modified).
        // Date is passed as null so only undated/currently-active overrides apply; dated
        // overrides encode "boat changed design between X and Y" and are only consulted at
        // import time.
        {
            List<Map.Entry<String, String>> designOverrideUpdates = new ArrayList<>();
            for (Boat b : boats.values())
            {
                String overrideId = designCatalogue.resolveDesignOverride(
                    b.sailNumber(), IdGenerator.normaliseName(b.name()), null);
                if (overrideId == null)
                    continue;
                if (overrideId.equals(b.designId()))
                    continue;
                if (!designs.containsKey(overrideId))
                    continue;
                designOverrideUpdates.add(Map.entry(b.id(), overrideId));
            }
            if (!designOverrideUpdates.isEmpty())
            {
                LOG.info("Correcting {} boat(s) with stale designId vs design override at startup",
                    designOverrideUpdates.size());
                for (Map.Entry<String, String> e : designOverrideUpdates)
                {
                    Boat b = boats.get(e.getKey());
                    if (b == null)
                        continue;
                    String canonDesignId = e.getValue();
                    String canonBoatId = IdGenerator.generateBoatId(
                        b.sailNumber(), b.name(), designs.get(canonDesignId));
                    if (boats.containsKey(canonBoatId))
                    {
                        LOG.info("Design override correction: merging {} into {} (designId {} → {})",
                            b.id(), canonBoatId, b.designId(), canonDesignId);
                        mergeBoats(canonBoatId, List.of(b.id()));
                    }
                    else
                    {
                        LOG.info("Design override correction: updating designId {} → {} for boat {}",
                            b.designId(), canonDesignId, b.id());
                        Boat updated = new Boat(canonBoatId, b.sailNumber(), b.name(), canonDesignId,
                            b.clubIds(), b.certificates(), b.sources(), Instant.now(), null);
                        removeBoat(b.id());
                        putBoat(updated);
                        rewriteFinisherBoatId(b.id(), canonBoatId);
                        ClubLoader.remapBoatId(configDir, b.id(), canonBoatId);
                    }
                }
            }
        }

        // Auto-fix stale boats: if the alias seed maps a boat's name to a different canonical
        // name and the canonical boat already exists, merge the stale boat into it.
        // This repairs boats that were created before the alias entry was added and prevents
        // them from persisting across imports via the direct boats.get(boatId) fast path.
        // After merging we also consolidate aliases.yaml: an orphan alias entry keyed by the
        // stale (sail, name) -- typically left over from a previous merge -- gets absorbed into
        // the canonical entry, otherwise the next import would re-resolve the stale identity
        // and recreate the JSON boat record.
        {
            record StalePair(String staleId, String canonId,
                             String staleSail, String staleName,
                             String canonSail, String canonName) {}
            List<StalePair> staleBoatPairs = new ArrayList<>();
            boolean aliasesChanged = false;
            for (Boat b : new ArrayList<>(boats.values()))
            {
                String normName = IdGenerator.normaliseName(b.name());
                // Aliases.lookupBoat expects a pure normalised name; passing the design
                // suffix concatenated here (a pre-existing bug) made this pass miss every
                // alias-stale boat that had a designId, defeating the whole safety net.
                var match = aliases.lookupBoat(b.sailNumber(), normName);
                if (match.isPresent() && match.get().normName() != null)
                {
                    String canonNorm = match.get().normName();
                    if (!canonNorm.equals(normName))
                    {
                        String canonSail = match.get().normSailNumber() != null
                            ? match.get().normSailNumber() : b.sailNumber();
                        String displayName = match.get().canonicalDisplayName() != null
                            ? match.get().canonicalDisplayName() : b.name();
                        // Use b.designId() (the string) directly instead of looking up the
                        // Design object: if the catalog doesn't have a matching Design (e.g.
                        // design file missing on disk, or excluded design after a config
                        // change) we still want to preserve the design suffix on the renamed
                        // boatId, otherwise the rename produces an inconsistent record where
                        // the id has no suffix but the designId field still does.
                        String canonId = renameBoatId(canonSail, displayName, b.designId());
                        if (boats.containsKey(canonId))
                        {
                            Boat canonical = boats.get(canonId);
                            staleBoatPairs.add(new StalePair(b.id(), canonId,
                                b.sailNumber(), normName,
                                canonSail, canonical.name()));
                        }
                        else if (!canonId.equals(b.id()))
                        {
                            // No canonical record exists yet. Rename the stale boat in place:
                            // change its id and sail/name to the canonical form, rewrite finisher
                            // refs in races, remap its club override entry, and absorb any orphan
                            // alias. Equivalent to what the next import would have done; doing it
                            // at startup eliminates the recurring warning. A subsequent stale boat
                            // mapping to the same canonical falls into the merge branch above
                            // because this one now occupies canonId.
                            LOG.info("Auto-renaming stale boat {} → {} (canonical sail per alias seed)",
                                b.id(), canonId);
                            Boat renamed = new Boat(canonId, canonSail, displayName, b.designId(),
                                b.clubIds(), b.certificates(), b.sources(), Instant.now(), null);
                            String oldId = b.id();
                            removeBoat(oldId);
                            putBoat(renamed);
                            rewriteFinisherBoatId(oldId, canonId);
                            ClubLoader.remapBoatId(configDir, oldId, canonId);
                            Aliases.addAliases(configDir, canonSail, displayName,
                                List.of(new Aliases.SailNumberName(b.sailNumber(), normName)));
                            aliasesChanged = true;
                        }
                    }
                }
            }
            if (!staleBoatPairs.isEmpty())
            {
                LOG.info("Auto-fixing {} stale boat(s) at startup", staleBoatPairs.size());
                for (StalePair pair : staleBoatPairs)
                {
                    LOG.info("Auto-merging stale boat {} into canonical {}", pair.staleId(), pair.canonId());
                    mergeBoats(pair.canonId(), List.of(pair.staleId()));
                    // Consolidate aliases.yaml so the orphan entry that resolved to the stale
                    // identity is absorbed into the canonical entry.
                    Aliases.addAliases(configDir, pair.canonSail(), pair.canonName(),
                        List.of(new Aliases.SailNumberName(pair.staleSail(), pair.staleName())));
                    aliasesChanged = true;
                }
            }
            if (aliasesChanged)
                aliases = Aliases.load(configDir);
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

        // Name normalisation pass (Phase A): rename any boat whose stored display name
        // carries a decorative suffix (now stripped by normaliseName). Before this change,
        // "Foobar - GM" produced boatId ".../foobargm" and stored name "Foobar - GM";
        // now the same input would produce ".../foobar". Rename existing records to match
        // so the IDs are stable across imports. Merge into an existing target if one is
        // already there at the new id.
        {
            boolean aliasesChangedA = false;
            for (Boat b : new ArrayList<>(boats.values()))
            {
                String cleaned = IdGenerator.stripStandardSuffixes(b.name());
                if (cleaned == null || cleaned.equals(b.name()))
                    continue;
                String newId = renameBoatId(b.sailNumber(), cleaned, b.designId());
                if (newId.equals(b.id()))
                    continue;
                if (boats.containsKey(newId))
                {
                    LOG.info("Suffix-strip merge: '{}' ({}) → existing '{}'", b.name(), b.id(), newId);
                    mergeBoats(newId, List.of(b.id()));
                }
                else
                {
                    LOG.info("Suffix-strip rename: '{}' ({}) → '{}' ({})", b.name(), b.id(), cleaned, newId);
                    Boat renamed = new Boat(newId, b.sailNumber(), cleaned, b.designId(),
                        b.clubIds(), b.certificates(), b.sources(), Instant.now(), null);
                    String oldId = b.id();
                    removeBoat(oldId);
                    putBoat(renamed);
                    rewriteFinisherBoatId(oldId, newId);
                    ClubLoader.remapBoatId(configDir, oldId, newId);
                    // Persist the original suffixed name as an alias so any importer still
                    // emitting "Foobar - GM" continues to resolve to this boat.
                    Aliases.addAliases(configDir, b.sailNumber(), cleaned,
                        List.of(new Aliases.SailNumberName(b.sailNumber(),
                            IdGenerator.normaliseName(b.name()))));
                    aliasesChangedA = true;
                }
            }
            if (aliasesChangedA)
                aliases = Aliases.load(configDir);
        }

        // Name-equivalence collapse (Phase B): group remaining boats by (sail, designId,
        // nameMatchKey) and merge each non-trivial group into one canonical boat. Handles
        // pre-existing duplicates introduced before findOrCreateBoat learned about
        // nameMatchKey (e.g. "Sticky" and "Sticky II" both already on disk).
        {
            record Key(String sail, String designId, String matchKey) {}
            Map<Key, List<Boat>> groups = new LinkedHashMap<>();
            for (Boat b : boats.values())
            {
                String mk = IdGenerator.nameMatchKey(b.name());
                if (mk.isEmpty())
                    continue;
                Key key = new Key(b.sailNumber(),
                    b.designId() == null ? "" : b.designId(), mk);
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(b);
            }
            boolean aliasesChangedB = false;
            for (Map.Entry<Key, List<Boat>> e : groups.entrySet())
            {
                List<Boat> group = e.getValue();
                if (group.size() < 2)
                    continue;

                String canonicalName = IdGenerator.preferredDisplayName(
                    group.stream().map(Boat::name).toList());
                String canonNorm = IdGenerator.normaliseName(canonicalName);

                // Pick the boat whose normalised name already equals the canonical; if
                // none, rename the first member to the canonical so we have a stable
                // target before merging the rest into it.
                Boat keep = null;
                for (Boat b : group)
                {
                    if (canonNorm.equalsIgnoreCase(IdGenerator.normaliseName(b.name())))
                    {
                        keep = b;
                        break;
                    }
                }
                if (keep == null)
                {
                    Boat first = group.getFirst();
                    String newId = renameBoatId(first.sailNumber(), canonicalName, first.designId());
                    if (boats.containsKey(newId) && !newId.equals(first.id()))
                    {
                        // A separate boat already occupies the canonical id (e.g. created in
                        // a parallel branch). Use that one as keep and merge "first" into it.
                        keep = boats.get(newId);
                    }
                    else
                    {
                        Boat renamed = new Boat(newId, first.sailNumber(), canonicalName,
                            first.designId(), first.clubIds(), first.certificates(),
                            first.sources(), Instant.now(), null);
                        String oldId = first.id();
                        if (!newId.equals(oldId))
                        {
                            removeBoat(oldId);
                            putBoat(renamed);
                            rewriteFinisherBoatId(oldId, newId);
                            ClubLoader.remapBoatId(configDir, oldId, newId);
                            LOG.info("Name-equivalence canonical rename: {} ('{}') → {} ('{}')",
                                oldId, first.name(), newId, canonicalName);
                        }
                        else
                        {
                            putBoat(renamed);
                        }
                        keep = renamed;
                    }
                }

                List<String> mergeIds = new ArrayList<>();
                List<Aliases.SailNumberName> aliasEntries = new ArrayList<>();
                for (Boat b : group)
                {
                    if (b.id().equals(keep.id()))
                        continue;
                    mergeIds.add(b.id());
                    String absorbedNorm = IdGenerator.normaliseName(b.name());
                    if (!absorbedNorm.equalsIgnoreCase(canonNorm))
                        aliasEntries.add(new Aliases.SailNumberName(b.sailNumber(), absorbedNorm));
                }
                if (!mergeIds.isEmpty())
                {
                    LOG.info("Name-equivalence collapse: merging {} into {} (canonical='{}')",
                        mergeIds, keep.id(), canonicalName);
                    mergeBoats(keep.id(), mergeIds);
                }
                if (!aliasEntries.isEmpty())
                {
                    Aliases.addAliases(configDir, keep.sailNumber(), canonicalName, aliasEntries);
                    aliasesChangedB = true;
                }
            }
            if (aliasesChangedB)
                aliases = Aliases.load(configDir);
        }

        // Second noclub correction pass: the earlier pass at the top of this method
        // catches the simple case where a boat already at its final boatId is in noclub.
        // But the design alias / design override / auto-fix-stale / design upgrade passes
        // can REGENERATE a boat under a new boatId (the noclub target). Re-run the noclub
        // correction so the post-migration boats also pick up their noclub assignment.
        {
            List<Boat> noclubViolations = boats.values().stream()
                .filter(b -> !b.clubIds().isEmpty() && isExplicitlyNoClub(b.id()))
                .toList();
            if (!noclubViolations.isEmpty())
            {
                LOG.warn("Correcting {} post-migration boat(s) with a club that are in noclub",
                    noclubViolations.size());
                for (Boat b : noclubViolations)
                {
                    LOG.warn("noclub correction (post-migration): clearing clubIds {} from boat {}",
                        b.clubIds(), b.id());
                    putBoat(new Boat(b.id(), b.sailNumber(), b.name(), b.designId(),
                        List.of(), b.certificates(), b.sources(), Instant.now(), null));
                }
            }
        }

        // Post-repair sanity scan: re-check every boat after all repair passes have run
        // and remember any residual violations. These get logged as ERROR and surfaced via
        // findStaleBoatViolations() (and ultimately /api/health). An empty list means the
        // dataset is clean. The four known violation patterns are:
        //   (1) a boat whose (sail, name) resolves via aliases.lookupBoat to a different
        //       canonical (alias-stale orphan that no pass managed to merge);
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
            // (1) alias-stale: lookup resolves to a different canonical (sail, name).
            var match = aliases.lookupBoat(b.sailNumber(), normName).orElse(null);
            if (match != null && match.normName() != null
                && (!match.normName().equalsIgnoreCase(normName)
                || (match.normSailNumber() != null
                && !match.normSailNumber().equalsIgnoreCase(b.sailNumber()))))
            {
                violations.add(String.format(
                    "alias-stale boat %s (sail=%s name=%s) should resolve to canonical sail=%s name=%s",
                    b.id(), b.sailNumber(), normName,
                    match.normSailNumber() != null ? match.normSailNumber() : b.sailNumber(),
                    match.normName()));
            }
            // (2) design-override drift.
            String overrideId = designCatalogue.resolveDesignOverride(
                b.sailNumber(), normName, null);
            if (overrideId != null && !overrideId.equals(b.designId())
                && designs.containsKey(overrideId))
            {
                violations.add(String.format(
                    "design-override drift on %s: stored designId=%s but override expects %s",
                    b.id(), b.designId(), overrideId));
            }
            // (3) stale noclub assignment.
            if (!b.clubIds().isEmpty() && isExplicitlyNoClub(b.id()))
            {
                violations.add(String.format(
                    "noclub violation on %s: clubIds=%s but boatId is in noclub list",
                    b.id(), b.clubIds()));
            }
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
