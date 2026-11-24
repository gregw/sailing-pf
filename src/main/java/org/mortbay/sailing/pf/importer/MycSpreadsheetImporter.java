package org.mortbay.sailing.pf.importer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Division;
import org.mortbay.sailing.pf.data.Finisher;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-off import of Manly Yacht Club's historic results (2012–2020) from the club's own
 * scoring workbooks, whose published HTML results have since been lost.
 * <p>
 * Reads every {@code .xlsx} in {@code <dataRoot>/myc-archive}, and those inside any
 * {@code .zip} there. A results workbook has a {@code Setup} sheet naming the series and
 * season, and one {@code Race N} sheet per race: a header block (race date, state) above
 * a table with one row per boat. Columns are found by header name, since the layout
 * changed over the years and headers were sometimes hand-edited.
 * <p>
 * Only the boat name, sail number, design, club, division and elapsed time are imported;
 * skippers and contact details in the workbooks are ignored. The import is idempotent:
 * IDs are derived from series, date and race number, and a series seen twice (the zips
 * repeat the top-level files) is imported once.
 */
public class MycSpreadsheetImporter
{
    private static final Logger LOG = LoggerFactory.getLogger(MycSpreadsheetImporter.class);

    public static final String CLUB_ID = "myc.org.au";
    public static final String SOURCE = "MYC spreadsheet";
    /** Directory under the data root holding the workbooks. */
    public static final String ARCHIVE_DIR = "myc-archive";

    /**
     * SailSys series that were trial runs of races scored in the workbooks. Their races are
     * excluded so each race is held once, from the workbook.
     */
    static final List<String> REPLACED_SAILSYS_SERIES = List.of(
        "myc.org.au/twilights-2019-2020-test",
        "myc.org.au/friday-twilights-single-race-demo");

    /** A race date further than this from the series' median date is a typo candidate. */
    private static final long MAX_DAYS_FROM_MEDIAN = 183;

    private static final Pattern RACE_SHEET = Pattern.compile("(?i)race\\s*(\\d+)");
    private static final Pattern SEASON = Pattern.compile("(\\d{4})\\s*-\\s*(\\d{4})");
    /** A club code or codes as the sheets write them: "MYC", "MHYC", "MYC/CYCA", "NH". */
    private static final Pattern CLUB_CODE = Pattern.compile("[A-Z]{2,6}(\\s*[/&]\\s*[A-Z]{2,6})*");

    private final DataStore store;

    private int workbooksImported;
    private int racesImported;
    private int finishersImported;
    private int racesReplaced;
    /** Boat IDs by normalised name, for {@link #knownBoat}. */
    private final Map<String, Set<String>> boatsByName = new HashMap<>();

    public record RunResult(int workbooks, int races, int finishers, int sailsysRacesExcluded) {}

    /** One boat's result row, as read from a race sheet. */
    record Row(String boatName, String sailNo, String division, String design, String club,
               Duration elapsed) {}

    /** One race sheet that holds results. */
    record RaceSheet(int number, LocalDate date, List<Row> rows) {}

    /** A results workbook, parsed. */
    record ParsedWorkbook(String seriesName, String seriesId, boolean nonSpinnaker,
                          List<RaceSheet> races) {}

    public MycSpreadsheetImporter(DataStore store)
    {
        this.store = store;
    }

    /** Runs the import against a data root (args as {@link DataStore#resolveDataRoot}) and saves it. */
    public static void main(String[] args) throws Exception
    {
        DataStore store = new DataStore(DataStore.resolveDataRoot(args));
        store.start();
        try
        {
            new MycSpreadsheetImporter(store).run();
            store.save();
        }
        finally
        {
            store.stop();
        }
    }

    public RunResult run() throws IOException
    {
        workbooksImported = racesImported = finishersImported = racesReplaced = 0;
        indexBoatsByName();
        Path dir = store.dataRoot().resolve(ARCHIVE_DIR);
        if (!Files.isDirectory(dir))
        {
            ImporterLog.warn(LOG, "MYC: no archive directory {}; nothing to import", dir);
            return new RunResult(0, 0, 0, 0);
        }

        Set<String> seenSeries = new HashSet<>();
        Set<String> seenContent = new HashSet<>();
        List<Path> files;
        try (Stream<Path> s = Files.list(dir))
        {
            files = s.filter(Files::isRegularFile).sorted().toList();
        }
        // Top-level workbooks first, so they win over copies inside the zips.
        for (Path f : files)
        {
            if (f.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".xlsx"))
                importWorkbook(f.getFileName().toString(), Files.readAllBytes(f), seenSeries, seenContent);
        }
        for (Path f : files)
        {
            if (!f.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".zip"))
                continue;
            try (ZipInputStream zin = new ZipInputStream(Files.newInputStream(f)))
            {
                ZipEntry e;
                while ((e = zin.getNextEntry()) != null)
                {
                    if (!e.isDirectory() && e.getName().toLowerCase(Locale.ROOT).endsWith(".xlsx"))
                        importWorkbook(f.getFileName() + "!" + e.getName(), zin.readAllBytes(), seenSeries, seenContent);
                }
            }
            catch (IOException ex)
            {
                ImporterLog.error(LOG, "MYC: cannot read zip {}: {}", f, ex.getMessage());
            }
        }

        excludeReplacedSailSysRaces();

        ImporterLog.info(LOG, "MYC: run complete — {} workbook(s), {} race(s), {} finishers; {} SailSys race(s) excluded",
            workbooksImported, racesImported, finishersImported, racesReplaced);
        return new RunResult(workbooksImported, racesImported, finishersImported, racesReplaced);
    }

    private void importWorkbook(String label, byte[] bytes, Set<String> seenSeries, Set<String> seenContent)
    {
        // The zips repeat the top-level files byte for byte; skip those before parsing.
        if (!seenContent.add(sha256(bytes)))
        {
            LOG.debug("MYC: {} is a copy of a workbook already read", label);
            return;
        }
        XlsxReader.Workbook wb;
        try
        {
            wb = XlsxReader.read(bytes);
        }
        catch (IOException e)
        {
            ImporterLog.error(LOG, "MYC: cannot read workbook {}: {}", label, e.getMessage());
            return;
        }
        ParsedWorkbook parsed = parseWorkbook(label, wb);
        if (parsed == null)
            return;
        if (!seenSeries.add(parsed.seriesId()))
        {
            LOG.info("MYC: {} repeats series {}; already imported", label, parsed.seriesId());
            return;
        }
        workbooksImported++;
        for (RaceSheet rs : parsed.races())
            storeRace(label, parsed, rs);
    }

    // --- Parsing (package-private for testing) ---

    /**
     * Parses a results workbook. Returns null (with a log line) for a workbook that is not
     * a results workbook, has no raced races, or is a Women's Challenge variant other than
     * the combined results.
     */
    ParsedWorkbook parseWorkbook(String label, XlsxReader.Workbook wb)
    {
        XlsxReader.Sheet setup = wb.sheet("Setup");
        if (setup == null)
        {
            LOG.info("MYC: {} has no Setup sheet; not a results workbook", label);
            return null;
        }
        String season = labelValue(setup.rows(), "Season").orElse(null);
        Matcher sm = season == null ? null : SEASON.matcher(season);
        if (sm == null || !sm.find())
        {
            ImporterLog.warn(LOG, "MYC: {} has no season in its Setup sheet; skipping", label);
            return null;
        }
        season = sm.group(1) + "-" + sm.group(2);

        String rawSeries = labelValue(setup.rows(), "Series")
            .orElseGet(() -> seriesFromFileName(label));
        if (isWomensChallenge(rawSeries) && !rawSeries.toLowerCase(Locale.ROOT).contains("combined"))
        {
            ImporterLog.info(LOG, "MYC: {} ('{}') is not the combined Women's Challenge results; skipping",
                label, rawSeries);
            return null;
        }
        String canonical = canonicalSeriesName(rawSeries);

        List<RaceSheet> races = new ArrayList<>();
        for (XlsxReader.Sheet sheet : wb.sheets().values())
        {
            Matcher m = RACE_SHEET.matcher(sheet.name().trim());
            if (!m.matches())
                continue;
            RaceSheet rs = parseRaceSheet(label, Integer.parseInt(m.group(1)), sheet, wb);
            if (rs != null)
                races.add(rs);
        }
        if (races.isEmpty())
        {
            ImporterLog.info(LOG, "MYC: {} ('{}' {}) has no raced races; skipping", label, rawSeries, season);
            return null;
        }
        races = correctDateTypos(label, races);

        LocalDate earliest = races.stream().map(RaceSheet::date).min(LocalDate::compareTo).orElseThrow();
        String prefix = isCalendarYearSeries(canonical) ? String.valueOf(earliest.getYear()) : season;
        String seriesName = prefix + " " + canonical;
        String seriesId = IdGenerator.generateSeriesId(CLUB_ID, seriesName);
        races.sort((a, b) -> Integer.compare(a.number(), b.number()));
        return new ParsedWorkbook(seriesName, seriesId,
            canonical.toLowerCase(Locale.ROOT).contains("twilight"), List.copyOf(races));
    }

    /**
     * Parses one race sheet. Returns null if the race was not sailed (state other than
     * RESULTS), has no date, or has no boat with an elapsed time.
     */
    RaceSheet parseRaceSheet(String label, int number, XlsxReader.Sheet sheet, XlsxReader.Workbook wb)
    {
        List<List<String>> rows = sheet.rows();
        int headerIdx = -1;
        for (int i = 0; i < rows.size(); i++)
        {
            if (rows.get(i).contains("ELAPSED"))
            {
                headerIdx = i;
                break;
            }
        }
        if (headerIdx < 0)
            return null;
        List<List<String>> block = rows.subList(0, headerIdx);

        // The state is "State: RESULTS" (2015 on) or a bare "RESULTS" in the first cell (earlier).
        String state = labelValue(block, "State")
            .orElse(!rows.isEmpty() && "RESULTS".equalsIgnoreCase(rows.get(0).get(0)) ? "RESULTS" : "");
        if (!"RESULTS".equalsIgnoreCase(state))
        {
            LOG.info("MYC: {} {} has state '{}'; not sailed", label, sheet.name(), state);
            return null;
        }

        // The date is "Date: <n>" (to 2014) or the second value of "Race: <number> <date>" (later).
        LocalDate date = labelValue(block, "Date").map(wb::toDate).orElse(null);
        if (date == null)
        {
            List<String> raceValues = labelValues(block, "Race");
            if (raceValues.size() >= 2)
                date = wb.toDate(raceValues.get(1));
        }
        if (date == null && !rows.isEmpty())
        {
            // Some sheets have the "Race:" label cell cleared or overtyped, leaving
            // "<number> <date>" after it.
            List<String> first = rows.get(0);
            if (first.size() >= 3 && Objects.equals(cleanSailNo(first.get(1)), String.valueOf(number)))
            {
                LocalDate d = wb.toDate(first.get(2));
                if (d != null && d.getYear() >= 2000 && d.getYear() <= 2030)
                    date = d;
            }
        }
        if (date == null)
        {
            ImporterLog.warn(LOG, "MYC: {} {} has no race date; skipping", label, sheet.name());
            return null;
        }

        List<String> header = rows.get(headerIdx);
        Columns cols = Columns.of(header);
        if (cols == null)
        {
            ImporterLog.warn(LOG, "MYC: {} {} header lacks BOAT/ELAPSED columns: {}; skipping",
                label, sheet.name(), header);
            return null;
        }

        List<Row> results = new ArrayList<>();
        for (List<String> r : rows.subList(headerIdx + 1, rows.size()))
        {
            String boat = cell(r, cols.boat());
            Duration elapsed = parseElapsed(cell(r, cols.elapsed()));
            if (boat.isEmpty() || elapsed == null || !cell(r, cols.code()).isEmpty())
                continue;
            int shift = cols.clubShift(r);
            String design = cols.design() < 0 ? null : emptyToNull(cell(r, cols.design() + shift));
            String club = cols.club() < 0 ? null : emptyToNull(cell(r, cols.club() + shift));
            results.add(new Row(boat, cleanSailNo(cell(r, cols.sail())), divisionName(cell(r, cols.div())),
                design, club, elapsed));
        }
        if (results.isEmpty())
            return null;
        return new RaceSheet(number, date, List.copyOf(results));
    }

    /** Column indices of a race table, found by header name with positional fallbacks. */
    record Columns(int boat, int sail, int div, int code, int elapsed, int design, int club)
    {
        static Columns of(List<String> header)
        {
            int boat = -1;
            for (int i = 0; i < header.size(); i++)
            {
                if (header.get(i).toUpperCase(Locale.ROOT).startsWith("BOAT"))
                {
                    boat = i;
                    break;
                }
            }
            int elapsed = header.indexOf("ELAPSED");
            // Hand-edited headers: "BOAT BOAT" for "BOAT SAIL", "x" for "BOAT", "State:" for "CODE".
            int sail = header.indexOf("SAIL");
            if (boat < 0 && sail > 0)
                boat = sail - 1;
            if (boat < 0 || elapsed < 0)
                return null;
            if (sail < 0)
                sail = boat + 1;
            int code = header.indexOf("CODE");
            if (code < 0 && header.indexOf("FINISH") >= 0)
                code = header.indexOf("FINISH") + 1;
            return new Columns(boat, sail, header.indexOf("DIV"), code, elapsed,
                header.indexOf("DESIGN"), header.indexOf("CLUB"));
        }

        /**
         * The offset of this row's CLUB and DESIGN values from their headers. Some sheets
         * have rows with a cell more or fewer than the header in the trailing
         * SKIPPER/CLUB/DESIGN block (in TL2 Race 20 finishers lack the skipper cell, DNC
         * rows have an extra one), so values can sit a column either side of their
         * header. The offset is the first of 0, +1, -1 that puts a club code ("MYC",
         * "MHYC/CYCA") under CLUB; 0 if none does.
         */
        int clubShift(List<String> row)
        {
            if (club < 0)
                return 0;
            for (int shift : new int[] {0, 1, -1})
            {
                if (CLUB_CODE.matcher(cell(row, club + shift)).matches())
                    return shift;
            }
            return 0;
        }
    }

    /**
     * Corrects race dates that are a year out: a date more than six months from the
     * series' median date that lands within six months when moved by a year (e.g. a
     * 2016-2017 race dated 2016-01-22 rather than 2017-01-22).
     */
    List<RaceSheet> correctDateTypos(String label, List<RaceSheet> races)
    {
        List<LocalDate> sorted = races.stream().map(RaceSheet::date).sorted().toList();
        LocalDate median = sorted.get(sorted.size() / 2);
        List<RaceSheet> result = new ArrayList<>(races.size());
        for (RaceSheet rs : races)
        {
            LocalDate d = rs.date();
            if (Math.abs(ChronoUnit.DAYS.between(median, d)) <= MAX_DAYS_FROM_MEDIAN)
            {
                result.add(rs);
                continue;
            }
            LocalDate fixed = null;
            for (LocalDate candidate : List.of(d.plusYears(1), d.minusYears(1)))
            {
                if (Math.abs(ChronoUnit.DAYS.between(median, candidate)) <= MAX_DAYS_FROM_MEDIAN)
                    fixed = candidate;
            }
            if (fixed != null)
            {
                ImporterLog.warn(LOG, "MYC: {} race {} dated {} is a year out from the series; using {}",
                    label, rs.number(), d, fixed);
                result.add(new RaceSheet(rs.number(), fixed, rs.rows()));
            }
            else
            {
                ImporterLog.warn(LOG, "MYC: {} race {} dated {} is far from the series' other races; kept as is",
                    label, rs.number(), d);
                result.add(rs);
            }
        }
        return result;
    }

    // --- Storing ---

    private void storeRace(String label, ParsedWorkbook wb, RaceSheet rs)
    {
        String raceId = IdGenerator.generateRaceId(CLUB_ID, wb.seriesId(), rs.date(), rs.number());
        Map<String, List<Finisher>> byDivision = new LinkedHashMap<>();
        Set<String> boatsInRace = new HashSet<>();
        int finishers = 0;
        for (Row row : rs.rows())
        {
            Boat boat = resolveBoat(row, rs.date(), raceId, label);
            if (boat == null)
                continue;
            if (!boatsInRace.add(boat.id()))
            {
                ImporterLog.warn(LOG, "MYC: boat {} appears twice in race {} ({}); keeping the first",
                    boat.id(), raceId, label);
                continue;
            }
            boat = store.assignClubFromCode(boat, primaryClubCode(row.club()), CLUB_ID, SOURCE,
                "MYC boat sail=" + row.sailNo() + " name=" + row.boatName() + " in " + label);
            byDivision.computeIfAbsent(row.division(), k -> new ArrayList<>())
                .add(new Finisher(boat.id(), row.elapsed(), wb.nonSpinnaker(), null));
            finishers++;
        }
        if (finishers == 0)
            return;

        List<Division> divisions = byDivision.entrySet().stream()
            .map(e -> new Division(e.getKey(), List.copyOf(e.getValue())))
            .toList();
        store.putRace(new Race(raceId, CLUB_ID, List.of(wb.seriesId()), rs.date(), rs.number(),
            null, divisions, SOURCE + " - " + label, Instant.now(), null));
        store.addRaceToClubSeries(CLUB_ID, wb.seriesId(), wb.seriesName(), raceId);
        racesImported++;
        finishersImported += finishers;
        ImporterLog.info(LOG, "MYC: imported race {} ({} finishers, {} division(s)) from {}",
            raceId, finishers, divisions.size(), label);
    }

    private Boat resolveBoat(Row row, LocalDate date, String raceId, String label)
    {
        if (row.sailNo() != null)
        {
            Boat known = knownBoat(row);
            Boat boat;
            if (known != null)
            {
                String design = known.designId() == null ? row.design()
                    : Optional.ofNullable(store.designs().get(known.designId()))
                        .map(d -> d.canonicalName()).orElse(known.designId());
                if (!known.sailNumber().equalsIgnoreCase(IdGenerator.normaliseSailNumber(row.sailNo()))
                    || !Objects.equals(known.designId(), IdGenerator.normaliseName(row.design())))
                    ImporterLog.info(LOG, "MYC: '{}' sail={} design='{}' in {} taken as existing boat {}",
                        row.boatName(), row.sailNo(), row.design(), label, known.id());
                boat = store.findOrCreateBoat(known.sailNumber(), known.name(), design, date, SOURCE);
            }
            else
            {
                boat = store.findOrCreateBoat(row.sailNo(), row.boatName(), row.design(), date, SOURCE);
            }
            boatsByName.computeIfAbsent(IdGenerator.normaliseName(boat.name()), k -> new LinkedHashSet<>())
                .add(boat.id());
            return boat;
        }
        Optional<Boat> match = store.findBoatByNameAndClub(row.boatName(), CLUB_ID);
        if (match.isEmpty())
        {
            ImporterLog.warn(LOG, "MYC: skipping '{}' in race {} ({}): no sail number and no unique MYC boat of that name",
                row.boatName(), raceId, label);
            return null;
        }
        return match.get();
    }

    /**
     * The one existing boat this row is, when the sheet's hand-typed details differ from
     * how other sources record it: a boat of the same name whose sail number is the
     * row's, or the row's with a prefix added or dropped ("55KA16" / "KA16",
     * "YAUS56" / "56", "MYC820" / "820"). The sheet's design is then ignored in favour of
     * the boat's, since the sheets spell designs loosely ("Sydney 38od", "Masrm").
     * Null if no boat, or more than one, qualifies; an exact sail match wins over prefixed ones.
     */
    Boat knownBoat(Row row)
    {
        String sail = IdGenerator.normaliseSailNumber(row.sailNo());
        Set<String> ids = boatsByName.get(IdGenerator.normaliseName(row.boatName()));
        if (sail == null || sail.isEmpty() || ids == null)
            return null;
        List<Boat> exact = new ArrayList<>();
        List<Boat> prefixed = new ArrayList<>();
        for (String id : ids)
        {
            Boat b = store.boat(id);
            if (b == null || b.sailNumber() == null)
                continue;
            String other = b.sailNumber().toUpperCase(Locale.ROOT);
            if (other.equals(sail))
                exact.add(b);
            else if ((other.endsWith(sail) && sail.length() >= 2)
                || (sail.endsWith(other) && other.length() >= 2))
                prefixed.add(b);
        }
        if (exact.size() == 1)
            return exact.get(0);
        if (exact.isEmpty() && prefixed.size() == 1)
            return prefixed.get(0);
        return null;
    }

    private void indexBoatsByName()
    {
        boatsByName.clear();
        // Every identity's name, to the identity: a row under a former name is that identity
        for (Boat b : store.boats().values())
            b.identities().forEach((id, i) ->
                boatsByName.computeIfAbsent(IdGenerator.normaliseName(i.name()), k -> new LinkedHashSet<>()).add(id));
    }

    private void excludeReplacedSailSysRaces()
    {
        for (Race race : store.races().values())
        {
            if (race.seriesIds() == null || store.isRaceExcluded(race.id()))
                continue;
            if (race.seriesIds().stream().anyMatch(REPLACED_SAILSYS_SERIES::contains))
            {
                store.setRaceExcluded(race.id(), true, "Replaced by MYC spreadsheet import");
                racesReplaced++;
                ImporterLog.info(LOG, "MYC: excluded SailSys race {} (series {}); replaced by the workbook results",
                    race.id(), race.seriesIds());
            }
        }
    }

    // --- Helpers (package-private for testing) ---

    /**
     * The value after a label cell such as "Season:" in a sheet's header block: the first
     * non-empty cell to its right. Labels match case-insensitively, with or without colon.
     */
    static Optional<String> labelValue(List<List<String>> rows, String label)
    {
        List<String> values = labelValues(rows, label);
        return values.isEmpty() ? Optional.empty() : Optional.of(values.get(0));
    }

    /** All non-empty cells to the right of the first cell labelled {@code label}. */
    static List<String> labelValues(List<List<String>> rows, String label)
    {
        for (List<String> row : rows)
        {
            for (int i = 0; i < row.size(); i++)
            {
                String c = row.get(i).trim();
                if (c.endsWith(":"))
                    c = c.substring(0, c.length() - 1).trim();
                if (!c.equalsIgnoreCase(label))
                    continue;
                List<String> values = new ArrayList<>();
                for (String v : row.subList(i + 1, row.size()))
                {
                    if (!v.isBlank())
                        values.add(v.trim());
                }
                if (!values.isEmpty())
                    return values;
            }
        }
        return List.of();
    }

    /** Elapsed time from an Excel day fraction; null if blank, unparseable, or not in (0, 1 day). */
    static Duration parseElapsed(String value)
    {
        if (value == null || value.isBlank())
            return null;
        try
        {
            double days = Double.parseDouble(value);
            if (!(days > 0) || days >= 1)
                return null;
            return Duration.ofSeconds(Math.round(days * 86400));
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /** "5865.0" → "5865" (Excel stores numeric sail numbers as numbers); blank → null. */
    static String cleanSailNo(String raw)
    {
        if (raw == null || raw.isBlank())
            return null;
        String s = raw.trim();
        if (s.matches("\\d+\\.0+"))
            s = s.substring(0, s.indexOf('.'));
        return s;
    }

    /**
     * "2.0" → "Division 2". The scoring sheets use 123 for a boat entered in every
     * division; it is treated as Division 1.
     */
    static String divisionName(String raw)
    {
        String s = cleanSailNo(raw);
        if (s == null || s.equals("123"))
            return "Division 1";
        return "Division " + s;
    }

    /** "MYC/CYCA" → "MYC": the first of several clubs listed. */
    static String primaryClubCode(String club)
    {
        if (club == null)
            return null;
        String first = club.split("[/&,]")[0].trim();
        return first.isEmpty() ? null : first;
    }

    static boolean isWomensChallenge(String series)
    {
        String s = series.toLowerCase(Locale.ROOT);
        return s.contains("women") || s.contains("hhwc");
    }

    /** The series name used across seasons, so like series get like IDs. */
    static String canonicalSeriesName(String raw)
    {
        String s = raw.trim();
        String lower = s.toLowerCase(Locale.ROOT);
        if (isWomensChallenge(s))
            return "Helly Hansen Women's Challenge";
        if (lower.startsWith("nigel holman"))
            return "Nigel Holman Race 4 Rum";
        if (lower.equals("twilights"))
            return "Twilight Series";
        return s;
    }

    /**
     * Series held within one calendar year are named by that year (as SailSys names them,
     * e.g. "2021 Winter Series"); the rest by season ("2021-2022 Twilight Series").
     */
    static boolean isCalendarYearSeries(String canonical)
    {
        String s = canonical.toLowerCase(Locale.ROOT);
        return s.contains("winter") || s.contains("women") || s.contains("pittwater");
    }

    /** "Twilight Series 2016-2017.xlsx" → "Twilight Series" (for workbooks with no series name). */
    static String seriesFromFileName(String label)
    {
        String name = label.substring(label.lastIndexOf('!') + 1);
        name = name.substring(name.lastIndexOf('/') + 1).replaceAll("(?i)\\.xlsx$", "");
        return name.replaceAll("\\d{4}(\\s*-\\s*\\d{4})?", "").trim();
    }

    private static String sha256(byte[] bytes)
    {
        try
        {
            return java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        }
        catch (java.security.NoSuchAlgorithmException e)
        {
            throw new IllegalStateException(e);
        }
    }

    private static String cell(List<String> row, int idx)
    {
        return idx >= 0 && idx < row.size() ? row.get(idx).trim() : "";
    }

    private static String emptyToNull(String s)
    {
        return s == null || s.isBlank() ? null : s;
    }
}
