package org.mortbay.sailing.pf.store;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.data.Club;
import org.mortbay.sailing.pf.data.Design;
import org.mortbay.sailing.pf.data.Race;
import org.mortbay.sailing.pf.data.SeriesType;

/**
 * A read-only view of a {@link DataStore} as it would have been at a cutoff date: only the
 * races sailed before the cutoff, and each boat with only the certificates for years up to
 * the cutoff's year. Exclusions, aliases and design flags are today's, passed through from
 * the underlying store. Used to rerun the analysis as at a past date (see
 * {@link DataStore#asOf}).
 * <p>
 * Only the reads the analysis makes are supported. The view is never started, so any other
 * call fails with "not started", and it has no data root, so the analysis writes no logs.
 */
final class DatedDataStore extends DataStore
{
    private final DataStore store;
    private final LocalDate cutoff;
    private final Map<String, Race> races;
    private final Map<String, Boat> boats;

    DatedDataStore(DataStore store, LocalDate cutoff)
    {
        super(store.dataRoot());
        this.store = store;
        this.cutoff = cutoff;

        Map<String, Race> rs = new LinkedHashMap<>();
        for (Race race : store.races().values())
        {
            if (race.date() != null && race.date().isBefore(cutoff))
                rs.put(race.id(), race);
        }
        races = Collections.unmodifiableMap(rs);

        int year = cutoff.getYear();
        Map<String, Boat> bs = new LinkedHashMap<>();
        for (Boat boat : store.boats().values())
        {
            List<Certificate> known = boat.certificates().stream().filter(c -> c.year() <= year).toList();
            bs.put(boat.id(), known.size() == boat.certificates().size() ? boat : boat.withCertificates(known));
        }
        boats = Collections.unmodifiableMap(bs);
    }

    /** The cutoff: races on or after this date are left out. */
    public LocalDate cutoff()
    {
        return cutoff;
    }

    @Override
    public DataStore asOf(LocalDate date)
    {
        return store.asOf(date);
    }

    @Override
    public Path dataRoot()
    {
        return null;
    }

    @Override
    public Map<String, Race> races()
    {
        return races;
    }

    @Override
    public Map<String, Boat> boats()
    {
        return boats;
    }

    @Override
    public Map<String, Design> designs()
    {
        return store.designs();
    }

    @Override
    public Map<String, Club> clubs()
    {
        return store.clubs();
    }

    @Override
    public boolean isDesignExcluded(String designId)
    {
        return store.isDesignExcluded(designId);
    }

    @Override
    public boolean isClubExcluded(String clubId)
    {
        return store.isClubExcluded(clubId);
    }

    @Override
    public boolean isBoatExcluded(String boatId)
    {
        return store.isBoatExcluded(boatId);
    }

    @Override
    public boolean isRaceExcluded(String raceId)
    {
        return store.isRaceExcluded(raceId);
    }

    @Override
    public boolean isBoatNoSpinnakerDesign(String boatId)
    {
        return store.isBoatNoSpinnakerDesign(boatId);
    }

    @Override
    public boolean isDesignNoSpinnaker(String designId)
    {
        return store.isDesignNoSpinnaker(designId);
    }

    @Override
    public boolean isDesignOneDesign(String designId)
    {
        return store.isDesignOneDesign(designId);
    }

    @Override
    public boolean isBoatModified(String boatId)
    {
        return store.isBoatModified(boatId);
    }

    @Override
    public SeriesType seriesType(String seriesId)
    {
        return store.seriesType(seriesId);
    }

    @Override
    public SeriesType raceSeriesType(Race race)
    {
        return store.raceSeriesType(race);
    }
}
