package org.mortbay.sailing.pf.importer;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.mortbay.sailing.pf.data.Boat;
import org.mortbay.sailing.pf.data.Certificate;
import org.mortbay.sailing.pf.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One row of the ORC active-certificates list, as returned by both the legacy
 * WPub.dll XML feed ({@link OrcImporter}) and the ORC API JSON feed ({@link OrcApiImporter}).
 * The two feeds carry the same fields; only the GPH source differs (HTML cert page vs JSON).
 * <p>
 * All fields are raw strings (null when absent or blank); validation happens in
 * {@link #importInto(DataStore, double)}.
 */
record OrcCertRow(String dxtId, String yachtName, String sailNo, String vppYear,
                  String certType, String expiry, String className, String familyName)
{
    private static final Logger LOG = LoggerFactory.getLogger(OrcCertRow.class);

    static final String SOURCE = "ORC";

    // CertType values indicating non-spinnaker
    private static final Set<String> NON_SPIN_CERT_TYPES = Set.of("10", "11");

    // CertType values indicating double-handed
    private static final Set<String> DH_CERT_TYPES = Set.of("8", "9");

    // CertType values for international certificates; all others are club.
    // 1 = IRC+ORC intl, 2 = ORC intl, 8 = DH intl, 10 = NS intl
    private static final Set<String> INTL_CERT_TYPES = Set.of("1", "2", "8", "10");

    /** The dxtIDs of all ORC certificates already in the store (certificateNumber holds the dxtID). */
    static Set<String> importedDxtIds(DataStore store)
    {
        return store.boats().values().stream()
            .flatMap(b -> b.certificates().stream())
            .map(Certificate::certificateNumber)
            .filter(n -> n != null)
            .collect(Collectors.toSet());
    }

    /**
     * Validates required fields and GPH, then creates or updates Design, Boat and Certificate
     * in the store. GPH is converted to TCF (600/GPH). Invalid rows are logged and skipped.
     */
    void importInto(DataStore store, double gph)
    {
        if (dxtId == null)
            return;
        LOG.info("Process {} {} {} {} {} {} {} {}", dxtId, yachtName, sailNo, vppYear, certType, expiry, className, familyName);

        if (yachtName == null || sailNo == null || vppYear == null)
        {
            ImporterLog.warn(LOG, "Skipping cert dxtID={}: missing required fields", dxtId);
            return;
        }

        int year;
        try
        {
            year = Integer.parseInt(vppYear.trim());
        }
        catch (NumberFormatException e)
        {
            ImporterLog.warn(LOG, "Skipping cert dxtID={}: invalid VPPYear={}", dxtId, vppYear);
            return;
        }

        if (Double.isNaN(gph))
        {
            ImporterLog.warn(LOG, "Skipping cert dxtID={}: GPH not found", dxtId);
            return;
        }
        if (gph < 250 || gph > 950)
        {
            ImporterLog.warn(LOG, "Skipping cert dxtID={}: implausible GPH={} (expected 250-950)", dxtId, gph);
            return;
        }
        double tcf = 600.0 / gph;

        String type = certType == null ? "" : certType.trim();
        boolean nonSpinnaker = (familyName != null && familyName.contains("Non Spinnaker"))
            || NON_SPIN_CERT_TYPES.contains(type);
        boolean twoHanded = (familyName != null && familyName.contains("Double Handed"))
            || DH_CERT_TYPES.contains(type);
        boolean club = !INTL_CERT_TYPES.contains(type);

        LocalDate expiryDate = parseDate(expiry);

        // Find-or-create Boat (design resolved inside findOrCreateBoat)
        Boat boat = store.findOrCreateBoat(sailNo, yachtName.trim(), className, null, SOURCE);

        // Upsert certificate: remove old cert with same dxtId first, then add new one
        List<Certificate> updatedCerts = new ArrayList<>(boat.certificates());
        updatedCerts.removeIf(c -> dxtId.equals(c.certificateNumber()));
        updatedCerts.add(new Certificate("ORC",
            year, tcf, nonSpinnaker, twoHanded, club, false, dxtId, expiryDate));
        updatedCerts.sort(Comparator.comparingInt(Certificate::year).reversed());

        store.putBoat(new Boat(boat.id(), boat.sailNumber(), boat.name(),
            boat.designId(), boat.clubIds(),
            List.copyOf(updatedCerts), addSource(boat.sources(), SOURCE), Instant.now(), null));
    }

    /** Parses a yyyy-MM-dd date, ignoring any trailing time part; null if absent or invalid. */
    private LocalDate parseDate(String s)
    {
        if (s == null || s.isBlank())
            return null;
        String t = s.trim();
        try
        {
            return LocalDate.parse(t.length() > 10 ? t.substring(0, 10) : t);
        }
        catch (Exception e)
        {
            LOG.debug("Cannot parse expiry date for dxtID={}: {}", dxtId, s);
            return null;
        }
    }

    private static List<String> addSource(List<String> existing, String source)
    {
        if (existing.contains(source))
            return existing;
        List<String> updated = new ArrayList<>(existing);
        updated.add(source);
        return List.copyOf(updated);
    }
}
