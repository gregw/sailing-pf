### Notice to Handicappers

### October 2026

- **Series type**: Each series now has a forced type — Spin, NS (non-spinnaker), 2H (two-handed), Mixed or Unknown, that can override any 
  (not infrequently) incorrect data from the source data.
- **Series/Compare Optimiser**: Added an optimiser for allocated handicaps in the calculators on the series and compare pages.
- **Duplicate races**: A race imported more than once (two series, two clubs, or SailSys and TopYacht) now counts each boat's result once.
- **Sliding average fix**: The "best k of N" average on the comparison charts dropped the best races instead of the worst; it now drops the worst and weights each finish, so a down-weighted outlier no longer drags the line down.
- **Equalise**: With several allocated handicap columns, one click scales each to the same average as the ticked column, keeping its ratios.
- **Mobile menu**: On phones the page menu folds into a ☰ button that opens a full-width list of pages.
- **Head to head chart**: The elapsed time chart has its own Hide legend box, "From 0" now really zooms in on the races when unticked, and the best fit says which boat is faster and by how much.
- **Boat comparison**: Boats ticked in the handicap calculator are highlighted in the BCF chart (hover highlighting is paused while any are ticked), and "Common races only" is now a selector: All races, Common races, or Common divisions (only results where the boats raced in the same division, as the head to head chart uses).
- **Series Optimise is fairer**: It now tilts all handicaps together (each scaled by a power of itself) to level the trend slope, so boats with the same handicap change by the same percentage however many races they sailed, and the order of handicaps is kept.
- **Use Best Fit fix**: On the comparison pages, Use Best Fit now rebalances just the two boats to the best-fit ratio while keeping their average handicap, instead of resetting both from their PFs.
- **Compare Optimise uses the zoom**: Optimise on the boat comparison page levels the lines over the period zoomed to on the chart (or the whole chart), judging every boat at the same date; boats with too few results in that period keep their handicap.
- **ORC from certificates only**: ORC handicaps read from race results are no longer used — clubs score ORC with many different ratings (time on time, triple number, GPH), which spread the RFs of identical boats. Only official ORC certificates are used.
- **One design**: A design can be ticked as one design; every boat of it then shares the design's RF, built from its boats' certificates (PFs still differ boat to boat).
- **Modified boats**: A boat can be ticked as modified from its design; it keeps its own RF and does not count toward the design's. "… Modified" designs have been folded into their base design with the boat ticked modified.
- **Design flags shown**: (OD), (no spin) and (modified) now follow design names wherever they are shown.
- **Performance profile**: Diversity and Consistency have swapped spokes, so racing often counts for more against many opponents, and inconsistency counts for less when it is only in disorderly races.
- **Results spoke**: The performance profile has a sixth spoke, Results — the boat's average finishing place on PF-corrected time in its divisions, independent of how often it races. It sits between Stability and Frequency.
- **Design profile**: Designs with at least 3 active boats have a design profile (DP) — Quantity, Ubiquity (effective number of clubs), Frequency, Results (the boats' PF-corrected placings), Consistency (how closely the boats' PFs match the design RF) and Certified (share with a recent certificate). Shown on the designs page (a DP column, and a chart when a design is clicked) and in a popup when hovering a design elsewhere.
- **Boat comparison defaults**: "All available" ticked, PF and RF lines off, and an average trend.
- **Clubs**: The no-current-feed ⚠ now only trusts races from a feed the club still has configured.

### August 2026

- This site has switched to an opt-in model for the collection of race data from SailSys.
  Please send a message if you wish your sailsys data to be included in this analysis.
- Per event Topyacht imports added

### July 2026

- Automatic collection of race data from sailsys has been suspended.

### June 2026

- removed the fetch handicap feature.
- a new [video](https://youtu.be/rXwKmlnDd8Y) of using this tool to set the MYC handicaps for the winter series.

### May 2026

- **Dubious Data**: Allow users to flag dubious data.
- **Boat's club**: Allow multiple clubs to be selected and edited.
- **Back Calculated Factor charts**: Updated by selected factor set.
- **Elapsed Time Comparison**: Added to handicap calculator.
- **PF Optimizer Invariants**: Many improvements to PF optimizer.
- **Handicap Calculator Enhancements**: Better handling of mixed fleets. More sort and navigation options.
- **JSON View**: Re-added JSON view of items.

### April 2026

- **Design Comparison Analysis**: New graphical comparison tools for analyzing performance across boat designs.
- **Series Performance Tracking**: Enhanced series-level metrics with improved dispersion calculations.

### March 2026

- **Initial Release**: Comprehensive Australian keelboat racing performance analysis database with:
    - Reference Factor (RF) calculations
    - Performance Factor (PF) optimization
    - Performance Profile (PP) ratings
    - Multi-source data integration (AMS, BWPS, ORC, SailSys, TopYacht)

## Known Issues

- Poor measurement certificate analysis
- Unknown unknowns - feedback wanted!!!

## Coming Soon

- Rename Reference Factor to Design Factor???
- Improve drill down of single boats finishes.
- Alternate optimisation algorithms
