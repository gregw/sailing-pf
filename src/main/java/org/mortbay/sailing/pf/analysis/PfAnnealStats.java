package org.mortbay.sailing.pf.analysis;

import java.util.List;

/**
 * How a {@link PfAnnealer} run went: the schedule it followed and why it stopped.
 */
public record PfAnnealStats(
    int stages,                     // temperatures visited
    long moves,                     // moves tried
    long acceptedMoves,             // moves accepted
    int boatVariantsActive,         // boat-variants the annealer could move
    int boatVariantsPerMove,        // boat-variants perturbed in each move
    double initialTemperature,
    double finalTemperature,
    int bestStage,                  // stage whose PFs were kept (lowest PfScore energy); 0 = the RF start
    String stopReason,              // "cooled", "frozen" (acceptance < 1% for 3 stages) or "max stages"
    List<Double> acceptanceTrace,   // fraction of moves accepted at each stage
    List<Double> energyTrace,       // PfScore energy at the end of each stage
    long elapsedMillis,
    PfAnnealConfig config
) {}
