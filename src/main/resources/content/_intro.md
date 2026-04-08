The HPF project provides an online database of elapsed times and time corrected factors
from Australian sail racing, with statistical analysis and graphical presentation. It is primarily
for keel boats, but OTB and multihulls may also be included.

A **HPF (Historical Performance Factor)** is a back-calculated time correction factor a boat
would have needed, averaged across its recorded racing history, to have been equal-time with a
hypothetical 1.000 reference boat. The hypothetical reference boat is a boat is created by analysis
of measurement handicaps and performance against boats with measurement handicaps. 

The **HPF** is a historical performance measure — not a handicap system. A Handicap can also be a
time correction factor, but one which often includes policy decisions of how to age handicaps race to race
and possibly proprietary algorithms to: normalise values: clip and/or exclude results and sometimes have
punitive intent for good performances. HPF has none of these. 

The HPF calculation uses measurement based handicaps to establish Reference Factors (RFs) for most boats. 
Boats are assigned a RF based on either their own measurement handicaps or their performance against boats 
already assigned an RF.  These RFs are then used to calculate a reference time for every race for a 
hypothetical 1.000 reference boat.  Boats are then assigned an **HPF** by an optimisation process that 
compares their actual elapsed times with the reference 
for each race.

The result is each boat is assigned a single number time correction factor for spinnaker,
non-spinnaker and 2-handed.  There are no separate factors for inshore, offshore, windward-leeward, 
strong/medium/soft conditions etc.  Note that **the Handicaps/Factors are ALWAYS WRONG**, because if 
they were correct then every race would end in a tie. At best, they are an approximate model of a 
possible reality — a poor substitute for One Design racing.

Measurement handicap for ORC and AMS data is taken from there corresponding official websites. All 
measurement handicaps can also be loaded from public results pages.   Elapsed times are taken from the
public results pages of clubs which may be in club specific (BWPS, RSHYC), TopYacht or Sailsys formats.
No personal details (owners, skippers etc.) are loaded, nor are any club handicaps considered.
The HPF values and other analytical outputs are published under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

Note also that this project is in early development and almost certainly contains errors, omissions and
accidental inclusions in both the data loaded and the analysis performed. Feedback, comments, requests, 
and contributions are welcome via [GitHub](https://github.com/gregw/sailing-hpf).
