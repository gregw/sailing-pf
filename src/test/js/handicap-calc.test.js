'use strict';

// Node-runnable unit tests for the variant-action decision in handicap-calc.js.
// Run via `node --test src/test/js/handicap-calc.test.js` (wired into `mvn verify`).

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const HandicapCalc = require(path.join(__dirname, '..', '..', 'main', 'resources', 'content', 'handicap-calc.js'));
const decide = HandicapCalc.decideVariantAction;
const formatStatus = HandicapCalc.formatStatus;

test('filter mode skips when row variant disagrees with existing boat variant', () => {
    // The exact bug the user reported: existing boat is 'spin', a fetched row claims
    // 'nonSpin'. Under default 'filter' mode the row must NOT load, and the boat's
    // variant must stay 'spin'.
    assert.equal(decide('filter', 'nonSpin', 'spin'), 'skip');
    assert.equal(decide('filter', 'twoHanded', 'spin'), 'skip');
    assert.equal(decide('filter', 'spin', 'nonSpin'), 'skip');
});

test('filter mode applies when row variant matches boat variant', () => {
    assert.equal(decide('filter', 'spin', 'spin'), 'apply');
    assert.equal(decide('filter', 'nonSpin', 'nonSpin'), 'apply');
    assert.equal(decide('filter', 'twoHanded', 'twoHanded'), 'apply');
});

test('filter mode applies when row carries no variant info', () => {
    // Source didn't tell us the variant — fall through to the boat's existing one.
    assert.equal(decide('filter', null, 'spin'), 'apply');
    assert.equal(decide('filter', undefined, 'nonSpin'), 'apply');
    assert.equal(decide('filter', '', 'twoHanded'), 'apply');
});

test('set mode overrides boat variant when row variant disagrees', () => {
    assert.equal(decide('set', 'nonSpin', 'spin'), 'override');
    assert.equal(decide('set', 'twoHanded', 'spin'), 'override');
});

test('set mode applies (no override) when variants already match', () => {
    assert.equal(decide('set', 'spin', 'spin'), 'apply');
    assert.equal(decide('set', 'nonSpin', 'nonSpin'), 'apply');
});

test('set mode applies (no override) when row has no variant', () => {
    assert.equal(decide('set', null, 'spin'), 'apply');
    assert.equal(decide('set', undefined, 'spin'), 'apply');
});

test('ignore mode always applies regardless of variant disagreement', () => {
    assert.equal(decide('ignore', 'nonSpin', 'spin'), 'apply');
    assert.equal(decide('ignore', 'spin', 'twoHanded'), 'apply');
    assert.equal(decide('ignore', null, 'spin'), 'apply');
});

test('unknown mode behaves like ignore (defensive default)', () => {
    assert.equal(decide('bogus', 'nonSpin', 'spin'), 'apply');
    assert.equal(decide(undefined, 'nonSpin', 'spin'), 'apply');
});

// formatStatus drives the fetch/load status line. The wording shows three independent
// numbers — entries returned, boats matched (or added), handicaps applied — so the user
// can tell when the source is missing handicaps from when the calc failed to match.
test('formatStatus: empty array reports zero entries', () => {
    const r = formatStatus([], {matched: 0, matchedBoats: 0}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 0 entries');
    assert.equal(r.ok, false);
});

test('formatStatus: match-path with all handicaps applied → green', () => {
    const rows = Array.from({length: 24}, () => ({boatId: 'b', handicap: 1.0}));
    const r = formatStatus(rows, {matched: 24, matchedBoats: 24}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 24 entries — matched 24 boats, applied 24 handicaps');
    assert.equal(r.ok, true);
});

test('formatStatus: match-path with boats matched but source has no handicaps (Scenario 1)', () => {
    // The bug the user reported: rows from race 35355 match all 25 boats, but every
    // row has handicap=null because SailSys hasn't allocated handicaps yet. Old wording
    // collapsed this to "matched 0 boats"; new wording surfaces both counts.
    const rows = Array.from({length: 25}, () => ({boatId: 'b', handicap: null}));
    const r = formatStatus(rows, {matched: 0, matchedBoats: 25}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 25 entries — matched 25 boats, applied 0 handicaps (source has no allocated handicaps)');
    assert.equal(r.ok, false);
});

test('formatStatus: match-path with partial handicap coverage → red with unmatched note', () => {
    // 3 rows have handicaps, only 2 of them matched a boat (the third row's boatId
    // didn't appear in the table). Surface the gap explicitly.
    const rows = [
        {boatId: 'a', handicap: 1.0},
        {boatId: 'b', handicap: 1.0},
        {boatId: 'c', handicap: 1.0},
    ];
    const r = formatStatus(rows, {matched: 2, matchedBoats: 2}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 3 entries — matched 2 boats, applied 2 handicaps (1 of 3 unmatched)');
    assert.equal(r.ok, true);
});

test('formatStatus: match-path with no boats matched and source has handicaps', () => {
    const rows = Array.from({length: 5}, () => ({boatId: 'b', handicap: 1.0}));
    const r = formatStatus(rows, {matched: 0, matchedBoats: 0}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 5 entries — matched 0 boats, applied 0 handicaps (5 of 5 unmatched)');
    assert.equal(r.ok, false);
});

test('formatStatus: add-boats path with handicaps in source (Scenario 2 happy)', () => {
    const rows = Array.from({length: 24}, () => ({boatId: 'b', handicap: 1.0}));
    const r = formatStatus(rows, {matched: 24}, true, 'Fetched');
    assert.equal(r.msg, 'Fetched 24 entries — added 24 boats, 24 handicaps in source');
    assert.equal(r.ok, true);
});

test('formatStatus: add-boats path with no handicaps in source (Scenario 2 sad)', () => {
    const rows = Array.from({length: 25}, () => ({boatId: 'b', handicap: null}));
    const r = formatStatus(rows, {matched: 25}, true, 'Fetched');
    assert.equal(r.msg, 'Fetched 25 entries — added 25 boats, 0 handicaps in source');
    assert.equal(r.ok, false);
});

test('formatStatus: singular pluralisation', () => {
    const r = formatStatus([{boatId: 'a', handicap: 1.0}], {matched: 1, matchedBoats: 1}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 1 entry — matched 1 boat, applied 1 handicap');
    assert.equal(r.ok, true);
});

test('formatStatus: leadVerb threads through (file-load uses "Loaded")', () => {
    const rows = Array.from({length: 25}, () => ({boatId: 'b', handicap: null}));
    const r = formatStatus(rows, {matched: 0, matchedBoats: 25}, false, 'Loaded');
    assert.equal(r.msg, 'Loaded 25 entries — matched 25 boats, applied 0 handicaps (source has no allocated handicaps)');
});

test('formatStatus: match-path falls back to matched when matchedBoats omitted (defensive)', () => {
    // Older callers may pass {matched} without matchedBoats; treat them as equal.
    const rows = Array.from({length: 5}, () => ({boatId: 'b', handicap: 1.0}));
    const r = formatStatus(rows, {matched: 5}, false, 'Fetched');
    assert.equal(r.msg, 'Fetched 5 entries — matched 5 boats, applied 5 handicaps');
    assert.equal(r.ok, true);
});

// normaliseVariant maps hand-written variant spellings to the calculator's three values.
const normaliseVariant = HandicapCalc.normaliseVariant;

test('normaliseVariant: accepts common spellings', () => {
    for (const v of ['nonspin', 'nonSpin', 'NS', 'non-spin', 'Non Spinnaker'])
        assert.equal(normaliseVariant(v), 'nonSpin', v);
    for (const v of ['spin', 'Spin', 'SPINNAKER'])
        assert.equal(normaliseVariant(v), 'spin', v);
    for (const v of ['2h', '2HD', 'two-handed', 'twoHanded', 'Double Handed', 'DH', 'shorthanded'])
        assert.equal(normaliseVariant(v), 'twoHanded', v);
});

test('normaliseVariant: missing or unknown is undefined', () => {
    assert.equal(normaliseVariant(undefined), undefined);
    assert.equal(normaliseVariant(null), undefined);
    assert.equal(normaliseVariant('genoa'), undefined);
});

test('formatStatus: add-boats path reports boats not found', () => {
    const rows = [{sailno: 'R350', handicap: 1.0}, {sailno: 'X1', handicap: 1.1}];
    const r = formatStatus(rows, {matched: 1, notFound: ['X1 Ghost']}, true, 'Loaded');
    assert.match(r.msg, /added 1 boat/);
    assert.match(r.msg, /1 not found: X1 Ghost/);
    assert.equal(r.ok, false);
});

// Smallest-change optimisers behind "Flatten slope" (series) and "Level lines" (compare).
const {minChangeToZeroLinear, minChangeToEqualLevels} = HandicapCalc;
const relChange = (h0, h) => [...h0.keys()].reduce((a, b) => a + (h.get(b) / h0.get(b) - 1) ** 2, 0);

test('minChangeToZeroLinear: makes the linear function zero', () => {
    const h0 = new Map([['a', 0.9], ['b', 1.0], ['c', 1.1], ['x', 1.3]]);
    const c = new Map([['a', -2], ['b', 0.5], ['c', 3]]);   // 'x' does not affect it
    const h = minChangeToZeroLinear(h0, c);
    const s = [...c].reduce((a, [b, cb]) => a + cb * h.get(b), 0);
    assert.ok(Math.abs(s) < 1e-12, `S = ${s}`);
    assert.equal(h.get('x'), 1.3, 'uninvolved boat unchanged');
});

test('minChangeToZeroLinear: no other zeroing change is smaller', () => {
    const h0 = new Map([['a', 0.9], ['b', 1.0], ['c', 1.1]]);
    const c = new Map([['a', -2], ['b', 0.5], ['c', 3]]);
    const h = minChangeToZeroLinear(h0, c);
    const best = relChange(h0, h);
    // Move along the constraint surface: any direction v with Σ c·h0·v = 0 keeps S at 0.
    for (const [va, vb] of [[0.01, 0], [0, 0.01], [-0.02, 0.01]]) {
        const vc = -(c.get('a') * 0.9 * va + c.get('b') * 1.0 * vb) / (c.get('c') * 1.1);
        const alt = new Map([['a', h.get('a') + 0.9 * va], ['b', h.get('b') + 1.0 * vb],
            ['c', h.get('c') + 1.1 * vc]]);
        assert.ok(relChange(h0, alt) >= best - 1e-12);
    }
});

test('minChangeToZeroLinear: null when nothing affects it', () => {
    assert.equal(minChangeToZeroLinear(new Map([['a', 1]]), new Map()), null);
});

test('minChangeToEqualLevels: every line ends at the same level', () => {
    const h0 = new Map([['a', 0.9], ['b', 1.0], ['c', 1.1], ['x', 1.3]]);
    const levels = new Map([['a', 1.02], ['b', 0.97], ['c', 1.05]]);
    const h = minChangeToEqualLevels(h0, levels);
    // A boat's level scales as 1/handicap.
    const after = [...levels].map(([b, l]) => l * h0.get(b) / h.get(b));
    after.forEach(v => assert.ok(Math.abs(v - after[0]) < 1e-12, String(after)));
    assert.equal(h.get('x'), 1.3, 'boat without a line unchanged');
});

test('minChangeToEqualLevels: the common level is the smallest-change one', () => {
    const h0 = new Map([['a', 0.9], ['b', 1.0], ['c', 1.1]]);
    const levels = new Map([['a', 1.02], ['b', 0.97], ['c', 1.05]]);
    const h = minChangeToEqualLevels(h0, levels);
    const best = relChange(h0, h);
    for (const k of [0.99, 1.01]) {
        const alt = new Map([...h].map(([b, v]) => [b, v * k]));   // another common level
        assert.ok(relChange(h0, alt) >= best - 1e-12);
    }
});

// mergeEntryLists backs the calculator's "merge set into the one on its left" button.
const mergeEntryLists = HandicapCalc.mergeEntryLists;

test('mergeEntryLists: averages shared boats and keeps the rest', () => {
    const a = [{boatId: 'b1', sailno: 'AUS1', name: 'One', handicap: 1.0},
               {boatId: 'b2', sailno: 'AUS2', name: 'Two', handicap: 0.9}];
    const b = [{boatId: 'b1', sailno: 'AUS1', name: 'One', handicap: 1.1},
               {boatId: 'b3', sailno: 'AUS3', name: 'Three', handicap: 0.8}];
    const m = new Map(mergeEntryLists(a, b).map(e => [e.boatId, e.handicap]));
    assert.equal(m.get('b1'), 1.05);
    assert.equal(m.get('b2'), 0.9);
    assert.equal(m.get('b3'), 0.8);
    assert.equal(m.size, 3);
    assert.equal(a[0].handicap, 1.0, 'inputs not mutated');
});

test('mergeEntryLists: matches entries without boatId by sail number and name', () => {
    const m = mergeEntryLists([{sailno: 'AUS 12', name: 'Blue Streak', handicap: 0.95}],
                              [{sailno: '12', name: 'blue streak', handicap: 1.05}]);
    assert.equal(m.length, 1);
    assert.equal(m[0].handicap, 1.0);
});
