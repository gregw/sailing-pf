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
