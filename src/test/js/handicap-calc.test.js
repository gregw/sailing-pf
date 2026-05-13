'use strict';

// Node-runnable unit tests for the variant-action decision in handicap-calc.js.
// Run via `node --test src/test/js/handicap-calc.test.js` (wired into `mvn verify`).

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const HandicapCalc = require(path.join(__dirname, '..', '..', 'main', 'resources', 'content', 'handicap-calc.js'));
const decide = HandicapCalc.decideVariantAction;

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
