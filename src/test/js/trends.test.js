'use strict';

// Node-runnable unit tests for trends.js. Run via `node --test src/test/js/trends.test.js`
// (wired into `mvn test`).

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const {slidingAverage} = require(path.join(__dirname, '..', '..', 'main', 'resources', 'content', 'trends.js'));

const day = i => `2025-01-${String(i + 1).padStart(2, '0')}`;
const entries = (values, weights) =>
    values.map((v, i) => ({date: day(i), backCalcFactor: v, weight: weights ? weights[i] : 1}));
const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-9, `${a} ≠ ${b}`);

test('slidingAverage: plain mean of the last n with equal weights and no drops', () => {
    const s = slidingAverage(entries([1.0, 1.1, 1.2, 1.3]), 2, 0, null);
    assert.deepEqual(s.x, [day(0), day(1), day(2), day(3)]);
    near(s.y[0], 1.0);
    near(s.y[1], 1.05);
    near(s.y[3], 1.25);
});

test('slidingAverage: drops the lowest factors (the worst races), not the highest', () => {
    // Best 2 of 3: the window [1.0, 1.2, 0.6] keeps 1.2 and 1.0.
    const s = slidingAverage(entries([1.0, 1.2, 0.6]), 3, 1, null);
    near(s.y[2], 1.1);
});

test('slidingAverage: weighted by each result\'s weight', () => {
    const s = slidingAverage(entries([1.0, 1.3], [0.75, 0.25]), 2, 0, null);
    near(s.y[1], 1.075);
});

test('slidingAverage: a down-weighted outlier barely moves the line (the Mondo case)', () => {
    // Steady ~1.03 then one near-zero-weight 0.655 — before the fix it pulled a best-7-of-8
    // line down ~0.056 and held it there for 8 finishes.
    const values = [1.03, 1.02, 1.04, 1.03, 1.05, 1.02, 1.04, 1.03, 0.655, 1.03];
    const weights = values.map(v => v < 0.7 ? 0.005 : 1);
    const s = slidingAverage(entries(values, weights), 8, 1, 1.03);
    s.y.forEach(y => assert.ok(y > 1.02, `line dipped to ${y}`));
    // With full weight and no drops it would still show, as it should.
    const unweighted = slidingAverage(entries(values), 8, 0, 1.03);
    assert.ok(unweighted.y[8] < 0.99);
});

test('slidingAverage: all-zero weights fall back to the plain mean', () => {
    const s = slidingAverage(entries([1.0, 1.2], [0, 0]), 2, 0, null);
    near(s.y[1], 1.1);
});

test('slidingAverage: the seed fills the window, weighted as a typical result', () => {
    // Seed 1.0 × 2 virtual entries at the boats' mean weight (0.5) beside the first result.
    const s = slidingAverage(entries([1.3, 1.3], [0.5, 0.5]), 3, 0, 1.0);
    near(s.y[0], (1.0 + 1.0 + 1.3) / 3);
    near(s.y[1], (1.0 + 1.3 + 1.3) / 3);
});

test('slidingAverage: null with fewer than two entries', () => {
    assert.equal(slidingAverage(entries([1.0]), 8, 0, 1.0), null);
});
