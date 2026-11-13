"use strict";

// Smoke tests for the Video page "Results" dropdown.
//
// The dropdown sets how many results are shown in total, not per source, so
// candidates from every source are ranked together and the best matches win
// regardless of which source returned them.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const APP_JS = path.join(__dirname, "..", "app.js");

function makeStub(label) {
  const target = function stub() {
    return makeStub(`${label}()`);
  };
  return new Proxy(target, {
    get(obj, prop) {
      if (prop === Symbol.toPrimitive) return () => "";
      if (prop === Symbol.iterator) return function* empty() {};
      if (prop === "then") return undefined;
      if (prop === "toString") return () => "";
      if (prop === "valueOf") return () => 0;
      if (prop === "length") return 0;
      if (!(prop in obj)) obj[prop] = makeStub(`${label}.${String(prop)}`);
      return obj[prop];
    },
    set(obj, prop, value) {
      obj[prop] = value;
      return true;
    },
    apply() {
      return makeStub(`${label}()`);
    },
  });
}

function loadApp() {
  const sandbox = {
    console,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    queueMicrotask,
    URL,
    URLSearchParams,
    TextEncoder,
    TextDecoder,
    fetch: () => Promise.resolve({ ok: true, status: 200, json: async () => ({}), text: async () => "" }),
    localStorage: { getItem: () => null, setItem() {}, removeItem() {}, clear() {} },
    MutationObserver: class {
      observe() {}
      disconnect() {}
      takeRecords() {
        return [];
      }
    },
    ResizeObserver: class {
      observe() {}
      disconnect() {}
    },
    IntersectionObserver: class {
      observe() {}
      disconnect() {}
    },
    addEventListener() {},
    removeEventListener() {},
    dispatchEvent() {},
    requestAnimationFrame: (callback) => setTimeout(callback, 0),
    cancelAnimationFrame() {},
    alert() {},
    confirm: () => false,
    prompt: () => null,
    open() {},
    scrollTo() {},
    innerWidth: 1280,
    innerHeight: 800,
    devicePixelRatio: 1,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.self = sandbox;
  sandbox.top = sandbox;
  sandbox.document = makeStub("document");
  sandbox.navigator = { userAgent: "node" };
  sandbox.location = { href: "http://localhost/", search: "", hash: "", pathname: "/" };
  sandbox.history = { pushState() {}, replaceState() {} };
  sandbox.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  sandbox.getComputedStyle = () => ({ getPropertyValue: () => "" });

  const context = vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(APP_JS, "utf8"), context, { filename: "app.js" });

  const read = (expression) => vm.runInContext(expression, context);
  return {
    context,
    state: read("state"),
    getHomeVisibleCandidateIdsByItem: read("getHomeVisibleCandidateIdsByItem"),
    getHomeVideoResultsLimitValue: read("getHomeVideoResultsLimitValue"),
    getHomeVideoCandidatePoolBudget: read("getHomeVideoCandidatePoolBudget"),
  };
}

const app = loadApp();

function candidate(id, source, score) {
  return { id, source, final_score: score, title: `${source} ${id}` };
}

function setPool(candidates) {
  app.state.homeVideoSort = "best_match";
  app.state.homeCandidatesByItem = {
    "item-1": { item: { id: "item-1" }, candidates },
  };
}

test("results limit caps the total number of visible results", () => {
  app.state.homeVideoResultsLimit = 24;
  setPool([
    ...Array.from({ length: 20 }, (_, i) => candidate(`yt-${i}`, "youtube", 0.9 - i / 100)),
    ...Array.from({ length: 4 }, (_, i) => candidate(`rb-${i}`, "rumble", 0.8 - i / 100)),
  ]);

  const visible = app.getHomeVisibleCandidateIdsByItem().get("item-1");
  assert.equal(visible.size, 24);
});

test("top matches are kept first regardless of source", () => {
  app.state.homeVideoResultsLimit = 3;
  setPool([
    candidate("yt-low", "youtube", 0.1),
    candidate("rb-high", "rumble", 0.95),
    candidate("yt-high", "youtube", 0.9),
    candidate("rb-mid", "rumble", 0.5),
  ]);

  const visible = app.getHomeVisibleCandidateIdsByItem().get("item-1");
  assert.equal(visible.size, 3);
  assert.equal(visible.has("rb-high"), true);
  assert.equal(visible.has("yt-high"), true);
  assert.equal(visible.has("yt-low"), false);
});

test("a single source can fill the whole limit", () => {
  app.state.homeVideoResultsLimit = 24;
  setPool(
    Array.from({ length: 24 }, (_, i) => candidate(`yt-${i}`, "youtube", 0.9 - i / 100))
  );

  const visible = app.getHomeVisibleCandidateIdsByItem().get("item-1");
  assert.equal(visible.size, 24);
});

test("All results keeps every candidate", () => {
  app.state.homeVideoResultsLimit = 0;
  setPool(
    Array.from({ length: 40 }, (_, i) => candidate(`yt-${i}`, "youtube", 0.9 - i / 100))
  );

  const visible = app.getHomeVisibleCandidateIdsByItem().get("item-1");
  assert.equal(visible.size, 40);
});

test("the per-source fetch budget follows the dropdown", () => {
  app.state.homeVideoResultsLimit = 24;
  assert.equal(app.getHomeVideoCandidatePoolBudget(), 24);
  app.state.homeVideoResultsLimit = 0;
  assert.equal(app.getHomeVideoCandidatePoolBudget(), 50);
});
