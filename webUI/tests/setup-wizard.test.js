"use strict";

// Smoke tests for the guided setup wizard's pure functions.
//
// The wizard is plain browser JavaScript with no build step, so the test
// loads webUI/app.js into a vm context with a permissive DOM stub and then
// exercises the functions that decide what the wizard shows and whether a
// step may be left. Those functions are the ones that broke silently before:
// a ReferenceError inside validateSetupWizardStep used to abort the whole
// "Next" handler with no visible message.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const APP_JS = path.join(__dirname, "..", "app.js");

// A permissive stub: any property access returns another callable stub, so a
// new DOM API used by app.js does not break the load.
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
    buildSetupWizardDraft: read("buildSetupWizardDraft"),
    getSetupWizardSteps: read("getSetupWizardSteps"),
    getSetupWizardStepState: read("getSetupWizardStepState"),
    validateSetupWizardStep: read("validateSetupWizardStep"),
    buildSetupWizardSummaryRows: read("buildSetupWizardSummaryRows"),
  };
}

const app = loadApp();

const BASE_STEP_IDS = [
  "paths",
  "arr-mode",
  "tmdb",
  "vpn",
  "youtube",
  "telegram",
  "jellyfin",
  "preflight",
  "review",
];

function draftWith(overrides) {
  return { ...app.buildSetupWizardDraft(), ...overrides };
}

function useDraft(draft) {
  app.state.setupWizard = { stepIndex: 0, draft };
  return draft;
}

test("app.js loads and exposes the wizard functions", () => {
  assert.equal(typeof app.getSetupWizardSteps, "function");
  assert.equal(typeof app.validateSetupWizardStep, "function");
  assert.equal(typeof app.getSetupWizardStepState, "function");
  assert.equal(typeof app.buildSetupWizardSummaryRows, "function");
});

test("the base wizard flow is paths, arr-mode, then the optional services", () => {
  useDraft(draftWith({ arr_setup_mode: "none" }));
  const steps = app.getSetupWizardSteps();
  assert.deepEqual(
    Array.from(steps, (step) => step.id),
    BASE_STEP_IDS,
  );
  assert.equal(steps[0].required, true);
  assert.equal(steps[steps.length - 1].id, "review");
});

test("the managed path adds the feature and control steps plus key discovery", () => {
  useDraft(draftWith({ arr_setup_mode: "managed" }));
  const ids = Array.from(app.getSetupWizardSteps(), (step) => step.id);
  assert.ok(ids.includes("arr-managed-features"));
  assert.ok(ids.includes("arr-managed-control"));
  assert.ok(ids.includes("key-discovery"));
  assert.ok(!ids.includes("arr-existing-services"));
});

test("the existing path adds the service picker instead of the managed steps", () => {
  useDraft(draftWith({ arr_setup_mode: "existing" }));
  const ids = Array.from(app.getSetupWizardSteps(), (step) => step.id);
  assert.ok(ids.includes("arr-existing-services"));
  assert.ok(ids.includes("key-discovery"));
  assert.ok(!ids.includes("arr-managed-features"));
});

test("every step answers a validation message and a status without throwing", () => {
  const modes = ["none", "managed", "existing"];
  for (const mode of modes) {
    const draft = useDraft(draftWith({ arr_setup_mode: mode }));
    for (const step of app.getSetupWizardSteps()) {
      const message = app.validateSetupWizardStep(step, draft);
      assert.ok(
        message === null || typeof message === "string",
        `validateSetupWizardStep(${step.id}, ${mode}) returned ${typeof message}`,
      );
      const status = app.getSetupWizardStepState(step, draft);
      assert.ok(
        ["pending", "success", "warning", "error", "skipped"].includes(status.tone),
        `getSetupWizardStepState(${step.id}, ${mode}) returned tone ${status.tone}`,
      );
      assert.equal(typeof status.label, "string");
      assert.equal(typeof status.message, "string");
    }
  }
});

test("an existing service without an API key asks for the key", () => {
  const draft = draftWith({
    arr_setup_mode: "existing",
    existing_radarr_enabled: true,
    existing_radarr_base_url: "http://localhost:7878",
    existing_radarr_api_key: "",
    key_discovery_enabled: false,
  });
  const step = { id: "arr-existing-services" };
  assert.equal(
    app.validateSetupWizardStep(step, draft),
    "Add your Radarr API key to continue, or enable key discovery for Radarr.",
  );
});

test("key discovery in scope satisfies a missing API key", () => {
  const draft = draftWith({
    arr_setup_mode: "existing",
    existing_radarr_enabled: true,
    existing_radarr_base_url: "http://localhost:7878",
    existing_radarr_api_key: "",
    key_discovery_enabled: true,
    key_discovery_scope_radarr: true,
  });
  const step = { id: "arr-existing-services" };
  assert.equal(app.validateSetupWizardStep(step, draft), null);
});

test("a service address that is not http(s) is rejected", () => {
  const draft = draftWith({
    arr_setup_mode: "existing",
    existing_radarr_enabled: true,
    existing_radarr_base_url: "localhost:7878",
    existing_radarr_api_key: "key",
  });
  const step = { id: "arr-existing-services" };
  assert.equal(
    app.validateSetupWizardStep(step, draft),
    "Radarr app address must start with http:// or https://.",
  );
});

test("a required path is reported by name", () => {
  const draft = draftWith({ media_root: "" });
  const step = { id: "paths" };
  assert.equal(app.validateSetupWizardStep(step, draft), "Media folder is required before moving on.");
});

test("an unanswered yes/no question blocks the step", () => {
  const draft = draftWith({ wants_tmdb: null });
  const step = { id: "tmdb" };
  assert.equal(app.validateSetupWizardStep(step, draft), "Choose Yes or No before moving on.");
});

test("a skipped optional service reads as skipped rather than pending", () => {
  const draft = draftWith({ arr_setup_mode: "none", enable_jellyfin: false });
  const step = { id: "jellyfin" };
  assert.equal(app.getSetupWizardStepState(step, draft).tone, "skipped");
  assert.equal(app.validateSetupWizardStep(step, draft), null);
});

test("the Next handler validates inside the try block", () => {
  const source = fs.readFileSync(APP_JS, "utf8");
  const handlerStart = source.indexOf("if (navButton) {");
  assert.ok(handlerStart > -1, "the wizard navigation handler was not found");
  const handlerEnd = source.indexOf("const choiceButton", handlerStart);
  const handler = source.slice(handlerStart, handlerEnd);
  const tryIndex = handler.indexOf("try {");
  const validateIndex = handler.indexOf("validateSetupWizardStep(currentStep)");
  assert.ok(tryIndex > -1, "the wizard navigation handler has no try block");
  assert.ok(validateIndex > -1, "the wizard navigation handler does not validate the current step");
  assert.ok(
    tryIndex < validateIndex,
    "validation runs outside the try block, so a throw leaves the Next button dead",
  );
});

test("the summary lists the mode, the services and the file roots", () => {
  const draft = useDraft(
    draftWith({
      arr_setup_mode: "existing",
      existing_radarr_enabled: true,
      media_root: "/media",
      movies_root: "/media/movies",
      tv_root: "/media/tv",
      downloads_root: "/downloads",
      books_root: "/media/books",
    }),
  );
  const rows = app.buildSetupWizardSummaryRows();
  const asObject = Object.fromEntries(rows);
  assert.equal(asObject["Mode"], "Existing services");
  assert.equal(asObject["Services"], "Radarr");
  assert.equal(asObject["Media root"], "/media");
  assert.equal(asObject["Downloads root"], "/downloads");
});
