"use strict";

// Rendering check for the Video page "Results" dropdown: the number of
// rendered result rows must match the dropdown, with the top matches across all
// sources winning the visible slots.

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const APP_JS = path.join(__dirname, "..", "app.js");

class FakeClassList {
  constructor(element) {
    this.element = element;
  }

  get values() {
    return String(this.element.attributes.class || "").split(/\s+/).filter(Boolean);
  }

  add(...names) {
    const values = new Set(this.values);
    names.forEach((name) => values.add(name));
    this.element.attributes.class = Array.from(values).join(" ");
  }

  remove(...names) {
    const values = new Set(this.values);
    names.forEach((name) => values.delete(name));
    this.element.attributes.class = Array.from(values).join(" ");
  }

  contains(name) {
    return this.values.includes(name);
  }
}

function matchesSelector(element, selector) {
  const parts = selector.match(/^([a-zA-Z]*)|\.([\w-]+)|\[([\w-]+)=?"?([^\]"]*)"?\]/g) || [];
  return parts.every((part) => {
    if (part.startsWith(".")) {
      return element.classList.contains(part.slice(1));
    }
    if (part.startsWith("[")) {
      const [, name, value] = part.match(/\[([\w-]+)=?"?([^\]"]*)"?\]/);
      const actual = element.dataset[name] !== undefined ? element.dataset[name] : element.attributes[name];
      return String(actual) === value;
    }
    return element.tagName === part.toUpperCase();
  });
}

class FakeElement {
  constructor(tagName) {
    this.tagName = String(tagName).toUpperCase();
    this.children = [];
    this.parentElement = null;
    this.attributes = {};
    this.dataset = {};
    this.classList = new FakeClassList(this);
    this._text = "";
    this.disabled = false;
  }

  get className() {
    return this.attributes.class || "";
  }

  set className(value) {
    this.attributes.class = value;
  }

  get textContent() {
    return this._text;
  }

  set textContent(value) {
    this._text = String(value);
    this.children = [];
  }

  set innerHTML(value) {
    this._text = String(value);
  }

  get innerHTML() {
    return this._text;
  }

  appendChild(child) {
    child.parentElement = this;
    this.children.push(child);
    return child;
  }

  remove() {
    if (!this.parentElement) return;
    const index = this.parentElement.children.indexOf(this);
    if (index >= 0) this.parentElement.children.splice(index, 1);
    this.parentElement = null;
  }

  setAttribute(name, value) {
    this.attributes[name] = value;
  }

  getAttribute(name) {
    return this.attributes[name];
  }

  addEventListener() {}

  descendants() {
    const out = [];
    this.children.forEach((child) => {
      out.push(child);
      out.push(...child.descendants());
    });
    return out;
  }

  querySelector(selector) {
    return this.descendants().find((node) => matchesSelector(node, selector)) || null;
  }

  querySelectorAll(selector) {
    return this.descendants().filter((node) => matchesSelector(node, selector));
  }
}

function loadApp() {
  const document = {
    createElement: (tag) => new FakeElement(tag),
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener() {},
    body: new FakeElement("body"),
  };
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
    CSS: { escape: (value) => String(value) },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.self = sandbox;
  sandbox.top = sandbox;
  sandbox.document = document;
  sandbox.navigator = { userAgent: "node" };
  sandbox.location = { href: "http://localhost/", search: "", hash: "", pathname: "/" };
  sandbox.history = { pushState() {}, replaceState() {} };
  sandbox.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
  sandbox.getComputedStyle = () => ({ getPropertyValue: () => "" });

  const context = vm.createContext(sandbox);
  vm.runInContext(fs.readFileSync(APP_JS, "utf8"), context, { filename: "app.js" });

  const read = (expression) => vm.runInContext(expression, context);
  return {
    state: read("state"),
    FakeElement,
    renderHomeCandidatesIntoContainer: read("renderHomeCandidatesIntoContainer"),
  };
}

const app = loadApp();

function candidate(id, source, score) {
  return { id, source, final_score: score, title: `${source} ${id}`, url: `https://example.test/${id}` };
}

function renderWithLimit(limit, candidates) {
  app.state.homeVideoResultsLimit = limit;
  app.state.homeVideoSort = "best_match";
  app.state.homeSearchMode = "searchOnly";
  const item = { id: "item-1", media_type: "video" };
  app.state.homeCandidatesByItem = { "item-1": { item, candidates } };
  const container = new app.FakeElement("div");
  app.renderHomeCandidatesIntoContainer(item, container, candidates);
  return container;
}

test("rendered rows match the Results dropdown limit", () => {
  const candidates = [
    ...Array.from({ length: 20 }, (_, i) => candidate(`yt-${i}`, "youtube", 0.9 - i / 100)),
    ...Array.from({ length: 4 }, (_, i) => candidate(`rb-${i}`, "rumble", 0.8 - i / 100)),
  ];
  const container = renderWithLimit(24, candidates);
  assert.equal(container.children.length, 24);
  assert.equal(
    container.children.every((row) => row.classList.contains("home-candidate-row")),
    true
  );
});

test("rendered rows are the top matches across sources", () => {
  const candidates = [
    candidate("yt-low", "youtube", 0.1),
    candidate("rb-high", "rumble", 0.95),
    candidate("yt-high", "youtube", 0.9),
  ];
  const container = renderWithLimit(2, candidates);
  assert.equal(container.children.length, 2);
  const ids = container.children.map((row) => row.dataset.candidateId);
  assert.deepEqual(ids, ["rb-high", "yt-high"]);
});

test("All results renders every candidate", () => {
  const candidates = Array.from({ length: 30 }, (_, i) => candidate(`yt-${i}`, "youtube", 0.9 - i / 100));
  const container = renderWithLimit(0, candidates);
  assert.equal(container.children.length, 30);
});
