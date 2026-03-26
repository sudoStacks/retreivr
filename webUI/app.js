const state = {
  config: null,
  timers: {},
  pollingPaused: false,
  configDirty: false,
  inputFocused: false,
  suppressDirty: false,
  lastLogsText: null,
  lastHistoryKey: null,
  lastFilesKey: null,
  configNoticeTimer: null,
  configNoticeClearable: false,
  currentPage: "home",
  reviewItems: [],
  reviewSelectedIds: new Set(),
  reviewPreviewItemId: null,
  actionButtons: null,
  runtimeInfo: null,
  watcherStatus: null,
  searchSelectedRequestId: null,
  searchSelectedItemId: null,
  lastSearchRequestsKey: null,
  lastSearchItemsKey: null,
  lastSearchCandidatesKey: null,
  lastSearchQueueKey: null,
  searchRequestsSort: "desc",
  homeSearchRequestId: null,
  homeResultsTimer: null,
  homeSearchMode: "searchOnly",
  homeMediaMode: "video",
  homeMusicMode: false,
  homeMusicSearchSeq: 0,
  homeAlbumCandidatesRequestId: null,
  homeQueuedAlbumReleaseGroups: new Set(),
  homeAlbumCoverCache: {},
  homeArtistCoverCache: {},
  homeMusicRenderToken: 0,
  homeMusicResultMap: {},
  homeMusicCurrentView: null,
  homeMusicViewStack: [],
  homeRequestContext: {},
  homeBestScores: {},
  homeCandidateCache: {},
  homeCandidatesLoading: {},
  homeCandidateRefreshPending: {},
  homeCandidateData: {},
  homeSearchPollStart: null,
  homeResultPollInFlight: false,
  homeSearchControlsEnabled: true,
  pendingAdvancedRequestId: null,
  spotifyPlaylistStatus: {},
  homeNoCandidateStreaks: {},
  homeDestinationInvalid: false,
  homeLastDefaultDestination: "",
  homeLastDefaultVideoFormat: "",
  homeDirectPreview: null,
  homeDirectJob: null,
  homeDirectJobTimer: null,
  homeJobTimer: null,
  homeJobSnapshot: null,
  spotifyOauthConnected: false,
  spotifyConnectedNoticeShown: false,
  playlistImportJobId: null,
  playlistImportPollTimer: null,
  playlistImportInProgress: false,
  communityPublishStatusTimer: null,
  communityPublishStatus: null,
  settingsActiveSectionId: "settings-core",
  settingsLayoutObserver: null,
  logsStickToBottom: true,
};
const browserState = {
  open: false,
  root: "downloads",
  mode: "dir",
  ext: "",
  path: "",
  currentAbs: "",
  selected: "",
  target: null,
  renderToken: 0,
  limit: 500,
};
const oauthState = {
  open: false,
  sessionId: null,
  authUrl: "",
  account: "",
};
const previewState = {
  open: false,
  source: "",
  title: "",
  embedUrl: "",
};
const BROWSE_DEFAULTS = {
  configDir: "",
  libraryExportsRoot: "",
  mediaRoot: "",
  tokensDir: "",
  roots: {},
};
const MUSIC_EXPORT_TYPES = ["copy", "transcode"];
const VERSION_LATEST_URL = "/api/version/latest";
const VERSION_REFERENCE_PAGE = "https://github.com/sudoStacks/retreivr/pkgs/container/retreivr";
const RELEASE_CHECK_KEY = "yt_archiver_release_checked_at_v2";
const RELEASE_CACHE_KEY = "yt_archiver_release_cache_v2";
const RELEASE_VERSION_KEY = "yt_archiver_release_app_version_v2";
const HOME_MUSIC_MODE_KEY = "retreivr.home.music_mode";
const HOME_MUSIC_DEBUG_KEY = "retreivr.debug.music";
const HOME_SOURCE_PRIORITY_MAP = {
  auto: null,
  youtube: ["youtube"],
  youtube_music: ["youtube_music"],
  rumble: ["rumble"],
  archive_org: ["archive_org"],
  soundcloud: ["soundcloud"],
  bandcamp: ["bandcamp"],
};
const HOME_GENERIC_SOURCE_PRIORITY = [
  "youtube",
  "youtube_music",
  "rumble",
  "archive_org",
  "soundcloud",
  "bandcamp",
];
const HOME_VIDEO_SOURCE_PRIORITY = [
  "youtube",
  "youtube_music",
  "rumble",
  "archive_org",
  "soundcloud",
  "bandcamp",
];
const HOME_VIDEO_KEYWORDS = ["show", "podcast", "episode", "interview"];
const HOME_MUSIC_MODE_FORMATS = ["mp3", "m4a"];
const HOME_MUSIC_VIDEO_MODE_FORMATS = ["mp4", "mkv", "webm"];
const SETTINGS_ADVANCED_MODE_KEY = "retreivr_settings_advanced_mode";
const SETTINGS_ALL_SECTION_ID = "settings-all";
const HOME_PREVIEW_EMBED_BUILDERS = {
  youtube: buildYouTubeHomePreviewEmbedUrl,
  youtube_music: buildYouTubeHomePreviewEmbedUrl,
  rumble: buildRumbleHomePreviewEmbedUrl,
  archive_org: buildArchiveOrgHomePreviewEmbedUrl,
};
const HOME_STATUS_LABELS = {
  queued: "Searching",
  searching: "Searching",
  candidate_found: "Matched",
  selected: "Matched",
  enqueued: "Queued",
  claimed: "Downloading",
  downloading: "Downloading",
  postprocessing: "Downloading",
  completed: "Completed",
  failed: "Failed",
  skipped: "Failed",
};
const HOME_STATUS_CLASS_MAP = {
  queued: "searching",
  searching: "searching",
  candidate_found: "matched",
  selected: "matched",
  enqueued: "queued",
  claimed: "queued",
  downloading: "queued",
  postprocessing: "queued",
  completed: "matched",
  failed: "failed",
  skipped: "failed",
};
const HOME_FINAL_STATUSES = new Set(["completed", "completed_with_skips", "failed"]);
const HOME_RESULT_TIMEOUT_MS = 18000;
const HOME_RESULT_POLL_INTERVAL_MS = 300;
const HOME_NO_CANDIDATE_STREAK_LIMIT = 12;
const DIRECT_URL_PLAYLIST_ERROR =
  "Playlist URLs are not supported in Direct URL mode. Please add this playlist via Scheduler or Playlist settings.";
const HOME_PLAYLIST_SEARCH_ONLY_MESSAGE =
  "Playlist URL detected. Use Search & Download to enqueue all videos in the playlist.";

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));

function formatSourceLabel(source) {
  const key = String(source || "").trim().toLowerCase();
  const labelMap = {
    youtube: "YouTube",
    youtube_music: "YouTube Music",
    rumble: "Rumble",
    archive_org: "Archive.org",
    soundcloud: "SoundCloud",
    bandcamp: "Bandcamp",
  };
  if (labelMap[key]) return labelMap[key];
  return key
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

async function refreshHomeSourceOptions() {
  const panel = $("#home-source-panel");
  if (!panel) return;
  let sources = [];
  try {
    const data = await fetchJson("/api/search/sources");
    sources = Array.isArray(data?.sources) ? data.sources.map((v) => String(v || "").trim()).filter(Boolean) : [];
  } catch (_err) {
    sources = [];
  }
  if (!sources.length) {
    sources = ["youtube", "youtube_music", "rumble", "archive_org", "soundcloud", "bandcamp"];
  }
  if ((state.homeMediaMode || "video") === "video") {
    const videoExcluded = new Set(["youtube_music", "soundcloud", "bandcamp"]);
    sources = sources.filter((source) => !videoExcluded.has(String(source || "").trim().toLowerCase()));
  }
  const existingChecked = new Set(
    Array.from(panel.querySelectorAll("input[type=checkbox][data-source]:checked")).map((el) => el.dataset.source)
  );
  panel.textContent = "";
  sources.forEach((source, index) => {
    const label = document.createElement("label");
    label.className = "home-source-pill";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.dataset.source = source;
    input.checked = existingChecked.size ? existingChecked.has(source) : source === "youtube" || index === 0;
    const span = document.createElement("span");
    span.textContent = formatSourceLabel(source);
    label.appendChild(input);
    label.appendChild(span);
    panel.appendChild(label);
  });
  updateHomeSourceToggleLabel();
}

function normalizePageName(page) {
  if (!page) {
    return "home";
  }
  const cleanPage = String(page).split("?")[0] || page;
  if (cleanPage === "settings" || String(cleanPage).startsWith("settings-")) {
    return "config";
  }
  if (cleanPage === "search" || cleanPage === "advanced") {
    return "info";
  }
  if (["downloads", "history", "logs"].includes(cleanPage)) {
    return "status";
  }
  return cleanPage;
}

function setNotice(el, message, isError = false) {
  if (!el) return;
  el.textContent = message;
  el.style.color = isError ? "#ff7b7b" : "#59b0ff";
}

function toUserErrorMessage(err, fallback = "Unexpected error") {
  if (typeof err === "string" && err.trim()) {
    return err.trim();
  }
  if (err instanceof Error) {
    const msg = typeof err.message === "string" ? err.message.trim() : "";
    if (msg && msg !== "[object Object]") {
      return msg;
    }
  }
  if (err && typeof err === "object") {
    const detail = err.detail ?? err.error ?? err.message;
    if (typeof detail === "string" && detail.trim()) {
      return detail.trim();
    }
    if (detail && typeof detail === "object") {
      try {
        const encoded = JSON.stringify(detail);
        if (encoded && encoded !== "{}") {
          return encoded;
        }
      } catch (_err) {
        // Best effort only.
      }
    }
    try {
      const encoded = JSON.stringify(err);
      if (encoded && encoded !== "{}") {
        return encoded;
      }
    } catch (_err) {
      // Best effort only.
    }
  }
  try {
    const plain = String(err || "").trim();
    if (plain && plain !== "[object Object]") {
      return plain;
    }
  } catch (_err) {
    // Best effort only.
  }
  return fallback;
}

function runPrioritizedThumbnailJobs(jobs, renderToken, maxConcurrent = 2) {
  const tasks = Array.isArray(jobs) ? jobs.filter((job) => typeof job === "function") : [];
  if (!tasks.length) {
    return;
  }
  const concurrency = Math.max(1, Math.min(maxConcurrent, tasks.length));
  let nextIndex = 0;
  const worker = async () => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= tasks.length) {
        return;
      }
      try {
        await tasks[index](renderToken);
      } catch (_err) {
        // Thumbnail hydration is best-effort only.
      }
      // Small spacing helps avoid burst throttling on cover-art endpoints.
      await new Promise((resolve) => window.setTimeout(resolve, 120));
    }
  };
  for (let i = 0; i < concurrency; i += 1) {
    worker();
  }
}

function createMusicCardThumb(altText) {
  const shell = document.createElement("div");
  shell.className = "music-card-thumb-shell loading";

  const img = document.createElement("img");
  img.className = "music-card-thumb";
  img.alt = altText || "Artwork";
  img.loading = "lazy";

  const placeholder = document.createElement("div");
  placeholder.className = "music-card-thumb-placeholder";
  placeholder.textContent = "Loading artwork";

  img.addEventListener("load", () => {
    shell.classList.remove("loading", "no-art");
    shell.classList.add("loaded");
  });
  img.addEventListener("error", () => {
    shell.classList.remove("loading", "loaded");
    shell.classList.add("no-art");
    img.removeAttribute("src");
    placeholder.textContent = "No artwork";
  });

  const setLoading = () => {
    shell.classList.remove("loaded", "no-art");
    shell.classList.add("loading");
    placeholder.textContent = "Loading artwork";
  };

  const setNoArt = () => {
    shell.classList.remove("loading", "loaded");
    shell.classList.add("no-art");
    img.removeAttribute("src");
    placeholder.textContent = "No artwork";
  };

  const setImage = (url) => {
    const normalizedUrl = normalizeArtworkUrl(url);
    if (!normalizedUrl) {
      setNoArt();
      return;
    }
    setLoading();
    img.src = normalizedUrl;
  };

  shell.appendChild(img);
  shell.appendChild(placeholder);
  return { shell, setImage, setNoArt, setLoading };
}

function normalizeArtworkUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  if (raw.startsWith("//")) {
    return `https:${raw}`;
  }
  try {
    const parsed = new URL(raw);
    const host = (parsed.hostname || "").toLowerCase();
    if (
      parsed.protocol === "http:" &&
      (host === "coverartarchive.org" || host.endsWith(".coverartarchive.org"))
    ) {
      parsed.protocol = "https:";
      return parsed.toString();
    }
    return parsed.toString();
  } catch (_err) {
    return raw;
  }
}

function triggerClientDeliveryDownload(downloadUrl, filename = "") {
  const url = String(downloadUrl || "").trim();
  if (!url) {
    return false;
  }

  // Async callbacks (polling/fetch continuations) can lose trusted click context.
  // Hidden iframe delivery is more reliable across browsers for attachment responses.
  const frame = document.createElement("iframe");
  frame.style.display = "none";
  frame.src = url;
  document.body.appendChild(frame);
  window.setTimeout(() => {
    frame.remove();
  }, 60000);

  return true;
}

function setClientDeliveryNotice(messageEl, baseMessage, downloadUrl, filename = "") {
  setNotice(messageEl, baseMessage, false);
  if (!messageEl || !downloadUrl) {
    return;
  }
  const spacer = document.createTextNode(" ");
  const link = document.createElement("a");
  link.href = downloadUrl;
  link.rel = "noopener";
  link.target = "_blank";
  link.textContent = "Download file";
  if (filename) {
    link.download = filename;
  }
  messageEl.appendChild(spacer);
  messageEl.appendChild(link);
}

function clearConfigNotice() {
  if (!state.configNoticeClearable) {
    return;
  }
  const el = $("#config-message");
  if (el) {
    el.textContent = "";
  }
  state.configNoticeClearable = false;
  if (state.configNoticeTimer) {
    clearTimeout(state.configNoticeTimer);
    state.configNoticeTimer = null;
  }
}

function normalizeMusicExports(cfg) {
  const music = (cfg && typeof cfg.music === "object") ? cfg.music : {};
  const exports = Array.isArray(music.exports) ? music.exports : [];
  return exports
    .filter((entry) => entry && typeof entry === "object")
    .map((entry) => ({
      name: entry.name ?? "",
      enabled: !!entry.enabled,
      type: MUSIC_EXPORT_TYPES.includes(String(entry.type || "").trim().toLowerCase())
        ? String(entry.type || "").trim().toLowerCase()
        : "copy",
      path: entry.path ?? "",
      codec: entry.codec ?? "aac",
      bitrate: entry.bitrate ?? "256k",
    }));
}

function syncMusicExportRow(row) {
  if (!row) return;
  const enabled = !!row.querySelector(".music-export-enabled")?.checked;
  const type = String(row.querySelector(".music-export-type")?.value || "copy").trim().toLowerCase();
  const transcodeFields = row.querySelector(".music-export-transcode-fields");
  if (transcodeFields) {
    transcodeFields.style.display = enabled && type === "transcode" ? "" : "none";
  }
}

function addMusicExportRow(entry = {}) {
  const container = $("#music-exports-list");
  if (!container) return;

  const row = document.createElement("div");
  row.className = "group";
  row.dataset.musicExportRow = "1";
  row.innerHTML = `
    <div class="row" style="justify-content: space-between; align-items: center;">
      <div class="group-title" style="margin: 0;">Export Target</div>
      <button type="button" class="button ghost small music-export-remove">Remove</button>
    </div>
    <div class="grid two">
      <label class="field inline short">
        <span>Enable Export</span>
        <input class="music-export-enabled" type="checkbox">
      </label>
      <label class="field medium">
        <span>Export Name</span>
        <input class="music-export-name" type="text" placeholder="apple_music">
      </label>
      <label class="field medium">
        <span>Export Type</span>
        <select class="music-export-type">
          <option value="copy">Copy</option>
          <option value="transcode">Transcode</option>
        </select>
      </label>
      <label class="field full">
        <span>Export Path</span>
        <div class="row path-picker">
          <input class="music-export-path" type="text" placeholder="/Users/<user>/Music/Music/Media/Automatically Add to Music">
          <button type="button" class="button ghost small music-export-browse">Browse</button>
        </div>
      </label>
    </div>
    <div class="grid two music-export-transcode-fields">
      <label class="field short">
        <span>Codec</span>
        <input class="music-export-codec" type="text" placeholder="aac">
      </label>
      <label class="field short">
        <span>Bitrate</span>
        <input class="music-export-bitrate" type="text" placeholder="256k">
      </label>
    </div>
  `;
  container.appendChild(row);

  row.querySelector(".music-export-enabled").checked = !!entry.enabled;
  row.querySelector(".music-export-name").value = entry.name ?? "";
  row.querySelector(".music-export-type").value = MUSIC_EXPORT_TYPES.includes(String(entry.type || "").trim().toLowerCase())
    ? String(entry.type || "").trim().toLowerCase()
    : "copy";
  row.querySelector(".music-export-path").value = entry.path ?? "";
  row.querySelector(".music-export-path").dataset.browserAbsolute = "1";
  row.querySelector(".music-export-codec").value = entry.codec ?? "aac";
  row.querySelector(".music-export-bitrate").value = entry.bitrate ?? "256k";

  row.querySelector(".music-export-enabled").addEventListener("change", () => syncMusicExportRow(row));
  row.querySelector(".music-export-type").addEventListener("change", () => syncMusicExportRow(row));
  row.querySelector(".music-export-remove").addEventListener("click", () => row.remove());
  row.querySelector(".music-export-browse").addEventListener("click", () => {
    const input = row.querySelector(".music-export-path");
    const rootKey = preferredMusicLibraryBrowseRoot(input?.value || "");
    openBrowser(input, rootKey, "dir", "", resolveBrowseStart(rootKey, input?.value || ""));
  });
  syncMusicExportRow(row);
}

function setConfigNotice(message, isError = false, autoClear = false) {
  const el = $("#config-message");
  setNotice(el, message, isError);
  if (state.configNoticeTimer) {
    clearTimeout(state.configNoticeTimer);
    state.configNoticeTimer = null;
  }
  state.configNoticeClearable = !!autoClear;
  if (autoClear) {
    state.configNoticeTimer = setTimeout(clearConfigNotice, 20000);
  }
}

function renderCommunityPublishStatus(status) {
  state.communityPublishStatus = status || null;
  const el = $("#community-publish-status");
  if (!el) return;
  if (!status || typeof status !== "object") {
    el.textContent = "Status unavailable";
    return;
  }
  const queue = status.queue?.counts || {};
  const outbox = status.outbox || {};
  const task = status.active_task || null;
  const lastSummary = status.last_summary || null;
  const backfillSummary = status.backfill_last_summary || null;
  const lines = [
    `Mode: ${status.mode || "off"} · Enabled: ${status.enabled ? "yes" : "no"} · Worker: ${status.worker_enabled ? "yes" : "no"}`,
    `Publisher: ${status.publisher || "-"}`,
    `Token env: ${status.token_env || "-"} · Token present: ${status.token_present ? "yes" : "no"}`,
    `Repo: ${status.repo || "-"} · Branch: ${status.branch || "-"} -> ${status.target_branch || "-"}`,
    `Next scheduled run: ${status.next_run_at || "-"}`,
    `Outbox: ${outbox.outbox_dir || "-"} · Files: ${Number(outbox.file_count || 0)} · Proposal lines: ${Number(outbox.proposal_lines || 0)}`,
    `Queue: pending ${Number(queue.pending || 0)} · published ${Number(queue.published || 0)} · error ${Number(queue.error || 0)} · total ${Number(queue.total || 0)}`,
  ];
  if (task) {
    lines.push(`Active task: ${task.kind || "unknown"} · ${task.status || "running"}${task.running ? " · running" : ""}`);
  }
  if (lastSummary && typeof lastSummary === "object") {
    lines.push(`Last publish tick: ${lastSummary.status || "unknown"} · published ${Number(lastSummary.published_proposals || 0)} · errors ${Number(lastSummary.errors || 0)}`);
  }
  if (backfillSummary && typeof backfillSummary === "object") {
    lines.push(`Last backfill: ${backfillSummary.status || "unknown"} · eligible ${Number(backfillSummary.eligible || 0)} · written ${Number(backfillSummary.proposals_written || 0)} · repaired ${Number(backfillSummary.repaired_tags || 0)} · errors ${Number(backfillSummary.errors || 0)}`);
  }
  el.innerHTML = lines.map((line) => escapeHtml(String(line))).join("<br>");
}

function renderCommunityCacheSyncStatus(status) {
  state.communityCacheSyncStatus = status || null;
  const el = $("#community-cache-sync-status");
  if (!el) return;
  if (!status || typeof status !== "object") {
    el.textContent = "Status unavailable";
    return;
  }
  const lastSummary = status.last_summary || null;
  const stored = status.stored_status || null;
  const task = status.active_task || null;
  const lines = [
    `Enabled: ${status.enabled ? "yes" : "no"} · Upstream: ${status.api_base_url || "-"}`,
    `Sync interval: ${Number(status.poll_minutes || 0)} min · Batch size: ${Number(status.batch_size || 0)}`,
    `Next scheduled run: ${status.next_run_at || "-"}`,
  ];
  if (lastSummary && typeof lastSummary === "object") {
    lines.push(`Last sync: ${lastSummary.status || "unknown"} · mode ${lastSummary.mode || "-"} · records ${Number(lastSummary.results_count || 0)} · files ${Number(lastSummary.files_written || 0)}`);
  }
  if (stored && typeof stored === "object") {
    lines.push(`Stored cursor: ${stored.cursor || "-"} · Updated: ${stored.updated_at || "-"}`);
  }
  if (task) {
    lines.push(`Active task: ${task.kind || "unknown"} · ${task.status || "running"}${task.running ? " · running" : ""}`);
  }
  el.innerHTML = lines.map((line) => escapeHtml(String(line))).join("<br>");
}

async function refreshCommunityPublishStatus() {
  if (state.communityPublishStatusTimer) {
    clearTimeout(state.communityPublishStatusTimer);
    state.communityPublishStatusTimer = null;
  }
  try {
    const status = await fetchJson("/api/community-cache/publish/status");
    renderCommunityPublishStatus(status);
    if (status?.active_task?.running) {
      state.communityPublishStatusTimer = setTimeout(() => {
        refreshCommunityPublishStatus().catch(() => {});
      }, 5000);
    }
  } catch (err) {
    renderCommunityPublishStatus(null);
    const messageEl = $("#community-publish-message");
    if (messageEl) {
      setNotice(messageEl, `Publish status error: ${err.message}`, true);
    }
  }
}

async function refreshCommunityCacheSyncStatus() {
  if (state.communityCacheSyncStatusTimer) {
    clearTimeout(state.communityCacheSyncStatusTimer);
    state.communityCacheSyncStatusTimer = null;
  }
  try {
    const status = await fetchJson("/api/community-cache/sync/status");
    renderCommunityCacheSyncStatus(status);
    if (status?.active_task?.running) {
      state.communityCacheSyncStatusTimer = setTimeout(() => {
        refreshCommunityCacheSyncStatus().catch(() => {});
      }, 5000);
    }
  } catch (err) {
    renderCommunityCacheSyncStatus(null);
    const messageEl = $("#community-cache-sync-message");
    if (messageEl) {
      setNotice(messageEl, `Cache sync status error: ${err.message}`, true);
    }
  }
}

function setPage(page) {
  const normalized = normalizePageName(page);
  const allowed = new Set(["home", "review", "config", "status", "info"]);
  const target = allowed.has(normalized) ? normalized : "home";
  state.currentPage = target;
  if (target === "home") {
    if (state.homeSearchRequestId) {
      startHomeResultPolling(state.homeSearchRequestId);
    }
    setHomeSearchActive(Boolean(state.homeSearchRequestId || state.homeDirectPreview));
    updateHomeViewAdvancedLink();
    refreshReviewQueue();
  } else {
    stopHomeResultPolling();
    updateHomeViewAdvancedLink();
  }
  document.body.classList.remove("nav-open");
  // Home-only root class for scoping styles
  document.body.classList.toggle("home-page", target === "home");
  if (target !== "home") {
    setHomeSearchActive(false);
    setHomeResultsState({ hasResults: false, terminal: false });
    stopHomeJobPolling();
    const panel = document.querySelector("#home-advanced-panel");
    if (panel) {
      panel.classList.add("hidden");
    }
  }
  document.body.dataset.page = target;
  const navToggle = $("#nav-toggle");
  if (navToggle) {
    navToggle.setAttribute("aria-expanded", "false");
  }
  const sections = $$("section[data-page]");
  sections.forEach((section) => {
    const show = section.dataset.page === target;
    section.classList.toggle("page-hidden", !show);
  });
  $$(".nav-button").forEach((button) => {
    button.classList.toggle("active", button.dataset.page === target);
  });
  if (target === "status") {
    refreshStatus();
    refreshMetrics();
    refreshVersion();
    refreshSearchQueue();
    refreshDownloads();
    refreshHistory();
    refreshLogs();
  } else if (target === "review") {
    refreshReviewQueue();
  } else if (target === "config") {
    if (!state.config || !state.configDirty) {
      loadConfig().then(async () => {
        await refreshSpotifyConfig();
        const sectionHash = String(window.location.hash || "").replace("#", "");
        if (sectionHash.startsWith("settings-")) {
          setActiveSettingsSection(sectionHash, { jump: false, smooth: false });
        } else {
          setActiveSettingsSection(state.settingsActiveSectionId || "settings-core", { jump: false, smooth: false });
        }
        if (consumeSpotifyConnectedHashFlag()) {
          await refreshSpotifyConfig();
          setConfigNotice("Spotify connected successfully.", false, true);
        }
        refreshCommunityPublishStatus().catch(() => {});
        refreshCommunityCacheSyncStatus().catch(() => {});
      });
    }
    refreshSchedule();
    const sectionHash = String(window.location.hash || "").replace("#", "");
    if (sectionHash.startsWith("settings-")) {
      requestAnimationFrame(() => setActiveSettingsSection(sectionHash, { jump: false, smooth: false }));
    } else {
      requestAnimationFrame(() => setActiveSettingsSection(state.settingsActiveSectionId || "settings-core", { jump: false, smooth: false }));
    }
  } else if (target === "info") {
    refreshMetrics();
    refreshVersion();
    if (!state.config) {
      fetchJson("/api/config")
        .then((cfg) => {
          state.config = cfg;
          updateSearchDestinationDisplay();
        })
        .catch(() => {});
    } else {
      updateSearchDestinationDisplay();
    }
    const handoffRequestId = state.pendingAdvancedRequestId;
    state.pendingAdvancedRequestId = null;
    refreshSearchRequests(handoffRequestId)
      .then(() => {
        const requestIdToLoad = handoffRequestId || state.searchSelectedRequestId;
        if (requestIdToLoad) {
          return refreshSearchRequestDetails(requestIdToLoad);
        }
      })
      .catch(() => {});
  }
}

function consumeSpotifyConnectedHashFlag() {
  const hash = window.location.hash || "";
  if (!hash.includes("spotify=connected")) {
    return false;
  }
  if (state.spotifyConnectedNoticeShown) {
    return false;
  }
  history.replaceState(null, "", window.location.pathname + window.location.search);
  state.spotifyConnectedNoticeShown = true;
  return true;
}

function isValidHttpUrl(value) {
  if (!value) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch (err) {
    return false;
  }
}

function extractPlaylistIdFromUrl(value) {
  if (!value) return null;
  try {
    const parsed = new URL(value, window.location.origin);
    const listParam = parsed.searchParams.get("list");
    if (listParam) {
      return listParam;
    }
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.includes("playlist")) {
      const index = segments.indexOf("playlist");
      if (segments[index + 1]) {
        return segments[index + 1];
      }
    }
    if (segments[0] && segments[0].startsWith("PL")) {
      return segments[0];
    }
  } catch (err) {
    return null;
  }
  return null;
}

function normalizeYouTubePlaylistIdentifier(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const extracted = extractPlaylistIdFromUrl(raw);
  if (extracted) {
    return String(extracted).trim();
  }
  const fallbackMatch = raw.match(/[?&]list=([A-Za-z0-9_-]+)/i);
  if (fallbackMatch && fallbackMatch[1]) {
    return String(fallbackMatch[1]).trim();
  }
  return raw;
}

function normalizePreviewSourceKey(source) {
  return String(source || "").trim().toLowerCase();
}

function extractYouTubeVideoIdFromUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    const host = (parsed.hostname || "").toLowerCase();
    if (host === "youtu.be") {
      const id = parsed.pathname.replace(/^\/+/, "").split("/")[0];
      return id || null;
    }
    if (host.includes("youtube.com")) {
      const id = parsed.searchParams.get("v");
      if (id) return id;
      const parts = parsed.pathname.split("/").filter(Boolean);
      const embedIdx = parts.findIndex((part) => part === "embed" || part === "shorts");
      if (embedIdx >= 0 && parts[embedIdx + 1]) {
        return parts[embedIdx + 1];
      }
    }
  } catch (_err) {
    return null;
  }
  return null;
}

function buildYouTubeHomePreviewEmbedUrl(url) {
  const videoId = extractYouTubeVideoIdFromUrl(url);
  if (!videoId) {
    return null;
  }
  return `https://www.youtube.com/embed/${encodeURIComponent(videoId)}?autoplay=1&rel=0&modestbranding=1`;
}

function buildRumbleHomePreviewEmbedUrl(url, candidate) {
  const parseRawMeta = () => {
    const raw = candidate?.raw_meta_json;
    if (!raw || typeof raw !== "string") return {};
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_err) {
      return {};
    }
  };
  const rawMeta = parseRawMeta();
  const embedded = String(rawMeta.embed_url || "").trim();
  if (embedded) {
    try {
      const parsed = new URL(embedded);
      const host = (parsed.hostname || "").toLowerCase();
      if (host.endsWith("rumble.com") && parsed.pathname.includes("/embed/")) {
        const next = new URL(embedded);
        next.searchParams.set("autoplay", "2");
        return next.toString();
      }
    } catch (_err) {
      // continue to URL-derived fallback
    }
  }
  const raw = String(url || "").trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    const host = (parsed.hostname || "").toLowerCase();
    if (!host.endsWith("rumble.com")) return null;
    const parts = parsed.pathname.split("/").filter(Boolean);
    if (!parts.length) return null;
    const slug = String(parts[0] || "").toLowerCase();
    // Common Rumble video path is /v<id>-title.html
    const idMatch = slug.match(/^(v[a-z0-9]+)/i);
    if (!idMatch || !idMatch[1]) return null;
    return `https://rumble.com/embed/${encodeURIComponent(idMatch[1])}/?autoplay=2`;
  } catch (_err) {
    return null;
  }
}

function buildArchiveOrgHomePreviewEmbedUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    const host = (parsed.hostname || "").toLowerCase();
    if (!host.endsWith("archive.org")) return null;
    const parts = parsed.pathname.split("/").filter(Boolean);
    if (parts.length >= 2 && parts[0] === "details") {
      const identifier = String(parts[1] || "").trim();
      if (!identifier) return null;
      return `https://archive.org/embed/${encodeURIComponent(identifier)}`;
    }
  } catch (_err) {
    return null;
  }
  return null;
}

function buildHomePreviewDescriptor(candidate) {
  if (!candidate || typeof candidate !== "object") {
    return null;
  }
  const source = normalizePreviewSourceKey(candidate.source);
  const url = String(candidate.url || "").trim();
  if (!source || !url || !isValidHttpUrl(url)) {
    return null;
  }
  const builder = HOME_PREVIEW_EMBED_BUILDERS[source];
  if (typeof builder !== "function") {
    return null;
  }
  const embedUrl = builder(url, candidate);
  if (!embedUrl || !isValidHttpUrl(embedUrl)) {
    return null;
  }
  return {
    source,
    title: String(candidate.title || "").trim() || "Preview",
    embedUrl,
  };
}

function buildHomePreviewDescriptorFromRow(row) {
  if (!row) return null;
  const previewButton = row.querySelector('button[data-action="home-preview"]');
  if (!previewButton) return null;
  const embedUrl = String(previewButton.dataset.previewEmbedUrl || "").trim();
  if (!embedUrl || !isValidHttpUrl(embedUrl)) return null;
  return {
    embedUrl,
    source: String(previewButton.dataset.previewSource || "").trim(),
    title: String(previewButton.dataset.previewTitle || "").trim() || "Preview",
  };
}

function openHomePreviewModal(descriptor) {
  if (!descriptor || !descriptor.embedUrl) {
    return;
  }
  const modal = $("#home-preview-modal");
  const frame = $("#home-preview-frame");
  const titleEl = $("#home-preview-title");
  const sourceEl = $("#home-preview-source");
  if (!modal || !frame || !titleEl || !sourceEl) {
    return;
  }
  const sourceLabels = {
    youtube: "YouTube",
    youtube_music: "YouTube Music",
    rumble: "Rumble",
    archive_org: "Archive.org",
  };
  previewState.open = true;
  previewState.source = descriptor.source || "";
  previewState.title = descriptor.title || "Preview";
  previewState.embedUrl = descriptor.embedUrl;
  titleEl.textContent = descriptor.title || "Preview";
  sourceEl.textContent = `Source: ${sourceLabels[descriptor.source] || descriptor.source || "Unknown"}`;
  frame.src = descriptor.embedUrl;
  modal.classList.remove("hidden");
  updatePollingState();
}

function closeHomePreviewModal() {
  const modal = $("#home-preview-modal");
  const frame = $("#home-preview-frame");
  if (frame) {
    frame.src = "about:blank";
  }
  if (modal) {
    modal.classList.add("hidden");
  }
  previewState.open = false;
  previewState.source = "";
  previewState.title = "";
  previewState.embedUrl = "";
  updatePollingState();
}

function computeResolvedDestinationPath(raw) {
  const base = BROWSE_DEFAULTS.mediaRoot || "/downloads";
  const trimmed = (raw || "").trim();
  if (!trimmed) {
    return base || "/downloads";
  }
  if (trimmed.startsWith("/")) {
    return trimmed;
  }
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  const cleaned = trimmed.replace(/^\/+/, "");
  if (!normalizedBase) {
    return `/${cleaned}`;
  }
  return `${normalizedBase}/${cleaned}`;
}

function shouldPreferVideoAdapters(raw) {
  if (!raw) return false;
  const normalized = raw.toLowerCase();
  const words = normalized.split(/\s+/).filter(Boolean);
  if (words.length >= 3) {
    return true;
  }
  return HOME_VIDEO_KEYWORDS.some((keyword) => normalized.includes(keyword));
}

function hasInvalidDestinationValue(value) {
  if (!value) return false;
  const trimmed = value.trim();
  if (!trimmed) return false;
  if (trimmed.includes("..") || trimmed.includes("\\") || trimmed.includes("~")) {
    return true;
  }
  return false;
}

function updateHomeDestinationResolved() {
  const display = $("#home-destination-resolved");
  if (!display) return;
  const value = $("#home-destination")?.value;
  const resolved = computeResolvedDestinationPath(value);
  display.textContent = `Resolved: ${resolved}`;
}

function loadSettingsAdvancedModePreference() {
  return false;
}

function isSettingsAdvancedModeEnabled() {
  return !!$("#settings-advanced-mode")?.checked;
}

function getAllowedSettingsSectionIds() {
  const advancedEnabled = isSettingsAdvancedModeEnabled();
  return $$(".settings-section[data-settings-section]")
    .filter((section) => {
      const isAdvanced = String(section.dataset.settingsLevel || "").trim().toLowerCase() === "advanced";
      return advancedEnabled || !isAdvanced;
    })
    .map((section) => section.id)
    .filter(Boolean);
}

function updateSettingsSectionNavState(activeSectionId = "") {
  const normalizedActive = String(activeSectionId || "").trim();
  const advancedEnabled = isSettingsAdvancedModeEnabled();
  $$(".settings-nav-link[data-settings-link]").forEach((link) => {
    const href = String(link.getAttribute("href") || "");
    const sectionId = href.startsWith("#") ? href.slice(1) : href;
    const isAdvanced = String(link.dataset.settingsLevel || "").trim().toLowerCase() === "advanced";
    link.classList.toggle("hidden", isAdvanced && !advancedEnabled);
    link.classList.toggle("active", normalizedActive && sectionId === normalizedActive);
  });
  const select = $("#settings-section-select");
  if (select) {
    Array.from(select.options).forEach((option) => {
      const isAdvanced = String(option.dataset.settingsLevel || "").trim().toLowerCase() === "advanced";
      const shouldHide = isAdvanced && !advancedEnabled;
      option.hidden = shouldHide;
      option.disabled = shouldHide;
    });
    if (normalizedActive) {
      select.value = normalizedActive;
    }
  }
}

function setActiveSettingsSection(sectionId, { jump = false, smooth = true } = {}) {
  const allowed = getAllowedSettingsSectionIds();
  if (!allowed.length) {
    return;
  }
  const requested = String(sectionId || "").trim();
  const nextActive = requested === SETTINGS_ALL_SECTION_ID
    ? SETTINGS_ALL_SECTION_ID
    : (allowed.includes(requested) ? requested : (allowed[0] || "settings-core"));
  state.settingsActiveSectionId = nextActive;
  $$(".settings-section[data-settings-section]").forEach((section) => {
    const isAllowed = allowed.includes(section.id);
    const shouldShow = nextActive === SETTINGS_ALL_SECTION_ID ? isAllowed : (isAllowed && section.id === nextActive);
    section.classList.toggle("hidden", !shouldShow);
    section.dataset.settingsVisible = shouldShow ? "1" : "0";
    section.setAttribute("aria-hidden", shouldShow ? "false" : "true");
  });
  syncSettingsMainWidthLock();
  updateSettingsSectionNavState(nextActive);
  if (jump) {
    if (nextActive === SETTINGS_ALL_SECTION_ID) {
      const firstVisible = $$(".settings-section[data-settings-section]").find((section) => !section.classList.contains("hidden"));
      if (firstVisible) {
        firstVisible.scrollIntoView({ behavior: smooth ? "smooth" : "auto", block: "start" });
      }
      return;
    }
    const activeSection = document.getElementById(nextActive);
    if (activeSection) {
      activeSection.scrollIntoView({ behavior: smooth ? "smooth" : "auto", block: "start" });
    }
  }
}

function applySettingsAdvancedMode(enabled, { persist = true } = {}) {
  const normalized = !!enabled;
  const toggle = $("#settings-advanced-mode");
  if (toggle) {
    toggle.checked = normalized;
  }
  if (persist) {
    try {
      localStorage.setItem(SETTINGS_ADVANCED_MODE_KEY, normalized ? "1" : "0");
    } catch {
      // ignore storage failures
    }
  }
  $$("[data-settings-level='advanced']").forEach((node) => {
    const tag = String(node.tagName || "").toUpperCase();
    const isStructural = tag === "OPTION"
      || node.classList.contains("settings-nav-link")
      || node.classList.contains("settings-section");
    if (isStructural) {
      return;
    }
    node.classList.toggle("hidden", !normalized);
  });
  setActiveSettingsSection(state.settingsActiveSectionId);
}

function syncSettingsMainWidthLock() {
  const panel = $("#config-panel");
  const layout = panel?.querySelector(".settings-layout");
  if (!panel || !layout) {
    return;
  }
  const isStacked = window.matchMedia("(max-width: 1200px)").matches;
  let width = 0;
  if (isStacked) {
    width = Math.floor(layout.clientWidth || 0);
  } else {
    const sidebar = layout.querySelector(".settings-sidebar");
    const sidebarWidth = sidebar ? Math.floor(sidebar.getBoundingClientRect().width || 0) : 0;
    const computed = window.getComputedStyle(layout);
    const gapValue = (computed.columnGap && computed.columnGap !== "normal")
      ? computed.columnGap
      : computed.gap;
    const gap = Number.parseFloat(gapValue || "0") || 0;
    width = Math.floor((layout.clientWidth || 0) - sidebarWidth - gap);
  }
  if (width > 0) {
    requestAnimationFrame(() => {
      panel.style.setProperty("--settings-main-locked-width", `${width}px`);
    });
  }
}

function syncConfigSectionCollapsedStates() {
  const watcherEnabled = !!$("#cfg-watcher-enabled")?.checked;
  const watcherDetails = $("#watcher-details");
  if (watcherDetails) {
    watcherDetails.classList.toggle("hidden", !watcherEnabled);
  }

  const schedulerEnabled = !!$("#schedule-enabled")?.checked;
  const schedulerDetails = $("#scheduler-details");
  if (schedulerDetails) {
    schedulerDetails.classList.toggle("hidden", !schedulerEnabled);
  }

  const musicMetaEnabled = !!$("#cfg-music-meta-enabled")?.checked;
  const musicMetaDetails = $("#music-meta-details");
  if (musicMetaDetails) {
    musicMetaDetails.classList.toggle("hidden", !musicMetaEnabled);
  }

  const spotifyEnabled = !!$("#spotify-enabled")?.checked;
  const spotifyDetails = $("#spotify-details");
  if (spotifyDetails) {
    spotifyDetails.classList.toggle("hidden", !spotifyEnabled);
  }
}

function getHomeDefaultDestination(mode = state.homeMediaMode) {
  const cfg = state.config || {};
  const normalized = normalizeHomeMediaMode(mode);
  if (normalized === "music") {
    return (
      cfg.home_music_download_folder
      || cfg.music_download_folder
      || cfg.single_download_folder
      || ""
    );
  }
  if (normalized === "music_video") {
    return (
      cfg.home_music_video_download_folder
      || cfg.single_download_folder
      || ""
    );
  }
  return (
    cfg.single_download_folder
    || ""
  );
}

function getHomeDefaultFormat(mode = state.homeMediaMode) {
  const cfg = state.config || {};
  const normalized = normalizeHomeMediaMode(mode);
  if (normalized === "music") {
    return (
      cfg.home_music_final_format
      || cfg.music_final_format
      || ""
    );
  }
  if (normalized === "music_video") {
    return (
      cfg.home_music_video_final_format
      || cfg.final_format
      || cfg.video_final_format
      || ""
    );
  }
  return (
    cfg.final_format
    || cfg.video_final_format
    || ""
  );
}

function setHomeDestinationValue(value) {
  const normalized = normalizeDownloadsRelative(String(value || "").trim());
  const primary = $("#home-destination");
  if (primary) {
    primary.value = normalized;
  }
  updateHomeDestinationResolved();
}

function applyHomeDefaultDestination({ force = false } = {}) {
  const primary = $("#home-destination");
  if (!primary) return;
  const current = String(primary.value || "").trim();
  const nextDefault = normalizeDownloadsRelative(getHomeDefaultDestination(state.homeMediaMode));
  const canReplace = force || !current || current === (state.homeLastDefaultDestination || "");
  if (canReplace) {
    setHomeDestinationValue(nextDefault);
  } else {
    updateHomeDestinationResolved();
  }
  state.homeLastDefaultDestination = nextDefault;
}

function applyHomeDefaultVideoFormat({ force = false } = {}) {
  const selector = $("#home-format");
  if (!selector) return;
  const current = String(selector.value || "").trim().toLowerCase();
  const nextDefault = String(getHomeDefaultFormat("video") || "").trim().toLowerCase();
  const canReplace = force || !current || current === (state.homeLastDefaultVideoFormat || "");
  if (canReplace) {
    selector.value = nextDefault;
  }
  state.homeLastDefaultVideoFormat = nextDefault;
}

function updateMusicModeFormatControl() {
  const field = $("#music-video-format-field");
  const selector = $("#music-video-final-format");
  if (!field || !selector) return;
  const activeMode = normalizeHomeMediaMode(state.homeMediaMode);
  const isMusicLikeMode = activeMode === "music" || activeMode === "music_video";
  field.classList.toggle("hidden", !isMusicLikeMode);
  if (!isMusicLikeMode) {
    selector.innerHTML = "";
    return;
  }
  const options = activeMode === "music" ? HOME_MUSIC_MODE_FORMATS : HOME_MUSIC_VIDEO_MODE_FORMATS;
  const current = String(selector.value || "").trim().toLowerCase();
  const configuredDefault = String(getHomeDefaultFormat(activeMode) || "").trim().toLowerCase();
  const preferred = options.includes(configuredDefault)
    ? configuredDefault
    : (options.includes(current) ? current : options[0]);
  selector.innerHTML = options.map((value) => `<option value="${value}">${value}</option>`).join("");
  selector.value = preferred;
}

function setupNavActions() {
  const topActions = $("#top-actions");
  const navActions = $("#nav-actions");
  if (!topActions || !navActions) {
    return;
  }
  if (!state.actionButtons) {
    state.actionButtons = Array.from(topActions.children);
  }
  const mql = window.matchMedia("(max-width: 900px)");
  const sync = () => {
    const target = mql.matches ? navActions : topActions;
    state.actionButtons.forEach((button) => {
      if (button.parentElement !== target) {
        target.appendChild(button);
      }
    });
  };
  sync();
  if (mql.addEventListener) {
    mql.addEventListener("change", sync);
  } else if (mql.addListener) {
    mql.addListener(sync);
  }
}

function updatePollingState() {
  const importModalOpen = !$("#import-progress-modal")?.classList.contains("hidden");
  state.pollingPaused = browserState.open || oauthState.open || previewState.open || importModalOpen || state.configDirty || state.inputFocused;
}

function withPollingGuard(fn) {
  if (state.pollingPaused) {
    return;
  }
  fn();
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  return `${size.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
}

function formatSpeed(speed) {
  if (!Number.isFinite(speed)) return "-";
  return `${formatBytes(speed)}/s`;
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds)) return "-";
  const total = Math.max(0, Math.floor(seconds));
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  if (mins > 0) {
    return `${mins}m ${secs}s`;
  }
  return `${secs}s`;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatCandidatePostedDate(candidate) {
  if (!candidate || typeof candidate !== "object") {
    return "";
  }
  const postedLabel = String(candidate.posted_label || "").trim();
  if (postedLabel) {
    return postedLabel;
  }

  const parseRawMeta = () => {
    const raw = candidate.raw_meta_json;
    if (!raw || typeof raw !== "string") return {};
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_err) {
      return {};
    }
  };

  const rawMeta = parseRawMeta();
  const rawLabel = String(
    rawMeta.posted_label || rawMeta.published_time_text || rawMeta.publishedTimeText || rawMeta.published_label || ""
  ).trim();
  if (rawLabel) {
    return rawLabel;
  }
  const direct = [
    candidate.posted_at,
    candidate.published_at,
    candidate.publish_date,
    candidate.upload_date,
    rawMeta.upload_date,
    rawMeta.release_date,
    rawMeta.timestamp,
    rawMeta.release_timestamp,
  ];

  const parseValue = (value) => {
    if (value === null || value === undefined || value === "") return null;
    if (typeof value === "number" && Number.isFinite(value)) {
      const ms = value > 1e12 ? value : value * 1000;
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    const text = String(value).trim();
    if (!text) return null;
    if (/^\d{8}$/.test(text)) {
      const year = Number(text.slice(0, 4));
      const month = Number(text.slice(4, 6));
      const day = Number(text.slice(6, 8));
      const d = new Date(Date.UTC(year, month - 1, day));
      return Number.isNaN(d.getTime()) ? null : d;
    }
    if (/^\d{10,13}$/.test(text)) {
      const n = Number(text);
      if (!Number.isFinite(n)) return null;
      const ms = text.length === 13 ? n : n * 1000;
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    const d = new Date(text);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  for (const value of direct) {
    const parsed = parseValue(value);
    if (parsed) {
      try {
        return parsed.toLocaleDateString(undefined, {
          year: "numeric",
          month: "short",
          day: "numeric",
        });
      } catch (_err) {
        return parsed.toISOString().slice(0, 10);
      }
    }
  }
  return "";
}

function toFiniteNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatCountdown(serverTime, targetTime) {
  if (!serverTime || !targetTime) return "";
  const now = new Date(serverTime).getTime();
  const target = new Date(targetTime).getTime();
  if (Number.isNaN(now) || Number.isNaN(target)) return "";
  const diff = Math.max(0, Math.floor((target - now) / 1000));
  return formatDuration(diff);
}

function normalizeVersionTag(tag) {
  if (!tag) return "";
  return tag.trim().replace(/^v/i, "");
}

function sanitizeVersionTag(tag) {
  return (tag || "").replace(/[^0-9A-Za-z._-]/g, "");
}

function parseVersion(tag) {
  const clean = normalizeVersionTag(tag);
  const parts = clean.split(".");
  return parts.map((part) => parseInt(part, 10) || 0);
}

function compareVersions(current, latest) {
  const a = parseVersion(current);
  const b = parseVersion(latest);
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i += 1) {
    const left = a[i] || 0;
    const right = b[i] || 0;
    if (left > right) return 1;
    if (left < right) return -1;
  }
  return 0;
}

function formatTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function downloadUrl(fileId) {
  return `/api/files/${encodeURIComponent(fileId)}/download`;
}

function resolveTheme() {
  const saved = localStorage.getItem("yt_archiver_theme");
  if (saved === "light" || saved === "dark") {
    return saved;
  }
  return "dark";
}

function getThemeToggleIconSvg(theme) {
  if (theme === "light") {
    return (
      '<svg viewBox="0 0 24 24" aria-hidden="true">'
      + '<path d="M14.5 3.5a8.5 8.5 0 1 0 6 13.9A9.5 9.5 0 0 1 14.5 3.5Z" fill="currentColor"></path>'
      + '</svg>'
    );
  }
  return (
    '<svg viewBox="0 0 24 24" aria-hidden="true">'
    + '<circle cx="12" cy="12" r="4.5" fill="currentColor"></circle>'
    + '<path d="M12 2.5v2.5M12 19v2.5M4.9 4.9l1.8 1.8M17.3 17.3l1.8 1.8M2.5 12H5M19 12h2.5M4.9 19.1l1.8-1.8M17.3 6.7l1.8-1.8" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" fill="none"></path>'
    + '</svg>'
  );
}

function applyTheme(theme) {
  const root = document.documentElement;
  if (theme === "light") {
    root.dataset.theme = "light";
  } else {
    delete root.dataset.theme;
  }
  const button = $("#toggle-theme");
  if (button) {
    const nextModeLabel = theme === "light" ? "dark mode" : "light mode";
    button.innerHTML = getThemeToggleIconSvg(theme);
    button.setAttribute("aria-label", `Switch to ${nextModeLabel}`);
    button.setAttribute("title", `Switch to ${nextModeLabel}`);
  }
  localStorage.setItem("yt_archiver_theme", theme);
}

async function copyText(text) {
  if (!text) return false;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (err) {
      /* fall through */
    }
  }
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  let ok = false;
  try {
    ok = document.execCommand("copy");
  } catch (err) {
    ok = false;
  }
  document.body.removeChild(textarea);
  return ok;
}

function displayPath(path, baseDir, showInternal) {
  if (!path) return "";
  if (showInternal) {
    return path;
  }
  if (baseDir) {
    const normalized = baseDir.endsWith("/") ? baseDir : `${baseDir}/`;
    if (path.startsWith(normalized)) {
      return path.slice(normalized.length);
    }
  }
  return path;
}

function normalizeDownloadsRelative(value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  if (raw === "." || raw === "./") return ".";
  if (raw.startsWith("./")) {
    return raw.slice(2);
  }

  const base = BROWSE_DEFAULTS.mediaRoot || "/downloads";
  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  if (raw === base || raw === "/downloads") {
    return ".";
  }
  if (raw.startsWith(normalizedBase)) {
    return raw.slice(normalizedBase.length);
  }
  if (raw.startsWith("/downloads/")) {
    return raw.slice("/downloads/".length);
  }
  return raw;
}

function getBrowseRootBase(rootKey) {
  if (rootKey === "downloads") {
    return BROWSE_DEFAULTS.mediaRoot || "";
  }
  if (rootKey === "config") {
    return BROWSE_DEFAULTS.configDir || "";
  }
  if (rootKey === "tokens") {
    return BROWSE_DEFAULTS.tokensDir || "";
  }
  if (rootKey === "library_exports") {
    return BROWSE_DEFAULTS.libraryExportsRoot || "";
  }
  const roots = BROWSE_DEFAULTS.roots || {};
  return typeof roots[rootKey] === "string" ? roots[rootKey] : "";
}

function getBrowseRootLabel(rootKey) {
  if (rootKey === "downloads") return "Downloads";
  if (rootKey === "library_exports") return "Library Exports";
  if (rootKey === "config") return "Config";
  if (rootKey === "tokens") return "Tokens";
  return String(rootKey || "").replaceAll("_", " ");
}

function availableBrowserRootsForMode(mode = "dir") {
  const ordered = ["downloads", "library_exports", "config", "tokens"];
  const roots = BROWSE_DEFAULTS.roots || {};
  const result = [];
  ordered.forEach((rootKey) => {
    if (!roots[rootKey]) {
      return;
    }
    if (mode === "dir" && rootKey === "tokens") {
      return;
    }
    result.push(rootKey);
  });
  Object.keys(roots).sort().forEach((rootKey) => {
    if (ordered.includes(rootKey)) {
      return;
    }
    if (mode === "dir" && rootKey === "tokens") {
      return;
    }
    result.push(rootKey);
  });
  return result;
}

function preferredMusicLibraryBrowseRoot(value = "") {
  const raw = String(value || "").trim();
  const libraryExportsBase = getBrowseRootBase("library_exports");
  if (libraryExportsBase) {
    const normalizedBase = libraryExportsBase.endsWith("/") ? libraryExportsBase : `${libraryExportsBase}/`;
    if (!raw || raw === libraryExportsBase || raw.startsWith(normalizedBase)) {
      return "library_exports";
    }
  }
  return "downloads";
}

function updateBrowserRootSelect() {
  const rootSelect = $("#browser-root");
  if (!rootSelect) {
    return;
  }
  const roots = availableBrowserRootsForMode(browserState.mode);
  rootSelect.innerHTML = "";
  roots.forEach((rootKey) => {
    const option = document.createElement("option");
    option.value = rootKey;
    option.textContent = getBrowseRootLabel(rootKey);
    rootSelect.appendChild(option);
  });
  if (roots.includes(browserState.root)) {
    rootSelect.value = browserState.root;
  } else if (roots.length > 0) {
    browserState.root = roots[0];
    rootSelect.value = browserState.root;
  }
  rootSelect.disabled = roots.length <= 1;
}

function resolveBrowseStart(rootKey, value) {
  const raw = (value || "").trim();
  if (!raw) return "";
  if (raw.startsWith("..")) return "";
  if (raw.startsWith("./")) {
    return raw.slice(2);
  }

  const base = getBrowseRootBase(rootKey);

  if (!base) {
    return raw.startsWith("/") ? "" : raw;
  }

  const normalizedBase = base.endsWith("/") ? base : `${base}/`;
  if (raw === base) {
    return "";
  }
  if (raw.startsWith(normalizedBase)) {
    return raw.slice(normalizedBase.length);
  }
  if (!raw.startsWith("/")) {
    return raw;
  }
  return "";
}


async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text}`);
  }
  return response.json();
}

// Helper to cancel a job by ID
async function cancelJob(jobId) {
  if (!jobId) return;
  return fetchJson(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ reason: "User cancelled" }),
  });
}

function updateVersionDisplay(info) {
  if (!info) return;
  const appVersion = normalizeVersionTag(info.app_version || "") || "0.0.0";
  const ytDlpVersion = info.yt_dlp_version || "-";
  const pyVersion = info.python_version || "-";
  const appEl = $("#status-version-app");
  const ytdlpEl = $("#status-version-ytdlp");
  const pyEl = $("#status-version-python");
  if (appEl) appEl.textContent = `App ${appVersion}`;
  if (ytdlpEl) ytdlpEl.textContent = `yt-dlp ${ytDlpVersion}`;
  if (pyEl) pyEl.textContent = `Py ${pyVersion}`;
}

function applyReleaseStatus(currentVersion, latestTag) {
  const updateEl = $("#status-update");
  if (!updateEl) return;
  const latest = normalizeVersionTag(latestTag);
  const current = normalizeVersionTag(currentVersion || "");
  if (!latest) {
    updateEl.textContent = "-";
    return;
  }
  const safeTag = sanitizeVersionTag(latest);
  const link = document.createElement("a");
  link.href = VERSION_REFERENCE_PAGE;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = `v${safeTag}`;

  updateEl.textContent = "";
  const cmp = compareVersions(current, latest);
  if (cmp < 0) {
    updateEl.append("App update: ");
    updateEl.appendChild(link);
    return;
  }
  if (!current || current === "0.0.0") {
    updateEl.append("Latest: ");
    updateEl.appendChild(link);
    return;
  }
  updateEl.append("Up to date: ");
  updateEl.appendChild(link);
}

async function checkRelease(currentVersion) {
  const now = Date.now();
  const lastCheck = parseInt(localStorage.getItem(RELEASE_CHECK_KEY) || "0", 10);
  const cachedVersion = localStorage.getItem(RELEASE_VERSION_KEY) || "";
  const cachedRaw = localStorage.getItem(RELEASE_CACHE_KEY);
  let cached = null;
  if (cachedRaw) {
    try {
      cached = JSON.parse(cachedRaw);
    } catch (err) {
      cached = null;
    }
  }

  const normalizedVersion = normalizeVersionTag(currentVersion || "");
  const versionChanged = cachedVersion !== normalizedVersion;
  if (versionChanged) {
    localStorage.removeItem(RELEASE_CHECK_KEY);
    localStorage.removeItem(RELEASE_CACHE_KEY);
  }

  if (lastCheck && now - lastCheck < 24 * 60 * 60 * 1000 && cached && !versionChanged) {
    applyReleaseStatus(currentVersion, cached.tag);
    return;
  }

  try {
    const response = await fetch(VERSION_LATEST_URL, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    const tag = data.latest_version || "";
    localStorage.setItem(RELEASE_CHECK_KEY, String(now));
    localStorage.setItem(RELEASE_CACHE_KEY, JSON.stringify({ tag }));
    localStorage.setItem(RELEASE_VERSION_KEY, normalizedVersion);
    applyReleaseStatus(currentVersion, tag);
  } catch (err) {
    if (cached) {
      applyReleaseStatus(currentVersion, cached.tag);
    }
  }
}

async function refreshVersion() {
  try {
    const info = await fetchJson("/api/version");
    state.runtimeInfo = info;
    updateVersionDisplay(info);
    await checkRelease(info.app_version || "");
  } catch (err) {
    const appEl = $("#status-version-app");
    const ytdlpEl = $("#status-version-ytdlp");
    const pyEl = $("#status-version-python");
    if (appEl) appEl.textContent = "App -";
    if (ytdlpEl) ytdlpEl.textContent = "yt-dlp -";
    if (pyEl) pyEl.textContent = "Py -";
  }
}

async function loadPaths() {
  try {
    const data = await fetchJson("/api/paths");
    BROWSE_DEFAULTS.configDir = data.config_dir || "";
    BROWSE_DEFAULTS.libraryExportsRoot = data.browse_roots?.library_exports || "";
    BROWSE_DEFAULTS.mediaRoot = data.downloads_dir || "";
    BROWSE_DEFAULTS.tokensDir = data.tokens_dir || "";
    BROWSE_DEFAULTS.roots = (data && typeof data.browse_roots === "object" && data.browse_roots) ? data.browse_roots : {};
  } catch (err) {
    setConfigNotice(`Path load error: ${err.message}`, true);
  }
}

function openBrowser(target, root, mode = "dir", ext = "", startPath = "") {
  browserState.open = true;
  browserState.root = root;
  browserState.mode = mode;
  browserState.ext = ext;
  browserState.path = "";
  browserState.currentAbs = "";
  browserState.selected = "";
  browserState.target = target;
  updateBrowserRootSelect();
  updatePollingState();
  $("#browser-modal").classList.remove("hidden");
  $("#browser-select").textContent = mode === "dir" ? "Use this folder" : "Use selected file";
  refreshBrowser(startPath || "");
}

function closeBrowser() {
  browserState.open = false;
  $("#browser-modal").classList.add("hidden");
  browserState.target = null;
  browserState.selected = "";
  updatePollingState();
}

async function refreshBrowser(path, allowFallback = true) {
  const params = new URLSearchParams();
  params.set("root", browserState.root);
  if (path) {
    params.set("path", path);
  }
  params.set("mode", browserState.mode);
  if (browserState.ext) {
    params.set("ext", browserState.ext);
  }
  if (browserState.limit) {
    params.set("limit", String(browserState.limit));
  }

  const list = $("#browser-list");
  list.textContent = "";
  const loading = document.createElement("div");
  loading.className = "browser-item empty";
  loading.textContent = "Loading...";
  list.appendChild(loading);
  const renderToken = ++browserState.renderToken;

  try {
    const data = await fetchJson(`/api/browse?${params.toString()}`);
    if (renderToken !== browserState.renderToken) {
      return;
    }
    browserState.path = data.path || "";
    browserState.currentAbs = data.abs_path || "";
    browserState.selected = "";
    $("#browser-path").textContent = data.abs_path || "/";
    if (browserState.mode === "dir") {
      $("#browser-selected").textContent = browserState.currentAbs ? `Current: ${browserState.currentAbs}` : "Select a folder";
    } else {
      $("#browser-selected").textContent = "No selection";
    }
    const hasParent = data.parent !== null && data.parent !== undefined;
    $("#browser-up").disabled = !hasParent;
    $("#browser-up").dataset.path = data.parent || "";
    const canSelect = browserState.mode === "dir" ? !!browserState.currentAbs : !!browserState.selected;
    $("#browser-select").disabled = !canSelect;

    list.textContent = "";

    if (!data.entries.length) {
      const empty = document.createElement("div");
      empty.className = "browser-item empty";
      empty.textContent = "No entries";
      list.appendChild(empty);
      return;
    }

    const entries = data.entries;
    const chunkSize = 100;
    let index = 0;

    const createItem = (entry) => {
      const item = document.createElement("button");
      item.className = "browser-item";
      item.type = "button";
      item.dataset.path = entry.path;
      item.dataset.absPath = entry.abs_path || "";
      item.dataset.type = entry.type;
      item.textContent = entry.type === "dir" ? `${entry.name}/` : entry.name;
      return item;
    };

    const renderChunk = () => {
      if (renderToken !== browserState.renderToken) {
        return;
      }
      const fragment = document.createDocumentFragment();
      if (index === 0 && browserState.limit && entries.length >= browserState.limit) {
        const notice = document.createElement("div");
        notice.className = "browser-item empty";
        notice.textContent = `Showing first ${browserState.limit} entries`;
        fragment.appendChild(notice);
      }
      for (let i = 0; i < chunkSize && index < entries.length; i += 1, index += 1) {
        fragment.appendChild(createItem(entries[index]));
      }
      list.appendChild(fragment);
      if (index < entries.length) {
        requestAnimationFrame(renderChunk);
      }
    };

    renderChunk();
  } catch (err) {
    if (allowFallback && path) {
      refreshBrowser("", false);
      return;
    }
    list.textContent = "";
    const errorItem = document.createElement("div");
    errorItem.className = "browser-item error";
    errorItem.textContent = `Failed to load: ${err.message}`;
    list.appendChild(errorItem);
  }
}

function applyBrowserSelection() {
  if (!browserState.target) return;
  if (browserState.mode === "dir") {
    if (!browserState.currentAbs) {
      return;
    }
    const useAbsolutePath = browserState.target.dataset.browserAbsolute === "1";
    const rel = browserState.path ? browserState.path : ".";
    const targetId = browserState.target.id;
    browserState.target.value = useAbsolutePath ? browserState.currentAbs : rel;
    console.info("Directory selected", { root: browserState.root, path: useAbsolutePath ? browserState.currentAbs : rel });
    closeBrowser();
    if (targetId === "home-destination") {
      updateHomeDestinationResolved();
    } else if (targetId === "search-destination") {
      updateSearchDestinationDisplay();
    }
    return;
  }
  if (browserState.selected) {
    const targetId = browserState.target.id;
    browserState.target.value = browserState.selected;
    console.info("File selected", { root: browserState.root, path: browserState.selected });
    closeBrowser();
    if (targetId === "home-destination") {
      updateHomeDestinationResolved();
    } else if (targetId === "search-destination") {
      updateSearchDestinationDisplay();
    }
  }
}

function openOauthModal() {
  oauthState.open = true;
  $("#oauth-modal").classList.remove("hidden");
  updatePollingState();
}

function closeOauthModal() {
  oauthState.open = false;
  $("#oauth-modal").classList.add("hidden");
  oauthState.sessionId = null;
  oauthState.authUrl = "";
  oauthState.account = "";
  updatePollingState();
}

function setPlaylistImportControlsEnabled(enabled) {
  const importButton = $("#home-import-button");
  if (importButton) {
    importButton.disabled = !enabled;
  }
  const importFile = $("#home-import-file");
  if (importFile) {
    importFile.disabled = !enabled;
  }
}

function openImportProgressModal() {
  const modal = $("#import-progress-modal");
  if (!modal) return;
  modal.classList.remove("hidden");
  updatePollingState();
}

function closeImportProgressModal() {
  if (state.playlistImportInProgress) {
    return;
  }
  const modal = $("#import-progress-modal");
  if (!modal) return;
  modal.classList.add("hidden");
  updatePollingState();
}

function renderPlaylistImportStatus(status) {
  const safe = status || {};
  const phase = String(safe.phase || safe.state || "queued");
  const phaseLower = phase.toLowerCase();
  const active =
    phaseLower === "queued" ||
    phaseLower === "parsing" ||
    phaseLower === "resolving" ||
    phaseLower === "finalizing";
  const total = Number.isFinite(Number(safe.total_tracks)) ? Number(safe.total_tracks) : 0;
  const processed = Number.isFinite(Number(safe.processed_tracks)) ? Number(safe.processed_tracks) : 0;
  const resolved = Number.isFinite(Number(safe.resolved)) ? Number(safe.resolved) : 0;
  const enqueued = Number.isFinite(Number(safe.enqueued)) ? Number(safe.enqueued) : 0;
  const failed = Number.isFinite(Number(safe.failed)) ? Number(safe.failed) : 0;
  const percent = total > 0 ? Math.max(0, Math.min(100, Math.round((processed / total) * 100))) : 0;
  const stateEl = $("#import-progress-state");
  if (stateEl) {
    stateEl.textContent = phase;
  }
  const msgEl = $("#import-progress-message");
  if (msgEl) {
    msgEl.textContent = safe.message || "Playlist import in progress...";
  }
  const processedEl = $("#import-progress-processed");
  if (processedEl) {
    processedEl.textContent = `${processed} / ${total}`;
  }
  const resolvedEl = $("#import-progress-resolved");
  if (resolvedEl) {
    resolvedEl.textContent = String(resolved);
  }
  const enqueuedEl = $("#import-progress-enqueued");
  if (enqueuedEl) {
    enqueuedEl.textContent = String(enqueued);
  }
  const failedEl = $("#import-progress-failed");
  if (failedEl) {
    failedEl.textContent = String(failed);
  }
  const progressBar = $("#import-progress-bar");
  if (progressBar) {
    progressBar.style.width = `${percent}%`;
  }
  const closeBtn = $("#import-progress-close");
  if (closeBtn) {
    closeBtn.disabled = active;
  }
  const errorEl = $("#import-progress-error");
  if (errorEl) {
    if (safe.error) {
      errorEl.textContent = `Error: ${safe.error}`;
      errorEl.style.color = "#ff7b7b";
    } else {
      errorEl.textContent = "";
    }
  }
}

function stopPlaylistImportPolling() {
  if (state.playlistImportPollTimer) {
    clearInterval(state.playlistImportPollTimer);
    state.playlistImportPollTimer = null;
  }
}

async function pollPlaylistImportStatus() {
  const jobId = state.playlistImportJobId;
  if (!jobId) {
    stopPlaylistImportPolling();
    return;
  }
  try {
    const data = await fetchJson(`/api/import/playlist/jobs/${encodeURIComponent(jobId)}`);
    const status = data.status || {};
    renderPlaylistImportStatus(status);
    const phase = String(status.phase || status.state || "").toLowerCase();
    const active =
      phase === "queued" ||
      phase === "parsing" ||
      phase === "resolving" ||
      phase === "finalizing";
    state.playlistImportInProgress = active;
    setPlaylistImportControlsEnabled(!active);
    if (!active) {
      stopPlaylistImportPolling();
      if (phase === "completed") {
        setNotice($("#home-search-message"), "Playlist import completed.", false);
        const summaryEl = $("#home-import-summary");
        if (summaryEl) {
          summaryEl.textContent =
            `Total: ${status.total_tracks || 0} | Resolved: ${status.resolved || 0} | ` +
            `Enqueued: ${status.enqueued || 0} | Unresolved: ${status.unresolved || 0}`;
        }
      } else {
        setNotice($("#home-search-message"), `Import failed: ${status.error || "unknown error"}`, true);
      }
    }
  } catch (err) {
    stopPlaylistImportPolling();
    state.playlistImportInProgress = false;
    setPlaylistImportControlsEnabled(true);
    setNotice($("#home-search-message"), `Import status failed: ${err.message}`, true);
  }
}

function startPlaylistImportPolling(jobId, initialStatus = null) {
  state.playlistImportJobId = jobId;
  state.playlistImportInProgress = true;
  setPlaylistImportControlsEnabled(false);
  openImportProgressModal();
  if (initialStatus) {
    renderPlaylistImportStatus(initialStatus);
  }
  stopPlaylistImportPolling();
  state.playlistImportPollTimer = setInterval(() => {
    pollPlaylistImportStatus();
  }, 1500);
  pollPlaylistImportStatus();
}

async function startOauthForRow(row) {
  const account = row.querySelector(".account-name").value.trim();
  const clientSecret = row.querySelector(".account-client").value.trim();
  const tokenOut = row.querySelector(".account-token").value.trim();
  if (!account) {
    setConfigNotice("Account name is required for OAuth.", true);
    return;
  }
  if (!clientSecret || !tokenOut) {
    setConfigNotice("Client secret and token paths are required for OAuth.", true);
    return;
  }
  try {
    setConfigNotice("Starting OAuth...", false);
    const data = await fetchJson("/api/oauth/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        account,
        client_secret: clientSecret,
        token_out: tokenOut,
      }),
    });
    oauthState.sessionId = data.session_id;
    oauthState.authUrl = data.auth_url || "";
    oauthState.account = account;
    $("#oauth-account").textContent = account;
    $("#oauth-url").value = oauthState.authUrl;
    $("#oauth-code").value = "";
    setNotice($("#oauth-message"), "", false);
    openOauthModal();
    if (oauthState.authUrl) {
      window.open(oauthState.authUrl, "_blank", "noopener");
    }
  } catch (err) {
    setConfigNotice(`OAuth start failed: ${err.message}`, true);
  }
}

async function completeOauth() {
  const code = $("#oauth-code").value.trim();
  if (!oauthState.sessionId) {
    setNotice($("#oauth-message"), "No active OAuth session.", true);
    return;
  }
  if (!code) {
    setNotice($("#oauth-message"), "Authorization code is required.", true);
    return;
  }
  try {
    setNotice($("#oauth-message"), "Completing OAuth...", false);
    await fetchJson("/api/oauth/complete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        session_id: oauthState.sessionId,
        code,
      }),
    });
    setNotice($("#oauth-message"), "Token saved.", false);
  } catch (err) {
    setNotice($("#oauth-message"), `OAuth failed: ${err.message}`, true);
  }
}

async function refreshConfigPath() {
  try {
    const data = await fetchJson("/api/config/path");
    $("#config-path").value = data.path || "";
  } catch (err) {
    setConfigNotice(`Config path error: ${err.message}`, true);
  }
}

async function setConfigPath() {
  const path = $("#config-path").value.trim();
  if (!path) {
    setConfigNotice("Config path is required", true);
    return;
  }
  try {
    await fetchJson("/api/config/path", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path }),
    });
    await loadConfig();
    setConfigNotice("Config path updated", false);
  } catch (err) {
    setConfigNotice(`Config path error: ${err.message}`, true);
  }
}

async function refreshStatus() {
  try {
    const data = await fetchJson("/api/status");
    const runningChip = $("#status-running");
    const watcher = data.watcher || {};
    const scheduler = data.scheduler || {};
    const automation = data.automation_effective || {};
    const watcherEffective = automation.watcher || {};
    const schedulerEffective = automation.scheduler || {};
    const watcherText = !watcherEffective.config_enabled
      ? "disabled"
      : (watcher.paused ? "paused" : (watcherEffective.effective_enabled ? "enabled" : "inactive"));
    $("#status-watcher").textContent = watcherText;
    $("#status-scheduler").textContent = !scheduler.enabled
      ? "disabled"
      : (schedulerEffective.effective_enabled ? "enabled" : "inactive");
    const watcherGroup = $("#status-group-watcher");
    if (watcherGroup) {
      watcherGroup.classList.toggle("hidden", !watcher.enabled);
    }
    const spotifyCfg = (state.config && typeof state.config.spotify === "object")
      ? state.config.spotify
      : {};
    const spotifyGroup = $("#status-group-spotify");
    const spotifyOperatorsEnabled = !!(
      spotifyCfg.sync_liked_songs ||
      spotifyCfg.sync_saved_albums ||
      spotifyCfg.sync_user_playlists ||
      (Array.isArray(spotifyCfg.watch_playlists) && spotifyCfg.watch_playlists.length > 0)
    );
    if (spotifyGroup) {
      spotifyGroup.classList.toggle("hidden", !spotifyOperatorsEnabled);
    }
    state.watcherStatus = watcher;
    const scheduleNote = $("#schedule-watcher-note");
    if (scheduleNote) {
      scheduleNote.style.display = watcher.enabled ? "block" : "none";
    }

    const status = data.status || {};
    const failures = status.run_failures || [];
    const watcherErrors = data.watcher_errors || [];
    let errorText = data.error || status.last_error_message || "";
    if (!errorText) {
      if (status.single_download_ok === false) {
        errorText = "Single download failed";
      } else if (failures.length) {
        errorText = failures[failures.length - 1];
      } else if (watcherErrors.length) {
        const last = watcherErrors[watcherErrors.length - 1];
        errorText = `Watcher: ${last.playlist_id} (${last.last_error})`;
      }
    }
    $("#status-error").textContent = errorText || "-";
    $("#status-success").textContent = (status.run_successes || []).length;
    $("#status-failed").textContent = failures.length;
    const queue = data.queue || {};
    const queueCounts = queue.counts || {};
    const queuedCount = Number.isFinite(Number(queueCounts.queued)) ? Number(queueCounts.queued) : 0;
    const claimedCount = Number.isFinite(Number(queueCounts.claimed)) ? Number(queueCounts.claimed) : 0;
    const downloadingCount = Number.isFinite(Number(queueCounts.downloading)) ? Number(queueCounts.downloading) : 0;
    const postprocessingCount = Number.isFinite(Number(queueCounts.postprocessing))
      ? Number(queueCounts.postprocessing)
      : 0;
    const failedCount = Number.isFinite(Number(queueCounts.failed)) ? Number(queueCounts.failed) : 0;
    const cancelledCount = Number.isFinite(Number(queueCounts.cancelled)) ? Number(queueCounts.cancelled) : 0;
    const activeQueueCount = Number.isFinite(Number(queue.active_count))
      ? Number(queue.active_count)
      : (queuedCount + claimedCount + downloadingCount + postprocessingCount);
    $("#status-queue-active").textContent = String(activeQueueCount);
    $("#status-queue-downloading").textContent = String(downloadingCount);
    $("#status-queue-queued").textContent = String(queuedCount);
    $("#status-queue-postprocessing").textContent = String(postprocessingCount);
    const queueSummary = [];
    if (claimedCount > 0) {
      queueSummary.push(`${claimedCount} claimed`);
    }
    const staleCounts = queue.stale_counts || {};
    const staleTotal = ["queued", "claimed", "downloading", "postprocessing"]
      .map((key) => Number(staleCounts[key] || 0))
      .reduce((sum, value) => sum + value, 0);
    if (staleTotal > 0) {
      queueSummary.push(`${staleTotal} stale`);
    }
    queueSummary.push(`${failedCount} failed backlog`);
    queueSummary.push(`${cancelledCount} cancelled`);
    const activeJobs = Array.isArray(queue.active_jobs) ? queue.active_jobs : [];
    const downloadingJobs = activeJobs.filter((job) => String(job?.status || "").toLowerCase() === "downloading");
    if (activeJobs.length) {
      const topJob = downloadingJobs[0] || activeJobs[0] || {};
      const topJobLabel = [topJob.status, topJob.source].filter(Boolean).join(" · ");
      const topPercentValue = toFiniteNumber(topJob.progress_percent);
      const topPercent = topPercentValue !== null
        ? `${Math.max(0, Math.min(100, Math.round(topPercentValue)))}%`
        : "";
      if (topJobLabel) {
        queueSummary.push(`head: ${topJobLabel}${topPercent ? ` (${topPercent})` : ""}`);
      }
    }
    $("#status-queue-summary").textContent = queueSummary.join(" · ");

    const importState = data.playlist_import || {};
    const importActive = !!importState.active;
    const importCurrent = importActive ? (importState.current_job || {}) : {};
    let importActiveCount = Number.isFinite(Number(importState.active_count))
      ? Number(importState.active_count)
      : 0;
    let importProcessed = Number.isFinite(Number(importCurrent.processed_tracks))
      ? Number(importCurrent.processed_tracks)
      : 0;
    let importTotal = Number.isFinite(Number(importCurrent.total_tracks))
      ? Number(importCurrent.total_tracks)
      : 0;
    let importEnqueued = Number.isFinite(Number(importCurrent.enqueued))
      ? Number(importCurrent.enqueued)
      : 0;
    let importDuplicateSkipped = Number.isFinite(Number(importCurrent.duplicate_skipped))
      ? Number(importCurrent.duplicate_skipped)
      : 0;
    let importFailed = Number.isFinite(Number(importCurrent.failed))
      ? Number(importCurrent.failed)
      : 0;
    let importResolved = Number.isFinite(Number(importCurrent.resolved))
      ? Number(importCurrent.resolved)
      : 0;
    let importStateText = importCurrent.state || (importActive ? "running" : "idle");
    let importPercent = importTotal > 0
      ? Math.max(0, Math.min(100, Math.round((importProcessed / importTotal) * 100)))
      : 0;
    let importSummaryParts = importActive
      ? [
        `${importPercent}% processed`,
        `${importResolved} resolved`,
        `${importEnqueued} enqueued`,
      ]
      : ["Idle"];
    if (importActive && importDuplicateSkipped > 0) {
      importSummaryParts.push(`${importDuplicateSkipped} linked`);
    }
    const importTopFailures = importCurrent.top_rejection_reasons || {};
    const topFailureEntry = Object.entries(importTopFailures)[0];
    if (importActive && topFailureEntry && topFailureEntry[0]) {
      importSummaryParts.push(`top fail: ${topFailureEntry[0]} (${topFailureEntry[1]})`);
    }
    if (importActive && importCurrent.message) {
      importSummaryParts.push(importCurrent.message);
    }
    if (importActive && importCurrent.error) {
      importSummaryParts.push(`error: ${importCurrent.error}`);
    }

    // Fallback when no playlist-import job is active:
    // surface live queue download progress so this status block does not sit at 0% during playlist runs.
    if (!importActive && activeQueueCount > 0) {
      const leadJob = downloadingJobs[0] || activeJobs[0] || null;
      const leadPercentRaw = toFiniteNumber(leadJob?.progress_percent);
      const leadDownloaded = toFiniteNumber(leadJob?.progress_downloaded_bytes);
      const leadTotal = toFiniteNumber(leadJob?.progress_total_bytes);
      const leadPercent = leadPercentRaw !== null
        ? Math.max(0, Math.min(100, Math.round(leadPercentRaw)))
        : ((leadDownloaded !== null && leadTotal !== null && leadTotal > 0)
          ? Math.max(0, Math.min(100, Math.round((leadDownloaded / leadTotal) * 100)))
          : 0);
      importStateText = "downloading";
      importActiveCount = activeQueueCount;
      importProcessed = downloadingCount;
      importTotal = activeQueueCount;
      importEnqueued = queuedCount + claimedCount;
      importFailed = failedCount;
      importResolved = downloadingCount + postprocessingCount;
      importPercent = leadPercent;
      importSummaryParts = [
        `${importPercent}% current`,
        `${downloadingCount} downloading`,
        `${queuedCount + claimedCount} pending`,
      ];
      if (postprocessingCount > 0) {
        importSummaryParts.push(`${postprocessingCount} postprocessing`);
      }
    }

    let operationsState = "idle";
    if (importActive) {
      operationsState = "importing";
    } else if (downloadingCount > 0) {
      operationsState = "downloading";
    } else if (postprocessingCount > 0) {
      operationsState = "postprocessing";
    } else if (claimedCount > 0) {
      operationsState = "claimed";
    } else if (queuedCount > 0) {
      operationsState = "queued";
    } else if ((data.watcher_status || {}).state === "polling") {
      operationsState = "polling";
    }
    if ((data.watcher_status || {}).state === "paused_downtime") {
      operationsState = "downtime";
    }
    runningChip.textContent = operationsState;
    if (operationsState === "idle") {
      runningChip.classList.add("idle");
      runningChip.classList.remove("running");
    } else {
      runningChip.classList.add("running");
      runningChip.classList.remove("idle");
    }

    const currentBatchId = importCurrent.import_batch_id || importCurrent.batch_id || "";
    const queueHeadJob = activeJobs[0] || null;
    const liveRunId = currentBatchId || data.run_id || queueHeadJob?.id || "";
    $("#status-run-id").textContent = `run: ${liveRunId || "-"}`;
    $("#status-started").textContent = formatTimestamp(
      importCurrent.started_at || queue.last_job_started_at || data.started_at,
    ) || "-";
    $("#status-finished").textContent = formatTimestamp(
      queue.last_job_completed_at || importCurrent.finished_at || data.finished_at,
    ) || "-";
    $("#status-success").textContent = String(
      Number.isFinite(Number(queueCounts.completed)) ? Number(queueCounts.completed) : ((data.status || {}).run_successes || []).length,
    );
    $("#status-failed").textContent = String(
      Number.isFinite(Number(queueCounts.failed)) ? Number(queueCounts.failed) : ((data.status || {}).run_failures || []).length,
    );

    $("#status-import-state").textContent = importStateText;
    $("#status-import-active-count").textContent = String(importActiveCount);
    $("#status-import-processed").textContent = `${importProcessed} / ${importTotal}`;
    $("#status-import-enqueued").textContent = String(importEnqueued);
    $("#status-import-failed").textContent = String(importFailed);
    $("#status-import-progress-bar").style.width = `${importPercent}%`;
    $("#status-import-progress-text").textContent = importSummaryParts.join(" · ");
    const downloadGroup = $("#status-group-download");
    if (downloadGroup) {
      downloadGroup.classList.remove("hidden");
    }

    const watcherStatus = data.watcher_status || {};
    const watcherStateMap = {
      idle: "Idle",
      polling: "Polling",
      waiting_quiet_window: "Waiting (quiet window)",
      batch_ready: "Batch ready",
      running_batch: "Running batch",
      paused_import: "Paused (playlist import active)",
      paused_downtime: "Paused (downtime)",
      disabled: "Disabled",
    };
    const watcherState = watcherStatus.state || (watcher.enabled ? "idle" : "disabled");
    $("#watcher-state").textContent = watcherStateMap[watcherState] || watcherState;
    const pendingCount = Number.isFinite(watcherStatus.pending_playlists_count)
      ? watcherStatus.pending_playlists_count
      : 0;
    $("#watcher-pending").textContent = String(pendingCount);
    $("#watcher-batch").textContent = watcherStatus.batch_active ? "Active" : "Inactive";
    $("#watcher-last-poll").textContent = watcherStatus.last_poll_ts
      ? formatTimestamp(watcherStatus.last_poll_ts)
      : "-";
    if (watcherStatus.next_poll_ts) {
      const countdown = formatCountdown(data.server_time, watcherStatus.next_poll_ts);
      const suffix = countdown ? ` (in ${countdown})` : "";
      $("#watcher-next-poll").textContent = `${formatTimestamp(watcherStatus.next_poll_ts)}${suffix}`;
    } else {
      $("#watcher-next-poll").textContent = "-";
    }
    if (Number.isFinite(watcherStatus.quiet_window_remaining_sec)) {
      $("#watcher-quiet-remaining").textContent = formatDuration(watcherStatus.quiet_window_remaining_sec);
    } else {
      $("#watcher-quiet-remaining").textContent = "-";
    }
    if (status.last_completed) {
      const suffix = status.last_completed_at ? ` (${formatTimestamp(status.last_completed_at)})` : "";
      $("#status-last-completed").textContent = `${status.last_completed}${suffix}`;
    } else {
      $("#status-last-completed").textContent = "-";
    }
    if (activeQueueCount > 0) {
      const queueLeadDownloading = downloadingJobs[0] || null;
      const leadDownloaded = toFiniteNumber(queueLeadDownloading?.progress_downloaded_bytes);
      const leadTotal = toFiniteNumber(queueLeadDownloading?.progress_total_bytes);
      const leadPercentRaw = toFiniteNumber(queueLeadDownloading?.progress_percent);
      const leadPercent = leadPercentRaw !== null
        ? Math.max(0, Math.min(100, Math.round(leadPercentRaw)))
        : ((leadDownloaded !== null && leadTotal !== null && leadTotal > 0)
          ? Math.max(0, Math.min(100, Math.round((leadDownloaded / leadTotal) * 100)))
          : null);
      const queuePendingCount = queuedCount + claimedCount;
      const summaryParts = [
        `${downloadingCount} downloading`,
        `${queuePendingCount} pending`,
      ];
      if (postprocessingCount > 0) {
        summaryParts.push(`${postprocessingCount} postprocessing`);
      }
      if (leadPercent !== null) {
        summaryParts.push(`${leadPercent}% lead`);
      }
      $("#status-playlist-progress-text").textContent = summaryParts.join(" · ");
      $("#status-playlist-progress-bar").style.width =
        leadPercent !== null ? `${leadPercent}%` : "0%";
    } else if (Number.isFinite(status.progress_total) && Number.isFinite(status.progress_current)) {
      const percent = Number.isFinite(status.progress_percent)
        ? status.progress_percent
        : (status.progress_total > 0
          ? Math.round((status.progress_current / status.progress_total) * 100)
          : 0);
      $("#status-playlist-progress-text").textContent =
        `${status.progress_current}/${status.progress_total} (${percent}%)`;
      $("#status-playlist-progress-bar").style.width = `${Math.max(0, Math.min(100, percent))}%`;
    } else {
      $("#status-playlist-progress-text").textContent = "-";
      $("#status-playlist-progress-bar").style.width = "0%";
    }
    const transferGroup = $("#status-group-transfer");
    const hasTransferProgress = (
      activeQueueCount > 0 ||
      importActive ||
      Boolean(status.last_completed) ||
      (Number.isFinite(status.progress_total) && Number.isFinite(status.progress_current))
    );
    if (transferGroup) {
      transferGroup.classList.toggle("hidden", !hasTransferProgress);
    }

    const videoContainer = $("#status-video-progress");
    const queueHead = downloadingJobs[0] || null;
    const downloaded = queueHead
      ? toFiniteNumber(queueHead.progress_downloaded_bytes)
      : toFiniteNumber(status.video_downloaded_bytes);
    const total = queueHead
      ? toFiniteNumber(queueHead.progress_total_bytes)
      : toFiniteNumber(status.video_total_bytes);
    const speedBps = queueHead
      ? toFiniteNumber(queueHead.progress_speed_bps)
      : toFiniteNumber(status.video_speed);
    const etaSeconds = queueHead
      ? toFiniteNumber(queueHead.progress_eta_seconds)
      : toFiniteNumber(status.video_eta);
    let videoPercent = queueHead
      ? toFiniteNumber(queueHead.progress_percent)
      : toFiniteNumber(status.video_progress_percent);
    if (videoPercent === null && downloaded !== null && total !== null && total > 0) {
      videoPercent = Math.round((downloaded / total) * 100);
    }
    const hasVideoProgress = activeQueueCount > 0 && queueHead && (
      videoPercent !== null ||
      downloaded !== null ||
      total !== null
    );
    if (hasVideoProgress) {
      videoContainer.classList.remove("hidden");
      $("#status-video-progress-text").textContent =
        videoPercent !== null ? `${videoPercent}%` : "-";
      $("#status-video-progress-bar").style.width =
        videoPercent !== null ? `${Math.max(0, Math.min(100, videoPercent))}%` : "0%";
      const downloadedText = downloaded !== null ? formatBytes(downloaded) : "-";
      const totalText = total !== null ? formatBytes(total) : "-";
      const speedText = speedBps !== null ? formatSpeed(speedBps) : "-";
      const etaText = etaSeconds !== null ? formatDuration(etaSeconds) : "-";
      const sourceText = queueHead
        ? [queueHead.status, queueHead.source, queueHead.media_intent].filter(Boolean).join(" · ")
        : "current";
      $("#status-video-progress-meta").textContent =
        `${sourceText} · ${downloadedText} / ${totalText} · ${speedText} · ETA ${etaText}`;
    } else {
      videoContainer.classList.add("hidden");
      $("#status-video-progress-text").textContent = "-";
      $("#status-video-progress-bar").style.width = "0%";
      $("#status-video-progress-meta").textContent = "-";
    }

    const cancelActiveBtn = $("#status-cancel-active");
    if (cancelActiveBtn) {
      cancelActiveBtn.disabled = !(claimedCount > 0 || downloadingCount > 0 || postprocessingCount > 0);
    }
    const recoverStaleBtn = $("#status-recover-stale");
    if (recoverStaleBtn) {
      recoverStaleBtn.disabled = !(staleTotal > 0);
    }
    const clearFailedBtn = $("#status-clear-failed");
    if (clearFailedBtn) {
      clearFailedBtn.disabled = !(failedCount > 0 || cancelledCount > 0);
    }
    const clearQueueBtn = $("#status-clear-queue");
    if (clearQueueBtn) {
      clearQueueBtn.disabled = !(activeQueueCount > 0);
    }
    if (!state.playlistImportInProgress) {
      const activeImport = !!importState.active;
      setPlaylistImportControlsEnabled(!activeImport);
    }

    try {
      if (spotifyOperatorsEnabled) {
        const spotifyStatus = await fetchJson("/api/spotify/status");
        const oauthConnected = !!spotifyStatus.oauth_connected;
        const oauthEl = $("#spotify-status-oauth");
        if (oauthEl) {
          oauthEl.textContent = oauthConnected ? "Connected" : "Not connected";
        }
        const likedEl = $("#spotify-status-liked");
        if (likedEl) {
          if (spotifyStatus.liked_sync_running) {
            likedEl.textContent = "Running...";
            likedEl.classList.add("running");
          } else {
            likedEl.classList.remove("running");
            likedEl.textContent = formatTimestamp(spotifyStatus.last_liked_sync) || "-";
          }
        }
        const savedEl = $("#spotify-status-saved");
        if (savedEl) {
          if (spotifyStatus.saved_sync_running) {
            savedEl.textContent = "Running...";
            savedEl.classList.add("running");
          } else {
            savedEl.classList.remove("running");
            savedEl.textContent = formatTimestamp(spotifyStatus.last_saved_sync) || "-";
          }
        }
        const playlistsEl = $("#spotify-status-playlists");
        if (playlistsEl) {
          if (spotifyStatus.playlists_sync_running) {
            playlistsEl.textContent = "Running...";
            playlistsEl.classList.add("running");
          } else {
            playlistsEl.classList.remove("running");
            playlistsEl.textContent = formatTimestamp(spotifyStatus.last_playlists_sync) || "-";
          }
        }
      }
    } catch (err) {
      // Best-effort status enrichment; ignore when endpoint is unavailable.
    }
    await refreshMusicFailures();
  } catch (err) {
    setNotice($("#home-search-message"), `Status error: ${err.message}`, true);
  }
}

async function refreshMusicFailures() {
  const countEl = $("#music-failures-count");
  const listEl = $("#music-failures-list");
  const messageEl = $("#music-failures-message");
  if (!countEl || !listEl) {
    return;
  }
  if (messageEl) {
    messageEl.textContent = "";
    messageEl.classList.remove("error");
  }
  try {
    const data = await fetchJson("/api/music/failures?limit=25");
    const rows = Array.isArray(data?.rows) ? data.rows : [];
    countEl.textContent = String(Number.isFinite(Number(data?.count)) ? Number(data.count) : rows.length);
    listEl.textContent = "";
    if (!rows.length) {
      const empty = document.createElement("div");
      empty.className = "meta";
      empty.textContent = "No music failures recorded.";
      listEl.appendChild(empty);
      return;
    }
    rows.forEach((row) => {
      const item = document.createElement("div");
      item.className = "meta";
      const who = [row.artist, row.track].filter(Boolean).join(" - ") || row.last_query || "Unknown track";
      const reasons = Array.isArray(row.reasons) && row.reasons.length ? row.reasons.join(", ") : "unknown_reason";
      const when = formatTimestamp(row.created_at) || row.created_at || "-";
      const batch = row.origin_batch_id ? `batch=${row.origin_batch_id}` : "batch=-";
      item.textContent = `${when} | ${batch} | ${who} | reason=${reasons}`;
      listEl.appendChild(item);
    });
  } catch (err) {
    countEl.textContent = "-";
    listEl.textContent = "";
    const failed = document.createElement("div");
    failed.className = "meta";
    failed.textContent = `Failed to load music failures: ${err.message}`;
    listEl.appendChild(failed);
    if (messageEl) {
      messageEl.textContent = "Failed to refresh music failures.";
      messageEl.classList.add("error");
    }
  }
}

async function clearMusicFailures() {
  const messageEl = $("#music-failures-message");
  const clearBtn = $("#music-failures-clear");
  const ok = confirm("Clear stored music failure diagnostics?");
  if (!ok) {
    return;
  }
  try {
    if (clearBtn) {
      clearBtn.disabled = true;
    }
    if (messageEl) {
      messageEl.textContent = "Clearing music failures...";
      messageEl.classList.remove("error");
    }
    let result = null;
    try {
      result = await fetchJson("/api/music/failures", { method: "DELETE" });
    } catch (_err) {
      result = await fetchJson("/api/music/failures/clear", { method: "POST" });
    }
    const deleted = Number.isFinite(Number(result?.deleted)) ? Number(result.deleted) : 0;
    if (messageEl) {
      messageEl.textContent = `Cleared ${deleted} music failure record(s).`;
      messageEl.classList.remove("error");
    }
    await refreshMusicFailures();
  } catch (err) {
    if (messageEl) {
      messageEl.textContent = `Clear music failures failed: ${err.message}`;
      messageEl.classList.add("error");
    }
  } finally {
    if (clearBtn) {
      clearBtn.disabled = false;
    }
  }
}

// Expose these handlers for inline fallback and defensive delegated binding.
window.refreshMusicFailures = refreshMusicFailures;
window.clearMusicFailures = clearMusicFailures;

async function refreshLogs() {
  const lines = parseInt($("#logs-lines").value, 10) || 200;
  const outputEl = $("#logs-output");
  if (!outputEl) {
    return;
  }
  try {
    const response = await fetch(`/api/logs?lines=${lines}`);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${response.status} ${text}`);
    }
    const text = await response.text();
    const shouldStick = !!state.logsStickToBottom;
    if (text !== state.lastLogsText) {
      outputEl.textContent = text;
      state.lastLogsText = text;
    }
    if (shouldStick) {
      requestAnimationFrame(() => {
        outputEl.scrollTop = outputEl.scrollHeight;
      });
    }
  } catch (err) {
    outputEl.textContent = `Failed to load logs: ${err.message}`;
  }
}

async function refreshHistory() {
  const limit = parseInt($("#history-limit").value, 10) || 50;
  try {
    const params = new URLSearchParams();
    params.set("limit", String(limit));
    const search = $("#history-search").value.trim();
    if (search) {
      params.set("search", search);
    }
    const playlist = $("#history-playlist").value.trim();
    if (playlist) {
      params.set("playlist_id", playlist);
    }
    const dateFrom = $("#history-from").value;
    if (dateFrom) {
      params.set("date_from", dateFrom);
    }
    const dateTo = $("#history-to").value;
    if (dateTo) {
      params.set("date_to", dateTo);
    }
    const sortBy = $("#history-sort").value;
    if (sortBy) {
      params.set("sort_by", sortBy);
    }
    const sortDir = $("#history-dir").value;
    if (sortDir) {
      params.set("sort_dir", sortDir);
    }

    const rows = await fetchJson(`/api/history?${params.toString()}`);
    const key = JSON.stringify(rows);
    if (key === state.lastHistoryKey) {
      return;
    }
    state.lastHistoryKey = key;
    const body = $("#history-body");
    body.textContent = "";
    const showPaths = $("#history-show-paths").checked;
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const downloadHref = row.file_id ? downloadUrl(row.file_id) : "";
      const downloadButton = row.file_id
        ? `<a class="button ghost small" href="${downloadHref}">⬇ Download</a>`
        : `<span class="meta">-</span>`;
      const videoUrl = row.video_url || (row.video_id ? `https://www.youtube.com/watch?v=${row.video_id}` : "");
      const copyUrlButton = videoUrl
        ? `<button class="button ghost small" data-copy="url" data-value="${encodeURIComponent(videoUrl)}">Copy URL</button>`
        : "";
      const pathDisplay = displayPath(row.filepath || "", BROWSE_DEFAULTS.mediaRoot, showPaths);
      const copyPathButton = showPaths && row.filepath
        ? `<button class="button ghost small" data-copy="path" data-value="${encodeURIComponent(row.filepath)}">Copy Path</button>`
        : "";
      const jsonPayload = encodeURIComponent(JSON.stringify(row, null, 2));
      tr.innerHTML = `
        <td>${row.video_id || ""}</td>
        <td>${row.playlist_id || ""}</td>
        <td>${formatTimestamp(row.downloaded_at) || ""}</td>
        <td>${pathDisplay}</td>
        <td>
          <div class="action-group">
            ${downloadButton}
            ${copyUrlButton}
            ${copyPathButton}
            <button class="button ghost small" data-copy="json" data-value="${jsonPayload}">Copy JSON</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    const body = $("#history-body");
    body.textContent = "";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5">Failed to load history: ${err.message}</td>`;
    body.appendChild(tr);
  }
}

async function refreshMetrics() {
  try {
    const data = await fetchJson("/api/metrics");
    $("#metrics-downloads-count").textContent = data.downloads_files ?? "-";
    $("#metrics-downloads-size").textContent = formatBytes(data.downloads_bytes);
    const free = formatBytes(data.disk_free_bytes);
    const total = formatBytes(data.disk_total_bytes);
    const percent = Number.isFinite(data.disk_free_percent) ? ` (${data.disk_free_percent}%)` : "";
    $("#metrics-disk-free").textContent = free ? `${free}${percent}` : "-";
    $("#metrics-disk-total").textContent = total || "-";
    const message = $("#metrics-message");
    message.classList.remove("warn", "critical");
    if (Number.isFinite(data.disk_free_percent)) {
      if (data.disk_free_percent < 5) {
        message.textContent = "Warning: disk space below 5%";
        message.classList.add("critical");
      } else if (data.disk_free_percent < 10) {
        message.textContent = "Warning: disk space below 10%";
        message.classList.add("warn");
      } else {
        message.textContent = "";
      }
    } else {
      message.textContent = "";
    }
  } catch (err) {
    const message = $("#metrics-message");
    message.classList.remove("warn", "critical");
    message.textContent = `Metrics error: ${err.message}`;
  }
}

async function refreshSchedule() {
  try {
    const data = await fetchJson("/api/schedule");
    const schedule = data.schedule || {};
    $("#schedule-enabled").checked = !!schedule.enabled;
    $("#schedule-interval").value = schedule.interval_hours ?? 6;
    $("#schedule-startup").checked = !!schedule.run_on_startup;
    if (state.config && typeof state.config === "object") {
      state.config.schedule = {
        enabled: !!schedule.enabled,
        mode: "interval",
        interval_hours: Number.isFinite(Number(schedule.interval_hours)) ? Number(schedule.interval_hours) : 6,
        run_on_startup: !!schedule.run_on_startup,
      };
    }
    $("#schedule-last-run").textContent = data.last_run ? formatTimestamp(data.last_run) : "-";
    $("#schedule-next-run").textContent = data.next_run ? formatTimestamp(data.next_run) : "-";
    syncConfigSectionCollapsedStates();
    setNotice($("#schedule-message"), "", false);
  } catch (err) {
    setNotice($("#schedule-message"), `Schedule error: ${err.message}`, true);
  }
}

function buildSchedulePayloadFromForm() {
  const enabledEl = $("#schedule-enabled");
  const intervalEl = $("#schedule-interval");
  const startupEl = $("#schedule-startup");
  const fallbackSchedule = (state.config && typeof state.config.schedule === "object")
    ? state.config.schedule
    : {};
  const interval = parseInt(intervalEl?.value || "", 10);
  const fallbackInterval = Number.isFinite(Number(fallbackSchedule.interval_hours))
    ? Number(fallbackSchedule.interval_hours)
    : 6;
  return {
    enabled: !!enabledEl?.checked,
    mode: "interval",
    interval_hours: Number.isFinite(interval) ? Math.max(1, interval) : Math.max(1, fallbackInterval),
    run_on_startup: !!startupEl?.checked,
  };
}

async function saveSchedule() {
  const payload = buildSchedulePayloadFromForm();
  try {
    const data = await fetchJson("/api/schedule", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (state.config && typeof state.config === "object") {
      const returnedSchedule = data?.schedule || payload;
      state.config.schedule = {
        enabled: !!returnedSchedule.enabled,
        mode: "interval",
        interval_hours: Number.isFinite(Number(returnedSchedule.interval_hours))
          ? Number(returnedSchedule.interval_hours)
          : payload.interval_hours,
        run_on_startup: !!returnedSchedule.run_on_startup,
      };
    }
    setNotice($("#schedule-message"), "Schedule updated", false);
    await refreshSchedule();
  } catch (err) {
    setNotice($("#schedule-message"), `Schedule update failed: ${err.message}`, true);
  }
}

async function runScheduleNow() {
  try {
    setNotice($("#schedule-message"), "Starting run...", false);
    await fetchJson("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    setNotice($("#schedule-message"), "Run started", false);
    await refreshStatus();
  } catch (err) {
    setNotice($("#schedule-message"), `Run failed: ${err.message}`, true);
  }
}

async function refreshDownloads() {
  try {
    const search = ($("#downloads-search")?.value || "").trim().toLowerCase();
    const limitRaw = parseInt($("#downloads-limit")?.value, 10);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 50;
    const rows = await fetchJson("/api/files");
    const key = JSON.stringify({ rows, search, limit });
    if (key === state.lastFilesKey) {
      return;
    }
    state.lastFilesKey = key;
    const body = $("#downloads-body");
    body.textContent = "";
    const filtered = search
      ? rows.filter((row) => {
        const hay = `${row.relative_path || ""} ${row.name || ""}`.toLowerCase();
        return hay.includes(search);
      })
      : rows;
    const sliced = filtered.slice(0, limit);
    if (!sliced.length) {
      const tr = document.createElement("tr");
      const label = search ? "No downloads match this filter." : "No downloads found.";
      tr.innerHTML = `<td colspan="4">${label}</td>`;
      body.appendChild(tr);
      return;
    }
    sliced.forEach((row) => {
      const tr = document.createElement("tr");
      const downloadHref = downloadUrl(row.id);
      const copyUrl = encodeURIComponent(downloadHref);
      tr.innerHTML = `
        <td>${row.relative_path || row.name || ""}</td>
        <td>${formatTimestamp(row.modified_at) || ""}</td>
        <td>${formatBytes(row.size_bytes)}</td>
        <td>
          <div class="action-group">
            <a class="button ghost small" href="${downloadHref}">⬇ Download</a>
            <button class="button ghost small" data-copy="url" data-value="${copyUrl}">Copy URL</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    const body = $("#downloads-body");
    body.textContent = "";
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="4">Failed to load downloads: ${err.message}</td>`;
    body.appendChild(tr);
  }
}



function getSearchSourcePriority() {
  const list = $("#search-source-list");
  if (!list) return [];
  return Array.from(list.querySelectorAll(".source-priority-row"))
    .map((row) => {
      const enabled = row.querySelector(".source-enabled");
      return enabled && enabled.checked ? row.dataset.source : null;
    })
    .filter(Boolean);
}

function updateSearchSortLabel() {
  const button = $("#search-requests-sort");
  if (!button) return;
  const label = state.searchRequestsSort === "asc" ? "Sort: Oldest" : "Sort: Newest";
  button.textContent = label;
}

function moveSourcePriorityRow(row, direction) {
  if (!row) return;
  const parent = row.parentElement;
  if (!parent) return;
  if (direction === "up") {
    const prev = row.previousElementSibling;
    if (prev) {
      parent.insertBefore(row, prev);
    }
  } else if (direction === "down") {
    const next = row.nextElementSibling;
    if (next) {
      parent.insertBefore(next, row);
    }
  }
}

function buildSearchRequestPayload(sources, { autoEnqueue = true } = {}) {
  const artist = $("#search-artist").value.trim();
  const album = $("#search-album").value.trim();
  const track = $("#search-track").value.trim();
  const intent = $("#search-intent").value || "track";
  const minScoreRaw = parseFloat($("#search-min-score").value);
  const maxCandidatesNode = $("#search-max-candidates");
  const maxCandidatesRaw = maxCandidatesNode ? parseInt(maxCandidatesNode.value, 10) : NaN;
  const destination = $("#search-destination").value.trim();

  return {
    intent,
    media_type: "music",
    artist,
    album: album || null,
    track: track || null,
    destination_dir: destination || null,
    include_albums: $("#search-include-albums").checked,
    include_singles: $("#search-include-singles").checked,
    lossless_only: $("#search-lossless-only").checked,
    auto_enqueue: autoEnqueue,
    min_match_score: Number.isFinite(minScoreRaw) ? minScoreRaw : 0.92,
    max_candidates_per_source: Number.isFinite(maxCandidatesRaw) ? maxCandidatesRaw : 5,
    source_priority: sources && sources.length ? sources : null,
  };
}



function getSearchDefaultDestination() {
  if (state.config && state.config.home_music_download_folder) {
    return state.config.home_music_download_folder;
  }
  if (state.config && state.config.music_download_folder) {
    return state.config.music_download_folder;
  }
  if (state.config && state.config.single_download_folder) {
    return state.config.single_download_folder;
  }
  return "";
}

function updateSearchDestinationDisplay() {
  const el = $("#search-default-destination");
  if (!el) return;
  const value = getSearchDefaultDestination();
  el.textContent = value || "-";
  const resolvedEl = $("#search-destination-resolved");
  if (resolvedEl) {
    const customValue = $("#search-destination")?.value.trim();
    const resolved = computeResolvedDestinationPath(customValue);
    resolvedEl.textContent = `Resolved: ${resolved}`;
  }
}
function setSearchSelectedRequest(requestId) {
  state.searchSelectedRequestId = requestId || null;
  const label = $("#search-selected-request");
  if (label) {
    label.textContent = requestId || "-";
  }
}

function setSearchSelectedItem(itemId) {
  state.searchSelectedItemId = itemId || null;
  const label = $("#search-selected-item");
  if (label) {
    label.textContent = itemId || "-";
  }
}

function renderSearchEmptyRow(body, colspan, message) {
  if (!body) return;
  body.textContent = "";
  const tr = document.createElement("tr");
  tr.innerHTML = `<td colspan="${colspan}">${message}</td>`;
  body.appendChild(tr);
}

function getHomeSourcePriority() {
  const panel = $("#home-source-panel");
  if (!panel) return null;

  const checked = Array.from(
    panel.querySelectorAll("input[type=checkbox][data-source]:checked")
  ).map((el) => el.dataset.source);

  if (!checked.length) {
    return null;
  }
  // Return explicit checked list even when all are selected so custom sources
  // are included without relying on static fallback defaults.
  return checked;
}

function updateHomeSourceToggleLabel() {
  const toggle = $("#home-source-toggle");
  const panel = $("#home-source-panel");
  if (!toggle || !panel) return;
  const inputs = Array.from(
    panel.querySelectorAll("input[type=checkbox][data-source]")
  );
  if (!inputs.length) return;
  const checked = inputs.filter((input) => input.checked);
  inputs.forEach((input) => {
    const label = input.closest("label");
    if (label) {
      label.classList.toggle("selected", input.checked);
    }
  });
  if (checked.length === inputs.length) {
    toggle.textContent = "Sources: All";
    return;
  }
  if (!checked.length) {
    toggle.textContent = "Sources: None";
    return;
  }
  const labels = checked.map((input) => formatSourceLabel(input.dataset.source));
  const summary = labels.length <= 2 ? labels.join(", ") : `${labels.length} selected`;
  toggle.textContent = `Sources: ${summary}`;
}

function parseHomeSearchQuery(value, preferAlbum) {
  const trimmed = (value || "").trim();
  if (!trimmed) {
    return null;
  }
  const isUrl = /^https?:\/\//i.test(trimmed);
  let artist = trimmed;
  let album = null;
  let track = trimmed;
  let intent = preferAlbum ? "album" : "track";
  const separators = [" - ", " / ", ":"];
  const separator = separators.find((sep) => trimmed.includes(sep));
  if (separator && !isUrl) {
    const [first, second] = trimmed.split(separator);
    artist = first.trim() || trimmed;
    const remainder = second.trim();
    if (preferAlbum) {
      intent = "album";
      album = remainder || trimmed;
      track = "";
    } else {
      intent = "track";
      track = remainder || trimmed;
    }
  } else if (isUrl) {
    intent = "track";
    artist = trimmed;
    track = trimmed;
  } else {
    if (preferAlbum) {
      intent = "album";
      album = trimmed;
      track = "";
    } else {
      intent = "track";
      track = trimmed;
    }
  }
  if (intent === "track" && !track) {
    track = artist || trimmed;
  }
  return {
    intent,
    artist: artist || trimmed,
    album: album || null,
    track: track || null,
  };
}

function homeMusicDebugEnabled() {
  try {
    return localStorage.getItem(HOME_MUSIC_DEBUG_KEY) === "1";
  } catch (_err) {
    return false;
  }
}

function homeMusicDebugLog(...args) {
  if (!homeMusicDebugEnabled()) {
    return;
  }
  console.debug(...args);
}

function getMusicModeFinalFormatOverride() {
  const activeMode = normalizeHomeMediaMode(state.homeMediaMode);
  if (activeMode !== "music_video" && activeMode !== "music") {
    return null;
  }
  const selector = document.getElementById("music-video-final-format");
  const value = String(selector?.value || "").trim().toLowerCase();
  if (!value || value === "default") {
    return null;
  }
  const allowed = activeMode === "music" ? HOME_MUSIC_MODE_FORMATS : HOME_MUSIC_VIDEO_MODE_FORMATS;
  if (!allowed.includes(value)) {
    return null;
  }
  return value;
}

function placeHomeDeliveryControl() {
  const field = $("#home-delivery-field");
  if (!field) return;
  const target = state.homeMusicMode ? $("#home-delivery-music-slot") : $("#home-delivery-video-slot");
  if (!target) return;
  if (field.parentElement !== target) {
    target.appendChild(field);
  }
  field.classList.toggle("field", !!state.homeMusicMode);
  field.classList.toggle("music-search-delivery-field", !!state.homeMusicMode);
  field.classList.toggle("home-advanced-field", !state.homeMusicMode);
}

function placeHomeDestinationControl() {
  const field = $("#home-destination-field");
  if (!field) return;
  const target = state.homeMusicMode ? $("#home-destination-music-slot") : $("#home-destination-video-slot");
  if (!target) return;
  if (field.parentElement !== target) {
    target.appendChild(field);
  }
  field.classList.toggle("field", !!state.homeMusicMode);
  field.classList.toggle("medium", !!state.homeMusicMode);
  field.classList.toggle("home-advanced-field", !state.homeMusicMode);
}

function updateHomeMusicModeUI() {
  const modeSelect = $("#home-media-mode");
  if (modeSelect) {
    modeSelect.value = state.homeMediaMode || "video";
  }
  const modeToggle = $("#home-media-mode-toggle");
  if (modeToggle) {
    modeToggle.querySelectorAll("button[data-mode]").forEach((button) => {
      const isActive = button.dataset.mode === (state.homeMediaMode || "video");
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-checked", isActive ? "true" : "false");
    });
  }
  const standardSearchContainer = $("#standard-search-container");
  if (standardSearchContainer) {
    standardSearchContainer.classList.toggle("hidden", !!state.homeMusicMode);
  }
  const musicModeConsole = $("#music-mode-console");
  if (musicModeConsole) {
    musicModeConsole.classList.toggle("hidden", !state.homeMusicMode);
  }
  placeHomeDeliveryControl();
  placeHomeDestinationControl();
  updateMusicModeFormatControl();
  applyHomeDefaultDestination();
  if (!state.homeMusicMode) {
    applyHomeDefaultVideoFormat();
  }
  // Keep this badge strictly tied to the live toggle state.
  const badge = $("#home-music-mode-badge");
  if (badge) {
    const toggleEnabled = state.homeMediaMode !== "video";
    const resultsVisible = !$("#home-results")?.classList.contains("hidden");
    badge.classList.toggle("hidden", !(toggleEnabled && state.homeMusicMode && resultsVisible));
  }
}

function updateMusicModeToggleUI(mode) {
  const modeSelect = $("#music-mode-select");
  if (modeSelect) {
    modeSelect.value = String(mode || "auto").trim().toLowerCase() || "auto";
  }
  const modeToggle = $("#music-mode-toggle");
  if (!modeToggle) {
    return;
  }
  const normalized = String(mode || "auto").trim().toLowerCase() || "auto";
  modeToggle.querySelectorAll("button[data-mode]").forEach((button) => {
    const isActive = String(button.dataset.mode || "").trim().toLowerCase() === normalized;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-checked", isActive ? "true" : "false");
  });
}

function setMusicModeSelection(mode) {
  const normalized = String(mode || "auto").trim().toLowerCase() || "auto";
  const allowed = new Set(["auto", "artist", "album", "track"]);
  updateMusicModeToggleUI(allowed.has(normalized) ? normalized : "auto");
}

function normalizeHomeMediaMode(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "video" || normalized === "music" || normalized === "music_video") {
    return normalized;
  }
  return "video";
}

function loadHomeMediaModePreference() {
  try {
    const saved = localStorage.getItem(HOME_MUSIC_MODE_KEY);
    if (!saved) {
      return "video";
    }
    if (saved === "true") {
      return "music";
    }
    if (saved === "false") {
      return "video";
    }
    return normalizeHomeMediaMode(saved);
  } catch (_err) {
    return "video";
  }
}

function setHomeMediaMode(mode, { persist = true, clearResultsOnDisable = true } = {}) {
  const nextMode = normalizeHomeMediaMode(mode);
  const previous = !!state.homeMusicMode;
  state.homeMediaMode = nextMode;
  state.homeMusicMode = nextMode !== "video";
  updateHomeMusicModeUI();
  updateHomeDeliveryModeUI();
  refreshHomeSourceOptions();
  if (previous !== state.homeMusicMode) {
    clearMusicResultsHistory();
  }
  if (previous && !state.homeMusicMode && clearResultsOnDisable) {
    // Invalidate any in-flight music metadata responses so stale results cannot render.
    state.homeMusicSearchSeq += 1;
    const results = document.getElementById("music-results-container");
    if (results) {
      results.innerHTML = "";
    }
    const musicMessage = document.getElementById("home-search-message");
    if (musicMessage) {
      setNotice(musicMessage, "Press Enter or click Search to discover media", false);
    }
    const albumDetails = document.getElementById("home-album-failed-details");
    if (albumDetails) {
      albumDetails.innerHTML = "";
      albumDetails.classList.add("hidden");
    }
    clearLegacyHomeSearchState();
  }
  if (persist) {
    saveHomeMusicModePreference();
  }
}

function setHomeMusicMode(enabled, { persist = true, clearResultsOnDisable = true } = {}) {
  setHomeMediaMode(enabled ? "music" : "video", { persist, clearResultsOnDisable });
}

function saveHomeMusicModePreference() {
  localStorage.setItem(HOME_MUSIC_MODE_KEY, state.homeMediaMode || "video");
}

function getHomeDeliveryMode() {
  const value = String($("#home-delivery-mode")?.value || "server").trim().toLowerCase();
  return value === "client" ? "client" : "server";
}

function updateHomeDeliveryModeUI() {
  const mode = getHomeDeliveryMode();
  const destinationField = $("#home-destination-field");
  const destinationInput = $("#home-destination");
  const browseButton = $("#home-destination-browse");
  if (destinationField) {
    destinationField.classList.toggle("hidden", mode === "client");
  }
  if (browseButton) {
    browseButton.disabled = mode === "client";
  }
  if (mode === "client" && destinationInput) {
    destinationInput.value = "";
  }
  if (mode === "client") {
    updateHomeDestinationResolved();
  }

  const downloadButton = $("#home-search-download");
  if (downloadButton) {
    const blocked = mode === "client";
    downloadButton.disabled = blocked || !state.homeSearchControlsEnabled;
    downloadButton.setAttribute("aria-disabled", String(downloadButton.disabled));
    downloadButton.title = blocked
      ? "Search & Download is unavailable for client delivery. Use Search and click Download on a result."
      : "";
  }

  const toggle = $("#home-delivery-toggle");
  if (toggle) {
    toggle.querySelectorAll("button[data-mode]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.mode === mode);
      btn.setAttribute("aria-pressed", String(btn.dataset.mode === mode));
    });
  }
}

function bindHomeDeliveryToggle(element) {
  if (!element) return;
  element.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-mode]");
    if (!button) return;
    setHomeDeliveryMode(button.dataset.mode || "server");
  });
}

function updateMusicForceToggleUI() {
  const checkbox = document.getElementById("music-force-redownload");
  const toggle = document.getElementById("music-force-toggle");
  if (!checkbox || !toggle) {
    return;
  }
  const enabled = !!checkbox.checked;
  toggle.querySelectorAll("button[data-force]").forEach((button) => {
    const isOn = String(button.dataset.force || "").trim().toLowerCase() === "on";
    const isActive = enabled ? isOn : !isOn;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  });
}

function bindMusicForceToggle(element) {
  const checkbox = document.getElementById("music-force-redownload");
  if (!element || !checkbox) return;
  element.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-force]");
    if (!button) return;
    checkbox.checked = String(button.dataset.force || "").trim().toLowerCase() === "on";
    updateMusicForceToggleUI();
  });
  checkbox.addEventListener("change", updateMusicForceToggleUI);
  updateMusicForceToggleUI();
}

function getMusicModeDestinationValue() {
  return $("#home-destination")?.value.trim() || "";
}

function getMusicModeDeliveryMode() {
  return getHomeDeliveryMode();
}

function assertMusicModeDeliveryAllowed(messageEl) {
  const mode = getMusicModeDeliveryMode();
  const destination = getMusicModeDestinationValue();
  if (mode === "client" && destination) {
    setNotice(messageEl || $("#home-search-message"), "Client delivery does not use a server destination.", true);
    return false;
  }
  return true;
}

function setHomeDeliveryMode(mode) {
  const normalized = mode === "client" ? "client" : "server";
  const input = $("#home-delivery-mode");
  if (input) {
    input.value = normalized;
  }
  updateHomeDeliveryModeUI();
}

function buildHomeSearchPayload(autoEnqueue, rawQuery = "") {
  const preferAlbum = $("#home-prefer-albums")?.checked;
  const parsed = parseHomeSearchQuery($("#home-search-input")?.value, preferAlbum);
  if (!parsed) {
    throw new Error("Enter an artist, track, album, or playlist URL");
  }
  const minScoreRaw = parseFloat($("#home-min-score")?.value);
  const destination = $("#home-destination")?.value.trim();
  const treatAsMusic = state.homeMediaMode === "music";
  const formatOverride = $("#home-format")?.value.trim();
  const deliveryMode = ($("#home-delivery-mode")?.value || "server").toLowerCase();
  const rawText = rawQuery || $("#home-search-input")?.value || "";
  const selectedSource = $("#home-search-source")?.value;
  let sources = getHomeSourcePriority();
  if (!sources && selectedSource === "auto" && !treatAsMusic) {
    const preferVideo = shouldPreferVideoAdapters(rawText);
    sources = preferVideo ? HOME_VIDEO_SOURCE_PRIORITY : HOME_GENERIC_SOURCE_PRIORITY;
  }
  return {
    query: rawText,
    intent: parsed.intent,
    media_type: treatAsMusic ? "music" : "generic",
    artist: parsed.artist,
    album: parsed.album,
    track: parsed.track,
    destination_dir: destination || null,
    destination_type: deliveryMode,
    destination_path: destination || null,
    delivery_mode: deliveryMode,
    include_albums: 1,
    include_singles: preferAlbum ? 0 : 1,
    min_match_score: Number.isFinite(minScoreRaw) ? minScoreRaw : 0.92,
    lossless_only: treatAsMusic ? 1 : 0,
    auto_enqueue: autoEnqueue,
    search_only: !autoEnqueue,
    music_mode: treatAsMusic,
    media_mode: state.homeMediaMode || "video",
    final_format: formatOverride || null,
    source_priority: sources && sources.length ? sources : null,
    max_candidates_per_source: 10,
  };
}

function showHomeResults(visible) {
  const section = $("#home-results");
  if (!section) return;
  section.classList.toggle("hidden", !visible);
  section.classList.remove("has-results");
  section.classList.remove("search-complete");
}

function setHomeSearchActive(active) {
  const shell = document.querySelector(".home-search-shell");
  document.body.classList.toggle("search-active", !!active);
  if (shell) {
    shell.classList.toggle("search-active", !!active);
  }
}

function setHomeResultsState({ hasResults = false, terminal = false } = {}) {
  const section = $("#home-results");
  if (!section) return;
  section.classList.toggle("has-results", !!hasResults);
  section.classList.toggle("search-complete", !!terminal);
}

function clearHomeEnqueueError(container) {
  if (!container) return;
  const existing = container.querySelector(".home-enqueue-error");
  if (existing) {
    existing.remove();
  }
}

function showHomeEnqueueError(container, message) {
  if (!container) return;
  let errorEl = container.querySelector(".home-enqueue-error");
  if (!errorEl) {
    errorEl = document.createElement("div");
    errorEl.className = "home-enqueue-error";
    container.appendChild(errorEl);
  }
  errorEl.textContent = message;
}

function updateHomeViewAdvancedLink() {
  const button = $("#home-view-advanced");
  if (!button) return;
  const enabled = !!state.homeSearchRequestId;
  button.disabled = !enabled;
  button.setAttribute("aria-disabled", (!enabled).toString());
}

function handleHomeViewAdvanced() {
  const requestId = state.homeSearchRequestId;
  if (!requestId) {
    return;
  }
  state.pendingAdvancedRequestId = requestId;
  window.location.hash = "#info";
}

function setHomeSearchControlsEnabled(enabled) {
  state.homeSearchControlsEnabled = enabled;
  const searchOnly = $("#home-search-only");
  if (searchOnly) {
    searchOnly.disabled = !enabled;
    searchOnly.setAttribute("aria-disabled", (!enabled).toString());
  }
  const download = $("#home-search-download");
  if (download) {
    const deliveryMode = getHomeDeliveryMode();
    const blockedByMode = deliveryMode === "client";
    download.disabled = !enabled || blockedByMode;
    download.setAttribute("aria-disabled", String(download.disabled));
    download.title = blockedByMode
      ? "Search & Download is unavailable for client delivery. Use Search and click Download on a result."
      : "";
  }
}

function maybeReleaseHomeSearchControls(requestId, requestStatus, hasVisibleCandidates = false) {
  if (!requestId) return;
  if (state.homeSearchRequestId !== requestId) {
    return;
  }
  if (hasVisibleCandidates) {
    setHomeSearchControlsEnabled(true);
    return;
  }
  if (HOME_FINAL_STATUSES.has(requestStatus)) {
    setHomeSearchControlsEnabled(true);
  }
}

function stopHomeResultPolling() {
  if (state.homeResultsTimer) {
    clearTimeout(state.homeResultsTimer);
    state.homeResultsTimer = null;
  }
  state.homeResultPollInFlight = false;
  state.homeSearchPollStart = null;
}

function setHomeResultsStatus(text) {
  const statusEl = $("#home-results-status-text");
  if (statusEl) {
    statusEl.textContent = text;
  }
}

function setHomeResultsDetail(text, isError = false) {
  const detailEl = $("#home-results-detail");
  if (!detailEl) {
    return;
  }
  if (!text) {
    detailEl.textContent = "";
    detailEl.classList.remove("home-results-error");
    detailEl.classList.add("hidden");
    return;
  }
  detailEl.textContent = text;
  detailEl.classList.toggle("home-results-error", isError);
  detailEl.classList.remove("hidden");
}

function clearHomeAlbumCandidates() {
  const existing = document.getElementById("home-album-candidates");
  if (existing) {
    existing.remove();
  }
}

function normalizeMusicSearchResults(rawResults) {
  if (!Array.isArray(rawResults)) {
    return [];
  }
  return rawResults
    .map((item) => {
      const recordingMbid = String(item?.recording_mbid || "").trim();
      const releaseMbid = String(item?.release_mbid || item?.mb_release_id || "").trim();
      const releaseGroupMbid = String(item?.release_group_mbid || item?.mb_release_group_id || "").trim();
      const artist = String(item?.artist || "").trim();
      const track = String(item?.track || "").trim();
      if (!recordingMbid || !releaseMbid || !releaseGroupMbid || !artist || !track) {
        return null;
      }
      const releaseDate = String(item?.release_date || item?.release_year || "").trim();
      const durationMs = Number(item?.duration_ms);
      const trackNumber = Number(item?.track_number);
      const discNumber = Number(item?.disc_number);
      return {
        recording_mbid: recordingMbid,
        mb_release_id: releaseMbid,
        mb_release_group_id: releaseGroupMbid,
        artist,
        track,
        album: String(item?.album || "").trim(),
        release_date: releaseDate,
        release_year: String(item?.release_year || "").trim(),
        track_number: Number.isFinite(trackNumber) ? trackNumber : null,
        disc_number: Number.isFinite(discNumber) ? discNumber : null,
        duration_ms: Number.isFinite(durationMs) ? durationMs : null,
        artwork_url: typeof item?.artwork_url === "string" ? item.artwork_url : null,
      };
    })
    .filter(Boolean);
}

function snapshotMusicResultsView(response = {}, query = "") {
  return {
    response: {
      artists: Array.isArray(response?.artists) ? [...response.artists] : [],
      albums: Array.isArray(response?.albums) ? [...response.albums] : [],
      tracks: Array.isArray(response?.tracks) ? [...response.tracks] : [],
      mode_used: response?.mode_used || "auto",
    },
    query: String(query || ""),
  };
}

function clearMusicResultsHistory() {
  state.homeMusicCurrentView = null;
  state.homeMusicViewStack = [];
}

async function enqueueAlbum(releaseGroupMbid) {
  if (!assertMusicModeDeliveryAllowed($("#home-search-message"))) {
    throw new Error("Client delivery does not use a server destination.");
  }
  const forceRedownload = !!document.getElementById("music-force-redownload")?.checked;
  return fetchJson("/api/music/album/download", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      release_group_mbid: releaseGroupMbid,
      destination: getMusicModeDestinationValue() || null,
      final_format: getMusicModeFinalFormatOverride(),
      delivery_mode: getMusicModeDeliveryMode(),
      music_mode: state.homeMediaMode === "music",
      media_mode: state.homeMediaMode === "music_video" ? "music_video" : "music",
      force_redownload: forceRedownload,
    }),
  });
}

function renderAlbumQueueSummary(result, { albumTitle = "Album" } = {}) {
  const messageEl = $("#home-search-message");
  if (!messageEl) {
    return;
  }
  const tracksConsidered = Number.isFinite(Number(result?.tracks_considered))
    ? Number(result.tracks_considered)
    : 0;
  const tracksEnqueued = Number.isFinite(Number(result?.tracks_enqueued))
    ? Number(result.tracks_enqueued)
    : 0;
  const duplicateTracksCount = Number.isFinite(Number(result?.duplicate_tracks_count))
    ? Number(result.duplicate_tracks_count)
    : 0;
  const failedTracksCount = Number.isFinite(Number(result?.failed_tracks_count))
    ? Number(result.failed_tracks_count)
    : 0;
  const duplicateTracks = Array.isArray(result?.duplicate_tracks) ? result.duplicate_tracks : [];
  const failedTracks = Array.isArray(result?.failed_tracks) ? result.failed_tracks : [];
  const summaryParts = [
    tracksConsidered > 0
      ? `Album queue result: ${tracksEnqueued}/${tracksConsidered} tracks queued`
      : `Album queue result: ${tracksEnqueued} tracks queued`,
  ];
  if (duplicateTracksCount > 0) {
    summaryParts.push(`Already queued/downloaded: ${duplicateTracksCount}`);
  }
  if (failedTracksCount > 0) {
    summaryParts.push(`Failed: ${failedTracksCount}`);
  }
  const summary = summaryParts.join(" · ");
  const isZeroResult = tracksEnqueued <= 0;
  setNotice(messageEl, summary, isZeroResult && (duplicateTracksCount > 0 || failedTracksCount > 0));

  let detailsEl = document.getElementById("home-album-failed-details");
  if (!detailsEl) {
    detailsEl = document.createElement("details");
    detailsEl.id = "home-album-failed-details";
    detailsEl.className = "meta";
    messageEl.insertAdjacentElement("afterend", detailsEl);
  }
  const detailItems = [];
  if (duplicateTracksCount > 0 && duplicateTracks.length > 0) {
    detailItems.push(
      ...duplicateTracks.slice(0, 10).map((entry) => {
        const track = String(entry?.track || "Unknown track");
        const reason = String(entry?.reason || "duplicate");
        return `<li><strong>${track}</strong>: already queued/downloaded (${reason})</li>`;
      })
    );
  }
  if (failedTracksCount > 0 && failedTracks.length > 0) {
    detailItems.push(
      ...failedTracks
      .slice(0, 10)
      .map((entry) => {
        const track = String(entry?.track || "Unknown track");
        const reason = String(entry?.reason || "unknown");
        return `<li><strong>${track}</strong>: ${reason}</li>`;
      })
    );
  }
  if (detailItems.length > 0) {
    const skippedTotal = duplicateTracksCount + failedTracksCount;
    detailsEl.innerHTML = `<summary>${albumTitle}: ${skippedTotal} not queued</summary><ul>${detailItems.join("")}</ul>`;
    detailsEl.open = false;
    detailsEl.classList.remove("hidden");
  } else {
    detailsEl.innerHTML = "";
    detailsEl.classList.add("hidden");
  }
}

async function enqueueMusicTrack(payload = {}) {
  if (!assertMusicModeDeliveryAllowed($("#home-search-message"))) {
    throw new Error("Client delivery does not use a server destination.");
  }
  const forceRedownload = !!document.getElementById("music-force-redownload")?.checked;
  return fetchJson("/api/music/enqueue", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      recording_mbid: String(payload.recording_mbid || "").trim(),
      release_mbid: String(payload.release_mbid || payload.mb_release_id || "").trim() || null,
      release_group_mbid: String(payload.release_group_mbid || payload.mb_release_group_id || "").trim() || null,
      artist: String(payload.artist || "").trim() || null,
      track: String(payload.track || "").trim() || null,
      album: String(payload.album || "").trim() || null,
      release_date: String(payload.release_date || payload.release_year || "").trim() || null,
      track_number: Number.isFinite(Number(payload.track_number)) ? Number(payload.track_number) : null,
      disc_number: Number.isFinite(Number(payload.disc_number)) ? Number(payload.disc_number) : null,
      duration_ms: Number.isFinite(Number(payload.duration_ms)) ? Number(payload.duration_ms) : null,
      destination: String(payload.destination || payload.destination_dir || "").trim() || null,
      final_format: String(payload.final_format || "").trim() || null,
      delivery_mode: getMusicModeDeliveryMode(),
      music_mode: state.homeMediaMode === "music",
      media_mode: state.homeMediaMode === "music_video" ? "music_video" : "music",
      force_redownload: forceRedownload,
    }),
  });
}

function buildMusicTrackEnqueuePayload({ button, result }) {
  const recording = String(button?.dataset?.recordingMbid || "").trim();
  const release = String(button?.dataset?.releaseMbid || "").trim();
  const releaseGroup = String(button?.dataset?.releaseGroupMbid || "").trim();
  const payload = {
    recording_mbid: recording,
    release_mbid: release,
    release_group_mbid: releaseGroup,
    destination: getMusicModeDestinationValue() || null,
    final_format: getMusicModeFinalFormatOverride(),
  };
  if (result && typeof result === "object") {
    payload.artist = result.artist || null;
    payload.track = result.track || null;
    payload.album = result.album || null;
    payload.release_date = result.release_year || null;
    payload.track_number = result.track_number || null;
    payload.disc_number = result.disc_number || null;
    payload.duration_ms = result.duration_ms || null;
  }
  return payload;
}

function renderMusicModeResults(response, query = "", { pushHistory = false } = {}) {
  if (pushHistory && state.homeMusicCurrentView) {
    state.homeMusicViewStack.push(state.homeMusicCurrentView);
  }
  state.homeMusicCurrentView = snapshotMusicResultsView(response, query);
  const artists = Array.isArray(response?.artists) ? response.artists : [];
  const albums = Array.isArray(response?.albums) ? response.albums : [];
  const tracks = normalizeMusicSearchResults(response?.tracks);
  const container = document.getElementById("music-results-container");
  if (!container) {
    return;
  }
  state.homeMusicResultMap = {};
  const renderToken = ++state.homeMusicRenderToken;
  const thumbnailJobs = [];
  container.innerHTML = "";
  setHomeSearchActive(false);
  stopHomeResultPolling();
  stopHomeJobPolling();
  clearHomeAlbumCandidates();

  if (!artists.length && !albums.length && !tracks.length) {
    const empty = document.createElement("div");
    empty.className = "home-results-empty";
    empty.textContent = "No music metadata matches found.";
    container.appendChild(empty);
    setHomeResultsStatus("No music metadata matches");
    setHomeResultsDetail("Try a different query or mode.", true);
    return;
  }

  setHomeResultsStatus("Music metadata results");
  setHomeResultsDetail(
    query ? `Showing MusicBrainz metadata for “${query}”.` : "Showing MusicBrainz metadata.",
    false
  );

  if (state.homeMusicViewStack.length) {
    const nav = document.createElement("div");
    nav.className = "row";
    nav.style.marginBottom = "8px";
    const backButton = document.createElement("button");
    backButton.className = "button ghost small";
    const previousView = state.homeMusicViewStack[state.homeMusicViewStack.length - 1];
    const previousHasAlbums = Array.isArray(previousView?.response?.albums) && previousView.response.albums.length > 0;
    backButton.textContent = previousHasAlbums ? "Back to Albums" : "Back";
    backButton.addEventListener("click", () => {
      const previous = state.homeMusicViewStack.pop();
      if (!previous) {
        return;
      }
      renderMusicModeResults(previous.response, previous.query, { pushHistory: false });
    });
    nav.appendChild(backButton);
    container.appendChild(nav);
  }

  function appendSection(titleText) {
    const sectionHeader = document.createElement("div");
    sectionHeader.className = "group-title";
    sectionHeader.textContent = titleText;
    container.appendChild(sectionHeader);
  }

  if (artists.length) {
    appendSection("Artists");
    artists.forEach((artistItem) => {
      const card = document.createElement("article");
      card.className = "home-result-card music-meta-card";
      const artistThumb = createMusicCardThumb(
        artistItem?.name ? `${artistItem.name} artwork` : "Artist artwork"
      );
      card.appendChild(artistThumb.shell);
      const title = document.createElement("div");
      title.className = "home-candidate-title";
      title.textContent = artistItem?.name || "";
      const content = document.createElement("div");
      content.className = "music-meta-main";
      content.appendChild(title);
      const meta = document.createElement("div");
      meta.className = "home-candidate-meta";
      const metaParts = [];
      if (artistItem?.country) metaParts.push(String(artistItem.country));
      if (artistItem?.disambiguation) metaParts.push(String(artistItem.disambiguation));
      meta.textContent = metaParts.join(" • ");
      content.appendChild(meta);
      const artistRef = document.createElement("div");
      artistRef.className = "home-mb-entity-ref";
      const artistMbid = String(artistItem?.artist_mbid || "").trim();
      artistRef.textContent = artistMbid ? `MB: artist ${artistMbid}` : "MB: artist (unknown)";
      content.appendChild(artistRef);
      card.appendChild(content);
      const action = document.createElement("div");
      action.className = "home-candidate-action";
      const button = document.createElement("button");
      button.className = "button ghost small";
      button.textContent = "View Albums";
      const runViewAlbums = async () => {
        const nextQuery = String(artistItem?.name || "").trim();
        const nextArtistMbid = String(artistItem?.artist_mbid || artistItem?.id || "").trim();
        if (!nextQuery) {
          return;
        }
        const artistInput = document.getElementById("search-artist");
        const albumInput = document.getElementById("search-album");
        const trackInput = document.getElementById("search-track");
        if (artistInput) artistInput.value = nextQuery;
        if (albumInput) albumInput.value = "";
        if (trackInput) trackInput.value = "";
        setMusicModeSelection("album");

        button.disabled = true;
        const previousLabel = button.textContent;
        button.textContent = "Loading...";
        setNotice($("#home-search-message"), `Loading albums for ${nextQuery}...`, false);
        try {
          const albums = await fetchMusicAlbumsByArtist({ name: nextQuery, artist_mbid: nextArtistMbid });
          renderMusicModeResults(
            { artists: [], albums, tracks: [], mode_used: "album" },
            nextQuery,
            { pushHistory: true }
          );
          setNotice($("#home-search-message"), `Loaded ${albums.length} album candidates for ${nextQuery}.`, false);
        } catch (err) {
          button.disabled = false;
          button.textContent = previousLabel;
          setNotice($("#home-search-message"), `View Albums failed: ${toUserErrorMessage(err)}`, true);
        }
      };
      button.addEventListener("click", runViewAlbums);
      [artistThumb.shell, title].forEach((el) => {
        if (!el) return;
        el.classList.add("music-card-click-target");
        el.addEventListener("click", () => {
          if (button.disabled) return;
          runViewAlbums();
        });
      });
      action.appendChild(button);
      card.appendChild(action);
      container.appendChild(card);

      const artistMbidValue = String(artistItem?.artist_mbid || "").trim();
      if (artistMbidValue) {
        if (Object.prototype.hasOwnProperty.call(state.homeArtistCoverCache, artistMbidValue)) {
          const cachedCover = state.homeArtistCoverCache[artistMbidValue];
          if (cachedCover) {
            artistThumb.setImage(cachedCover);
          } else {
            artistThumb.setNoArt();
          }
        } else {
          thumbnailJobs.push(async (activeToken) => {
            if (state.homeMusicRenderToken !== activeToken) {
              return;
            }
            try {
              const albums = await fetchMusicAlbumsByArtist({
                name: String(artistItem?.name || "").trim(),
                artist_mbid: artistMbidValue,
              });
              const firstAlbum = Array.isArray(albums) ? albums[0] : null;
              const firstReleaseGroup = String(firstAlbum?.release_group_mbid || "").trim();
              if (!firstReleaseGroup) {
                state.homeArtistCoverCache[artistMbidValue] = null;
                return;
              }
              const coverUrl = await fetchHomeAlbumCoverUrl(firstReleaseGroup);
              if (!coverUrl) {
                state.homeArtistCoverCache[artistMbidValue] = null;
                if (state.homeMusicRenderToken === activeToken) {
                  artistThumb.setNoArt();
                }
                return;
              }
              state.homeArtistCoverCache[artistMbidValue] = coverUrl;
              if (state.homeMusicRenderToken !== activeToken) {
                return;
              }
              artistThumb.setImage(coverUrl);
            } catch (_err) {
              state.homeArtistCoverCache[artistMbidValue] = null;
              if (state.homeMusicRenderToken === activeToken) {
                artistThumb.setNoArt();
              }
            }
          });
        }
      } else {
        artistThumb.setNoArt();
      }
    });
  }

  if (albums.length) {
    appendSection("Albums");
    albums.forEach((albumItem) => {
      const releaseGroupMbid = String(albumItem?.release_group_mbid || "").trim();
      const card = document.createElement("article");
      card.className = "home-result-card album-card music-meta-card";
      card.dataset.releaseGroupMbid = releaseGroupMbid;
      const albumThumb = createMusicCardThumb(
        albumItem?.title ? `${albumItem.title} cover` : "Album cover"
      );
      card.appendChild(albumThumb.shell);
      const content = document.createElement("div");
      content.className = "music-meta-main";
      const title = document.createElement("div");
      title.className = "home-candidate-title";
      title.textContent = albumItem?.title || "";
      content.appendChild(title);
      const meta = document.createElement("div");
      meta.className = "home-candidate-meta";
      const year = albumItem?.release_year ? ` (${albumItem.release_year})` : "";
      meta.textContent = `${albumItem?.artist || ""}${year}`;
      content.appendChild(meta);
      const albumRef = document.createElement("div");
      albumRef.className = "home-mb-entity-ref";
      albumRef.textContent = releaseGroupMbid ? `MB: release-group ${releaseGroupMbid}` : "MB: release-group (unknown)";
      content.appendChild(albumRef);
      card.appendChild(content);
      const action = document.createElement("div");
      action.className = "home-candidate-action";
      const viewTracksButton = document.createElement("button");
      viewTracksButton.className = "button ghost small album-view-tracks-btn";
      viewTracksButton.dataset.releaseGroupMbid = releaseGroupMbid;
      viewTracksButton.dataset.releaseGroupId = releaseGroupMbid;
      viewTracksButton.dataset.albumTitle = String(albumItem?.title || "");
      viewTracksButton.dataset.artistCredit = String(albumItem?.artist || "");
      viewTracksButton.textContent = "View Tracks";
      const button = document.createElement("button");
      button.className = "button primary small album-download-btn";
      button.dataset.releaseGroupMbid = releaseGroupMbid;
      button.dataset.releaseGroupId = releaseGroupMbid;
      button.dataset.albumTitle = String(albumItem?.title || "");
      button.textContent = "Download";
      const runViewTracks = async () => {
        const releaseGroupMbidValue = String(viewTracksButton.dataset.releaseGroupMbid || "").trim();
        const artistQuery = String(albumItem?.artist || "").trim();
        const albumTitle = String(albumItem?.title || "").trim();
        if (!artistQuery && !albumTitle) return;
        const artistInput = document.getElementById("search-artist");
        const albumInput = document.getElementById("search-album");
        const trackInput = document.getElementById("search-track");
        if (artistInput) artistInput.value = artistQuery;
        if (albumInput) albumInput.value = albumTitle;
        if (trackInput) trackInput.value = "";
        setMusicModeSelection("track");

        viewTracksButton.disabled = true;
        button.disabled = true;
        const previousViewLabel = viewTracksButton.textContent;
        const previousDownloadLabel = button.textContent;
        viewTracksButton.textContent = "Loading...";
        setNotice($("#home-search-message"), `Loading tracks for ${albumTitle || "album"}...`, false);
        try {
          const tracks = await fetchMusicTracksByAlbum({
            artist: artistQuery,
            album: albumTitle,
            releaseGroupMbid: releaseGroupMbidValue,
            limit: 100,
          });
          renderMusicModeResults(
            { artists: [], albums: [], tracks, mode_used: "track" },
            `${artistQuery} ${albumTitle}`.trim(),
            { pushHistory: true }
          );
          setNotice($("#home-search-message"), `Loaded ${tracks.length} tracks for ${albumTitle || "album"}.`, false);
        } catch (err) {
          viewTracksButton.disabled = false;
          button.disabled = false;
          viewTracksButton.textContent = previousViewLabel;
          button.textContent = previousDownloadLabel;
          setNotice($("#home-search-message"), `View Tracks failed: ${toUserErrorMessage(err)}`, true);
        }
      };
      viewTracksButton.addEventListener("click", runViewTracks);
      [albumThumb.shell, title].forEach((el) => {
        if (!el) return;
        el.classList.add("music-card-click-target");
        el.addEventListener("click", () => {
          if (viewTracksButton.disabled) return;
          runViewTracks();
        });
      });
      button.addEventListener("click", async () => {
        const releaseGroupMbidValue = String(button.dataset.releaseGroupMbid || "").trim();
        if (!releaseGroupMbidValue) return;
        button.disabled = true;
        button.textContent = "Queueing...";
        setNotice($("#home-search-message"), `Queueing ${albumItem?.title || "album"}...`, false);
        try {
          const result = await enqueueAlbum(releaseGroupMbidValue);
          const count = Number.isFinite(Number(result?.tracks_enqueued))
            ? Number(result.tracks_enqueued)
            : 0;
          if (count > 0) {
            button.textContent = "Queued...";
          } else {
            button.disabled = false;
            button.textContent = "Download";
          }
          console.info("[MUSIC UI] album queued", { release_group_mbid: releaseGroupMbidValue, tracks_enqueued: count });
          renderAlbumQueueSummary(result, { albumTitle: albumItem?.title || "Album" });
        } catch (err) {
          button.disabled = false;
          button.textContent = "Download";
          setNotice($("#home-search-message"), `Album queue failed: ${err.message}`, true);
        }
      });
      action.appendChild(viewTracksButton);
      action.appendChild(button);
      card.appendChild(action);
      container.appendChild(card);

      if (releaseGroupMbid) {
        if (Object.prototype.hasOwnProperty.call(state.homeAlbumCoverCache, releaseGroupMbid)) {
          const cachedCover = state.homeAlbumCoverCache[releaseGroupMbid];
          if (cachedCover) {
            albumThumb.setImage(cachedCover);
          } else {
            albumThumb.setNoArt();
          }
        } else {
          thumbnailJobs.push(async (activeToken) => {
            const coverUrl = await fetchHomeAlbumCoverUrl(releaseGroupMbid);
            if (!coverUrl || state.homeMusicRenderToken !== activeToken) {
              if (!coverUrl && state.homeMusicRenderToken === activeToken) {
                albumThumb.setNoArt();
              }
              return;
            }
            albumThumb.setImage(coverUrl);
          });
        }
      } else {
        albumThumb.setNoArt();
      }
    });
  }
  runPrioritizedThumbnailJobs(thumbnailJobs, renderToken, 2);

  if (tracks.length) {
    appendSection("Tracks");
    const orderedTracks = [...tracks].sort((a, b) => {
      const discA = Number.isFinite(Number(a?.disc_number)) ? Number(a.disc_number) : Number.MAX_SAFE_INTEGER;
      const discB = Number.isFinite(Number(b?.disc_number)) ? Number(b.disc_number) : Number.MAX_SAFE_INTEGER;
      if (discA !== discB) return discA - discB;
      const trackA = Number.isFinite(Number(a?.track_number)) ? Number(a.track_number) : Number.MAX_SAFE_INTEGER;
      const trackB = Number.isFinite(Number(b?.track_number)) ? Number(b.track_number) : Number.MAX_SAFE_INTEGER;
      if (trackA !== trackB) return trackA - trackB;
      return String(a?.track || "").localeCompare(String(b?.track || ""));
    });
    orderedTracks.forEach((result) => {
      const key = `${result.recording_mbid}::${result.mb_release_id}`;
      state.homeMusicResultMap[key] = result;
      const card = document.createElement("article");
      card.className = "home-result-card";
      card.dataset.recordingMbid = String(result.recording_mbid || "").trim();
      card.dataset.releaseMbid = String(result.mb_release_id || "").trim();
      card.dataset.releaseGroupMbid = String(result.mb_release_group_id || "").trim();

      const header = document.createElement("div");
      header.className = "home-result-header";
      const title = document.createElement("div");
      title.className = "home-candidate-title";
      const trackNumber = Number.isFinite(Number(result?.track_number)) ? Number(result.track_number) : null;
      if (trackNumber && trackNumber > 0) {
        const trackLabel = String(trackNumber).padStart(2, "0");
        title.textContent = `${trackLabel}. ${result.track}`;
      } else {
        title.textContent = result.track;
      }
      header.appendChild(title);
      const badge = document.createElement("span");
      badge.className = "home-result-badge matched";
      badge.textContent = "MusicBrainz";
      header.appendChild(badge);
      card.appendChild(header);

      const meta = document.createElement("div");
      meta.className = "home-candidate-meta";
      const durationText = Number.isFinite(result.duration_ms) ? formatDuration(result.duration_ms / 1000) : "-";
      meta.textContent = `${result.artist} • ${durationText}`;
      card.appendChild(meta);
      const albumLine = document.createElement("div");
      albumLine.className = "home-candidate-meta";
      const yearText = result.release_year || "";
      if (result.album) {
        albumLine.textContent = `Album: ${result.album}${yearText ? ` (${yearText})` : ""}`;
      } else if (yearText) {
        albumLine.textContent = `Release year: ${yearText}`;
      } else {
        albumLine.textContent = "Album: (unknown)";
      }
      card.appendChild(albumLine);

      const entityRef = document.createElement("div");
      entityRef.className = "home-mb-entity-ref";
      const recordingMbid = String(result.recording_mbid || "").trim();
      const releaseMbid = String(result.mb_release_id || "").trim();
      if (recordingMbid && releaseMbid) {
        entityRef.textContent = `MB: rec ${recordingMbid} • rel ${releaseMbid}`;
      } else if (recordingMbid) {
        entityRef.textContent = `MB: rec ${recordingMbid}`;
      } else if (releaseMbid) {
        entityRef.textContent = `MB: rel ${releaseMbid}`;
      } else {
        entityRef.textContent = "MB: (unknown)";
      }
      card.appendChild(entityRef);

      const action = document.createElement("div");
      action.className = "home-candidate-action";
      const button = document.createElement("button");
      button.className = "button primary small music-download-btn";
      button.dataset.musicResultKey = key;
      button.dataset.recordingMbid = String(result.recording_mbid || "").trim();
      button.dataset.releaseMbid = String(result.mb_release_id || "").trim();
      button.dataset.releaseGroupMbid = String(result.mb_release_group_id || "").trim();
      button.textContent = "Download";
      action.appendChild(button);
      card.appendChild(action);
      container.appendChild(card);
    });
  }
}

async function fetchMusicAlbumsByArtist(artist) {
  const query = typeof artist === "object" && artist !== null
    ? String(artist.name || "").trim()
    : String(artist || "").trim();
  const artistMbid = typeof artist === "object" && artist !== null
    ? String(artist.artist_mbid || "").trim()
    : "";
  if (!query) {
    return [];
  }
  const params = new URLSearchParams();
  params.set("q", query);
  params.set("limit", "50");
  if (artistMbid) {
    params.set("artist_mbid", artistMbid);
  }
  const raw = await fetchJson(
    `/api/music/albums/search?${params.toString()}`
  );
  const entries = Array.isArray(raw)
    ? raw
    : (Array.isArray(raw?.album_candidates) ? raw.album_candidates : []);
  const out = [];
  for (const item of entries) {
    if (!item || typeof item !== "object") continue;
    const releaseGroupMbid = String(item.release_group_mbid || item.release_group_id || item.album_id || "").trim();
    if (!releaseGroupMbid) continue;
    const first = String(item.first_release_date || item.first_released || "").trim();
    const releaseYear = first && /^\d{4}/.test(first) ? first.slice(0, 4) : null;
    out.push({
      release_group_mbid: releaseGroupMbid,
      title: String(item.title || "").trim(),
      artist: String(item.artist || item.artist_credit || "").trim(),
      release_year: releaseYear,
    });
  }
  out.sort((a, b) => {
    const aYear = Number.parseInt(String(a?.release_year || ""), 10);
    const bYear = Number.parseInt(String(b?.release_year || ""), 10);
    const aHas = Number.isFinite(aYear);
    const bHas = Number.isFinite(bYear);
    if (aHas && bHas && aYear !== bYear) return bYear - aYear;
    if (aHas !== bHas) return aHas ? -1 : 1;
    return String(a?.title || "").localeCompare(String(b?.title || ""));
  });
  return out;
}

async function fetchMusicTracksByAlbum({ artist = "", album = "", releaseGroupMbid = "", limit = 1000 } = {}) {
  const artistQuery = String(artist || "").trim();
  const albumQuery = String(album || "").trim();
  const rgMbid = String(releaseGroupMbid || "").trim();
  const cappedLimit = Number.isFinite(Number(limit)) ? Math.min(1000, Math.max(1, Number(limit))) : 1000;
  if (rgMbid) {
    try {
      const payload = await fetchJson(
        `/api/music/albums/${encodeURIComponent(rgMbid)}/tracks?limit=${cappedLimit}`
      );
      const normalizedTracks = normalizeMusicSearchResults(payload?.tracks);
      if (normalizedTracks.length) {
        return normalizedTracks;
      }
    } catch (_err) {
      // Fall back to legacy search query path.
    }
  }
  if (!artistQuery && !albumQuery) {
    return [];
  }
  const response = await fetch(
    `/api/music/search?artist=${encodeURIComponent(artistQuery)}&album=${encodeURIComponent(albumQuery)}&track=&mode=track&offset=0&limit=${cappedLimit}`
  );
  let payload = {};
  try {
    payload = await response.json();
  } catch (_err) {
    payload = {};
  }
  if (!response.ok) {
    const detail = payload && payload.detail ? payload.detail : `HTTP ${response.status}`;
    throw new Error(toUserErrorMessage(detail, `HTTP ${response.status}`));
  }
  const normalized = normalizeMusicSearchResults(payload?.tracks);
  if (!rgMbid) {
    return normalized;
  }
  const matched = normalized.filter((item) => String(item?.mb_release_group_id || "").trim() === rgMbid);
  return matched.length ? matched : normalized;
}

async function performMusicModeSearch() {
  const requestSeq = ++state.homeMusicSearchSeq;
  const artist = String(document.getElementById("search-artist")?.value || "").trim();
  const album = String(document.getElementById("search-album")?.value || "").trim();
  const track = String(document.getElementById("search-track")?.value || "").trim();
  const musicModeEnabledNow = !!state.homeMusicMode;
  if (!musicModeEnabledNow) {
    return;
  }
  if (!artist && !album && !track) {
    if (requestSeq !== state.homeMusicSearchSeq) {
      return;
    }
    if (!state.homeMusicMode) {
      return;
    }
    clearMusicResultsHistory();
    renderMusicModeResults({ artists: [], albums: [], tracks: [], mode_used: "auto" });
    return;
  }
  const modeSelect = document.getElementById("music-mode-select");
  const mode = modeSelect ? modeSelect.value : "auto";
  const response = await fetch(
    `/api/music/search?artist=${encodeURIComponent(artist)}&album=${encodeURIComponent(album)}&track=${encodeURIComponent(track)}&mode=${encodeURIComponent(mode)}&offset=0&limit=100`
  );
  let payload = {};
  try {
    payload = await response.json();
  } catch (_err) {
    payload = {};
  }
  if (!response.ok) {
    const detail = payload && payload.detail ? payload.detail : `HTTP ${response.status}`;
    throw new Error(toUserErrorMessage(detail, `HTTP ${response.status}`));
  }
  if (requestSeq !== state.homeMusicSearchSeq) {
    return;
  }
  const musicModeStillEnabled = !!state.homeMusicMode;
  if (!musicModeStillEnabled) {
    return;
  }
  const displayQuery = [artist, album, track].filter(Boolean).join(" ");
  clearMusicResultsHistory();
  renderMusicModeResults(payload, displayQuery);
}

function clearLegacyHomeSearchState() {
  stopHomeResultPolling();
  stopHomeJobPolling();
  clearHomeAlbumCandidates();
  state.homeRequestContext = {};
  state.homeSearchRequestId = null;
  state.homeSearchMode = null;
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  state.homeCandidateRefreshPending = {};
  const resultsList = $("#home-results-list");
  if (resultsList) {
    resultsList.textContent = "";
  }
  showHomeResults(false);
  setHomeSearchActive(false);
  setHomeResultsState({ hasResults: false, terminal: false });
  setHomeResultsStatus("Ready to discover media");
  setHomeResultsDetail("", false);
  updateHomeViewAdvancedLink();
}

async function handleHomeMusicModeSearch(inputValue, messageEl) {
  setNotice(messageEl, "Music Mode: loading metadata results...", false);
  clearLegacyHomeSearchState();
  await performMusicModeSearch();
  setHomeSearchControlsEnabled(true);
  setHomeSearchActive(false);
}

async function handleHomeStandardSearch(autoEnqueue, inputValue, messageEl) {
  const payload = buildHomeSearchPayload(autoEnqueue, inputValue);
  const modeLabel = autoEnqueue ? "Search & Download" : "Search Only";
  setNotice(messageEl, `${modeLabel}: creating request...`, false);
  const data = await fetchJson("/api/search/requests", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const responseMusicMode = !!data?.music_mode;
  const responseMusicCandidates = normalizeMusicAlbumCandidates(data?.music_candidates || []);
  state.homeRequestContext.pending = {
    musicMode: responseMusicMode,
    musicCandidates: responseMusicCandidates,
  };
  if (state.homeMusicMode && inputValue) {
    await loadAndRenderHomeAlbumCandidates(inputValue, responseMusicCandidates);
  }
  if (data && data.detected_intent) {
    await runSpotifyIntentFlow(
      {
        intentType: data.detected_intent,
        identifier: data.identifier || "",
      },
      messageEl
    );
    return;
  }
  state.homeRequestContext = {};
  state.homeRequestContext[data.request_id] = {
    request: {},
    items: [],
    musicMode: responseMusicMode,
    musicCandidates: responseMusicCandidates,
  };
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  state.homeCandidateRefreshPending = {};
  state.homeSearchRequestId = data.request_id;
  state.homeSearchMode = autoEnqueue ? "download" : "searchOnly";
  updateHomeViewAdvancedLink();
  setNotice(messageEl, `${modeLabel}: created ${data.request_id}`, false);
  showHomeResults(true);
  startHomeResultPolling(data.request_id);
  triggerHomeSearchResolution(data.request_id);
}

function normalizeMusicAlbumCandidates(rawCandidates) {
  if (!Array.isArray(rawCandidates)) {
    return [];
  }
  return rawCandidates
    .map((item) => {
      const releaseGroupId = item?.release_group_id || item?.album_id || null;
      if (!releaseGroupId) {
        return null;
      }
      return {
        release_group_id: releaseGroupId,
        title: item?.title || "",
        artist_credit: item?.artist_credit || item?.artist || "",
        first_release_date: item?.first_release_date || item?.first_released || "",
        primary_type: item?.primary_type || "Album",
        secondary_types: Array.isArray(item?.secondary_types) ? item.secondary_types : [],
        score: Number.isFinite(Number(item?.score)) ? Number(item.score) : null,
        track_count: Number.isFinite(Number(item?.track_count)) ? Number(item.track_count) : null,
      };
    })
    .filter(Boolean);
}

function uniqueMusicAlbumCandidates(candidates) {
  const seen = new Set();
  return candidates.filter((candidate) => {
    const key = String(candidate?.release_group_id || "").trim();
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function renderHomeAlbumCandidates(candidates, query = "") {
  clearHomeAlbumCandidates();
  const homeResults = document.getElementById("home-results");
  const header = homeResults?.querySelector(".home-results-header");
  if (!homeResults || !header) {
    return;
  }

  const showPanel = !!state.homeMusicMode && !!String(query || "").trim();
  if (!showPanel) {
    return;
  }

  const normalized = uniqueMusicAlbumCandidates(normalizeMusicAlbumCandidates(candidates));
  const container = document.createElement("div");
  container.id = "home-album-candidates";
  container.className = "stack";
  const panelHeader = document.createElement("div");
  panelHeader.className = "row";
  const panelTitle = document.createElement("div");
  panelTitle.className = "group-title";
  panelTitle.textContent = "Albums (MusicBrainz)";
  panelHeader.appendChild(panelTitle);
  container.appendChild(panelHeader);

  if (!normalized.length) {
    const empty = document.createElement("div");
    empty.className = "meta";
    empty.textContent = "No album matches found";
    container.appendChild(empty);
    header.insertAdjacentElement("afterend", container);
    return;
  }

  normalized.forEach((candidate) => {
    const card = document.createElement("div");
    card.className = "home-result-card album-card";

    const cover = document.createElement("img");
    cover.className = "album-cover";
    cover.alt = candidate.title ? `${candidate.title} cover` : "Album cover";
    cover.loading = "lazy";
    cover.style.width = "64px";
    cover.style.height = "64px";
    cover.style.objectFit = "cover";
    cover.style.borderRadius = "8px";
    cover.style.display = "none";
    cover.style.flexShrink = "0";
    card.appendChild(cover);

    const body = document.createElement("div");
    body.className = "stack";
    body.style.flex = "1";

    const title = document.createElement("span");
    title.className = "album-title home-candidate-title";
    title.textContent = candidate.title || "";
    body.appendChild(title);

    const artist = document.createElement("span");
    artist.className = "album-artist meta";
    artist.textContent = candidate.artist_credit || "";
    body.appendChild(artist);

    const date = document.createElement("span");
    date.className = "album-date meta";
    date.textContent = candidate.first_release_date || "";
    body.appendChild(date);

    const entityRef = document.createElement("div");
    entityRef.className = "home-mb-entity-ref";
    const releaseGroupId = String(candidate.release_group_id || "").trim();
    entityRef.textContent = releaseGroupId ? `MB: release-group ${releaseGroupId}` : "MB: release-group (unknown)";
    body.appendChild(entityRef);

    const badges = document.createElement("div");
    badges.className = "row";
    const primary = document.createElement("span");
    primary.className = "chip idle";
    primary.textContent = candidate.primary_type || "Album";
    badges.appendChild(primary);
    (candidate.secondary_types || []).forEach((type) => {
      const secondary = document.createElement("span");
      secondary.className = "chip idle";
      secondary.textContent = String(type);
      badges.appendChild(secondary);
    });
    if (candidate.score !== null) {
      const score = document.createElement("span");
      score.className = "meta";
      score.textContent = `Score ${candidate.score}`;
      badges.appendChild(score);
    }
    body.appendChild(badges);
    card.appendChild(body);

    const actions = document.createElement("div");
    actions.className = "home-candidate-action";
    const viewTracksButton = document.createElement("button");
    viewTracksButton.className = "button ghost small album-view-tracks-btn";
    viewTracksButton.dataset.releaseGroupId = candidate.release_group_id || "";
    viewTracksButton.dataset.albumTitle = candidate.title || "";
    viewTracksButton.dataset.artistCredit = candidate.artist_credit || "";
    viewTracksButton.textContent = "View Tracks";
    actions.appendChild(viewTracksButton);

    const button = document.createElement("button");
    button.className = "button primary small album-download-btn";
    button.dataset.releaseGroupId = candidate.release_group_id || "";
    button.dataset.albumTitle = candidate.title || "";
    const alreadyQueued = state.homeQueuedAlbumReleaseGroups.has(candidate.release_group_id || "");
    button.textContent = alreadyQueued ? "Queued..." : "Download Album";
    button.disabled = alreadyQueued;
    actions.appendChild(button);
    card.appendChild(actions);

    container.appendChild(card);
  });
  container.addEventListener("click", async (event) => {
    const clickCard = event.target.closest(".album-card");
    if (clickCard && (event.target.closest(".album-cover") || event.target.closest(".album-title"))) {
      const linked = clickCard.querySelector(".album-view-tracks-btn");
      if (linked && !linked.disabled) {
        linked.click();
      }
      return;
    }
    const viewTracksButton = event.target.closest(".album-view-tracks-btn");
    if (viewTracksButton) {
      const releaseGroupId = String(viewTracksButton.dataset.releaseGroupId || "").trim();
      const albumTitle = String(viewTracksButton.dataset.albumTitle || "").trim();
      const artistCredit = String(viewTracksButton.dataset.artistCredit || "").trim();
      const relatedDownloadButton = viewTracksButton
        .closest(".home-candidate-action")
        ?.querySelector(".album-download-btn");
      if (!releaseGroupId && !albumTitle && !artistCredit) {
        return;
      }
      const artistInput = document.getElementById("search-artist");
      const albumInput = document.getElementById("search-album");
      const trackInput = document.getElementById("search-track");
      if (artistInput) artistInput.value = artistCredit;
      if (albumInput) albumInput.value = albumTitle;
      if (trackInput) trackInput.value = "";
      setMusicModeSelection("track");

      const previousDownloadDisabled = relatedDownloadButton ? relatedDownloadButton.disabled : false;
      viewTracksButton.disabled = true;
      if (relatedDownloadButton) relatedDownloadButton.disabled = true;
      const previousViewLabel = viewTracksButton.textContent;
      viewTracksButton.textContent = "Loading...";
      setNotice($("#home-search-message"), `Loading tracks for ${albumTitle || "album"}...`, false);
      try {
        const tracks = await fetchMusicTracksByAlbum({
          artist: artistCredit,
          album: albumTitle,
          releaseGroupMbid: releaseGroupId,
          limit: 1000,
        });
        renderMusicModeResults(
          { artists: [], albums: [], tracks, mode_used: "track" },
          `${artistCredit} ${albumTitle}`.trim(),
          { pushHistory: true }
        );
        setNotice($("#home-search-message"), `Loaded ${tracks.length} tracks for ${albumTitle || "album"}.`, false);
      } catch (err) {
        viewTracksButton.disabled = false;
        viewTracksButton.textContent = previousViewLabel;
        if (relatedDownloadButton) relatedDownloadButton.disabled = previousDownloadDisabled;
        setNotice($("#home-search-message"), `View Tracks failed: ${toUserErrorMessage(err)}`, true);
      }
      return;
    }
    const button = event.target.closest(".album-download-btn");
    if (!button) {
      return;
    }
    const releaseGroupId = button.dataset.releaseGroupId;
    if (!releaseGroupId) {
      return;
    }
    if (state.homeQueuedAlbumReleaseGroups.has(releaseGroupId)) {
      button.disabled = true;
      button.textContent = "Queued...";
      return;
    }
    const originalLabel = button.textContent;
    button.disabled = true;
    button.textContent = "Queueing...";
    setNotice($("#home-search-message"), `Queueing ${button.dataset.albumTitle || "album"}...`, false);
    try {
      const payload = {
        release_group_mbid: releaseGroupId,
        destination: $("#home-destination")?.value.trim() || null,
        final_format: getMusicModeFinalFormatOverride(),
        music_mode: state.homeMediaMode === "music",
        media_mode: state.homeMediaMode === "music_video" ? "music_video" : "music",
        force_redownload: !!document.getElementById("music-force-redownload")?.checked,
      };
      homeMusicDebugLog("[MUSIC UI] queue album", payload);
      const result = await fetchJson("/api/music/album/download", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const count = Number.isFinite(Number(result?.tracks_enqueued))
        ? Number(result.tracks_enqueued)
        : 0;
      if (count > 0) {
        state.homeQueuedAlbumReleaseGroups.add(releaseGroupId);
        container.querySelectorAll(`.album-download-btn[data-release-group-id="${CSS.escape(releaseGroupId)}"]`)
          .forEach((dupButton) => {
            dupButton.disabled = true;
            dupButton.textContent = "Queued...";
          });
        button.textContent = "Queued...";
      } else {
        button.disabled = false;
        button.textContent = originalLabel;
      }
      renderAlbumQueueSummary(result, { albumTitle: button.dataset.albumTitle || "Album" });
    } catch (err) {
      button.disabled = false;
      button.textContent = originalLabel;
      setNotice($("#home-search-message"), `Album queue failed: ${err.message}`, true);
    }
  });
  header.insertAdjacentElement("afterend", container);
  const cards = Array.from(container.querySelectorAll(".album-card"));
  normalized.forEach((candidate, index) => {
    if (!candidate?.release_group_id) {
      return;
    }
    const card = cards[index];
    if (!card) {
      return;
    }
    const cover = card.querySelector(".album-cover");
    if (!cover) {
      return;
    }
    setTimeout(async () => {
      const coverUrl = await fetchHomeAlbumCoverUrl(candidate.release_group_id);
      if (coverUrl) {
        cover.src = coverUrl;
        cover.style.display = "block";
      }
    }, index * 150);
  });
}

async function loadAndRenderHomeAlbumCandidates(query, preloadedCandidates = null) {
  const normalized = (query || "").trim();
  if (!normalized) {
    clearHomeAlbumCandidates();
    return;
  }
  let candidates = normalizeMusicAlbumCandidates(preloadedCandidates);
  if (!candidates.length) {
    const data = await fetchJson(
      `/api/music/albums/search?q=${encodeURIComponent(normalized)}&limit=10`
    );
    candidates = normalizeMusicAlbumCandidates(Array.isArray(data) ? data : data?.album_candidates);
  }
  homeMusicDebugLog("[MUSIC UI] album candidates", { query: normalized, count: candidates.length });
  renderHomeAlbumCandidates(candidates, normalized);
}

async function fetchHomeAlbumCoverUrl(albumId) {
  const key = String(albumId || "").trim();
  if (!key) {
    return null;
  }
  const directCoverUrl = `https://coverartarchive.org/release-group/${encodeURIComponent(key)}/front-250`;
  const cached = state.homeAlbumCoverCache[key];
  if (typeof cached === "string" && cached) {
    return cached;
  }
  try {
    const data = await fetchJson(`/api/music/album/art/${encodeURIComponent(key)}`);
    const url = normalizeArtworkUrl(data?.cover_url);
    // Cache positive hits only; avoid pinning transient misses/rate limits as permanent null.
    if (url) {
      state.homeAlbumCoverCache[key] = url;
      return url;
    }
    // Backend returned no result; allow browser to attempt direct cover-art endpoint.
    return directCoverUrl;
  } catch (_err) {
    // Backend lookup failed; still attempt direct cover-art endpoint from browser.
    return directCoverUrl;
  }
}

function buildHomeResultsStatusInfo(requestId) {
  const context = state.homeRequestContext[requestId];
  if (!context) {
    return {
      text: "Searching sources…",
      detail: "Results appear as soon as each source responds.",
      isError: false
    };
  }
  const request = context.request || {};
  const items = context.items || [];
  const requestStatus = request.status || "pending";
  const adaptersTotal = Number.isFinite(request.adapters_total)
    ? request.adapters_total
    : null;
  const adaptersCompleted = Number.isFinite(request.adapters_completed)
    ? request.adapters_completed
    : null;
  const error = request.error || "";
  const minScore = Number.isFinite(request.min_match_score) ? request.min_match_score : null;
  const thresholdText = Number.isFinite(minScore) ? minScore.toFixed(2) : "0.00";
  const hasCandidates = items.some((item) => item.candidate_count > 0);
  const allowQueued = state.homeSearchMode !== "searchOnly";
  const hasQueued = allowQueued && items.some((item) => ["enqueued", "skipped"].includes(item.status));

  if (requestStatus === "failed" && error && error !== "no_items_enqueued") {
    return { text: "FAILED", detail: error, isError: true, status: requestStatus };
  }

  if (requestStatus === "failed" && error === "no_items_enqueued") {
    if (hasCandidates) {
      const detail = `Results found below the auto-download threshold. Use Advanced Search to enqueue a result.`;
      return { text: "Results found (below auto-download threshold)", detail, isError: false, status: requestStatus };
    }
    return {
      text: "No results",
      detail: "No candidates were returned for this search.",
      isError: false,
      status: requestStatus,
    };
  }

  if (!items.length) {
    const searchingStates = new Set(["pending", "resolving"]);
    if (searchingStates.has(requestStatus)) {
      if (adaptersTotal !== null && adaptersCompleted !== null) {
        return {
          text: `Searching sources (${adaptersCompleted}/${adaptersTotal})`,
          detail: "Results stream in as they are found.",
          isError: false,
          status: requestStatus,
        };
      } else {
        return {
          text: "Searching sources…",
          detail: "Results stream in as they are found.",
          isError: false,
          status: requestStatus,
        };
      }
    }
    const fallback = formatHomeRequestStatus(requestStatus);
    return { text: fallback, detail: "", isError: false, status: requestStatus };
  }

  const shouldShowQueuedSummary = hasQueued || (requestStatus === "completed_with_skips" && allowQueued);
  if (shouldShowQueuedSummary) {
    const detail =
      hasQueued && requestStatus === "completed_with_skips"
        ? "Some matches were skipped because they already exist."
        : requestStatus === "completed_with_skips"
        ? "Matches were skipped, but the request resolved successfully."
        : "Items have been scheduled for download.";
    return {
      text: "Downloads queued",
      detail,
      isError: false,
      status: requestStatus,
    };
  }

  // If candidates are already visible, stop showing an indefinite "Searching for media…"
  // header state even while adapters are still resolving.
  if (hasCandidates && (requestStatus === "resolving" || requestStatus === "pending")) {
    return {
      text: "Results found",
      detail: "Additional sources may still be resolving.",
      isError: false,
      status: requestStatus,
    };
  }

  if (requestStatus === "resolving" && adaptersTotal !== null && adaptersCompleted !== null) {
    return {
      text: `Searching sources (${adaptersCompleted}/${adaptersTotal})`,
      detail: "More results may still appear.",
      isError: false,
      status: requestStatus,
    };
  }

  const detail =
    requestStatus === "completed_with_skips"
      ? "Some entries already existed in the queue."
      : "";
  return { text: formatHomeRequestStatus(requestStatus), detail, isError: false, status: requestStatus };
}

function updateHomeResultsStatusForRequest(requestId) {
  if (!requestId) {
    setHomeResultsStatus("Ready to discover media");
    setHomeResultsDetail("Search Only is the default discovery action; use Search & Download to enqueue jobs.", false);
    setHomeSearchActive(false);
    return;
  }
  const info = buildHomeResultsStatusInfo(requestId);
  setHomeResultsStatus(info.text);
  setHomeResultsDetail(info.detail, info.isError);
  const context = state.homeRequestContext[requestId] || {};
  const items = Array.isArray(context.items) ? context.items : [];
  const hasVisibleCandidates = items.some((item) => {
    const count = Number(item?.candidate_count || 0);
    if (count > 0) return true;
    const status = String(item?.status || "").toLowerCase();
    return ["candidate_found", "selected", "enqueued", "completed", "skipped"].includes(status);
  });
  maybeReleaseHomeSearchControls(requestId, info.status, hasVisibleCandidates);
}

function recordHomeCandidateScore(requestId, score) {
  if (!requestId || !Number.isFinite(score)) {
    return;
  }
  const current = state.homeBestScores[requestId];
  if (!Number.isFinite(current) || score > current) {
    state.homeBestScores[requestId] = score;
    updateHomeResultsStatusForRequest(requestId);
  }
}

function formatHomeRequestStatus(status) {
  if (!status) return "Searching for media…";
  if (status === "resolving" || status === "pending") return "Searching for media…";
  if (status === "completed") return "Results found";
  if (status === "completed_with_skips") {
    return state.homeSearchMode === "searchOnly" ? "Results found" : "Downloads queued";
  }
  if (status === "failed") return "Search failed";
  return status[0]?.toUpperCase() + status.slice(1);
}

function renderHomeStatusBadge(status) {
  const normalized = status || "queued";
  const label = HOME_STATUS_LABELS[normalized] || normalized;
  const className = HOME_STATUS_CLASS_MAP[normalized] || "searching";
  const badge = document.createElement("span");
  badge.className = `home-result-badge ${className} long`;
  badge.textContent = label;
  return badge;
}

function buildCompactMediaSummary(parts = []) {
  const normalized = [];
  const seen = new Set();
  (Array.isArray(parts) ? parts : []).forEach((value) => {
    const text = String(value || "").trim();
    if (!text) return;
    const key = text.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    normalized.push(text);
  });
  return normalized.join(" / ") || "-";
}

const HOME_CANDIDATE_STATE_LABELS = {
  queued: "Queued",
  searching: "Searching",
  candidate_found: "Matched",
  selected: "Selected",
  enqueued: "Queued",
  failed: "Failed",
  skipped: "Already queued",
  claimed: "Queued",
  downloading: "Downloading",
  postprocessing: "Downloading",
  completed: "Completed",
  cancelled: "Cancelled",
  canceled: "Cancelled",
};
function getHomeCandidateStateInfo(status, { searchOnly = false } = {}) {
  const key = status || "queued";
  const label = HOME_CANDIDATE_STATE_LABELS[key] || key;
  const className = {
    queued: "queued",
    searching: "searching",
    candidate_found: "matched",
    selected: "matched",
    enqueued: "queued",
    failed: "failed",
    skipped: "skipped",
    claimed: "queued",
    downloading: "queued",
    postprocessing: "queued",
    completed: "matched",
    cancelled: "failed",
    canceled: "failed",
  }[key] || "queued";
  if (searchOnly && key === "enqueued") {
    return { label: "Matched", className: "matched" };
  }
  return { label, className };
}

async function fetchHomeJobSnapshot(requestId) {
  const now = Date.now();
  const cached = state.homeJobSnapshot;
  if (cached && cached.requestId === requestId && now - cached.at < 3000) {
    return cached;
  }
  const data = await fetchJson("/api/download_jobs?limit=200");
  const jobs = data.jobs || [];
  const jobsByUrl = new Map();
  jobs.forEach((job) => {
    if (job.origin !== "search") {
      return;
    }
    if (requestId && job.origin_id && job.origin_id !== requestId) {
      return;
    }
    if (job.url) {
      jobsByUrl.set(job.url, job);
    }
  });
  const snapshot = { at: now, requestId, jobsByUrl };
  state.homeJobSnapshot = snapshot;
  return snapshot;
}

function updateHomeCandidateRowState(row, candidate, item, job) {
  if (!row) return;
  const action = row.querySelector(".home-candidate-action");
  if (!action) return;
  const button = action.querySelector(
    'button[data-action="home-download"], button[data-action="home-direct-download"]'
  );
  const stateEl = action.querySelector(".home-candidate-state");
  const allowDownload = candidate.allow_download ?? item.allow_download;
  const canDownload = allowDownload !== false && !!candidate.url;
  const jobStatus = job?.status || candidate.job_status || "";
  const jobError = job?.last_error || "";
  if (stateEl) {
    if (jobStatus) {
      const info = getHomeCandidateStateInfo(jobStatus, { searchOnly: state.homeSearchMode === "searchOnly" });
      stateEl.textContent = info.label;
      stateEl.className = `home-candidate-state ${info.className}`;
      stateEl.classList.remove("hidden");
    } else {
      stateEl.textContent = "";
      stateEl.className = "home-candidate-state";
      stateEl.classList.add("hidden");
    }
  }
  if (jobStatus === "failed") {
    showHomeEnqueueError(action, jobError || "Download failed.");
  } else {
    clearHomeEnqueueError(action);
  }
  if (button) {
    const active = ["queued", "claimed", "downloading", "postprocessing"].includes(jobStatus);
    const disabled = active || !canDownload || !item.id || !candidate.id;
    button.disabled = disabled;
    button.setAttribute("aria-disabled", disabled ? "true" : "false");
    if (button.dataset.action === "home-direct-download") {
      button.textContent = active ? "Queued" : "Download";
    } else {
      button.textContent = "Download";
    }
  }
}

function stopHomeJobPolling() {
  if (state.homeJobTimer) {
    clearInterval(state.homeJobTimer);
    state.homeJobTimer = null;
  }
}

async function refreshHomeJobStatuses(requestId) {
  if (!requestId) {
    stopHomeJobPolling();
    return;
  }
  let snapshot = null;
  try {
    snapshot = await fetchHomeJobSnapshot(requestId);
  } catch (err) {
    return;
  }
  let hasActive = false;
  Object.values(state.homeCandidateData).forEach((entry) => {
    if (!entry) return;
    const candidate = entry.candidate;
    const item = entry.item;
    if (!candidate || !item) return;
    if (item.request_id && item.request_id !== requestId) {
      return;
    }
    if (!candidate.id) return;
    const row = document.querySelector(
      `.home-candidate-row[data-candidate-id="${CSS.escape(candidate.id)}"]`
    );
    if (!row) return;
    const job = candidate.url ? snapshot.jobsByUrl.get(candidate.url) : null;
    updateHomeCandidateRowState(row, candidate, item, job);
    if (job && ["queued", "claimed", "downloading", "postprocessing"].includes(job.status)) {
      hasActive = true;
    }
  });
  if (!hasActive) {
    stopHomeJobPolling();
  }
}

function startHomeJobPolling(requestId) {
  stopHomeJobPolling();
  state.homeJobTimer = setInterval(() => {
    refreshHomeJobStatuses(requestId);
  }, 4000);
  refreshHomeJobStatuses(requestId);
}

function renderHomeResultItem(item) {
  const card = document.createElement("article");
  card.className = "home-result-card";
  if (item.id) {
    card.dataset.itemId = item.id;
  }
  const header = document.createElement("div");
  header.className = "home-result-header";
  const title = document.createElement("div");
  const summary = buildCompactMediaSummary([item.artist, item.album, item.track]);
  title.innerHTML = `<strong>${summary}</strong>`;
  header.appendChild(title);
  header.appendChild(renderHomeStatusBadge(item.status));
  if (item.media_type === "music") {
    const sourceTag = document.createElement("span");
    sourceTag.className = "home-candidate-source-tag";
    sourceTag.textContent = "Spotify Metadata";
    header.appendChild(sourceTag);
  }
  card.appendChild(header);
  updateHomeResultDuration(card, item);
  // Remove destination line for Home page result cards (visual polish)
  // No destination line
  const candidateList = document.createElement("div");
  candidateList.className = "home-candidate-list";
  candidateList.dataset.itemId = item.id || "";
  const placeholder = document.createElement("div");
  placeholder.className = "home-results-empty";
  placeholder.textContent = "Searching for candidates…";
  candidateList.appendChild(placeholder);
  card.appendChild(candidateList);
  return card;
}

function updateHomeResultItemCard(card, item) {
  if (!card || !item) {
    return;
  }
  const summary = buildCompactMediaSummary([item.artist, item.album, item.track]);
  const title = card.querySelector(".home-result-header strong");
  if (title) {
    title.textContent = summary;
  }
  const header = card.querySelector(".home-result-header");
  const existingBadge = header?.querySelector(".home-result-badge");
  const newBadge = renderHomeStatusBadge(item.status);
  if (existingBadge && existingBadge.parentElement) {
    existingBadge.replaceWith(newBadge);
  } else if (header) {
    header.appendChild(newBadge);
  }
  updateHomeResultDuration(card, item);
  const resolvedDestination = state.homeRequestContext[item.request_id]?.request?.resolved_destination;
  let destinationEl = card.querySelector(".home-result-destination");
  if (resolvedDestination) {
    if (!destinationEl) {
      destinationEl = document.createElement("div");
      destinationEl.className = "home-result-destination";
      const candidateList = card.querySelector(".home-candidate-list");
      card.insertBefore(destinationEl, candidateList);
    }
    destinationEl.textContent = `Destination: ${resolvedDestination}`;
  } else if (destinationEl) {
    destinationEl.remove();
  }
  const candidateList = card.querySelector(".home-candidate-list");
  if (candidateList) {
    scheduleHomeCandidateRefresh(item, candidateList);
  }
}

function scheduleHomeCandidateRefresh(item, container) {
  if (!item || !item.id || !container) {
    return;
  }
  if (state.homeCandidatesLoading[item.id]) {
    state.homeCandidateRefreshPending[item.id] = true;
    return;
  }
  const preloadedCandidates = Array.isArray(item.candidates) ? item.candidates : null;
  loadHomeCandidates(item, container, preloadedCandidates);
}

async function loadHomeCandidates(item, container, preloadedCandidates = null) {
  if (!item || !container) {
    return;
  }
  if (!item.id) {
    return;
  }
  if (state.homeCandidatesLoading[item.id]) {
    return;
  }
  state.homeCandidatesLoading[item.id] = true;
  try {
    let placeholder = container.querySelector(".home-results-empty");
    if (!placeholder) {
      placeholder = document.createElement("div");
      placeholder.className = "home-results-empty";
      container.appendChild(placeholder);
    }
    placeholder.textContent = "Fetching candidates…";
    let candidates = Array.isArray(preloadedCandidates) ? preloadedCandidates : [];
    if (!candidates.length) {
      const data = await fetchJson(
        `/api/search/items/${encodeURIComponent(item.id)}/candidates`,
        { cache: "no-store" }
      );
      candidates = data.candidates || [];
    }
    if (!candidates.length) {
      placeholder.textContent = "Searching…";
      return;
    }
    const card = item.id
      ? document.querySelector(`.home-result-card[data-item-id="${CSS.escape(item.id)}"]`)
      : null;
    if (card) {
      updateHomeResultDuration(card, item, candidates);
    }
    if (placeholder.parentElement) {
      placeholder.remove();
    }
    const requestId = item.request_id || state.homeSearchRequestId;
    setHomeResultsState({ hasResults: true, terminal: false });
    let rendered = state.homeCandidateCache[item.id];
    if (!rendered) {
      rendered = new Set();
    }
    let bestScore = Number.NEGATIVE_INFINITY;
    candidates.forEach((candidate) => {
      if (candidate.id && !rendered.has(candidate.id)) {
        rendered.add(candidate.id);
      }
      if (candidate.final_score !== undefined) {
        const score = Number(candidate.final_score);
        if (Number.isFinite(score) && score > bestScore) {
          bestScore = score;
        }
      }
      let row = null;
      if (candidate.id) {
        const selector = `[data-candidate-id="${CSS.escape(candidate.id)}"]`;
        row = container.querySelector(selector);
      }
      if (!row) {
        row = renderHomeCandidateRow(candidate, item);
        container.appendChild(row);
      }
      updateHomeCandidateRowState(row, candidate, item, null);
      if (candidate.id) {
        state.homeCandidateData[candidate.id] = { candidate, item };
      }
    });
    state.homeCandidateCache[item.id] = rendered;
    if (Number.isFinite(bestScore)) {
      recordHomeCandidateScore(requestId, bestScore);
    }
    // Enrich job-state asynchronously so rendering is never blocked.
    Promise.resolve()
      .then(async () => {
        if (!requestId) return null;
        try {
          return await fetchHomeJobSnapshot(requestId);
        } catch (_err) {
          return null;
        }
      })
      .then((jobSnapshot) => {
        if (!jobSnapshot) return;
        candidates.forEach((candidate) => {
          if (!candidate?.id) return;
          const selector = `[data-candidate-id="${CSS.escape(candidate.id)}"]`;
          const row = container.querySelector(selector);
          if (!row) return;
          const job = candidate.url ? jobSnapshot.jobsByUrl.get(candidate.url) : null;
          updateHomeCandidateRowState(row, candidate, item, job || null);
        });
        if (!state.homeJobTimer) {
          const hasActive = Array.from(jobSnapshot.jobsByUrl.values()).some((job) =>
            ["queued", "claimed", "downloading", "postprocessing"].includes(job.status)
          );
          if (hasActive) {
            startHomeJobPolling(requestId);
          }
        }
      })
      .catch(() => {});
  } catch (err) {
    container.textContent = "";
    const errorEl = document.createElement("div");
    errorEl.className = "home-results-empty";
    errorEl.textContent = `Failed to load candidates: ${err.message}`;
    container.appendChild(errorEl);
  } finally {
    state.homeCandidatesLoading[item.id] = false;
    if (state.homeCandidateRefreshPending[item.id]) {
      state.homeCandidateRefreshPending[item.id] = false;
      setTimeout(() => scheduleHomeCandidateRefresh(item, container), 0);
    }
  }
}

function renderHomeCandidateRow(candidate, item) {
  const row = document.createElement("div");
  row.className = "home-candidate-row";
  if (candidate.id) {
    row.dataset.candidateId = candidate.id;
  }
  if (item.id) {
    row.dataset.itemId = item.id;
  }
  if (candidate.url) {
    row.dataset.url = candidate.url;
  }

  const fallbackYouTubeThumb = (() => {
    const source = String(candidate.source || "").toLowerCase();
    if (!source.startsWith("youtube")) return null;
    const videoId = extractYouTubeVideoIdFromUrl(candidate.url);
    return videoId ? `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg` : null;
  })();

  const artworkUrl =
    candidate.thumbnail_url ||
    candidate.artwork_url ||
    fallbackYouTubeThumb ||
    null;
  const artwork = document.createElement("div");
  artwork.className = "home-candidate-artwork";
  if (artworkUrl) {
    const img = document.createElement("img");
    img.src = artworkUrl;
    img.alt = candidate.source || "";
    artwork.appendChild(img);
  }
  row.appendChild(artwork);

  const info = document.createElement("div");
  info.className = "home-candidate-info";
  const sourceKey = (candidate.source || "").toLowerCase();
  const sourceLabel = formatSourceLabel(sourceKey) || candidate.source || "Unknown";
  const title = candidate.title || candidate.source || "-";
  const detail = [candidate.artist_detected, candidate.album_detected, candidate.track_detected]
    .filter(Boolean)
    .join(" / ");
  const titleEl = document.createElement("div");
  titleEl.className = "home-candidate-title";
  titleEl.textContent = title;
  info.appendChild(titleEl);
  if (detail) {
    const metaEl = document.createElement("div");
    metaEl.className = "home-candidate-meta";
    metaEl.textContent = detail;
    info.appendChild(metaEl);
  }
  const sourceEl = document.createElement("span");
  sourceEl.className = "home-candidate-source-tag";
  sourceEl.textContent = sourceLabel;
  info.appendChild(sourceEl);
  row.appendChild(info);

  const action = document.createElement("div");
  action.className = "home-candidate-action";
  const allowDownload = (candidate.allow_download ?? item.allow_download);
  const canDownload = allowDownload !== false;
  const isSearchOnly = state.homeSearchMode === "searchOnly";
  const isSearchResult = Boolean(item.request_id);
  const stateEl = document.createElement("span");
  stateEl.className = "home-candidate-state hidden";
  action.appendChild(stateEl);

  if (isSearchResult) {
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.dataset.action = "home-download";
    button.dataset.itemId = item.id || "";
    button.dataset.candidateId = candidate.id || "";
    button.textContent = "Download";

    const disabled = !canDownload || !candidate.url || !item.id || !candidate.id;
    button.disabled = disabled;

    if (disabled) {
      button.title = "Not downloadable";
      button.setAttribute("aria-disabled", "true");
    }

    action.appendChild(button);
  } else if (isSearchOnly && canDownload) {
    const jobStatus = candidate.job_status || "";
    const queued = ["queued", "claimed", "downloading", "postprocessing", "completed"].includes(jobStatus);
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.dataset.action = "home-direct-download";
    button.dataset.directUrl = candidate.url || "";
    button.textContent = queued ? "Queued" : "Download";
    button.disabled = queued || !candidate.url;
    action.appendChild(button);
  } else if (isSearchOnly) {
    const button = document.createElement("button");
    button.className = "button ghost small";
    button.textContent = "Search only";
    button.disabled = true;
    action.appendChild(button);
  } else {
    const label = document.createElement("span");
    label.textContent = "Auto";
    label.className = "meta";
    action.appendChild(label);
  }
  if (candidate.url) {
    const openLink = document.createElement("a");
    openLink.className = "button ghost small home-candidate-open";
    openLink.textContent = "Open source";
    openLink.href = candidate.url;
    openLink.target = "_blank";
    openLink.rel = "noopener noreferrer";
    action.appendChild(openLink);

    const previewDescriptor = buildHomePreviewDescriptor(candidate);
    if (previewDescriptor) {
      const previewButton = document.createElement("button");
      previewButton.type = "button";
      previewButton.className = "button ghost small home-candidate-preview";
      previewButton.textContent = "Preview";
      previewButton.dataset.action = "home-preview";
      previewButton.dataset.previewEmbedUrl = previewDescriptor.embedUrl;
      previewButton.dataset.previewSource = previewDescriptor.source;
      previewButton.dataset.previewTitle = previewDescriptor.title;
      action.appendChild(previewButton);
      titleEl.dataset.previewEnabled = "true";
      artwork.dataset.previewEnabled = "true";
    }
  }
  row.appendChild(action);

  const postedDate = formatCandidatePostedDate(candidate);
  if (postedDate) {
    const postedEl = document.createElement("div");
    postedEl.className = "home-candidate-posted";
    postedEl.textContent = `Posted: ${postedDate}`;
    row.appendChild(postedEl);
  }

  return row;
}

function resolveHomeResultDurationSeconds(item, candidates = []) {
  const itemDuration = Number(item?.duration_sec);
  if (Number.isFinite(itemDuration) && itemDuration > 0) {
    return itemDuration;
  }
  let bestCandidate = null;
  candidates.forEach((candidate) => {
    const candidateDuration = Number(candidate?.duration_sec);
    if (!Number.isFinite(candidateDuration) || candidateDuration <= 0) {
      return;
    }
    const candidateScore = Number(candidate?.final_score);
    const bestScore = Number(bestCandidate?.final_score);
    if (!bestCandidate || (Number.isFinite(candidateScore) && (!Number.isFinite(bestScore) || candidateScore > bestScore))) {
      bestCandidate = candidate;
    }
  });
  const bestDuration = Number(bestCandidate?.duration_sec);
  return Number.isFinite(bestDuration) && bestDuration > 0 ? bestDuration : null;
}

function updateHomeResultDuration(card, item, candidates = []) {
  if (!card) return;
  const seconds = resolveHomeResultDurationSeconds(item, candidates);
  let metaEl = card.querySelector(".home-result-meta");
  if (!seconds) {
    metaEl?.remove();
    return;
  }
  if (!metaEl) {
    metaEl = document.createElement("div");
    metaEl.className = "home-result-meta";
    const candidateList = card.querySelector(".home-candidate-list");
    if (candidateList) {
      card.insertBefore(metaEl, candidateList);
    } else {
      card.appendChild(metaEl);
    }
  }
  metaEl.textContent = `Length: ${formatDuration(seconds)}`;
}

function renderHomeDirectUrlCard(preview, status, options = {}) {
  const hideStatusBadge = !!options.hideStatusBadge;
  const card = document.createElement("article");
  card.className = "home-result-card";
  card.dataset.directUrl = preview.url || "";
  const header = document.createElement("div");
  header.className = "home-result-header";
  const title = document.createElement("div");
  const summary = preview.title || preview.url || "Direct URL";
  title.innerHTML = `<strong>${summary}</strong>`;
  header.appendChild(title);
  if (!hideStatusBadge) {
    header.appendChild(renderHomeStatusBadge(status));
  }
  card.appendChild(header);
  const detail = document.createElement("div");
  detail.className = "home-candidate-title";
  detail.textContent = `Source: ${preview.source || "direct"}${preview.uploader ? ` · ${preview.uploader}` : ""}`;
  card.appendChild(detail);
  const candidateList = document.createElement("div");
  candidateList.className = "home-candidate-list";
  // Compose candidate and item
  const candidate = {
    id: "direct-url-candidate",
    title: preview.title,
    artist_detected: preview.uploader,
    album_detected: null,
    track_detected: null,
    final_score: null,
    source: preview.source || "direct",
    url: preview.url,
    thumbnail_url: preview.thumbnail_url,
    allow_download: preview.allow_download !== false,
    job_status: preview.job_status || "",
  };
  const item = {
    id: "direct-url-item",
    status,
    allow_download: preview.allow_download !== false,
  };
  // Insert candidate row, with Cancel button if needed
  const row = renderHomeCandidateRow(candidate, item);
  // Add Cancel button for active direct jobs
  if (
    ["queued", "claimed", "downloading", "postprocessing"].includes(status)
  ) {
    const cancelBtn = document.createElement("button");
    cancelBtn.className = "button ghost small";
    cancelBtn.textContent = "Cancel";
    cancelBtn.onclick = async () => {
      try {
        cancelBtn.disabled = true;
        await cancelJob(state.homeDirectJob?.runId || state.homeDirectJob?.jobId);
        stopHomeDirectJobPolling();
        state.homeDirectPreview.job_status = "cancelled";
        const container = $("#home-results-list");
        if (container) {
          container.textContent = "";
          container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "cancelled"));
        }
        setHomeResultsStatus("Cancelled");
        setHomeResultsDetail("Download cancelled by user", false);
        setHomeSearchControlsEnabled(true);
      } catch (err) {
        setHomeResultsDetail(`Cancel failed: ${err.message}`, true);
      }
    };
    row.querySelector(".home-candidate-action")?.appendChild(cancelBtn);
  }
  candidateList.appendChild(row);
  card.appendChild(candidateList);
  return card;
}

function formatDetectedIntentLabel(intentType) {
  const mapping = {
    spotify_album: "Album",
    spotify_playlist: "Playlist",
    spotify_track: "Track",
    spotify_artist: "Artist",
    youtube_playlist: "Playlist",
  };
  return mapping[intentType] || intentType || "Unknown";
}

function isSpotifyPreviewIntent(intentType) {
  return intentType === "spotify_album" || intentType === "spotify_playlist";
}

function normalize_spotify_playlist_identifier(value) {
  if (!value) {
    return "";
  }
  const raw = String(value).trim();
  if (!raw) {
    return "";
  }
  try {
    const parsed = new URL(raw);
    if (parsed.hostname.includes("open.spotify.com") && parsed.pathname.includes("/playlist/")) {
      return parsed.pathname.split("/playlist/").pop().split("?")[0];
    }
  } catch (err) {
    // ignore and continue
  }
  if (raw.startsWith("spotify:playlist:")) {
    return raw.split(":").pop();
  }
  return raw;
}

function detectSpotifyUrlIntent(raw) {
  const value = (raw || "").trim();
  if (!value) {
    return null;
  }
  try {
    const parsed = new URL(value);
    const host = (parsed.hostname || "").toLowerCase();
    if (host !== "open.spotify.com" && host !== "spotify.com" && host !== "www.spotify.com") {
      return null;
    }
    const segments = parsed.pathname.split("/").filter(Boolean);
    if (segments.length < 2) {
      return null;
    }
    const kind = String(segments[0] || "").toLowerCase();
    const identifier = String(segments[1] || "").trim();
    if (!identifier) {
      return null;
    }
    const mapping = {
      album: "spotify_album",
      playlist: "spotify_playlist",
      track: "spotify_track",
      artist: "spotify_artist",
    };
    const intentType = mapping[kind] || null;
    if (!intentType) {
      return null;
    }
    return { intentType, identifier };
  } catch (err) {
    return null;
  }
}

function detect_intent(rawInput) {
  const spotifyIntent = detectSpotifyUrlIntent(rawInput);
  if (spotifyIntent) {
    return {
      type: spotifyIntent.intentType,
      identifier: spotifyIntent.identifier,
    };
  }
  return {
    type: "search",
    identifier: (rawInput || "").trim(),
  };
}

async function fetchIntentPreview(intentType, identifier) {
  return fetchJson("/api/intent/preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      intent_type: intentType,
      identifier,
    }),
  });
}

async function runSpotifyIntentFlow(spotifyIntent, messageEl) {
  const intentType = spotifyIntent?.intentType || "";
  const identifier = spotifyIntent?.identifier || "";
  if (!intentType || !identifier) {
    return;
  }

  state.homeSearchRequestId = null;
  updateHomeViewAdvancedLink();
  stopHomeResultPolling();
  setHomeSearchActive(false);
  setHomeSearchControlsEnabled(true);
  showHomeResults(true);
  setHomeResultsState({ hasResults: true, terminal: true });
  setHomeResultsStatus("Intent detected");
  setHomeResultsDetail("Preparing intent preview...", false);

  const list = $("#home-results-list");
  if (!list) {
    return;
  }
  list.textContent = "";

  const needsPreview = isSpotifyPreviewIntent(intentType);
  list.appendChild(
    renderHomeIntentCard(intentType, identifier, {
      loading: needsPreview,
      canConfirm: !needsPreview,
    })
  );

  if (needsPreview) {
    try {
      const preview = await fetchIntentPreview(intentType, identifier);
      list.textContent = "";
      list.appendChild(
        renderHomeIntentCard(intentType, identifier, {
          preview,
          canConfirm: true,
        })
      );
      setHomeResultsStatus("Intent preview ready");
      setHomeResultsDetail("Review metadata and confirm to continue.", false);
      setNotice(messageEl, "Intent metadata loaded.", false);
    } catch (previewErr) {
      list.textContent = "";
      list.appendChild(
        renderHomeIntentCard(intentType, identifier, {
          error: previewErr.message || "Failed to fetch metadata",
          canConfirm: false,
        })
      );
      setHomeResultsStatus("Intent preview failed");
      setHomeResultsDetail("Could not fetch Spotify metadata. Please retry.", true);
      setNotice(messageEl, `Intent preview failed: ${previewErr.message}`, true);
      setHomeSearchControlsEnabled(true);
    }
  } else {
    setHomeResultsDetail("Confirm to proceed or cancel to return to search.", false);
    setNotice(messageEl, "Intent detected. Confirm to continue.", false);
  }
}

function renderHomeIntentCard(intentType, identifier, options = {}) {
  const {
    loading = false,
    error = "",
    preview = null,
    canConfirm = false,
  } = options;
  const card = document.createElement("article");
  card.className = "home-result-card";
  card.dataset.intentType = intentType || "";
  card.dataset.intentIdentifier = identifier || "";

  const header = document.createElement("div");
  header.className = "home-result-header";
  const title = document.createElement("div");
  const strong = document.createElement("strong");
  strong.textContent = `Detected: ${formatDetectedIntentLabel(intentType)}`;
  title.appendChild(strong);
  header.appendChild(title);
  header.appendChild(renderHomeStatusBadge("candidate_found"));
  card.appendChild(header);

  if (loading) {
    const loadingEl = document.createElement("div");
    loadingEl.className = "home-candidate-title";
    loadingEl.textContent = "Fetching Spotify metadata…";
    card.appendChild(loadingEl);
  } else if (error) {
    const errorEl = document.createElement("div");
    errorEl.className = "home-candidate-title";
    errorEl.textContent = `Preview failed: ${error}`;
    card.appendChild(errorEl);
  } else if (preview) {
    const titleEl = document.createElement("div");
    titleEl.className = "home-candidate-title";
    titleEl.textContent = `Title: ${preview.title || "-"}`;
    card.appendChild(titleEl);

    const artistEl = document.createElement("div");
    artistEl.className = "home-candidate-meta";
    artistEl.textContent = `Artist: ${preview.artist || "-"}`;
    card.appendChild(artistEl);

    const countEl = document.createElement("div");
    countEl.className = "home-candidate-meta";
    countEl.textContent = `Track count: ${Number.isFinite(preview.track_count) ? preview.track_count : "-"}`;
    card.appendChild(countEl);
  } else {
    const detail = document.createElement("div");
    detail.className = "home-candidate-title";
    detail.textContent = `Identifier: ${identifier || "-"}`;
    card.appendChild(detail);
  }

  const actions = document.createElement("div");
  actions.className = "row";
  if (canConfirm) {
    const confirmButton = document.createElement("button");
    confirmButton.className = "button";
    confirmButton.dataset.action = "home-intent-confirm";
    confirmButton.dataset.intentType = intentType || "";
    confirmButton.dataset.identifier = identifier || "";
    if (intentType === "spotify_album") {
      confirmButton.textContent = "Download Album";
    } else if (intentType === "spotify_playlist") {
      confirmButton.textContent = "Download Playlist";
    } else if (intentType === "spotify_track") {
      confirmButton.textContent = "Download Track";
    } else {
      confirmButton.textContent = "Confirm Download";
    }
    actions.appendChild(confirmButton);
  }

  const cancelButton = document.createElement("button");
  cancelButton.className = "button ghost";
  cancelButton.dataset.action = "home-intent-cancel";
  cancelButton.textContent = "Cancel";
  actions.appendChild(cancelButton);
  card.appendChild(actions);
  return card;
}

function resetHomeIntentConfirmation() {
  state.homeSearchRequestId = null;
  updateHomeViewAdvancedLink();
  stopHomeResultPolling();
  stopHomeJobPolling();
  setHomeSearchControlsEnabled(true);
  setHomeSearchActive(false);
  setHomeResultsState({ hasResults: false, terminal: false });
  showHomeResults(false);
  const list = $("#home-results-list");
  if (list) {
    list.textContent = "";
  }
  setHomeResultsStatus("Ready to discover media");
  setHomeResultsDetail(
    "Search Only is the default discovery action; use Search & Download to enqueue jobs.",
    false
  );
}

async function executeDetectedIntent(intentType, identifier) {
  return fetchJson("/api/intent/execute", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      intent_type: intentType,
      identifier,
    }),
  });
}

function showHomeDirectUrlError(url, message, messageEl) {
  const text = message || DIRECT_URL_PLAYLIST_ERROR;
  if (messageEl) {
    setNotice(messageEl, text, true);
  }
  stopHomeDirectJobPolling();
  stopHomeResultPolling();
  state.homeSearchMode = "searchOnly";
  state.homeDirectJob = null;
  state.homeDirectPreview = {
    title: url,
    url,
    source: "direct",
    uploader: null,
    thumbnail_url: null,
    allow_download: false,
    job_status: "failed",
  };
  state.homeSearchRequestId = null;
  state.homeRequestContext = {};
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  state.homeCandidateRefreshPending = {};
  state.homeCandidateData = {};
  stopHomeJobPolling();
  showHomeResults(true);
  setHomeResultsStatus("Failed");
  setHomeResultsDetail(text, true);
  const container = $("#home-results-list");
  if (container) {
    container.textContent = "";
    container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "failed"));
  }
  setHomeResultsState({ hasResults: true, terminal: true });
  setHomeSearchControlsEnabled(true);
}

function showHomeDirectUrlPreview(preview) {
  const container = $("#home-results-list");
  if (!container) return;
  state.homeSearchMode = "searchOnly";
  stopHomeDirectJobPolling();
  state.homeDirectPreview = preview;
  state.homeSearchRequestId = null;
  state.homeRequestContext = {};
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  state.homeCandidateRefreshPending = {};
  state.homeCandidateData = {};
  stopHomeJobPolling();
  showHomeResults(true);
  setHomeResultsStatus("Direct URL preview");
  setHomeResultsDetail("Review the metadata and click Download to enqueue.", false);
  stopHomeResultPolling();
  container.textContent = "";
  const card = renderHomeDirectUrlCard(preview, "candidate_found");
  container.appendChild(card);
  setHomeResultsState({ hasResults: true, terminal: false });
}

function stopHomeDirectJobPolling() {
  if (state.homeDirectJobTimer) {
    clearInterval(state.homeDirectJobTimer);
    state.homeDirectJobTimer = null;
  }
}

async function refreshHomeDirectJobStatus() {
  if (!state.homeDirectJob) {
    return;
  }
  const container = $("#home-results-list");
  if (!container) return;
  try {
    const data = await fetchJson("/api/download_jobs?limit=500");
    const jobs = data.jobs || [];
    const playlistId = state.homeDirectJob.playlistId || null;
    const startedAtMs = Date.parse(String(state.homeDirectJob.startedAt || ""));
    const isCurrentRunJob = (entry) => {
      if (!Number.isFinite(startedAtMs)) {
        return true;
      }
      const updatedAtMs = Date.parse(String(entry?.updated_at || ""));
      const createdAtMs = Date.parse(String(entry?.created_at || ""));
      const candidateTs = Number.isFinite(updatedAtMs) ? updatedAtMs : createdAtMs;
      if (!Number.isFinite(candidateTs)) {
        return true;
      }
      // Small tolerance for clock skew/order.
      return candidateTs >= (startedAtMs - 5000);
    };
    if (playlistId) {
      const playlistJobs = jobs.filter(
        (entry) =>
          entry.origin === "playlist" &&
          String(entry.origin_id || "") === String(playlistId) &&
          isCurrentRunJob(entry)
      );
      if (playlistJobs.length) {
        const statuses = new Set(playlistJobs.map((entry) => String(entry.status || "")));
        let aggregateStatus = "queued";
        if (statuses.has("downloading") || statuses.has("postprocessing") || statuses.has("claimed")) {
          aggregateStatus = statuses.has("claimed") && !statuses.has("downloading") && !statuses.has("postprocessing")
            ? "claimed"
            : "downloading";
        } else if (statuses.has("queued")) {
          aggregateStatus = "queued";
        } else if (statuses.has("failed")) {
          aggregateStatus = "failed";
        } else if (statuses.has("cancelled")) {
          aggregateStatus = "cancelled";
        } else {
          aggregateStatus = "completed";
        }
        const failedJob = playlistJobs.find((entry) => String(entry.status || "") === "failed");
        const representative = failedJob || playlistJobs[0];
        state.homeDirectJob.status = aggregateStatus;
        state.homeDirectPreview = {
          ...state.homeDirectPreview,
          job_status: aggregateStatus,
        };
        container.textContent = "";
        container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, aggregateStatus));
        setHomeResultsStatus(formatDirectJobStatus(aggregateStatus));
        setHomeResultsDetail(representative?.last_error || "", Boolean(representative?.last_error));
        setHomeResultsState({
          hasResults: true,
          terminal: ["completed", "failed", "cancelled"].includes(aggregateStatus),
        });
        if (["completed", "failed", "cancelled"].includes(aggregateStatus)) {
          stopHomeDirectJobPolling();
          setHomeSearchControlsEnabled(true);
        }
        return;
      }
    }
    const job = jobs.find((entry) => entry.url === state.homeDirectJob.url && isCurrentRunJob(entry));
    if (job) {
      state.homeDirectJob.status = String(job.status || "queued");
      state.homeDirectPreview = {
        ...state.homeDirectPreview,
        job_status: String(job.status || "queued"),
      };
      container.textContent = "";
      const card = renderHomeDirectUrlCard(state.homeDirectPreview, String(job.status || "queued"));
      container.appendChild(card);
      setHomeResultsStatus(formatDirectJobStatus(String(job.status || "queued")));
      setHomeResultsDetail(job.last_error || "", Boolean(job.last_error));
      setHomeResultsState({
        hasResults: true,
        terminal: ["completed", "failed", "cancelled"].includes(String(job.status || "")),
      });
      if (["completed", "failed", "cancelled"].includes(String(job.status || ""))) {
        stopHomeDirectJobPolling();
        setHomeSearchControlsEnabled(true);
      }
      return;
    }
    if (!state.homeDirectJob.runId) {
      return;
    }
    const runData = await fetchJson(`/api/status?run_id=${encodeURIComponent(state.homeDirectJob.runId)}`);
    if (runData.run_id !== state.homeDirectJob.runId) {
      return;
    }

    const deliveryMode = String(state.homeDirectJob.deliveryMode || "server").toLowerCase();
    const statusPayload = runData?.status || {};
    const clientDeliveryId = String(statusPayload.client_delivery_id || "").trim();
    const clientFilename = String(statusPayload.client_delivery_filename || "").trim();

    if (deliveryMode === "client" && clientDeliveryId) {
      const clientDeliveryUrl = `/api/deliveries/${encodeURIComponent(clientDeliveryId)}/download`;
      if (state.homeDirectJob.clientDeliveryId !== clientDeliveryId) {
        state.homeDirectJob.clientDeliveryId = clientDeliveryId;
        triggerClientDeliveryDownload(clientDeliveryUrl, clientFilename || "");
      }
      state.homeDirectJob.status = "completed";
      state.homeDirectPreview = {
        ...state.homeDirectPreview,
        job_status: "completed",
      };
      container.textContent = "";
      container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "completed"));
      setHomeResultsStatus("Completed");
      const downloadMessage = clientFilename
        ? `Client download started: ${clientFilename}`
        : "Client download started.";
      setClientDeliveryNotice(
        $("#home-search-message"),
        downloadMessage,
        clientDeliveryUrl,
        clientFilename || ""
      );
      setHomeResultsDetail(downloadMessage, false);
      setHomeResultsState({ hasResults: true, terminal: true });
      stopHomeDirectJobPolling();
      setHomeSearchControlsEnabled(true);
      return;
    }

    let runStatus = "queued";
    let runError = "";
    if (runData.state === "error" || runData.error) {
      runStatus = "failed";
      runError = runData.error || "";
    } else if (playlistId) {
      // Playlist runs can complete enqueueing before downstream download jobs finish.
      runStatus = runData.running || runData.state === "running" ? "downloading" : "queued";
    } else if (runData.state === "completed") {
      runStatus = "completed";
    } else if (runData.running || runData.state === "running") {
      runStatus = "downloading";
    } else {
      runStatus = "completed";
    }
    state.homeDirectJob.status = runStatus;
    state.homeDirectPreview = {
      ...state.homeDirectPreview,
      job_status: runStatus,
    };
    container.textContent = "";
    const card = renderHomeDirectUrlCard(state.homeDirectPreview, runStatus);
    container.appendChild(card);
    setHomeResultsStatus(formatDirectJobStatus(runStatus));
    setHomeResultsDetail(runError || "", Boolean(runError));
    setHomeResultsState({
      hasResults: true,
      terminal: ["completed", "failed", "cancelled"].includes(runStatus),
    });
    if (["completed", "failed", "cancelled"].includes(runStatus)) {
      stopHomeDirectJobPolling();
      setHomeSearchControlsEnabled(true);
    }
  } catch (err) {
    setHomeResultsStatus("Direct URL status error");
    setHomeResultsDetail(`Failed to load job status: ${err.message}`, true);
    stopHomeDirectJobPolling();
    setHomeSearchControlsEnabled(true);
  }
}

function startHomeDirectJobPolling() {
  stopHomeDirectJobPolling();
  const tick = async () => {
    await refreshHomeDirectJobStatus();
  };
  state.homeDirectJobTimer = setInterval(tick, 4000);
  tick();
}

function formatDirectJobStatus(status) {
  if (status === "cancelled") return "Cancelled";
  if (!status) return "Queued";
  if (status === "claimed" || status === "downloading" || status === "postprocessing") {
    return "Downloading";
  }
  if (status === "queued") return "Queued";
  if (status === "completed") return "Completed";
  if (status === "failed") return "Failed";
  return status[0]?.toUpperCase() + status.slice(1);
}

async function refreshHomeResults(requestId) {
  const container = $("#home-results-list");
  if (!container) return null;
  try {
    const previousContext = state.homeRequestContext[requestId] || {};
    const data = await fetchJson(
      `/api/search/requests/${encodeURIComponent(requestId)}`,
      { cache: "no-store" }
    );
    const requestStatus = data.request?.status || "queued";
    const requestMediaType = data.request?.media_type || "";
    const items = data.items || [];
    state.homeRequestContext[requestId] = {
      request: data.request || {},
      items,
      musicMode: previousContext.musicMode || false,
      musicCandidates: previousContext.musicCandidates || [],
    };
    updateHomeResultsStatusForRequest(requestId);
    const existingCards = new Map();
    container.querySelectorAll(".home-result-card").forEach((card) => {
      const itemId = card.dataset.itemId;
      if (itemId) {
        existingCards.set(itemId, card);
      }
    });
    if (!items.length) {
      container.textContent = "";
      const placeholder = document.createElement("div");
      placeholder.className = "home-results-empty";
      if (["completed", "completed_with_skips", "failed"].includes(requestStatus)) {
        placeholder.textContent = requestStatus === "failed" ? "Search failed." : "No results found.";
      } else {
        placeholder.textContent = "Searching sources…";
      }
      container.appendChild(placeholder);
      const terminal = ["completed", "completed_with_skips", "failed"].includes(requestStatus);
      setHomeResultsState({ hasResults: true, terminal });
      if (terminal) {
        setHomeSearchActive(false);
        stopHomeResultPolling();
        startHomeJobPolling(requestId);
        if (requestMediaType === "music" && state.homeAlbumCandidatesRequestId !== requestId) {
          state.homeAlbumCandidatesRequestId = requestId;
          const query = $("#home-search-input")?.value || "";
          const preloaded = state.homeRequestContext[requestId]?.musicCandidates || [];
          await loadAndRenderHomeAlbumCandidates(query, preloaded);
        } else if (requestMediaType !== "music") {
          clearHomeAlbumCandidates();
        }
      }
      return requestStatus;
    }
    const currentIds = new Set();
    items.forEach((item) => {
      if (!item.id) {
        return;
      }
      currentIds.add(item.id);
      let card = existingCards.get(item.id);
      if (card) {
        existingCards.delete(item.id);
        updateHomeResultItemCard(card, item);
      } else {
        card = renderHomeResultItem(item);
        container.appendChild(card);
      }
      const candidateList = card.querySelector(".home-candidate-list");
      if (candidateList) {
        scheduleHomeCandidateRefresh(item, candidateList);
      }
    });
    existingCards.forEach((card) => {
      card.remove();
    });
    const hasResults = items.length > 0;
    const terminal = ["completed", "completed_with_skips", "failed"].includes(requestStatus);
    setHomeResultsState({ hasResults: hasResults || terminal, terminal });
    if (terminal) {
      setHomeSearchActive(false);
      stopHomeResultPolling();
      startHomeJobPolling(requestId);
      if (requestMediaType === "music" && state.homeAlbumCandidatesRequestId !== requestId) {
        state.homeAlbumCandidatesRequestId = requestId;
        const query = $("#home-search-input")?.value || "";
        const preloaded = state.homeRequestContext[requestId]?.musicCandidates || [];
        await loadAndRenderHomeAlbumCandidates(query, preloaded);
      } else if (requestMediaType !== "music") {
        clearHomeAlbumCandidates();
      }
    }
    Object.keys(state.homeCandidateCache).forEach((key) => {
      if (!currentIds.has(key)) {
        delete state.homeCandidateCache[key];
      }
    });
    guardHomeSearchNoCandidates(requestId, requestStatus, items);
    return requestStatus;
  } catch (err) {
    container.textContent = "";
    const errorEl = document.createElement("div");
    errorEl.className = "home-results-empty";
    errorEl.textContent = `Failed to refresh results: ${err.message}`;
    container.appendChild(errorEl);
    setHomeResultsState({ hasResults: true, terminal: true });
    setHomeResultsStatus("FAILED");
    setHomeResultsDetail(`Failed to refresh results: ${err.message}`, true);
    setHomeSearchControlsEnabled(true);
    stopHomeResultPolling();
    return null;
  }
}

function guardHomeSearchNoCandidates(requestId, requestStatus, items) {
  if (!requestId || !items.length) {
    return;
  }
  const watchingStates = new Set(["pending", "resolving"]);
  const hasCandidates = items.some((item) => item.candidate_count > 0);
  if (watchingStates.has(requestStatus) && !hasCandidates) {
    const streak = (state.homeNoCandidateStreaks[requestId] || 0) + 1;
    state.homeNoCandidateStreaks[requestId] = streak;
    if (streak >= HOME_NO_CANDIDATE_STREAK_LIMIT) {
      stopHomeResultPolling();
      setHomeResultsStatus("No candidates returned");
      setHomeResultsDetail(
        "Adapters are not returning results for this query. Try refining the search or use Advanced Search.",
        true
      );
      setHomeResultsState({ hasResults: false, terminal: true });
      setHomeSearchActive(false);
      setHomeSearchControlsEnabled(true);
    }
    return;
  }
  state.homeNoCandidateStreaks[requestId] = 0;
}

function abortHomeResultPolling(message) {
  stopHomeResultPolling();
  setHomeResultsStatus("Search timed out");
  setHomeResultsDetail(message || "Adapters took too long; try again or use Advanced Search.", true);
  setHomeResultsState({ hasResults: false, terminal: true });
  setHomeSearchActive(false);
  setHomeSearchControlsEnabled(true);
}

function startHomeResultPolling(requestId) {
  stopHomeResultPolling();
  showHomeResults(true);
  state.homeSearchPollStart = Date.now();
  const tick = async () => {
    if (state.homeSearchRequestId !== requestId) {
      stopHomeResultPolling();
      return;
    }
    if (state.homeResultPollInFlight) {
      state.homeResultsTimer = setTimeout(tick, HOME_RESULT_POLL_INTERVAL_MS);
      return;
    }
    state.homeResultPollInFlight = true;
    let shouldContinue = true;
    try {
      const status = await refreshHomeResults(requestId);
      if (status === null) {
        stopHomeResultPolling();
        return;
      }
      const elapsed = state.homeSearchPollStart ? Date.now() - state.homeSearchPollStart : 0;
      const hasVisibleCandidates = document.querySelectorAll("#home-results-list .home-candidate-row").length > 0;
      if (elapsed >= HOME_RESULT_TIMEOUT_MS && !hasVisibleCandidates) {
        abortHomeResultPolling("No adapters responded in time. Please retry or use Advanced Search.");
        return;
      }
      if (status && ["completed", "completed_with_skips", "failed"].includes(status)) {
        stopHomeResultPolling();
        shouldContinue = false;
        return;
      }
    } finally {
      state.homeResultPollInFlight = false;
    }
    if (shouldContinue && state.homeSearchRequestId === requestId) {
      state.homeResultsTimer = setTimeout(tick, HOME_RESULT_POLL_INTERVAL_MS);
    }
  };
  tick();
}

async function submitHomeSearch(autoEnqueue) {
  const messageEl = $("#home-search-message");
  const inputValue = $("#home-search-input")?.value.trim() || "";
  const query = inputValue;
  const musicModeEnabled = !!state.homeMusicMode;
  if (musicModeEnabled) {
    clearLegacyHomeSearchState();
    await performMusicModeSearch();
    return;
  }
  const destinationValue = $("#home-destination")?.value.trim() || "";
  const deliveryMode = ($("#home-delivery-mode")?.value || "server").toLowerCase();
  if (!state.homeSearchControlsEnabled) {
    return;
  }
  setHomeSearchActive(true);
  showHomeResults(true);
  setHomeResultsState({ hasResults: false, terminal: false });
  setHomeResultsStatus("Searching sources…");
  setHomeResultsDetail("Results appear as soon as each source responds.", false);
  const resultsList = $("#home-results-list");
  if (resultsList) {
    resultsList.textContent = "";
  }
  setHomeSearchControlsEnabled(false);
  stopHomeDirectJobPolling();
  state.homeDirectJob = null;
  state.homeDirectPreview = null;
  stopHomeJobPolling();
  state.homeCandidateData = {};
  state.homeMusicResultMap = {};
  state.homeAlbumCandidatesRequestId = null;
  clearHomeAlbumCandidates();

  const intent = detect_intent(inputValue);
  if (intent.type !== "search") {
    try {
      await runSpotifyIntentFlow(
        {
          intentType: intent.type,
          identifier: intent.identifier,
        },
        messageEl
      );
    } catch (spotifyIntentErr) {
      setNotice(messageEl, `Intent preview failed: ${spotifyIntentErr.message}`, true);
      setHomeSearchControlsEnabled(true);
    }
    return;
  }

  if (deliveryMode === "client" && destinationValue) {
    setNotice(messageEl, "Client delivery does not use a server destination.", true);
    setHomeSearchControlsEnabled(true);
    setHomeSearchActive(false);
    return;
  }
  if (deliveryMode === "client" && autoEnqueue) {
    setNotice(messageEl, "Search & Download is not available for client delivery. Use Search only or select Server delivery.", true);
    setHomeSearchControlsEnabled(true);
    setHomeSearchActive(false);
    return;
  }
  if (deliveryMode !== "client" && hasInvalidDestinationValue(destinationValue)) {
    setNotice(messageEl, "Destination path is invalid; please select a folder within downloads.", true);
    setHomeSearchControlsEnabled(true);
    return;
  }
  try {
    if (isValidHttpUrl(inputValue)) {
      const playlistId = extractPlaylistIdFromUrl(inputValue);
      if (playlistId) {
        if (autoEnqueue) {
          await handleHomePlaylistUrl(inputValue, playlistId, destinationValue, autoEnqueue, messageEl);
        } else {
          await handleHomePlaylistUrlPreview(inputValue, playlistId, messageEl);
        }
        return;
      }
      if (!autoEnqueue) {
        await handleHomeDirectUrlPreview(inputValue, destinationValue, messageEl);
        return;
      }
      await handleHomeDirectUrl(inputValue, destinationValue, messageEl);
      return;
    }
    if (state.homeMusicMode) {
      await handleHomeMusicModeSearch(inputValue, messageEl);
      return;
    }
    await handleHomeStandardSearch(autoEnqueue, inputValue, messageEl);
  } catch (err) {
    setHomeSearchControlsEnabled(true);
    setNotice(messageEl, `Search failed: ${err.message}`, true);
    setHomeResultsState({ hasResults: false, terminal: true });
    setHomeSearchActive(false);
  }
}

async function handleHomePlaylistUrl(url, playlistId, destination, autoEnqueue, messageEl) {
  if (!messageEl) return;
  if (!autoEnqueue) {
    showHomeDirectUrlError(url, HOME_PLAYLIST_SEARCH_ONLY_MESSAGE, messageEl);
    setHomeSearchControlsEnabled(true);
    return;
  }
  const deliveryMode = ($("#home-delivery-mode")?.value || "server").toLowerCase();
  if (deliveryMode === "client") {
    setNotice(messageEl, "Client delivery is not available for playlist URL runs. Select Server delivery.", true);
    setHomeSearchControlsEnabled(true);
    setHomeSearchActive(false);
    return;
  }
  const formatOverride = $("#home-format")?.value.trim();
  const mediaMode = state.homeMediaMode || "video";
  const treatAsMusic = mediaMode === "music";
  const payload = {
    playlist_id: playlistId,
    music_mode: treatAsMusic,
    media_mode: mediaMode,
  };
  if (destination) {
    payload.destination = destination;
  }
  if (formatOverride) {
    payload.final_format_override = formatOverride;
  }
  setNotice(messageEl, `Enqueuing playlist ${playlistId}...`, false);
  try {
    const runInfo = await startRun(payload);
    if (!runInfo) {
      throw new Error("playlist_enqueue_failed");
    }
    state.homeSearchMode = "download";
    state.homeDirectJob = {
      url,
      playlistId,
      startedAt: new Date().toISOString(),
      runId: runInfo?.run_id || null,
      status: "queued",
      deliveryMode,
      clientDeliveryId: null,
    };
    state.homeDirectPreview = {
      title: `YouTube Playlist (${playlistId})`,
      url,
      source: "playlist",
      uploader: null,
      thumbnail_url: null,
      job_status: "queued",
    };
    showHomeResults(true);
    setHomeResultsStatus("Queued");
    setHomeResultsDetail("Playlist enqueue started. Jobs will appear as they are created.", false);
    const container = $("#home-results-list");
    if (container) {
      container.textContent = "";
      container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "enqueued"));
    }
    // Best-effort: fetch playlist thumbnail from first playlist item.
    fetchHomePlaylistPreview(playlistId)
      .then((preview) => {
        const thumbnailUrl = String(preview?.thumbnail_url || "").trim();
        const playlistTitle = String(preview?.playlist_title || "").trim();
        if (!state.homeDirectPreview) {
          return;
        }
        state.homeDirectPreview = {
          ...state.homeDirectPreview,
          title: playlistTitle || state.homeDirectPreview.title,
          thumbnail_url: thumbnailUrl || state.homeDirectPreview.thumbnail_url || null,
        };
        const liveContainer = $("#home-results-list");
        if (!liveContainer) {
          return;
        }
        const currentStatus = String(state.homeDirectJob?.status || "queued");
        liveContainer.textContent = "";
        liveContainer.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, currentStatus));
      })
      .catch(() => {});
    setHomeResultsState({ hasResults: true, terminal: false });
    startHomeDirectJobPolling();
    setNotice(messageEl, "Playlist enqueue started", false);
  } catch (err) {
    setNotice(messageEl, `Playlist enqueue failed: ${err.message}`, true);
  } finally {
    setHomeSearchControlsEnabled(true);
  }
}

async function fetchHomePlaylistPreview(playlistId) {
  if (!playlistId) {
    return {};
  }
  return fetchJson(`/api/playlist/preview?playlist_id=${encodeURIComponent(String(playlistId))}`);
}

async function handleHomePlaylistUrlPreview(url, playlistId, messageEl) {
  if (!messageEl) return;
  stopHomeDirectJobPolling();
  stopHomeResultPolling();
  stopHomeJobPolling();
  state.homeSearchMode = "searchOnly";
  state.homeDirectJob = null;
  state.homeSearchRequestId = null;
  state.homeRequestContext = {};
  state.homeBestScores = {};
  state.homeCandidateCache = {};
  state.homeCandidatesLoading = {};
  state.homeCandidateRefreshPending = {};
  state.homeCandidateData = {};
  state.homeDirectPreview = {
    title: `YouTube Playlist (${playlistId})`,
    url,
    source: "playlist",
    uploader: null,
    thumbnail_url: null,
    allow_download: true,
    job_status: "",
  };
  showHomeResults(true);
  setHomeResultsStatus("Playlist preview");
  setHomeResultsDetail("Review playlist preview and click Download to enqueue all videos.", false);
  const container = $("#home-results-list");
  if (container) {
    container.textContent = "";
    container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "candidate_found", { hideStatusBadge: true }));
  }
  setHomeResultsState({ hasResults: true, terminal: false });
  setHomeSearchActive(false);
  setHomeSearchControlsEnabled(true);
  setNotice(messageEl, "Playlist URL detected. Click Download to enqueue all playlist videos.", false);
  try {
    const preview = await fetchHomePlaylistPreview(playlistId);
    const thumbnailUrl = String(preview?.thumbnail_url || "").trim();
    const playlistTitle = String(preview?.playlist_title || "").trim();
    if (!state.homeDirectPreview) {
      return;
    }
    state.homeDirectPreview = {
      ...state.homeDirectPreview,
      title: playlistTitle || state.homeDirectPreview.title,
      thumbnail_url: thumbnailUrl || state.homeDirectPreview.thumbnail_url || null,
    };
    const liveContainer = $("#home-results-list");
    if (!liveContainer) {
      return;
    }
    liveContainer.textContent = "";
    liveContainer.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "candidate_found", { hideStatusBadge: true }));
  } catch (_err) {
    // Best-effort only.
  }
}

async function importHomePlaylistFile() {
  const inputEl = $("#home-import-file");
  const summaryEl = $("#home-import-summary");
  const messageEl = $("#home-search-message");
  const file = inputEl?.files?.[0];
  if (!file) {
    setNotice(messageEl, "Select a playlist file to import.", true);
    return;
  }
  if (state.playlistImportInProgress) {
    setNotice(messageEl, "A playlist import is already running.", true);
    openImportProgressModal();
    return;
  }

  const proceed = window.confirm(
    "Playlist import can temporarily reduce app performance.\n\n" +
    "During import, avoid running other downloads or heavy actions until it completes.\n\n" +
    "Start playlist import now?"
  );
  if (!proceed) {
    return;
  }

  const formData = new FormData();
  formData.append("file", file, file.name);
  if (state.homeMusicMode) {
    formData.append("media_mode", state.homeMediaMode === "music_video" ? "music_video" : "music");
    const destinationValue = getMusicModeDestinationValue();
    const finalFormat = getMusicModeFinalFormatOverride();
    if (destinationValue) {
      formData.append("destination_dir", destinationValue);
    }
    if (finalFormat) {
      formData.append("final_format", finalFormat);
    }
  }

  if (summaryEl) {
    summaryEl.textContent = "";
  }
  setNotice(messageEl, "Playlist import started. Tracking progress...", false);
  setPlaylistImportControlsEnabled(false);
  try {
    const payload = await fetchJson("/api/import/playlist", {
      method: "POST",
      body: formData,
    });
    const jobId = String(payload.job_id || "").trim();
    if (!jobId) {
      throw new Error("missing_job_id");
    }
    startPlaylistImportPolling(jobId, payload.status || null);
  } catch (err) {
    state.playlistImportInProgress = false;
    setPlaylistImportControlsEnabled(true);
    setNotice(messageEl, `Import failed: ${err.message}`, true);
  }
}

async function handleHomeDirectUrl(url, destination, messageEl) {
  if (!messageEl) return;
  setHomeSearchActive(true);
  const formatOverride = $("#home-format")?.value.trim();
  const mediaMode = state.homeMediaMode || "video";
  const treatAsMusic = mediaMode === "music";
  const effectiveFormatOverride = treatAsMusic ? getMusicModeFinalFormatOverride() : formatOverride;
  const deliveryMode = ($("#home-delivery-mode")?.value || "server").toLowerCase();
  const playlistId = extractPlaylistIdFromUrl(url);
  if (playlistId) {
    showHomeDirectUrlError(url, DIRECT_URL_PLAYLIST_ERROR, messageEl);
    return;
  }
  if (deliveryMode === "client" && destination) {
    setNotice(messageEl, "Client delivery does not use a server destination.", true);
    setHomeSearchControlsEnabled(true);
    setHomeSearchActive(false);
    return;
  }
  if (treatAsMusic && deliveryMode === "client") {
    setNotice(messageEl, "Client delivery is not available for Music Mode direct URLs. Select Server delivery.", true);
    setHomeSearchControlsEnabled(true);
    setHomeSearchActive(false);
    return;
  }
  const payload = {};
  payload.single_url = url;
  if (destination && deliveryMode !== "client") {
    payload.destination = destination;
  }
  if (effectiveFormatOverride) {
    payload.final_format_override = effectiveFormatOverride;
  }
  payload.music_mode = treatAsMusic;
  payload.media_mode = mediaMode;
  payload.delivery_mode = deliveryMode;
  setNotice(messageEl, "Direct URL download requested...", false);
  try {
    const runInfo = await startRun(payload);
    if (!runInfo) {
      throw new Error("Run failed");
    }
    state.homeSearchMode = "download";
    state.homeDirectJob = {
      url,
      playlistId: playlistId || null,
      startedAt: new Date().toISOString(),
      runId: runInfo?.run_id || null,
      status: "queued",
      deliveryMode,
      clientDeliveryId: null,
    };
    state.homeDirectPreview = {
      title: url,
      url,
      source: playlistId ? "playlist" : "direct",
      uploader: null,
      thumbnail_url: null,
      job_status: "queued",
    };
    showHomeResults(true);
    setHomeResultsStatus("Queued");
    setHomeResultsDetail("Direct URL download queued.", false);
    const container = $("#home-results-list");
    if (container) {
      container.textContent = "";
      container.appendChild(renderHomeDirectUrlCard(state.homeDirectPreview, "enqueued"));
    }
    setHomeResultsState({ hasResults: true, terminal: false });
    startHomeDirectJobPolling();
    setNotice(messageEl, "Direct URL download started", false);
  } catch (err) {
    setNotice(messageEl, `Direct download failed: ${err.message}`, true);
  } finally {
    setHomeSearchControlsEnabled(true);
  }
}

async function handleHomeDirectUrlPreview(url, destination, messageEl) {
  if (!messageEl) return;
  setHomeSearchActive(true);
  const playlistId = extractPlaylistIdFromUrl(url);
  if (playlistId) {
    showHomeDirectUrlError(url, DIRECT_URL_PLAYLIST_ERROR, messageEl);
    return;
  }
  setNotice(messageEl, "Fetching URL metadata...", false);
  try {
    const data = await fetchJson("/api/direct_url_preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });
    const preview = data.preview || {};
    preview.url = preview.url || url;
    showHomeDirectUrlPreview(preview);
    setNotice(messageEl, "Direct URL preview ready", false);
  } catch (err) {
    setNotice(messageEl, `Preview failed: ${err.message}`, true);
  } finally {
    setHomeSearchControlsEnabled(true);
  }
}


async function runSearchResolutionOnce({ preferRequestId = null, showMessage = true } = {}) {
  const messageEl = $("#search-requests-message");
  try {
    if (showMessage) {
      setNotice(messageEl, "Running resolution...", false);
    }
    const payload = {};
    if (preferRequestId) {
      payload.request_id = preferRequestId;
    }
    const data = await fetchJson("/api/search/resolve/once", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const resolvedId = data.request_id || preferRequestId || null;
    if (showMessage) {
      if (data.request_id) {
        setNotice(messageEl, `Resolved request ${data.request_id}`, false);
      } else {
        setNotice(messageEl, "No queued requests", false);
      }
    } else if (messageEl) {
      messageEl.textContent = "";
    }
    await refreshSearchRequests(resolvedId);
    if (resolvedId) {
      await refreshSearchRequestDetails(resolvedId);
    }
    await refreshSearchQueue();
  } catch (err) {
    if (showMessage) {
      setNotice(messageEl, `Resolution failed: ${err.message}`, true);
    }
  }
}

function triggerHomeSearchResolution(requestId) {
  if (!requestId) return;
  fetchJson("/api/search/resolve/once", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ request_id: requestId }),
  }).catch(() => {});
}


async function enqueueSearchCandidate(itemId, candidateId, options = {}) {
  if (!itemId || !candidateId) return;
  const messageEl = options.messageEl || $("#search-requests-message");
  const finalFormat = $("#home-format")?.value.trim();
  const deliveryMode = getHomeDeliveryMode();
  const payload = { candidate_id: candidateId, delivery_mode: deliveryMode };
  if (finalFormat) {
    payload.final_format = finalFormat;
  }
  try {
    setNotice(messageEl, `Enqueuing candidate ${candidateId}...`, false);
    const data = await fetchJson(`/api/search/items/${encodeURIComponent(itemId)}/enqueue`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (deliveryMode === "client" && data?.delivery_url) {
      const started = triggerClientDeliveryDownload(data.delivery_url, data.filename || "");
      const msg = `Client download started${data.filename ? `: ${data.filename}` : ""}`;
      if (started) {
        setClientDeliveryNotice(messageEl, msg, data.delivery_url, data.filename || "");
      } else {
        setNotice(messageEl, "Client delivery ready. Click Download file.", false);
      }
    } else if (data.created) {
      setNotice(messageEl, `Enqueued job ${data.job_id}`, false);
    } else {
      setNotice(messageEl, "Job already queued", false);
    }

    await refreshSearchRequestDetails(state.searchSelectedRequestId);
    await refreshSearchQueue();
  } catch (err) {
    const rawMessage = String(err && err.message ? err.message : "");
    if (rawMessage.includes("music_mode_mb_binding_failed")) {
      setNotice(messageEl, "Music Mode rejected — No canonical album release found", true);
      return;
    }
    setNotice(messageEl, `Enqueue failed: ${rawMessage}`, true);
  }
}
async function cancelSearchRequest(requestId) {
  if (!requestId) return;
  const messageEl = $("#search-requests-message");
  try {
    setNotice(messageEl, `Canceling request ${requestId}...`, false);
    await fetchJson(`/api/search/requests/${encodeURIComponent(requestId)}/cancel`, { method: "POST" });
    setNotice(messageEl, `Canceled request ${requestId}`, false);
    await refreshSearchRequests(requestId);
    if (state.searchSelectedRequestId === requestId) {
      await refreshSearchRequestDetails(requestId);
    }
  } catch (err) {
    setNotice(messageEl, `Cancel failed: ${err.message}`, true);
  }
}

async function refreshSearchRequests(preferRequestId = null) {
  const body = $("#search-requests-body");
  const messageEl = $("#search-requests-message");
  try {
    updateSearchSortLabel();
    const data = await fetchJson("/api/search/requests");
    const requests = (data.requests || []).slice();
    const sortDir = state.searchRequestsSort === "asc" ? "asc" : "desc";
    requests.sort((a, b) => {
      const aTime = Date.parse(a.created_at || "");
      const bTime = Date.parse(b.created_at || "");
      if (Number.isNaN(aTime) || Number.isNaN(bTime)) {
        const aKey = String(a.created_at || "");
        const bKey = String(b.created_at || "");
        return sortDir === "desc" ? bKey.localeCompare(aKey) : aKey.localeCompare(bKey);
      }
      return sortDir === "desc" ? bTime - aTime : aTime - bTime;
    });

    body.textContent = "";
    if (!requests.length) {
      renderSearchEmptyRow(body, 6, "No search requests found.");
      setSearchSelectedRequest(null);
      setSearchSelectedItem(null);
      renderSearchEmptyRow($("#search-items-body"), 6, "Select a request to view items.");
      renderSearchEmptyRow($("#search-candidates-body"), 8, "Select an item to view candidates.");
      if (messageEl) messageEl.textContent = "";
      return;
    }

    let selectedRequestId = preferRequestId || state.searchSelectedRequestId;
    if (selectedRequestId && !requests.some((req) => req.id === selectedRequestId)) {
      selectedRequestId = null;
    }
    setSearchSelectedRequest(selectedRequestId);

    requests.forEach((req) => {
      const tr = document.createElement("tr");
      tr.dataset.requestId = req.id || "";
      if (req.id && req.id === selectedRequestId) {
        tr.classList.add("selected");
      }
      const summary = [req.artist, req.album, req.track].filter(Boolean).join(" / ");
      const searchText = String(req.query || "").trim() || summary || "-";
      const status = req.status || "";
      const cancelDisabled = ["completed", "completed_with_skips", "failed"].includes(status);
      tr.innerHTML = `
        <td>${searchText}</td>
        <td>${req.intent || ""}</td>
        <td>${status}</td>
        <td>${formatTimestamp(req.created_at) || ""}</td>
        <td>${req.error || ""}</td>
        <td>
          <div class="action-group">
            <button class="button ghost small" data-action="cancel" data-request-id="${req.id}" ${cancelDisabled ? "disabled" : ""}>Cancel</button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });

    if (!selectedRequestId) {
      setSearchSelectedItem(null);
      renderSearchEmptyRow($("#search-items-body"), 6, "Select a request to view items.");
      renderSearchEmptyRow($("#search-candidates-body"), 8, "Select an item to view candidates.");
    }
    if (messageEl) messageEl.textContent = "";
  } catch (err) {
    renderSearchEmptyRow(body, 6, `Failed to load requests: ${err.message}`);
    setNotice(messageEl, `Failed to load requests: ${err.message}`, true);
  }
}

async function refreshSearchRequestDetails(requestId) {
  const itemsBody = $("#search-items-body");
  const candidatesBody = $("#search-candidates-body");
  if (!requestId) {
    renderSearchEmptyRow(itemsBody, 6, "Select a request to view items.");
    renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    setSearchSelectedItem(null);
    updateSearchDestinationDisplay();
    return;
  }
  try {
    const data = await fetchJson(`/api/search/requests/${encodeURIComponent(requestId)}`);
    const destEl = $("#search-default-destination");
    if (destEl) {
      destEl.textContent =
        data.request?.resolved_destination || getSearchDefaultDestination() || "-";
    }
    const items = data.items || [];
    const requestStatus = data.request ? data.request.status : null;
    itemsBody.textContent = "";

    if (!items.length) {
      renderSearchEmptyRow(itemsBody, 6, "No items found for this request.");
      renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
      setSearchSelectedItem(null);
      return;
    }

    let selectedItemId = state.searchSelectedItemId;
    if (selectedItemId && !items.some((item) => item.id === selectedItemId)) {
      selectedItemId = null;
    }
    if (!selectedItemId && items.length && ["completed", "failed"].includes(requestStatus)) {
      selectedItemId = items[0].id;
    }
    setSearchSelectedItem(selectedItemId);

    items.forEach((item) => {
      const tr = document.createElement("tr");
      tr.dataset.itemId = item.id || "";
      if (item.id && item.id === selectedItemId) {
        tr.classList.add("selected");
      }
      const summary = [item.artist, item.album, item.track].filter(Boolean).join(" / ") || "-";
      const score = Number.isFinite(item.chosen_score) ? item.chosen_score.toFixed(3) : "";
      tr.innerHTML = `
        <td>${item.id || ""}</td>
        <td>${item.position ?? ""}</td>
        <td>${summary}</td>
        <td>${item.status || ""}</td>
        <td>${item.chosen_source || ""}</td>
        <td>${score}</td>
      `;
      itemsBody.appendChild(tr);
    });

    if (selectedItemId) {
      await refreshSearchCandidates(selectedItemId);
    } else {
      renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    }
  } catch (err) {
    renderSearchEmptyRow(itemsBody, 6, `Failed to load items: ${err.message}`);
    renderSearchEmptyRow(candidatesBody, 8, "Select an item to view candidates.");
    setSearchSelectedItem(null);
  }
}

async function refreshSearchCandidates(itemId) {
  const body = $("#search-candidates-body");
  if (!itemId) {
    renderSearchEmptyRow(body, 8, "Select an item to view candidates.");
    return;
  }
  try {
    const data = await fetchJson(`/api/search/items/${encodeURIComponent(itemId)}/candidates`);
    const candidates = data.candidates || [];
    body.textContent = "";

    if (!candidates.length) {
      renderSearchEmptyRow(body, 8, "No candidates found for this item.");
      return;
    }

    candidates.forEach((candidate) => {
      const tr = document.createElement("tr");
      const canonical = candidate.canonical_metadata || {};
      const canonicalTitle = canonical.track || canonical.album || "";
      const canonicalDetailParts = [canonical.artist, canonical.album].filter(Boolean);
      const canonicalDetail = canonicalDetailParts.join(" / ");
      const canonicalType = canonical.album_type ? (canonicalDetail ? ` · ${canonical.album_type}` : canonical.album_type) : "";
      const canonicalYear = canonical.release_year ? ` (${canonical.release_year})` : "";
      const canonicalHtml = canonicalTitle
        ? `<div class="cell-title">${canonicalTitle}</div><div class="meta">${canonicalDetail}${canonicalType}${canonicalYear}</div>`
        : `<span class="meta">-</span>`;
      const artworkUrl = Array.isArray(canonical.artwork) && canonical.artwork.length
        ? canonical.artwork[0].url
        : "";
      const artworkHtml = artworkUrl
        ? `<img class="candidate-artwork" src="${artworkUrl}" alt="">`
        : `<span class="meta">-</span>`;
      const detected = [candidate.artist_detected, candidate.album_detected, candidate.track_detected]
        .filter(Boolean)
        .join(" / ");
      const sourceTitle = candidate.title || "";
      const sourceHtml = sourceTitle
        ? `<div class="cell-title">${sourceTitle}</div>${detected ? `<div class=\"meta\">${detected}</div>` : ""}`
        : `<span class="meta">-</span>`;
      const duration = Number.isFinite(candidate.duration_sec)
        ? formatDuration(candidate.duration_sec)
        : "";
      const score = Number.isFinite(candidate.final_score) ? candidate.final_score.toFixed(3) : "";
    const downloadDisabled = !candidate.id || !candidate.url;
    const sourceUrl = candidate.url ? candidate.url.replace(/"/g, "&quot;") : "";
    const openHtml = sourceUrl
      ? `<a class="button ghost small" href="${sourceUrl}" target="_blank" rel="noopener noreferrer">Open source</a>`
      : "";
    const actionHtml = `<div class="action-group">${openHtml}<button class="button ghost small" data-action="download" data-item-id="${itemId}" data-candidate-id="${candidate.id || ""}" ${downloadDisabled ? "disabled" : ""}>Download</button></div>`;
    tr.innerHTML = `
        <td>${candidate.source || ""}</td>
        <td>${canonicalHtml}</td>
        <td>${artworkHtml}</td>
        <td>${sourceHtml}</td>
        <td>${duration}</td>
        <td>${score}</td>
        <td>${candidate.rank ?? ""}</td>
        <td>${actionHtml}</td>
      `;
      body.appendChild(tr);
    });
  } catch (err) {
    renderSearchEmptyRow(body, 8, `Failed to load candidates: ${err.message}`);
  }
}

async function refreshSearchQueue() {
  const body = $("#search-queue-body");
  const messageEl = $("#search-queue-message");
  try {
    const limitRaw = parseInt($("#search-queue-limit").value, 10);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 100;
    const data = await fetchJson(`/api/download_jobs?limit=${limit}`);
    const jobs = (data.jobs || []).filter((job) => {
      const status = String(job?.status || "").trim().toLowerCase();
      return status !== "completed";
    });

    body.textContent = "";
    if (!jobs.length) {
      renderSearchEmptyRow(body, 10, "No active queue jobs found.");
      if (messageEl) messageEl.textContent = "";
      return;
    }

    jobs.forEach((job) => {
      const downloaded = toFiniteNumber(job.progress_downloaded_bytes);
      const total = toFiniteNumber(job.progress_total_bytes);
      const percent = toFiniteNumber(job.progress_percent) !== null
        ? Math.max(0, Math.min(100, Math.round(Number(job.progress_percent))))
        : ((downloaded !== null && total && total > 0)
          ? Math.max(0, Math.min(100, Math.round((downloaded / total) * 100)))
          : null);
      const progressParts = [];
      if (percent !== null) {
        progressParts.push(`${percent}%`);
      }
      if (downloaded !== null || total !== null) {
        progressParts.push(`${downloaded !== null ? formatBytes(downloaded) : "-"} / ${total !== null ? formatBytes(total) : "-"}`);
      }
      const speedBps = toFiniteNumber(job.progress_speed_bps);
      if (speedBps !== null) {
        progressParts.push(formatSpeed(speedBps));
      }
      const etaSeconds = toFiniteNumber(job.progress_eta_seconds);
      if (etaSeconds !== null) {
        progressParts.push(`ETA ${formatDuration(etaSeconds)}`);
      }
      const progressText = progressParts.length ? progressParts.join(" · ") : "-";
      const displayTitle = String(job.display_title || "").trim()
        || String(job.url || "").trim()
        || String(job.id || "").trim()
        || "-";
      const statusValue = String(job.status || "").trim().toLowerCase();
      const cancellable = ["queued", "claimed", "downloading", "postprocessing"].includes(statusValue);
      const actionHtml = cancellable
        ? `<button class="button ghost small" data-action="cancel-queue-job" data-job-id="${job.id || ""}">Cancel</button>`
        : `<span class="meta">-</span>`;
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${displayTitle}</td>
        <td>${job.origin || ""}</td>
        <td>${job.source || ""}</td>
        <td>${job.media_intent || ""}</td>
        <td>${job.status || ""}</td>
        <td>${progressText}</td>
        <td>${job.attempts ?? ""}</td>
        <td>${formatTimestamp(job.created_at) || ""}</td>
        <td>${job.last_error || ""}</td>
        <td>${actionHtml}</td>
      `;
      body.appendChild(tr);
    });
    if (messageEl) messageEl.textContent = "";
  } catch (err) {
    renderSearchEmptyRow(body, 10, `Failed to load queue: ${err.message}`);
    setNotice(messageEl, `Failed to load queue: ${err.message}`, true);
  }
}

async function clearFailedQueueJobs() {
  const messageEl = $("#search-queue-message");
  const confirmed = window.confirm("Clear all failed/cancelled jobs from the queue table?");
  if (!confirmed) {
    return;
  }
  try {
    setNotice(messageEl, "Clearing failed/cancelled queue jobs...", false);
    const data = await fetchJson("/api/download_jobs/clear_failed", { method: "POST" });
    const deleted = Number.isFinite(Number(data?.deleted)) ? Number(data.deleted) : 0;
    setNotice(messageEl, `Cleared ${deleted} failed/cancelled job${deleted === 1 ? "" : "s"}.`, false);
    await refreshSearchQueue();
  } catch (err) {
    setNotice(messageEl, `Failed to clear failed/cancelled jobs: ${err.message}`, true);
  }
}

async function runStatusQueueAction({ buttonId, endpoint, confirmText, progressText, successText, errorPrefix, refreshQueueTable = true }) {
  const button = $(buttonId);
  if (!button) {
    return;
  }
  const confirmed = !confirmText || window.confirm(confirmText);
  if (!confirmed) {
    return;
  }
  const previousDisabled = button.disabled;
  try {
    button.disabled = true;
    setNotice($("#home-search-message"), progressText, false);
    const data = await fetchJson(endpoint, { method: "POST" });
    const rendered = typeof successText === "function" ? successText(data || {}) : successText;
    setNotice($("#home-search-message"), rendered, false);
    await refreshStatus();
    if (refreshQueueTable) {
      await refreshSearchQueue();
    }
  } catch (err) {
    setNotice($("#home-search-message"), `${errorPrefix}: ${err.message}`, true);
  } finally {
    button.disabled = previousDisabled;
  }
}

async function cleanupTemp() {
  const ok = window.confirm("Clear temporary files? This does not affect completed downloads.");
  if (!ok) {
    return;
  }
  try {
    setNotice($("#downloads-message"), "Cleaning temp files...", false);
    const data = await fetchJson("/api/cleanup", { method: "POST" });
    const bytes = formatBytes(data.deleted_bytes || 0);
    setNotice($("#downloads-message"), `Removed ${data.deleted_files || 0} files (${bytes}).`, false);
  } catch (err) {
    setNotice($("#downloads-message"), `Cleanup failed: ${err.message}`, true);
  }
}

function addAccountRow(name = "", data = {}) {
  const row = document.createElement("div");
  row.className = "row account-row";
  row.dataset.original = JSON.stringify(data || {});
  row.innerHTML = `
    <input class="account-name" type="text" placeholder="name" value="${name}">
    <label class="field">
      <span>Client Secret</span>
      <div class="row tight path-picker">
        <input class="account-client" type="text" placeholder="tokens/client_secret.json" value="${data.client_secret || ""}">
        <button class="button ghost small browse-client" type="button">Browse</button>
      </div>
    </label>
    <label class="field">
      <span>Token</span>
      <div class="row tight path-picker">
        <input class="account-token" type="text" placeholder="tokens/token.json" value="${data.token || ""}">
        <button class="button ghost small browse-token" type="button">Browse</button>
      </div>
    </label>
    <div class="account-actions">
      <button class="button ghost small oauth-run" type="button">Run OAuth</button>
      <button class="button small danger remove account-delete-button">Delete</button>
    </div>
  `;
  row.querySelector(".remove").addEventListener("click", () => {
    if (!window.confirm("Delete this account?")) {
      return;
    }
    row.remove();
    refreshPlaylistAccountSelects();
  });
  row.querySelector(".account-name").addEventListener("input", () => {
    refreshPlaylistAccountSelects();
  });
  row.querySelector(".oauth-run").addEventListener("click", () => {
    startOauthForRow(row);
  });
  row.querySelector(".browse-client").addEventListener("click", () => {
    const input = row.querySelector(".account-client");
    openBrowser(input, "tokens", "file", ".json", resolveBrowseStart("tokens", input.value));
  });
  row.querySelector(".browse-token").addEventListener("click", () => {
    const input = row.querySelector(".account-token");
    openBrowser(input, "tokens", "file", ".json", resolveBrowseStart("tokens", input.value));
  });
  $("#accounts-list").appendChild(row);
  refreshPlaylistAccountSelects();
}

function getConfiguredAccountNamesFromForm() {
  const seen = new Set();
  const names = [];
  $$(".account-row .account-name").forEach((input) => {
    const name = String(input?.value || "").trim();
    if (!name || seen.has(name)) {
      return;
    }
    seen.add(name);
    names.push(name);
  });
  return names.sort((a, b) => a.localeCompare(b));
}

function setPlaylistAccountSelectOptions(selectEl, selectedValue = "") {
  if (!selectEl) return;
  const desired = String(selectedValue || "").trim();
  const accountNames = getConfiguredAccountNamesFromForm();
  const options = ['<option value="">Public</option>'];
  accountNames.forEach((name) => {
    options.push(`<option value="${name}">${name}</option>`);
  });
  selectEl.innerHTML = options.join("");
  if (desired && accountNames.includes(desired)) {
    selectEl.value = desired;
  } else {
    selectEl.value = "";
  }
}

function refreshPlaylistAccountSelects() {
  $$(".playlist-row .playlist-account").forEach((selectEl) => {
    const current = String(selectEl?.value || "").trim();
    setPlaylistAccountSelectOptions(selectEl, current);
  });
}

function addPlaylistRow(entry = {}) {
  const folderValue = normalizeDownloadsRelative(entry.folder || entry.directory || "");
  const normalizedPlaylistId = normalizeYouTubePlaylistIdentifier(entry.playlist_id || entry.id || "");
  const configuredModeRaw = String(entry.media_mode || "").trim().toLowerCase();
  const configuredMode = configuredModeRaw === "music_video"
    ? "music_video"
    : (configuredModeRaw === "music" ? "music" : (entry.music_video ? "music_video" : (entry.music_mode ? "music" : "video")));
  const row = document.createElement("div");
  row.className = "row playlist-row";
  row.dataset.original = JSON.stringify(entry || {});
  row.innerHTML = `
    <label class="field playlist-cell playlist-cell-name">
      <span>Name</span>
      <input class="playlist-name" type="text" placeholder="playlist name" value="${entry.name || ""}">
    </label>
    <label class="field playlist-cell playlist-cell-id">
      <span>Playlist URL or ID</span>
      <input class="playlist-id" type="text" placeholder="https://www.youtube.com/playlist?list=... or PL..." value="${normalizedPlaylistId}">
    </label>
    <label class="field playlist-cell playlist-cell-folder">
      <span>Destination</span>
      <div class="row tight path-picker destination-picker">
        <input class="playlist-folder" type="text" placeholder="downloads/folder" value="${folderValue}">
        <button class="button ghost small browse-folder" type="button">Browse</button>
      </div>
    </label>
    <label class="field playlist-cell playlist-cell-account">
      <span>Account</span>
      <select class="playlist-account"></select>
    </label>
    <label class="field inline playlist-cell playlist-cell-mode">
      <span>Media mode</span>
      <select class="playlist-media-mode hidden" aria-hidden="true" tabindex="-1">
        <option value="video">Video</option>
        <option value="music">Music</option>
        <option value="music_video">Music Video</option>
      </select>
      <div class="home-media-mode-toggle playlist-media-mode-toggle" role="radiogroup" aria-label="Playlist media mode">
        <button type="button" class="home-media-mode-pill" data-mode="video" role="radio" aria-checked="false">Video</button>
        <button type="button" class="home-media-mode-pill" data-mode="music_video" role="radio" aria-checked="false">MV</button>
        <button type="button" class="home-media-mode-pill" data-mode="music" role="radio" aria-checked="false">Music</button>
      </div>
    </label>
    <label class="field inline playlist-cell playlist-cell-format">
      <span>Format</span>
      <select class="playlist-format">
        <option value="">(default)</option>
        <option value="mkv">mkv</option>
        <option value="mp4">mp4</option>
        <option value="webm">webm</option>
        <option value="mp3">mp3</option>
        <option value="m4a">m4a</option>
      </select>
    </label>
    <label class="field inline playlist-cell playlist-cell-subscribe">
      <span class="playlist-subscribe-label">
        <span class="playlist-subscribe-primary">Only download new videos</span>
        <span class="playlist-subscribe-secondary">(subscribe mode)</span>
      </span>
      <input class="playlist-subscribe" type="checkbox" ${entry.mode === "subscribe" ? "checked" : ""}>
    </label>
    <label class="field inline playlist-cell playlist-cell-remove-toggle">
      <span>Remove after</span>
      <input class="playlist-remove" type="checkbox" ${entry.remove_after_download ? "checked" : ""}>
    </label>
    <button class="button danger remove playlist-remove-button">Delete</button>
  `;
  const separator = document.createElement("div");
  separator.className = "playlist-separator";
  row.appendChild(separator);
  row.querySelector(".remove").addEventListener("click", () => {
    if (!window.confirm("Delete this playlist?")) {
      return;
    }
    row.remove();
  });
  row.querySelector(".browse-folder").addEventListener("click", () => {
    const target = row.querySelector(".playlist-folder");
    openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", target.value));
  });
  row.querySelector(".playlist-id").addEventListener("blur", () => {
    const input = row.querySelector(".playlist-id");
    const normalized = normalizeYouTubePlaylistIdentifier(input?.value || "");
    if (input) {
      input.value = normalized;
    }
  });
  setPlaylistAccountSelectOptions(row.querySelector(".playlist-account"), entry.account || "");
  row.querySelector(".playlist-format").value = entry.final_format || "";
  const mediaModeSelect = row.querySelector(".playlist-media-mode");
  const mediaModeToggle = row.querySelector(".playlist-media-mode-toggle");
  const syncPlaylistMediaModeToggle = () => {
    const selected = String(mediaModeSelect?.value || "video").trim().toLowerCase();
    mediaModeToggle?.querySelectorAll("button[data-mode]").forEach((button) => {
      const mode = String(button.dataset.mode || "").trim().toLowerCase();
      const active = mode === selected;
      button.classList.toggle("active", active);
      button.setAttribute("aria-checked", active ? "true" : "false");
    });
  };
  if (mediaModeSelect) {
    mediaModeSelect.value = configuredMode;
    mediaModeSelect.addEventListener("change", syncPlaylistMediaModeToggle);
  }
  if (mediaModeToggle && mediaModeSelect) {
    mediaModeToggle.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-mode]");
      if (!button) return;
      const next = String(button.dataset.mode || "video").trim().toLowerCase();
      mediaModeSelect.value = next === "music_video" ? "music_video" : (next === "music" ? "music" : "video");
      syncPlaylistMediaModeToggle();
    });
  }
  syncPlaylistMediaModeToggle();
  $("#playlists-list").appendChild(row);
}

function renderConfig(cfg) {
  state.suppressDirty = true;
  $("#cfg-upload-date-format").value = cfg.upload_date_format ?? "";
  $("#cfg-filename-template").value = cfg.filename_template ?? "";
  $("#cfg-final-format").value = cfg.final_format ?? "";
  $("#cfg-home-music-final-format").value = cfg.home_music_final_format ?? cfg.music_final_format ?? "";
  $("#cfg-home-music-video-final-format").value = cfg.home_music_video_final_format ?? cfg.final_format ?? "";
  $("#cfg-js-runtime").value = cfg.js_runtime ?? "";
  $("#cfg-single-download-folder").value = normalizeDownloadsRelative(cfg.single_download_folder ?? "");
  $("#cfg-home-music-download-folder").value = normalizeDownloadsRelative(
    cfg.home_music_download_folder ?? cfg.music_download_folder ?? ""
  );
  $("#cfg-home-music-video-download-folder").value = normalizeDownloadsRelative(
    cfg.home_music_video_download_folder ?? ""
  );
  const musicCfg = (cfg && typeof cfg.music === "object") ? cfg.music : {};
  $("#cfg-music-library-path").value = musicCfg.library_path ?? "";
  const musicExportsList = $("#music-exports-list");
  if (musicExportsList) {
    musicExportsList.textContent = "";
    normalizeMusicExports(cfg).forEach((entry) => addMusicExportRow(entry));
  }
  $("#cfg-community-cache-lookup-enabled").checked = !!(
    cfg.community_cache_lookup_enabled ?? cfg.community_cache_enabled
  );
  $("#cfg-community-cache-publish-enabled").checked = !!cfg.community_cache_publish_enabled;
  $("#cfg-community-cache-publish-mode").value = cfg.community_cache_publish_mode ?? "off";
  $("#cfg-community-cache-publish-min-score").value = Number.isFinite(cfg.community_cache_publish_min_score)
    ? Number(cfg.community_cache_publish_min_score)
    : 0.78;
  $("#cfg-community-cache-publish-outbox-dir").value = cfg.community_cache_publish_outbox_dir ?? "";
  $("#cfg-community-cache-publish-repo").value = cfg.community_cache_publish_repo ?? "sudostacks/retreivr-community-cache";
  $("#cfg-community-cache-publish-target-branch").value = cfg.community_cache_publish_target_branch ?? "main";
  $("#cfg-community-cache-publish-branch").value = "Auto-managed: retreivr-community-publish/<instance>";
  $("#cfg-community-cache-publish-open-pr").checked = typeof cfg.community_cache_publish_open_pr === "boolean"
    ? cfg.community_cache_publish_open_pr
    : true;
  $("#cfg-community-cache-publish-poll-minutes").value = Number.isFinite(cfg.community_cache_publish_poll_minutes)
    ? Number(cfg.community_cache_publish_poll_minutes)
    : 15;
  $("#cfg-community-cache-publish-batch-size").value = Number.isFinite(cfg.community_cache_publish_batch_size)
    ? Number(cfg.community_cache_publish_batch_size)
    : 25;
  $("#cfg-community-cache-publish-token-env").value = cfg.community_cache_publish_token_env ?? "RETREIVR_COMMUNITY_CACHE_GITHUB_TOKEN";
  $("#cfg-community-cache-publish-publisher").value = cfg.community_cache_publish_publisher ?? "";
  const resolutionApi = (cfg && typeof cfg.resolution_api === "object") ? cfg.resolution_api : {};
  $("#cfg-resolution-api-upstream-base-url").value = resolutionApi.upstream_base_url ?? "";
  $("#cfg-resolution-api-sync-enabled").checked = !!resolutionApi.sync_enabled;
  $("#cfg-resolution-api-sync-poll-minutes").value = Number.isFinite(resolutionApi.sync_poll_minutes)
    ? Number(resolutionApi.sync_poll_minutes)
    : 1440;
  $("#cfg-resolution-api-sync-batch-size").value = Number.isFinite(resolutionApi.sync_batch_size)
    ? Number(resolutionApi.sync_batch_size)
    : 500;
  $("#cfg-resolution-api-local-node-id").value = resolutionApi.local_node_id ?? "local_node";
  $("#cfg-music-template").value = cfg.music_filename_template ?? "";
  $("#cfg-yt-dlp-cookies").value = cfg.yt_dlp_cookies ?? "";
  const musicMetaDefaults = {
    enabled: true,
    confidence_threshold: 70,
    use_acoustid: false,
    acoustid_api_key: "",
    embed_artwork: true,
    allow_overwrite_tags: true,
    max_artwork_size_px: 1500,
    rate_limit_seconds: 1.5,
    dry_run: false,
  };
  const musicMeta = cfg.music_metadata || {};
  const musicMatchThresholdPercent = Number.isFinite(cfg.music_source_match_threshold)
    ? Math.round(Number(cfg.music_source_match_threshold) * 100)
    : (Number.isFinite(cfg.music_mb_binding_threshold)
      ? Math.round(Number(cfg.music_mb_binding_threshold) * 100)
      : 78);
  $("#cfg-music-meta-enabled").checked = typeof musicMeta.enabled === "boolean"
    ? musicMeta.enabled
    : musicMetaDefaults.enabled;
  $("#cfg-music-meta-threshold").value = musicMatchThresholdPercent;
  $("#cfg-music-enrich-threshold").value = Number.isFinite(musicMeta.confidence_threshold)
    ? musicMeta.confidence_threshold
    : musicMetaDefaults.confidence_threshold;
  $("#cfg-music-meta-acoustid").checked = typeof musicMeta.use_acoustid === "boolean"
    ? musicMeta.use_acoustid
    : musicMetaDefaults.use_acoustid;
  $("#cfg-music-meta-acoustid-key").value = musicMeta.acoustid_api_key ?? "";
  $("#cfg-music-meta-artwork").checked = typeof musicMeta.embed_artwork === "boolean"
    ? musicMeta.embed_artwork
    : musicMetaDefaults.embed_artwork;
  $("#cfg-music-meta-overwrite").checked = typeof musicMeta.allow_overwrite_tags === "boolean"
    ? musicMeta.allow_overwrite_tags
    : musicMetaDefaults.allow_overwrite_tags;
  $("#cfg-music-meta-artwork-size").value = Number.isFinite(musicMeta.max_artwork_size_px)
    ? musicMeta.max_artwork_size_px
    : musicMetaDefaults.max_artwork_size_px;
  $("#cfg-music-meta-rate").value = Number.isFinite(musicMeta.rate_limit_seconds)
    ? musicMeta.rate_limit_seconds
    : musicMetaDefaults.rate_limit_seconds;
  $("#cfg-music-meta-dry-run").checked = typeof musicMeta.dry_run === "boolean"
    ? musicMeta.dry_run
    : musicMetaDefaults.dry_run;
  const watcher = cfg.watcher || {};
  const watcherEnabled = typeof cfg.enable_watcher === "boolean"
    ? cfg.enable_watcher
    : (typeof watcher.enabled === "boolean" ? watcher.enabled : true);
  const watcherToggle = $("#cfg-watcher-enabled");
  if (watcherToggle) {
    watcherToggle.checked = watcherEnabled;
  }

  const defaultPolicy = {
    min_interval_minutes: 5,
    max_interval_minutes: 360,
    idle_backoff_factor: 2,
    active_reset_minutes: 5,
    downtime: {
      enabled: false,
      start: "23:00",
      end: "09:00",
      timezone: "local",
    },
  };
  const policy = cfg.watch_policy || defaultPolicy;
  const minInterval = Number.isFinite(policy.min_interval_minutes)
    ? policy.min_interval_minutes
    : defaultPolicy.min_interval_minutes;
  const maxInterval = Number.isFinite(policy.max_interval_minutes)
    ? policy.max_interval_minutes
    : defaultPolicy.max_interval_minutes;
  $("#cfg-watcher-min-interval").value = minInterval;
  $("#cfg-watcher-max-interval").value = maxInterval;
  $("#cfg-watcher-idle-backoff").value = Number.isFinite(policy.idle_backoff_factor)
    ? policy.idle_backoff_factor
    : defaultPolicy.idle_backoff_factor;
  $("#cfg-watcher-active-reset").value = Number.isFinite(policy.active_reset_minutes)
    ? policy.active_reset_minutes
    : defaultPolicy.active_reset_minutes;
  const downtime = policy.downtime || defaultPolicy.downtime;
  $("#cfg-watcher-downtime-enabled").checked = !!downtime.enabled;
  $("#cfg-watcher-downtime-start").value = downtime.start || defaultPolicy.downtime.start;
  $("#cfg-watcher-downtime-end").value = downtime.end || defaultPolicy.downtime.end;
  $("#cfg-watcher-downtime-timezone").value = downtime.timezone || defaultPolicy.downtime.timezone;

  const telegram = cfg.telegram || {};
  $("#cfg-telegram-token").value = telegram.bot_token ?? "";
  $("#cfg-telegram-chat").value = telegram.chat_id ?? "";
  const spotify = cfg.spotify || {};
  const spotifyEnabledToggle = $("#spotify-enabled");
  if (spotifyEnabledToggle) {
    spotifyEnabledToggle.checked = typeof spotify.enabled === "boolean" ? spotify.enabled : true;
  }

  $("#accounts-list").textContent = "";
  const accounts = cfg.accounts || {};
  Object.keys(accounts).forEach((name) => addAccountRow(name, accounts[name] || {}));

  $("#playlists-list").textContent = "";
  const playlists = cfg.playlists || [];
  playlists.forEach((entry) => addPlaylistRow(entry));
  renderSpotifyPlaylists(cfg);
  refreshSpotifyPlaylistStatus();

  const opts = cfg.yt_dlp_opts || {};
  $("#cfg-yt-dlp-opts").value = Object.keys(opts).length ? JSON.stringify(opts, null, 2) : "";
  syncConfigSectionCollapsedStates();
  state.suppressDirty = false;
}

function renderSpotifyPlaylists(cfg) {
  const container = $("#spotify-playlists-list");
  if (!container) return;
  container.textContent = "";
  const entries = cfg.spotify_playlists || [];
  if (!entries.length) {
    const note = document.createElement("div");
    note.className = "notice";
    note.textContent = "No Spotify playlists configured.";
    container.appendChild(note);
    return;
  }
  entries.forEach((entry) => {
    const row = document.createElement("div");
    row.className = "row spotify-playlist-row";
    row.dataset.playlistUrl = entry.playlist_url || "";

    const info = document.createElement("div");
    info.className = "spotify-playlist-info";
    const title = document.createElement("div");
    title.className = "spotify-playlist-title";
    title.textContent = entry.name || "Spotify Playlist";
    const summary = document.createElement("div");
    summary.className = "meta spotify-playlist-summary";
    const summaryParts = [];
    if (entry.destination) {
      summaryParts.push(`Destination: ${entry.destination}`);
    } else {
      summaryParts.push("Destination: default");
    }
    summaryParts.push(`Auto-download: ${entry.auto_download === false ? "off" : "on"}`);
    const minScore = Number.isFinite(entry.min_match_score) ? entry.min_match_score.toFixed(2) : "0.65";
    summaryParts.push(`Min score: ${minScore}`);
    summary.textContent = summaryParts.join(" · ");
    const status = document.createElement("div");
    status.className = "spotify-playlist-status";
    const url = document.createElement("div");
    url.className = "meta";
    url.textContent = entry.playlist_url || "";
    info.append(title, summary, status, url);

    const button = document.createElement("button");
    button.type = "button";
    button.className = "button ghost small spotify-import-button";
    button.textContent = "Import Playlist";
    button.addEventListener("click", () => importSpotifyPlaylist(entry, row, button));

    container.append(row);
    row.append(info, button);
    updateSpotifyPlaylistRowStatus(row, entry.playlist_url);
  });
}

async function refreshSpotifyPlaylistStatus() {
  try {
    const data = await fetchJson("/api/spotify/playlists/status");
    state.spotifyPlaylistStatus = data.statuses || {};
    updateSpotifyPlaylistStatusDisplay();
  } catch (err) {
    // ignore
  }
}

function applySpotifyConfigState(cfg, oauthStatus) {
  const spotifyCfg = (cfg && cfg.spotify) || {};
  const spotifyEnabled = typeof spotifyCfg.enabled === "boolean" ? spotifyCfg.enabled : true;
  const connected = !!(oauthStatus && oauthStatus.connected);

  const spotifyEnabledToggle = $("#spotify-enabled");
  if (spotifyEnabledToggle) {
    spotifyEnabledToggle.checked = spotifyEnabled;
  }

  const spotifyClientId = $("#spotify-client-id");
  if (spotifyClientId) {
    spotifyClientId.value = spotifyCfg.client_id ?? "";
  }
  const spotifyClientSecret = $("#spotify-client-secret");
  if (spotifyClientSecret) {
    spotifyClientSecret.value = spotifyCfg.client_secret ?? "";
  }
  const spotifyRedirectUri = $("#spotify-redirect-uri");
  if (spotifyRedirectUri) {
    const explicitRedirectUri = String(spotifyCfg.redirect_uri || "").trim();
    const defaultRedirectUri = `${window.location.protocol}//${window.location.host}/api/spotify/oauth/callback`;
    spotifyRedirectUri.value = explicitRedirectUri || defaultRedirectUri;
    spotifyRedirectUri.readOnly = true;
  }
  const spotifyWatchPlaylists = $("#config-spotify-playlists");
  if (spotifyWatchPlaylists) {
    if (cfg.spotify && Array.isArray(cfg.spotify.watch_playlists)) {
      spotifyWatchPlaylists.value = cfg.spotify.watch_playlists.join("\n");
    } else {
      spotifyWatchPlaylists.value = "";
    }
  }

  const statusEl = $("#spotify-connection-status");
  if (statusEl) {
    statusEl.textContent = connected ? "Connected" : "Not connected";
    statusEl.classList.toggle("running", connected);
  }

  const connectBtn = $("#spotify-connect-btn");
  if (connectBtn) {
    connectBtn.style.display = connected ? "none" : "";
  }
  const disconnectBtn = $("#spotify-disconnect-btn");
  if (disconnectBtn) {
    disconnectBtn.style.display = connected ? "" : "none";
  }

  const syncLiked = $("#spotify-sync-liked");
  if (syncLiked) {
    syncLiked.checked = !!spotifyCfg.sync_liked_songs;
  }
  const syncSaved = $("#spotify-sync-saved");
  if (syncSaved) {
    syncSaved.checked = !!spotifyCfg.sync_saved_albums;
  }
  const syncPlaylists = $("#spotify-sync-playlists");
  if (syncPlaylists) {
    syncPlaylists.checked = !!spotifyCfg.sync_user_playlists;
  }

  const likedInterval = $("#spotify-liked-interval");
  if (likedInterval) {
    likedInterval.value = spotifyCfg.liked_songs_sync_interval_minutes ?? 15;
  }
  const savedInterval = $("#spotify-saved-interval");
  if (savedInterval) {
    savedInterval.value = spotifyCfg.saved_albums_sync_interval_minutes ?? 30;
  }
  const playlistsInterval = $("#spotify-playlists-interval");
  if (playlistsInterval) {
    playlistsInterval.value = spotifyCfg.user_playlists_sync_interval_minutes ?? 30;
  }

  const syncControls = [
    $("#spotify-sync-liked"),
    $("#spotify-sync-saved"),
    $("#spotify-sync-playlists"),
    $("#spotify-liked-interval"),
    $("#spotify-saved-interval"),
    $("#spotify-playlists-interval"),
  ];
  syncControls.forEach((el) => {
    if (!el) return;
    el.disabled = !connected;
    el.setAttribute("aria-disabled", (!connected).toString());
  });

  if (statusEl) {
    let helperEl = $("#spotify-connection-helper");
    if (!connected) {
      if (!helperEl) {
        helperEl = document.createElement("div");
        helperEl.id = "spotify-connection-helper";
        helperEl.className = "meta";
        statusEl.insertAdjacentElement("afterend", helperEl);
      }
      helperEl.textContent = "Connect Spotify to enable sync options.";
    } else if (helperEl) {
      helperEl.remove();
    }
  }
  syncConfigSectionCollapsedStates();
}

async function refreshSpotifyConfig() {
  try {
    const cfg = await fetchJson("/api/config");
    state.config = cfg;
    let oauthStatus = { connected: false };
    try {
      oauthStatus = await fetchJson("/api/spotify/oauth/status");
    } catch (err) {
      oauthStatus = { connected: false };
    }
    applySpotifyConfigState(cfg, oauthStatus);
    const connected = !!oauthStatus.connected;
    if (connected && !state.spotifyOauthConnected) {
      window.dispatchEvent(new CustomEvent("spotify-oauth-complete"));
    }
    state.spotifyOauthConnected = connected;
  } catch (err) {
    setConfigNotice(`Spotify config refresh failed: ${err.message}`, true);
  }
}

async function connectSpotify() {
  try {
    const data = await fetchJson("/api/spotify/oauth/connect");
    if (data && data.auth_url) {
      window.open(data.auth_url, "_blank", "noopener");
    }
    setConfigNotice("Complete Spotify authorization in the opened window.", false);
    await refreshSpotifyConfig();
  } catch (err) {
    setConfigNotice(`Spotify connect failed: ${err.message}`, true);
  }
}

async function disconnectSpotify() {
  try {
    await fetchJson("/api/spotify/oauth/disconnect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    await refreshSpotifyConfig();
    setConfigNotice("Spotify disconnected.", false);
  } catch (err) {
    setConfigNotice(`Spotify disconnect failed: ${err.message}`, true);
  }
}

function updateSpotifyPlaylistStatusDisplay() {
  $$(".spotify-playlist-row").forEach((row) => {
    const url = row.dataset.playlistUrl;
    updateSpotifyPlaylistRowStatus(row, url);
  });
}

function updateSpotifyPlaylistRowStatus(row, playlistUrl) {
  const statusEl = row.querySelector(".spotify-playlist-status");
  if (!statusEl) return;
  const status = state.spotifyPlaylistStatus[playlistUrl] || null;
  if (!status) {
    statusEl.textContent = "Status: Idle";
    return;
  }

  let statusLabel = "Status: Idle";
  if (status.state === "importing") {
    statusLabel = "Status: Importing...";
  } else if (status.state === "completed") {
    if (status.tracks_queued > 0) {
      statusLabel = "✅ Imported successfully";
    } else if (status.tracks_skipped > 0) {
      statusLabel = "🔁 Imported, tracks already queued";
    } else if (status.tracks_discovered > 0) {
      statusLabel = "⚠ Imported, no matches met threshold";
    } else {
      statusLabel = "⚠ Imported, no tracks found";
    }
  } else if (status.state === "error") {
    statusLabel = "❌ Import error";
  } else {
    statusLabel = `Status: ${status.state || "idle"}`;
  }

  const parts = [statusLabel];
  if (status.destination) {
    parts.push(`Destination: ${status.destination}`);
  }
  if (status.tracks_discovered != null) {
    parts.push(`Tracks discovered: ${status.tracks_discovered}`);
  }
  if (status.tracks_queued != null && status.tracks_queued > 0) {
    parts.push(`Queued: ${status.tracks_queued}`);
  }
  if (status.tracks_skipped != null && status.tracks_skipped > 0) {
    parts.push(`Duplicates skipped: ${status.tracks_skipped}`);
  }
  if (status.tracks_failed != null && status.tracks_failed > 0) {
    parts.push(`No matches: ${status.tracks_failed}`);
  }
  if (status.error) {
    parts.push(`Error: ${status.error}`);
  } else if (status.message && status.state === "importing") {
    parts.push(status.message);
  }
  statusEl.textContent = parts.join(" · ");
}

async function importSpotifyPlaylist(entry, row, button) {
  const playlistUrl = (entry.playlist_url || "").trim();
  if (!playlistUrl) {
    return;
  }
  button.disabled = true;
  state.spotifyPlaylistStatus[playlistUrl] = {
    state: "importing",
    message: "Importing playlist metadata...",
    tracks_discovered: 0,
    tracks_queued: 0,
    error: null,
    updated_at: new Date().toISOString(),
  };
  updateSpotifyPlaylistRowStatus(row, playlistUrl);
  try {
    const result = await fetchJson("/api/spotify/playlists/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ playlist_url: playlistUrl }),
    });
    state.spotifyPlaylistStatus[playlistUrl] = result.status || state.spotifyPlaylistStatus[playlistUrl];
    updateSpotifyPlaylistRowStatus(row, playlistUrl);
  } catch (err) {
    state.spotifyPlaylistStatus[playlistUrl] = {
      state: "error",
      message: "Import failed",
      error: err.message,
      tracks_discovered: 0,
      tracks_queued: 0,
      updated_at: new Date().toISOString(),
    };
    updateSpotifyPlaylistRowStatus(row, playlistUrl);
  } finally {
    button.disabled = false;
    await refreshSpotifyPlaylistStatus();
  }
}

async function loadConfig() {
  try {
    await refreshConfigPath();
    const cfg = await fetchJson("/api/config");
    state.config = cfg;
    renderConfig(cfg);
    refreshCommunityPublishStatus().catch(() => {});
    updateSearchDestinationDisplay();
    applyHomeDefaultDestination({ force: false });
    applyHomeDefaultVideoFormat({ force: true });
    updateMusicModeFormatControl();
    state.configDirty = false;
    updatePollingState();
    setConfigNotice("Config loaded", false);
  } catch (err) {
    setConfigNotice(`Config error: ${err.message}`, true);
  }
}

function buildConfigFromForm() {
  const base = state.config ? JSON.parse(JSON.stringify(state.config)) : {};
  const errors = [];

  const uploadFmt = $("#cfg-upload-date-format").value.trim();
  if (uploadFmt) {
    base.upload_date_format = uploadFmt;
  } else {
    delete base.upload_date_format;
  }

  const filenameTemplate = $("#cfg-filename-template").value.trim();
  if (filenameTemplate) {
    base.filename_template = filenameTemplate;
  } else {
    delete base.filename_template;
  }

  const finalFormat = $("#cfg-final-format").value.trim();
  if (finalFormat) {
    base.final_format = finalFormat;
  } else {
    delete base.final_format;
  }

  const homeMusicFinalFormat = $("#cfg-home-music-final-format").value.trim();
  if (homeMusicFinalFormat) {
    base.home_music_final_format = homeMusicFinalFormat;
  } else {
    delete base.home_music_final_format;
  }

  const homeMusicVideoFinalFormat = $("#cfg-home-music-video-final-format").value.trim();
  if (homeMusicVideoFinalFormat) {
    base.home_music_video_final_format = homeMusicVideoFinalFormat;
  } else {
    delete base.home_music_video_final_format;
  }

  const jsRuntime = $("#cfg-js-runtime").value.trim();
  if (jsRuntime) {
    base.js_runtime = jsRuntime;
  } else {
    delete base.js_runtime;
  }

  const musicTemplate = $("#cfg-music-template").value.trim();
  if (musicTemplate) {
    base.music_filename_template = musicTemplate;
  } else {
    delete base.music_filename_template;
  }

  const cookiesPath = $("#cfg-yt-dlp-cookies").value.trim();
  if (cookiesPath) {
    base.yt_dlp_cookies = cookiesPath;
  } else {
    delete base.yt_dlp_cookies;
  }

  const metaDefaults = {
    enabled: true,
    confidence_threshold: 70,
    use_acoustid: false,
    acoustid_api_key: "",
    embed_artwork: true,
    allow_overwrite_tags: true,
    max_artwork_size_px: 1500,
    rate_limit_seconds: 1.5,
    dry_run: false,
  };
  const matchThresholdPercent = parseInt($("#cfg-music-meta-threshold").value, 10);
  const enrichThreshold = parseInt($("#cfg-music-enrich-threshold").value, 10);
  const metaArtworkSize = parseInt($("#cfg-music-meta-artwork-size").value, 10);
  const metaRate = parseFloat($("#cfg-music-meta-rate").value);
  const normalizedMatchThresholdPercent = Number.isInteger(matchThresholdPercent)
    ? Math.max(0, Math.min(100, matchThresholdPercent))
    : 78;
  base.music_mb_binding_threshold = normalizedMatchThresholdPercent / 100;
  base.music_source_match_threshold = normalizedMatchThresholdPercent / 100;
  base.music_metadata = {
    enabled: $("#cfg-music-meta-enabled").checked,
    confidence_threshold: Number.isInteger(enrichThreshold) ? enrichThreshold : metaDefaults.confidence_threshold,
    use_acoustid: $("#cfg-music-meta-acoustid").checked,
    acoustid_api_key: $("#cfg-music-meta-acoustid-key").value.trim(),
    embed_artwork: $("#cfg-music-meta-artwork").checked,
    allow_overwrite_tags: $("#cfg-music-meta-overwrite").checked,
    max_artwork_size_px: Number.isInteger(metaArtworkSize) ? metaArtworkSize : metaDefaults.max_artwork_size_px,
    rate_limit_seconds: Number.isFinite(metaRate) ? metaRate : metaDefaults.rate_limit_seconds,
    dry_run: $("#cfg-music-meta-dry-run").checked,
  };

  const watcherEnabled = $("#cfg-watcher-enabled").checked;
  base.enable_watcher = watcherEnabled;
  base.watcher = { ...(base.watcher || {}), enabled: watcherEnabled };
  const watcherPolicy = {
    min_interval_minutes: parseInt($("#cfg-watcher-min-interval").value, 10),
    max_interval_minutes: parseInt($("#cfg-watcher-max-interval").value, 10),
    idle_backoff_factor: parseInt($("#cfg-watcher-idle-backoff").value, 10),
    active_reset_minutes: parseInt($("#cfg-watcher-active-reset").value, 10),
    downtime: {
      enabled: $("#cfg-watcher-downtime-enabled").checked,
      start: $("#cfg-watcher-downtime-start").value.trim(),
      end: $("#cfg-watcher-downtime-end").value.trim(),
      timezone: $("#cfg-watcher-downtime-timezone").value.trim(),
    },
  };
  const policyErrors = [];
  if (!Number.isInteger(watcherPolicy.min_interval_minutes) || watcherPolicy.min_interval_minutes < 1) {
    policyErrors.push("Watcher min interval must be an integer >= 1");
  }
  if (!Number.isInteger(watcherPolicy.max_interval_minutes) || watcherPolicy.max_interval_minutes < 1) {
    policyErrors.push("Watcher max interval must be an integer >= 1");
  }
  if (Number.isInteger(watcherPolicy.min_interval_minutes)
      && Number.isInteger(watcherPolicy.max_interval_minutes)
      && watcherPolicy.max_interval_minutes < watcherPolicy.min_interval_minutes) {
    policyErrors.push("Watcher max interval must be >= min interval");
  }
  if (!Number.isInteger(watcherPolicy.idle_backoff_factor) || watcherPolicy.idle_backoff_factor < 1) {
    policyErrors.push("Watcher idle backoff factor must be an integer >= 1");
  }
  if (!Number.isInteger(watcherPolicy.active_reset_minutes) || watcherPolicy.active_reset_minutes < 1) {
    policyErrors.push("Watcher active reset must be an integer >= 1");
  }
  if (!watcherPolicy.downtime.start) {
    watcherPolicy.downtime.start = "23:00";
  }
  if (!watcherPolicy.downtime.end) {
    watcherPolicy.downtime.end = "09:00";
  }
  if (!watcherPolicy.downtime.timezone) {
    watcherPolicy.downtime.timezone = "local";
  }
  if (policyErrors.length) {
    errors.push(...policyErrors);
  } else {
    base.watch_policy = watcherPolicy;
  }

  let singleFolder = $("#cfg-single-download-folder").value.trim();
  singleFolder = normalizeDownloadsRelative(singleFolder);
  if (singleFolder) {
    base.single_download_folder = singleFolder;
  } else {
    delete base.single_download_folder;
  }

  let homeMusicFolder = $("#cfg-home-music-download-folder").value.trim();
  homeMusicFolder = normalizeDownloadsRelative(homeMusicFolder);
  if (homeMusicFolder) {
    base.home_music_download_folder = homeMusicFolder;
  } else {
    delete base.home_music_download_folder;
  }

  let homeMusicVideoFolder = $("#cfg-home-music-video-download-folder").value.trim();
  homeMusicVideoFolder = normalizeDownloadsRelative(homeMusicVideoFolder);
  if (homeMusicVideoFolder) {
    base.home_music_video_download_folder = homeMusicVideoFolder;
  } else {
    delete base.home_music_video_download_folder;
  }

  const existingMusic = (base.music && typeof base.music === "object") ? base.music : {};
  const musicLibraryPath = $("#cfg-music-library-path").value.trim();
  const musicExports = [];
  $$("[data-music-export-row='1']").forEach((row, idx) => {
    const enabled = !!row.querySelector(".music-export-enabled")?.checked;
    const name = row.querySelector(".music-export-name")?.value.trim() || "";
    const type = String(row.querySelector(".music-export-type")?.value || "copy").trim().toLowerCase();
    const exportPath = row.querySelector(".music-export-path")?.value.trim() || "";
    const codec = row.querySelector(".music-export-codec")?.value.trim() || "aac";
    const bitrate = row.querySelector(".music-export-bitrate")?.value.trim() || "256k";
    if (!name && !exportPath && !enabled) {
      return;
    }
    if (!name) {
      errors.push(`Music export ${idx + 1} requires a name`);
      return;
    }
    if (!enabled) {
      musicExports.push({ name, enabled: false, type, path: exportPath, codec, bitrate });
      return;
    }
    if (!MUSIC_EXPORT_TYPES.includes(type)) {
      errors.push(`Music export ${idx + 1} type must be copy or transcode`);
      return;
    }
    if (!exportPath) {
      errors.push(`Music export ${idx + 1} requires an export path when enabled`);
      return;
    }
    if (type === "transcode" && !bitrate) {
      errors.push(`Music export ${idx + 1} requires a bitrate for transcode`);
      return;
    }
    musicExports.push({ name, enabled: true, type, path: exportPath, codec, bitrate });
  });
  base.music = { ...existingMusic, library_path: musicLibraryPath, exports: musicExports };

  base.community_cache_lookup_enabled = !!$("#cfg-community-cache-lookup-enabled").checked;
  base.community_cache_enabled = base.community_cache_lookup_enabled;
  base.community_cache_publish_enabled = !!$("#cfg-community-cache-publish-enabled").checked;

  const communityCachePublishMode = String(
    $("#cfg-community-cache-publish-mode").value || "off"
  ).trim().toLowerCase();
  const allowedPublishModes = new Set(["off", "dry_run", "write_outbox"]);
  if (!allowedPublishModes.has(communityCachePublishMode)) {
    errors.push("Community cache publish mode must be off, dry_run, or write_outbox");
  } else {
    base.community_cache_publish_mode = communityCachePublishMode;
  }

  const publishMinScoreRaw = $("#cfg-community-cache-publish-min-score").value.trim();
  if (publishMinScoreRaw) {
    const publishMinScore = Number.parseFloat(publishMinScoreRaw);
    if (!Number.isFinite(publishMinScore) || publishMinScore < 0 || publishMinScore > 1) {
      errors.push("Community cache publish minimum score must be between 0 and 1");
    } else {
      base.community_cache_publish_min_score = publishMinScore;
    }
  } else {
    delete base.community_cache_publish_min_score;
  }

  const publishOutboxDir = $("#cfg-community-cache-publish-outbox-dir").value.trim();
  if (publishOutboxDir) {
    base.community_cache_publish_outbox_dir = publishOutboxDir;
  } else {
    delete base.community_cache_publish_outbox_dir;
  }

  const publishRepo = $("#cfg-community-cache-publish-repo").value.trim();
  if (publishRepo) {
    base.community_cache_publish_repo = publishRepo;
  } else {
    delete base.community_cache_publish_repo;
  }

  const publishTargetBranch = $("#cfg-community-cache-publish-target-branch").value.trim();
  if (publishTargetBranch) {
    base.community_cache_publish_target_branch = publishTargetBranch;
  } else {
    delete base.community_cache_publish_target_branch;
  }

  delete base.community_cache_publish_branch;

  base.community_cache_publish_open_pr = !!$("#cfg-community-cache-publish-open-pr").checked;

  const publishPollMinutesRaw = $("#cfg-community-cache-publish-poll-minutes").value.trim();
  if (publishPollMinutesRaw) {
    const publishPollMinutes = Number.parseInt(publishPollMinutesRaw, 10);
    if (!Number.isInteger(publishPollMinutes) || publishPollMinutes < 1) {
      errors.push("Community cache publish poll interval must be an integer >= 1");
    } else {
      base.community_cache_publish_poll_minutes = publishPollMinutes;
    }
  } else {
    delete base.community_cache_publish_poll_minutes;
  }

  const publishBatchSizeRaw = $("#cfg-community-cache-publish-batch-size").value.trim();
  if (publishBatchSizeRaw) {
    const publishBatchSize = Number.parseInt(publishBatchSizeRaw, 10);
    if (!Number.isInteger(publishBatchSize) || publishBatchSize < 1) {
      errors.push("Community cache publish batch size must be an integer >= 1");
    } else {
      base.community_cache_publish_batch_size = publishBatchSize;
    }
  } else {
    delete base.community_cache_publish_batch_size;
  }

  const publishTokenEnv = $("#cfg-community-cache-publish-token-env").value.trim();
  if (publishTokenEnv) {
    base.community_cache_publish_token_env = publishTokenEnv;
  } else {
    delete base.community_cache_publish_token_env;
  }

  const publishPublisher = $("#cfg-community-cache-publish-publisher").value.trim();
  if (publishPublisher) {
    base.community_cache_publish_publisher = publishPublisher;
  } else {
    delete base.community_cache_publish_publisher;
  }

  const resolutionApi = (base.resolution_api && typeof base.resolution_api === "object")
    ? { ...base.resolution_api }
    : {};
  resolutionApi.upstream_base_url = $("#cfg-resolution-api-upstream-base-url").value.trim();
  resolutionApi.sync_enabled = !!$("#cfg-resolution-api-sync-enabled").checked;
  const resolutionSyncPollRaw = $("#cfg-resolution-api-sync-poll-minutes").value.trim();
  if (resolutionSyncPollRaw) {
    const parsed = Number.parseInt(resolutionSyncPollRaw, 10);
    if (!Number.isInteger(parsed) || parsed < 1) {
      errors.push("Resolution API sync interval must be an integer >= 1");
    } else {
      resolutionApi.sync_poll_minutes = parsed;
    }
  }
  const resolutionSyncBatchRaw = $("#cfg-resolution-api-sync-batch-size").value.trim();
  if (resolutionSyncBatchRaw) {
    const parsed = Number.parseInt(resolutionSyncBatchRaw, 10);
    if (!Number.isInteger(parsed) || parsed < 1) {
      errors.push("Resolution API sync batch size must be an integer >= 1");
    } else {
      resolutionApi.sync_batch_size = parsed;
    }
  }
  const resolutionLocalNodeId = $("#cfg-resolution-api-local-node-id").value.trim();
  if (!resolutionLocalNodeId) {
    errors.push("Resolution API local node identity is required");
  } else {
    resolutionApi.local_node_id = resolutionLocalNodeId;
  }
  base.resolution_api = resolutionApi;

  base.schedule = buildSchedulePayloadFromForm();

  const telegramToken = $("#cfg-telegram-token").value.trim();
  const telegramChat = $("#cfg-telegram-chat").value.trim();
  const existingTelegram = (base.telegram && typeof base.telegram === "object")
    ? base.telegram
    : {};
  const hasExplicitTelegramEnabled = Object.prototype.hasOwnProperty.call(existingTelegram, "enabled");
  if (telegramToken || telegramChat || hasExplicitTelegramEnabled) {
    base.telegram = { ...existingTelegram };
    if (telegramToken) {
      base.telegram.bot_token = telegramToken;
    } else {
      delete base.telegram.bot_token;
    }
    if (telegramChat) {
      base.telegram.chat_id = telegramChat;
    } else {
      delete base.telegram.chat_id;
    }
  } else {
    delete base.telegram;
  }

  base.spotify = base.spotify || {};
  base.spotify.enabled = !!$("#spotify-enabled")?.checked;
  const spotifyClientId = $("#spotify-client-id")?.value.trim() || "";
  const spotifyClientSecret = $("#spotify-client-secret")?.value.trim() || "";
  const spotifyRedirectUri = $("#spotify-redirect-uri")?.value.trim() || "";
  if (spotifyClientId) {
    base.spotify.client_id = spotifyClientId;
  } else {
    delete base.spotify.client_id;
  }
  if (spotifyClientSecret) {
    base.spotify.client_secret = spotifyClientSecret;
  } else {
    delete base.spotify.client_secret;
  }
  if (spotifyRedirectUri) {
    base.spotify.redirect_uri = spotifyRedirectUri;
  } else {
    delete base.spotify.redirect_uri;
  }
  const spotifyWatchPlaylistsInput = $("#config-spotify-playlists");
  if (spotifyWatchPlaylistsInput) {
    const rawPlaylists = spotifyWatchPlaylistsInput.value.trim();
    const entries = rawPlaylists
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);
    base.spotify.watch_playlists = entries;
  }
  base.spotify.sync_liked_songs = !!$("#spotify-sync-liked")?.checked;
  base.spotify.sync_saved_albums = !!$("#spotify-sync-saved")?.checked;
  base.spotify.sync_user_playlists = !!$("#spotify-sync-playlists")?.checked;

  const clampSpotifyInterval = (rawValue) => {
    const parsed = parseInt(rawValue, 10);
    if (!Number.isFinite(parsed)) {
      return 30;
    }
    if (parsed < 1) {
      return 1;
    }
    return parsed;
  };

  const likedInterval = clampSpotifyInterval($("#spotify-liked-interval")?.value);
  base.spotify.liked_songs_sync_interval_minutes = likedInterval;

  const savedInterval = clampSpotifyInterval($("#spotify-saved-interval")?.value);
  base.spotify.saved_albums_sync_interval_minutes = savedInterval;

  const playlistsInterval = clampSpotifyInterval($("#spotify-playlists-interval")?.value);
  base.spotify.user_playlists_sync_interval_minutes = playlistsInterval;

  const accounts = {};
  $$(".account-row").forEach((row) => {
    const name = row.querySelector(".account-name").value.trim();
    if (!name) {
      return;
    }
    const original = row.dataset.original ? JSON.parse(row.dataset.original) : {};
    original.client_secret = row.querySelector(".account-client").value.trim();
    original.token = row.querySelector(".account-token").value.trim();
    accounts[name] = original;
  });
  base.accounts = accounts;

  const playlists = [];
  $$(".playlist-row").forEach((row, idx) => {
    const name = row.querySelector(".playlist-name").value.trim();
    const playlistInput = row.querySelector(".playlist-id").value.trim();
    const playlistId = normalizeYouTubePlaylistIdentifier(playlistInput);
    let folder = row.querySelector(".playlist-folder").value.trim();
    folder = normalizeDownloadsRelative(folder);
    if (!playlistId && !folder) {
      return;
    }
    if (!playlistId || !folder) {
      errors.push(`Playlist ${idx + 1} missing playlist_id or folder`);
      return;
    }
    const original = row.dataset.original ? JSON.parse(row.dataset.original) : {};
    if (name) {
      original.name = name;
    } else {
      delete original.name;
    }
    original.playlist_id = playlistId;
    delete original.id;
    original.folder = folder;
    delete original.directory;
    const account = row.querySelector(".playlist-account").value.trim();
    if (account) {
      original.account = account;
    } else {
      delete original.account;
    }
    const format = row.querySelector(".playlist-format").value.trim();
    if (format) {
      original.final_format = format;
    } else {
      delete original.final_format;
    }
    const selectedMediaMode = String(row.querySelector(".playlist-media-mode")?.value || "video").trim().toLowerCase();
    original.media_mode = selectedMediaMode === "music_video"
      ? "music_video"
      : (selectedMediaMode === "music" ? "music" : "video");
    // Legacy compatibility flags retained for older runtimes/config readers.
    if (original.media_mode === "music") {
      original.music_mode = true;
      delete original.music_video;
    } else if (original.media_mode === "music_video") {
      original.music_video = true;
      delete original.music_mode;
    } else {
      delete original.music_mode;
      delete original.music_video;
    }
    if (row.querySelector(".playlist-subscribe").checked) {
      original.mode = "subscribe";
    } else {
      delete original.mode;
    }
    original.remove_after_download = row.querySelector(".playlist-remove").checked;
    playlists.push(original);
  });
  base.playlists = playlists;

  const optsRaw = $("#cfg-yt-dlp-opts").value.trim();
  if (optsRaw) {
    try {
      base.yt_dlp_opts = JSON.parse(optsRaw);
    } catch (err) {
      errors.push(`yt-dlp options JSON error: ${err.message}`);
    }
  } else {
    delete base.yt_dlp_opts;
  }

  return { config: base, errors };
}

async function saveConfig() {
  const rawText = $("#config-spotify-playlists")?.value.trim() || "";
  if (rawText) {
    const entries = rawText.split(/\r?\n/).map((s) => s.trim());
    for (const entry of entries) {
      if (!entry) {
        continue;
      }
      const id = normalize_spotify_playlist_identifier(entry);
      if (!id.match(/^[A-Za-z0-9]+$/)) {
        setConfigNotice(`Invalid playlist identifier: ${entry}`, true);
        return;
      }
    }
  }

  const result = buildConfigFromForm();
  if (result.errors.length) {
    setConfigNotice(result.errors.join("; "), true);
    return;
  }

  try {
    await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.config),
    });
    setConfigNotice("Spotify playlists updated", false, true);
    state.config = result.config;
    renderConfig(state.config);
    applyHomeDefaultDestination({ force: false });
    applyHomeDefaultVideoFormat({ force: true });
    updateMusicModeFormatControl();
    updateSearchDestinationDisplay();
    await refreshSchedule();
    state.configDirty = false;
    updatePollingState();
  } catch (err) {
    setConfigNotice(`Save failed: ${err.message}`, true);
  }
}

async function updateYtdlp() {
  try {
    setNotice($("#ytdlp-update-message"), "Starting yt-dlp update...", false);
    await fetchJson("/api/yt-dlp/update", { method: "POST" });
    setNotice($("#ytdlp-update-message"), "Update started. Restart container after completion.", false);
  } catch (err) {
    setNotice($("#ytdlp-update-message"), `Update failed: ${err.message}`, true);
  }
}

function getSelectedReviewIds() {
  return Array.from(state.reviewSelectedIds || []).filter(Boolean);
}

function updateReviewToolbarState() {
  const selectedCount = getSelectedReviewIds().length;
  const hasPending = Array.isArray(state.reviewItems) && state.reviewItems.length > 0;
  const acceptSelected = $("#review-accept-selected");
  const rejectSelected = $("#review-reject-selected");
  const clearSelection = $("#review-clear-selection");
  const acceptAll = $("#review-accept-all");
  if (acceptSelected) acceptSelected.disabled = selectedCount === 0;
  if (rejectSelected) rejectSelected.disabled = selectedCount === 0;
  if (clearSelection) clearSelection.disabled = selectedCount === 0;
  if (acceptAll) acceptAll.disabled = !hasPending;
}

function updateReviewPendingIndicators() {
  const pendingCount = Array.isArray(state.reviewItems) ? state.reviewItems.length : 0;
  const navBadge = $("#review-nav-badge");
  if (navBadge) {
    navBadge.textContent = String(pendingCount);
    navBadge.classList.toggle("hidden", pendingCount <= 0);
  }
  const homeAlert = $("#home-review-alert");
  const homeAlertText = $("#home-review-alert-text");
  if (homeAlert && homeAlertText) {
    if (pendingCount > 0) {
      homeAlertText.textContent = `${pendingCount} item${pendingCount === 1 ? "" : "s"} waiting for review.`;
      homeAlert.classList.remove("hidden");
    } else {
      homeAlert.classList.add("hidden");
    }
  }
}

function buildReviewMetricLabel(item, key, label, formatter = null) {
  const details = item?.candidate_details || {};
  let value = details[key];
  if (value === undefined || value === null || value === "") {
    if (key === "bitrate_kbps") {
      value = item?.bitrate_kbps;
    } else if (key === "duration_ms") {
      value = item?.duration_ms;
    }
  }
  if (value === undefined || value === null || value === "") {
    return "";
  }
  const rendered = typeof formatter === "function" ? formatter(value) : String(value);
  if (!rendered) return "";
  return `<span class="chip">${label}: ${escapeHtml(String(rendered))}</span>`;
}

function renderReviewQueue() {
  const listEl = $("#review-list");
  const summaryEl = $("#review-summary");
  if (!listEl || !summaryEl) {
    return;
  }
  const pendingCount = Array.isArray(state.reviewItems) ? state.reviewItems.length : 0;
  summaryEl.textContent = `Pending: ${pendingCount}`;
  updateReviewPendingIndicators();
  const validIds = new Set((state.reviewItems || []).map((item) => String(item.id || "").trim()).filter(Boolean));
  state.reviewSelectedIds = new Set(getSelectedReviewIds().filter((id) => validIds.has(id)));
  updateReviewToolbarState();
  if (!pendingCount) {
    listEl.innerHTML = `<div class="notice">No items currently need review.</div>`;
    return;
  }
  listEl.innerHTML = state.reviewItems.map((item) => {
    const id = String(item.id || "").trim();
    const artist = String(item.artist || item.album_artist || "").trim();
    const track = String(item.track || item.filename || "Unknown Track").trim();
    const album = String(item.album || "").trim();
    const source = String(item.source || "").trim();
    const gate = String(item.top_failed_gate || "").trim();
    const failureReason = String(item.failure_reason || "").trim();
    const durationDeltaMs = item?.candidate_details?.duration_delta_ms;
    const selected = state.reviewSelectedIds.has(id);
    const previewOpen = state.reviewPreviewItemId === id;
    const previewTag = String(item.mime_type || "").startsWith("video/") ? "video" : "audio";
    const metrics = [
      buildReviewMetricLabel(item, "final_score", "Score", (value) => Number(value).toFixed(3)),
      buildReviewMetricLabel(item, "title_similarity", "Title", (value) => Number(value).toFixed(3)),
      buildReviewMetricLabel(item, "artist_similarity", "Artist", (value) => Number(value).toFixed(3)),
      buildReviewMetricLabel(item, "album_similarity", "Album", (value) => Number(value).toFixed(3)),
      buildReviewMetricLabel(item, "duration_delta_ms", "Delta", (value) => `${Math.round(Number(value))}ms`),
      buildReviewMetricLabel(item, "duration_ms", "File length", (value) => formatDuration(Math.round(Number(value) / 1000))),
      buildReviewMetricLabel(item, "bitrate_kbps", "Bitrate", (value) => `${Math.round(Number(value))} kbps`),
    ].filter(Boolean).join("");
    return `
      <article class="review-card" data-review-id="${escapeHtml(id)}" data-artist="${escapeHtml(artist.toLowerCase())}" data-album="${escapeHtml(album.toLowerCase())}">
        <div class="review-card-header">
          <label class="review-select">
            <input type="checkbox" data-action="review-select" data-review-id="${escapeHtml(id)}" ${selected ? "checked" : ""}>
            <span>Select</span>
          </label>
          <div class="review-card-heading">
            <div class="review-card-title">${escapeHtml(artist ? `${artist} - ${track}` : track)}</div>
            <div class="meta">${escapeHtml(album || "Unknown album")}</div>
          </div>
          <span class="chip">${escapeHtml(source || "unknown source")}</span>
        </div>
        <div class="review-card-reasons">
          ${failureReason ? `<span class="chip">${escapeHtml(failureReason.replaceAll("_", " "))}</span>` : ""}
          ${gate ? `<span class="chip">${escapeHtml(gate.replaceAll("_", " "))}</span>` : ""}
          ${durationDeltaMs !== undefined && durationDeltaMs !== null ? `<span class="chip">Duration delta: ${escapeHtml(String(durationDeltaMs))}ms</span>` : ""}
        </div>
        <div class="review-card-metrics">${metrics}</div>
        <div class="review-card-actions">
          <button class="button ghost small" type="button" data-action="review-toggle-preview" data-review-id="${escapeHtml(id)}">${previewOpen ? "Hide Preview" : "Preview"}</button>
          <button class="button ghost small" type="button" data-action="review-accept" data-review-id="${escapeHtml(id)}">Accept</button>
          <button class="button ghost small remove" type="button" data-action="review-reject" data-review-id="${escapeHtml(id)}">Reject</button>
          <button class="button ghost small" type="button" data-action="review-accept-artist" data-review-id="${escapeHtml(id)}">Accept Artist</button>
          <button class="button ghost small" type="button" data-action="review-accept-album" data-review-id="${escapeHtml(id)}">Accept Album</button>
        </div>
        <div class="review-preview ${previewOpen ? "" : "hidden"}" data-review-preview="${escapeHtml(id)}">
          <div class="meta">${escapeHtml(String(item.filename || "").trim())}</div>
          <${previewTag} controls preload="metadata" src="/api/review_queue/${encodeURIComponent(id)}/preview"></${previewTag}>
        </div>
      </article>
    `;
  }).join("");
}

async function refreshReviewQueue() {
  const listEl = $("#review-list");
  const summaryEl = $("#review-summary");
  try {
    if (summaryEl) {
      summaryEl.textContent = "Loading review queue...";
    }
    const data = await fetchJson("/api/review_queue?status=pending&limit=300");
    state.reviewItems = Array.isArray(data.items) ? data.items : [];
    if (state.reviewPreviewItemId) {
      const previewStillExists = state.reviewItems.some((item) => String(item.id || "").trim() === state.reviewPreviewItemId);
      if (!previewStillExists) {
        state.reviewPreviewItemId = null;
      }
    }
    renderReviewQueue();
  } catch (err) {
    state.reviewItems = [];
    state.reviewPreviewItemId = null;
    updateReviewPendingIndicators();
    if (listEl) {
      listEl.innerHTML = `<div class="notice error">Review queue failed: ${escapeHtml(err.message || "Unknown error")}</div>`;
    }
    if (summaryEl) {
      summaryEl.textContent = "Review queue unavailable";
    }
  }
}

async function runReviewQueueAction(action, itemIds, message) {
  const ids = Array.isArray(itemIds) ? itemIds.filter(Boolean) : [];
  if (!ids.length) {
    setNotice($("#review-message"), "No review items selected.", true);
    return;
  }
  try {
    setNotice($("#review-message"), message, false);
    const data = await fetchJson(`/api/review_queue/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ item_ids: ids }),
    });
    const errorCount = Array.isArray(data.errors) ? data.errors.length : 0;
    const doneCount = Number(data.accepted || data.rejected || 0);
    const label = action === "accept" ? "accepted" : "rejected";
    setNotice(
      $("#review-message"),
      `${doneCount} item${doneCount === 1 ? "" : "s"} ${label}${errorCount ? ` • ${errorCount} error${errorCount === 1 ? "" : "s"}` : ""}.`,
      errorCount > 0
    );
    ids.forEach((id) => state.reviewSelectedIds.delete(id));
    await refreshReviewQueue();
  } catch (err) {
    setNotice($("#review-message"), `Review action failed: ${err.message}`, true);
  }
}

async function reconcileLibrary() {
  const button = $("#library-reconcile-button");
  const messageEl = $("#library-reconcile-message");
  const originalLabel = button ? button.textContent : "Reconcile Library";
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "Reconciling...";
    }
    setNotice(messageEl, "Scanning existing music, video, and music-video files and backfilling database entries...", false);
    const data = await fetchJson("/api/library/reconcile", { method: "POST" });
    const summary = [
      `${data.jobs_inserted || 0} jobs`,
      `${data.history_inserted || 0} history`,
      `${data.isrc_records_inserted || 0} ISRC`,
      `${data.audio_files_seen || 0} audio scanned`,
      `${data.video_files_seen || 0} video scanned`,
      `${data.skipped_existing_jobs || 0} already known`,
      `${data.skipped_missing_identity || 0} missing tags`,
      `${data.errors || 0} errors`,
    ].join(" • ");
    setNotice(messageEl, `Library reconcile completed: ${summary}.`, false);
  } catch (err) {
    setNotice(messageEl, `Library reconcile failed: ${err.message}`, true);
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = originalLabel;
    }
  }
}

async function startRun(payload, opts = {}) {
  const messageEl = opts.messageEl || $("#home-search-message");
  try {
    setNotice(messageEl, "Starting run...", false);
    const data = await fetchJson("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setNotice(messageEl, "Run started", false);
    await refreshStatus();
    return data;
  } catch (err) {
    setNotice(messageEl, `Run failed: ${err.message}`, true);
    return null;
  }
}

async function handleCopy(event, noticeEl) {
  const button = event.target.closest("button[data-copy]");
  if (!button) return;
  const raw = button.dataset.value || "";
  let text = "";
  try {
    text = decodeURIComponent(raw);
  } catch (err) {
    text = raw;
  }
  const ok = await copyText(text);
  const label = button.dataset.copy || "value";
  if (ok) {
    setNotice(noticeEl, `${label} copied`, false);
  } else {
    setNotice(noticeEl, `Copy failed for ${label}`, true);
  }
}

function setupTimers() {
  if (state.timers.status) {
    clearInterval(state.timers.status);
  }
  state.timers.status = setInterval(() => {
    withPollingGuard(refreshStatus);
    if (state.currentPage === "status") {
      withPollingGuard(refreshSearchQueue);
    } else if (state.currentPage === "review") {
      if (!state.reviewPreviewItemId) {
        withPollingGuard(refreshReviewQueue);
      }
    } else if (state.currentPage === "home") {
      withPollingGuard(refreshReviewQueue);
    }
  }, 3000);

  if (state.timers.metrics) {
    clearInterval(state.timers.metrics);
  }
  state.timers.metrics = setInterval(() => {
    withPollingGuard(refreshMetrics);
  }, 8000);

  if (state.timers.schedule) {
    clearInterval(state.timers.schedule);
  }
  state.timers.schedule = setInterval(() => {
    withPollingGuard(refreshSchedule);
  }, 8000);

  if (state.timers.logs) {
    clearInterval(state.timers.logs);
  }
  state.timers.logs = setInterval(() => {
    const logsAuto = $("#logs-auto");
    if (!logsAuto || !logsAuto.checked) {
      return;
    }
    if (state.currentPage !== "status") {
      return;
    }
    withPollingGuard(refreshLogs);
  }, 4000);
}

function bindEvents() {
  const navToggle = $("#nav-toggle");
  if (navToggle) {
    navToggle.addEventListener("click", () => {
      const isOpen = document.body.classList.toggle("nav-open");
      navToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
    });
  }
  $$(".filters-toggle").forEach((button) => {
    button.addEventListener("click", () => {
      const targetId = button.dataset.target;
      if (!targetId) return;
      const block = document.getElementById(targetId);
      if (!block) return;
      const open = block.classList.toggle("open");
      button.textContent = open ? "Hide filters" : "Filters";
    });
  });
  $$(".nav-button").forEach((button) => {
    button.addEventListener("click", () => {
      const page = button.dataset.page || "home";
      setPage(page);
      const hashTarget = button.dataset.hash || page;
      window.location.hash = hashTarget;
    });
  });

  $("#logs-refresh").addEventListener("click", refreshLogs);
  $("#logs-auto").addEventListener("change", () => {
    if ($("#logs-auto").checked) {
      state.logsStickToBottom = true;
      refreshLogs();
    }
  });
  const logsOutput = $("#logs-output");
  if (logsOutput) {
    logsOutput.addEventListener("scroll", () => {
      const threshold = 24;
      const distanceFromBottom =
        logsOutput.scrollHeight - (logsOutput.scrollTop + logsOutput.clientHeight);
      state.logsStickToBottom = distanceFromBottom <= threshold;
    });
  }
  $("#downloads-refresh").addEventListener("click", refreshDownloads);
  $("#downloads-apply").addEventListener("click", refreshDownloads);
  $("#downloads-clear").addEventListener("click", async () => {
    $("#downloads-search").value = "";
    $("#downloads-limit").value = 50;
    await refreshDownloads();
  });
  $("#cleanup-temp").addEventListener("click", cleanupTemp);
  $("#history-refresh").addEventListener("click", refreshHistory);
  $("#history-apply").addEventListener("click", refreshHistory);
  $("#history-clear").addEventListener("click", async () => {
    $("#history-search").value = "";
    $("#history-playlist").value = "";
    $("#history-from").value = "";
    $("#history-to").value = "";
    $("#history-limit").value = 50;
    $("#history-sort").value = "date";
    $("#history-dir").value = "desc";
    await refreshHistory();
  });
  $("#history-show-paths").addEventListener("change", refreshHistory);
  $("#history-body").addEventListener("click", async (event) => {
    await handleCopy(event, $("#history-message"));
  });
  $("#downloads-body").addEventListener("click", async (event) => {
    await handleCopy(event, $("#downloads-message"));
  });
  const musicSearchOnly = $("#search-create-only");
  if (musicSearchOnly) {
    const onMusicSearchEnter = (event) => {
      if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        event.preventDefault();
        if (!musicSearchOnly.disabled) {
          musicSearchOnly.click();
        }
      }
    };
    ["#search-artist", "#search-album", "#search-track"].forEach((selector) => {
      const input = $(selector);
      if (input) {
        input.addEventListener("keydown", onMusicSearchEnter);
      }
    });
    musicSearchOnly.addEventListener("click", async () => {
      const artist = String(document.getElementById("search-artist")?.value || "").trim();
      const album = String(document.getElementById("search-album")?.value || "").trim();
      const track = String(document.getElementById("search-track")?.value || "").trim();
      if (!artist && !album && !track) {
        setNotice($("#home-search-message"), "Enter artist, album, or track for Music Search.", true);
        return;
      }
      try {
        setNotice($("#home-search-message"), "Music Mode: searching metadata...", false);
        await performMusicModeSearch();
      } catch (err) {
        setNotice($("#home-search-message"), `Music search failed: ${err.message}`, true);
      }
    });
  }
  $("#search-requests-refresh").addEventListener("click", refreshSearchRequests);
  $("#search-requests-sort").addEventListener("click", () => {
    state.searchRequestsSort = state.searchRequestsSort === "asc" ? "desc" : "asc";
    updateSearchSortLabel();
    refreshSearchRequests();
  });
  $("#search-queue-refresh").addEventListener("click", refreshSearchQueue);
  $("#search-queue-clear-failed").addEventListener("click", clearFailedQueueJobs);
  $("#search-queue-body").addEventListener("click", async (event) => {
    const button = event.target.closest('button[data-action="cancel-queue-job"]');
    if (!button) return;
    const jobId = String(button.dataset.jobId || "").trim();
    if (!jobId) return;
    const ok = window.confirm("Cancel this queued/download job?");
    if (!ok) return;
    button.disabled = true;
    try {
      await cancelJob(jobId);
      setNotice($("#search-queue-message"), `Cancel requested for job ${jobId}.`, false);
      await refreshSearchQueue();
      await refreshStatus();
    } catch (err) {
      button.disabled = false;
      setNotice($("#search-queue-message"), `Cancel failed: ${err.message}`, true);
    }
  });
  const sourceList = $("#search-source-list");
  if (sourceList) {
    sourceList.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-action]");
      if (!button) return;
      const row = button.closest(".source-priority-row");
      if (!row) return;
      moveSourcePriorityRow(row, button.dataset.action);
    });
  }
  const homeSearchDownload = $("#home-search-download");
  if (homeSearchDownload) {
    homeSearchDownload.addEventListener("click", () => submitHomeSearch(true));
  }
  const homeSearchOnly = $("#home-search-only");
  if (homeSearchOnly) {
    homeSearchOnly.addEventListener("click", () => submitHomeSearch(false));
  }
  const homeImportButton = $("#home-import-button");
  if (homeImportButton) {
    homeImportButton.addEventListener("click", importHomePlaylistFile);
  }
  const mediaModeSelect = document.getElementById("home-media-mode");
  if (mediaModeSelect) {
    mediaModeSelect.addEventListener("change", () => {
      setHomeMediaMode(mediaModeSelect.value || "video");
    });
  }
  const mediaModeToggle = $("#home-media-mode-toggle");
  if (mediaModeToggle) {
    mediaModeToggle.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-mode]");
      if (!button) return;
      setHomeMediaMode(button.dataset.mode || "video");
    });
  }
  const musicModeSelect = document.getElementById("music-mode-select");
  if (musicModeSelect) {
    musicModeSelect.addEventListener("change", () => {
      updateMusicModeToggleUI(musicModeSelect.value || "auto");
    });
    updateMusicModeToggleUI(musicModeSelect.value || "auto");
  }
  const musicModeToggle = $("#music-mode-toggle");
  if (musicModeToggle) {
    musicModeToggle.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-mode]");
      if (!button) return;
      setMusicModeSelection(button.dataset.mode || "auto");
    });
  }
  const homeDeliveryToggle = $("#home-delivery-toggle");
  bindHomeDeliveryToggle(homeDeliveryToggle);
  const musicForceToggle = $("#music-force-toggle");
  bindMusicForceToggle(musicForceToggle);
  const importToggle = document.getElementById("import-playlist-toggle");
  const importPanel = document.getElementById("import-playlist-panel");
  if (importToggle && importPanel) {
    importToggle.addEventListener("click", () => {
      importPanel.classList.toggle("hidden");
    });
  }
  const homeSourceToggle = $("#home-source-toggle");
  const homeSourcePanel = $("#home-source-panel");
  if (homeSourceToggle && homeSourcePanel) {
    const closePanel = () => {
      homeSourcePanel.classList.remove("open");
      homeSourceToggle.setAttribute("aria-expanded", "false");
    };
    homeSourceToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      const open = homeSourcePanel.classList.toggle("open");
      homeSourceToggle.setAttribute("aria-expanded", open ? "true" : "false");
    });
    homeSourcePanel.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    document.addEventListener("click", (event) => {
      if (homeSourcePanel.contains(event.target) || homeSourceToggle.contains(event.target)) {
        return;
      }
      closePanel();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closePanel();
      }
    });
    homeSourcePanel.addEventListener("change", (event) => {
      if (event.target && event.target.matches('input[type="checkbox"][data-source]')) {
        updateHomeSourceToggleLabel();
      }
    });
    refreshHomeSourceOptions();
  }
  const homeAdvancedToggle = document.querySelector("#home-advanced-toggle");
  const homeAdvancedPanel = document.querySelector("#home-advanced-panel");
  if (homeAdvancedToggle && homeAdvancedPanel) {
    homeAdvancedPanel.classList.add("hidden");
    homeAdvancedToggle.setAttribute("aria-expanded", "false");
    homeAdvancedToggle.addEventListener("click", () => {
      const isOpen = !homeAdvancedPanel.classList.contains("hidden");
      homeAdvancedPanel.classList.toggle("hidden", isOpen);
      homeAdvancedToggle.setAttribute("aria-expanded", String(!isOpen));
    });
  }
  const homeViewAdvanced = $("#home-view-advanced");
  if (homeViewAdvanced) {
    homeViewAdvanced.addEventListener("click", handleHomeViewAdvanced);
    updateHomeViewAdvancedLink();
  }
  const homeResultsList = $("#home-results-list");
  if (homeResultsList) {
    homeResultsList.addEventListener("click", async (event) => {
      const previewTarget = event.target.closest(".home-candidate-artwork, .home-candidate-title");
      if (previewTarget) {
        const row = previewTarget.closest(".home-candidate-row");
        const descriptor = buildHomePreviewDescriptorFromRow(row);
        if (descriptor) {
          openHomePreviewModal(descriptor);
          return;
        }
      }
      const cancelIntentButton = event.target.closest('button[data-action="home-intent-cancel"]');
      if (cancelIntentButton) {
        resetHomeIntentConfirmation();
        setNotice($("#home-search-message"), "Intent confirmation cancelled.", false);
        return;
      }
      const confirmIntentButton = event.target.closest('button[data-action="home-intent-confirm"]');
      if (confirmIntentButton) {
        const messageEl = $("#home-search-message");
        const intentType = confirmIntentButton.dataset.intentType || "";
        const identifier = confirmIntentButton.dataset.identifier || "";
        if (!intentType || !identifier) {
          setNotice(messageEl, "Intent payload is incomplete.", true);
          return;
        }
        confirmIntentButton.disabled = true;
        setNotice(messageEl, "Submitting intent...", false);
        try {
          await executeDetectedIntent(intentType, identifier);
          setNotice(messageEl, "Intent submitted.", false);
          setHomeResultsStatus("Intent submitted");
          setHomeResultsDetail("Download flow will continue once backend execution is implemented.", false);
        } catch (err) {
          setNotice(messageEl, `Intent submit failed: ${err.message}`, true);
          setHomeResultsDetail(`Intent submit failed: ${err.message}`, true);
        } finally {
          confirmIntentButton.disabled = false;
        }
        return;
      }
      const previewButton = event.target.closest('button[data-action="home-preview"]');
      if (previewButton) {
        const previewEmbedUrl = String(previewButton.dataset.previewEmbedUrl || "").trim();
        if (!previewEmbedUrl || !isValidHttpUrl(previewEmbedUrl)) {
          return;
        }
        openHomePreviewModal({
          embedUrl: previewEmbedUrl,
          source: String(previewButton.dataset.previewSource || "").trim(),
          title: String(previewButton.dataset.previewTitle || "").trim() || "Preview",
        });
        return;
      }
      const directButton = event.target.closest('button[data-action="home-direct-download"]');
      if (directButton) {
        if (directButton.disabled) return;
        const directUrl = directButton.dataset.directUrl;
        if (!directUrl) return;
        directButton.disabled = true;
        const playlistId = extractPlaylistIdFromUrl(directUrl);
        if (playlistId) {
          await handleHomePlaylistUrl(
            directUrl,
            playlistId,
            $("#home-destination")?.value.trim() || "",
            true,
            $("#home-search-message")
          );
        } else {
          await handleHomeDirectUrl(directUrl, $("#home-destination")?.value.trim() || "", $("#home-search-message"));
        }
        return;
      }
      const button = event.target.closest('button[data-action="home-download"]');
      if (!button) return;
      const itemId = button.dataset.itemId;
      const candidateId = button.dataset.candidateId;
      if (!itemId || !candidateId) return;
      if (button.disabled) return;
      button.disabled = true;
      const actionContainer = button.closest(".home-candidate-action");
      clearHomeEnqueueError(actionContainer);
      const originalText = button.textContent;
      button.textContent = "Queued";
      try {
        await enqueueSearchCandidate(itemId, candidateId, { messageEl: $("#home-search-message") });
        await refreshHomeResults(state.homeSearchRequestId);
      } catch (err) {
        showHomeEnqueueError(actionContainer, err.message || "Failed to enqueue download.");
      } finally {
        button.disabled = false;
        button.textContent = originalText;
      }
    });
  }
  document.addEventListener("click", async (event) => {
    const btn = event.target.closest(".music-download-btn");
    if (!btn) return;
    if (btn.disabled) return;

    const recording = String(btn.dataset.recordingMbid || "").trim();
    const resultKey = String(btn.dataset.musicResultKey || "").trim();
    const selectedResult = resultKey ? state.homeMusicResultMap[resultKey] : null;

    if (!recording) {
      console.error("Missing recording MBID on music download button");
      return;
    }

    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = "Queuing...";
    try {
      const payload = buildMusicTrackEnqueuePayload({ button: btn, result: selectedResult });
      const response = await enqueueMusicTrack(payload);
      const created = !!response?.created;
      if (created) {
        btn.textContent = "Queued...";
        setNotice($("#home-search-message"), "Track queued.", false);
      } else {
        btn.disabled = false;
        btn.textContent = originalText;
        const reason = String(response?.dedupe_reason || "").trim();
        const message = reason
          ? `Track not queued (${reason.replaceAll("_", " ")}).`
          : "Track already queued/downloaded; not queued again.";
        setNotice($("#home-search-message"), message, false);
      }
    } catch (err) {
      btn.disabled = false;
      btn.textContent = originalText;
      setNotice($("#home-search-message"), `Music enqueue failed: ${err.message}`, true);
    }
  });
  $("#search-requests-body").addEventListener("click", async (event) => {
    const actionButton = event.target.closest("button[data-action]");
    if (actionButton) {
      const requestId = actionButton.dataset.requestId;
      const action = actionButton.dataset.action;
      if (action === "cancel") {
        await cancelSearchRequest(requestId);
      }
      return;
    }
    const row = event.target.closest("tr[data-request-id]");
    if (!row) return;
    const requestId = row.dataset.requestId;
    if (!requestId) return;
    setSearchSelectedRequest(requestId);
    setSearchSelectedItem(null);
    $$("#search-requests-body tr.selected").forEach((el) => el.classList.remove("selected"));
    row.classList.add("selected");
    await refreshSearchRequestDetails(requestId);
  });
  $("#search-items-body").addEventListener("click", async (event) => {
    const row = event.target.closest("tr[data-item-id]");
    if (!row) return;
    const itemId = row.dataset.itemId;
    if (!itemId) return;
    setSearchSelectedItem(itemId);
    $$("#search-items-body tr.selected").forEach((el) => el.classList.remove("selected"));
    row.classList.add("selected");
    await refreshSearchCandidates(itemId);
  });

  $("#search-candidates-body").addEventListener("click", async (event) => {
    const button = event.target.closest('button[data-action="download"]');
    if (!button) return;
    const itemId = button.dataset.itemId || state.searchSelectedItemId;
    const candidateId = button.dataset.candidateId;
    await enqueueSearchCandidate(itemId, candidateId);
  });

  $("#schedule-save").addEventListener("click", saveSchedule);
  $("#schedule-run-now").addEventListener("click", runScheduleNow);
  $("#schedule-enabled").addEventListener("change", syncConfigSectionCollapsedStates);
  $("#save-config").addEventListener("click", saveConfig);
  const ytdlpUpdate = $("#ytdlp-update");
  if (ytdlpUpdate) {
    ytdlpUpdate.addEventListener("click", updateYtdlp);
  }
  $("#reset-config").addEventListener("click", async () => {
    await loadConfig();
    setConfigNotice("Config reloaded", false);
  });
  $("#load-config-path").addEventListener("click", setConfigPath);
  $("#browse-config-path").addEventListener("click", () => {
    const input = $("#config-path");
    openBrowser(input, "config", "file", ".json", resolveBrowseStart("config", input.value));
  });
  $("#browse-single-download").addEventListener("click", () => {
    const input = $("#cfg-single-download-folder");
    openBrowser(input, "downloads", "dir", "", resolveBrowseStart("downloads", input.value));
  });
  const browseHomeMusicFolder = $("#browse-home-music-download");
  if (browseHomeMusicFolder) {
    browseHomeMusicFolder.addEventListener("click", () => {
      const input = $("#cfg-home-music-download-folder");
      openBrowser(input, "downloads", "dir", "", resolveBrowseStart("downloads", input?.value || ""));
    });
  }
  const browseHomeMusicVideoFolder = $("#browse-home-music-video-download");
  if (browseHomeMusicVideoFolder) {
    browseHomeMusicVideoFolder.addEventListener("click", () => {
      const input = $("#cfg-home-music-video-download-folder");
      openBrowser(input, "downloads", "dir", "", resolveBrowseStart("downloads", input?.value || ""));
    });
  }
  const browseMusicLibraryPath = $("#browse-music-library-path");
  if (browseMusicLibraryPath) {
    browseMusicLibraryPath.addEventListener("click", () => {
      const input = $("#cfg-music-library-path");
      const rootKey = preferredMusicLibraryBrowseRoot(input?.value || "");
      openBrowser(input, rootKey, "dir", "", resolveBrowseStart(rootKey, input?.value || ""));
    });
  }
  const addMusicExportButton = $("#add-music-export");
  if (addMusicExportButton) {
    addMusicExportButton.addEventListener("click", () => addMusicExportRow({ enabled: true, type: "copy" }));
  }
  const communityPublishRunNow = $("#community-publish-run-now");
  if (communityPublishRunNow) {
    communityPublishRunNow.addEventListener("click", async () => {
      const messageEl = $("#community-publish-message");
      setNotice(messageEl, "Starting publish worker...", false);
      try {
        await fetchJson("/api/community-cache/publish/run", { method: "POST" });
        setNotice(messageEl, "Publish worker started.", false);
        refreshCommunityPublishStatus().catch(() => {});
      } catch (err) {
        setNotice(messageEl, `Publish run failed: ${err.message}`, true);
      }
    });
  }
  const communityPublishBackfill = $("#community-publish-backfill");
  if (communityPublishBackfill) {
    communityPublishBackfill.addEventListener("click", async () => {
      const messageEl = $("#community-publish-message");
      setNotice(messageEl, "Starting historical library backfill...", false);
      try {
        await fetchJson("/api/community-cache/publish/backfill", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dry_run: false }),
        });
        setNotice(messageEl, "Historical backfill started.", false);
        refreshCommunityPublishStatus().catch(() => {});
      } catch (err) {
        setNotice(messageEl, `Backfill failed: ${err.message}`, true);
      }
    });
  }
  const communityPublishRefresh = $("#community-publish-refresh-status");
  if (communityPublishRefresh) {
    communityPublishRefresh.addEventListener("click", () => {
      refreshCommunityPublishStatus().catch(() => {});
    });
  }
  const communityCacheSyncRunNow = $("#community-cache-sync-run-now");
  if (communityCacheSyncRunNow) {
    communityCacheSyncRunNow.addEventListener("click", async () => {
      const messageEl = $("#community-cache-sync-message");
      setNotice(messageEl, "Starting local cache sync...", false);
      try {
        await fetchJson("/api/community-cache/sync/run", { method: "POST" });
        setNotice(messageEl, "Local cache sync started.", false);
        refreshCommunityCacheSyncStatus().catch(() => {});
      } catch (err) {
        setNotice(messageEl, `Cache sync failed: ${err.message}`, true);
      }
    });
  }
  const communityCacheSyncRefresh = $("#community-cache-sync-refresh-status");
  if (communityCacheSyncRefresh) {
    communityCacheSyncRefresh.addEventListener("click", () => {
      refreshCommunityCacheSyncStatus().catch(() => {});
    });
  }
  // TODO(webUI/app.js::legacy-run): keep this marker while legacy-run removal rolls out across user docs.
  const homeBrowse = $("#home-destination-browse");
  if (homeBrowse) {
    homeBrowse.addEventListener("click", () => {
      const target = $("#home-destination");
      if (!target) return;
      const defaultStart = getHomeDefaultDestination(state.homeMediaMode);
      const startValue = target.value.trim() || defaultStart;
      openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", startValue));
    });
  }
  const searchBrowse = $("#search-destination-browse");
  if (searchBrowse) {
    searchBrowse.addEventListener("click", () => {
      const target = $("#search-destination");
      if (!target) return;
      openBrowser(target, "downloads", "dir", "", resolveBrowseStart("downloads", target.value));
    });
  }
  const homeDestinationInput = $("#home-destination");
  if (homeDestinationInput) {
    homeDestinationInput.addEventListener("input", () => {
      updateHomeDestinationResolved();
    });
  }
  const searchDestinationInput = $("#search-destination");
  if (searchDestinationInput) {
    searchDestinationInput.addEventListener("input", () => {
      updateSearchDestinationDisplay();
    });
  }
  $("#browse-yt-dlp-cookies").addEventListener("click", () => {
    const input = $("#cfg-yt-dlp-cookies");
    openBrowser(input, "tokens", "file", ".txt", resolveBrowseStart("tokens", input.value));
  });

  $("#toggle-telegram-token").addEventListener("click", () => {
    const input = $("#cfg-telegram-token");
    if (input.type === "password") {
      input.type = "text";
      $("#toggle-telegram-token").textContent = "Hide";
    } else {
      input.type = "password";
      $("#toggle-telegram-token").textContent = "Show";
    }
  });

  $("#browser-close").addEventListener("click", closeBrowser);
  $("#browser-root").addEventListener("change", (event) => {
    const nextRoot = String(event.target?.value || "").trim();
    if (!nextRoot || nextRoot === browserState.root) {
      return;
    }
    browserState.root = nextRoot;
    refreshBrowser("");
  });
  $("#browser-up").addEventListener("click", (event) => {
    const next = event.currentTarget.dataset.path;
    if (next !== undefined) {
      refreshBrowser(next);
    }
  });
  $("#browser-select").addEventListener("click", applyBrowserSelection);
  $("#browser-list").addEventListener("click", (event) => {
    const item = event.target.closest(".browser-item");
    if (!item || item.classList.contains("empty") || item.classList.contains("error")) {
      return;
    }
    const type = item.dataset.type;
    const relPath = item.dataset.path || "";
    const absPath = item.dataset.absPath || "";
    if (type === "dir") {
      refreshBrowser(relPath);
      return;
    }
    if (type === "file" && browserState.mode === "file") {
      browserState.selected = absPath;
      $("#browser-selected").textContent = absPath;
      $$(".browser-item.selected").forEach((el) => el.classList.remove("selected"));
      item.classList.add("selected");
      $("#browser-select").disabled = false;
    }
  });

  $("#oauth-close").addEventListener("click", closeOauthModal);
  $("#oauth-open").addEventListener("click", () => {
    if (oauthState.authUrl) {
      window.open(oauthState.authUrl, "_blank", "noopener");
    }
  });
  $("#oauth-complete").addEventListener("click", completeOauth);
  const importProgressClose = $("#import-progress-close");
  if (importProgressClose) {
    importProgressClose.addEventListener("click", closeImportProgressModal);
  }
  const importProgressModal = $("#import-progress-modal");
  if (importProgressModal) {
    importProgressModal.addEventListener("click", (event) => {
      if (event.target === importProgressModal) {
        closeImportProgressModal();
      }
    });
  }
  const homePreviewClose = $("#home-preview-close");
  if (homePreviewClose) {
    homePreviewClose.addEventListener("click", closeHomePreviewModal);
  }
  const homePreviewModal = $("#home-preview-modal");
  if (homePreviewModal) {
    homePreviewModal.addEventListener("click", (event) => {
      if (event.target === homePreviewModal) {
        closeHomePreviewModal();
      }
    });
  }
  const spotifyConnectBtn = $("#spotify-connect-btn");
  if (spotifyConnectBtn) {
    spotifyConnectBtn.addEventListener("click", connectSpotify);
  }
  const spotifyDisconnectBtn = $("#spotify-disconnect-btn");
  if (spotifyDisconnectBtn) {
    spotifyDisconnectBtn.addEventListener("click", disconnectSpotify);
  }
  const libraryReconcileButton = $("#library-reconcile-button");
  if (libraryReconcileButton) {
    libraryReconcileButton.addEventListener("click", reconcileLibrary);
  }
  const reviewRefresh = $("#review-refresh");
  if (reviewRefresh) {
    reviewRefresh.addEventListener("click", refreshReviewQueue);
  }
  const homeReviewAlertOpen = $("#home-review-alert-open");
  if (homeReviewAlertOpen) {
    homeReviewAlertOpen.addEventListener("click", () => {
      setPage("review");
      window.location.hash = "review";
    });
  }
  const reviewAcceptSelected = $("#review-accept-selected");
  if (reviewAcceptSelected) {
    reviewAcceptSelected.addEventListener("click", () => {
      runReviewQueueAction("accept", getSelectedReviewIds(), "Accepting selected review items...");
    });
  }
  const reviewRejectSelected = $("#review-reject-selected");
  if (reviewRejectSelected) {
    reviewRejectSelected.addEventListener("click", () => {
      runReviewQueueAction("reject", getSelectedReviewIds(), "Rejecting selected review items...");
    });
  }
  const reviewAcceptAll = $("#review-accept-all");
  if (reviewAcceptAll) {
    reviewAcceptAll.addEventListener("click", () => {
      const ids = (state.reviewItems || []).map((item) => String(item.id || "").trim()).filter(Boolean);
      runReviewQueueAction("accept", ids, "Accepting all pending review items...");
    });
  }
  const reviewClearSelection = $("#review-clear-selection");
  if (reviewClearSelection) {
    reviewClearSelection.addEventListener("click", () => {
      state.reviewSelectedIds = new Set();
      renderReviewQueue();
    });
  }
  const reviewList = $("#review-list");
  if (reviewList) {
    reviewList.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-action]");
      if (button) {
        const action = String(button.dataset.action || "").trim();
        const reviewId = String(button.dataset.reviewId || "").trim();
        const item = (state.reviewItems || []).find((entry) => String(entry.id || "").trim() === reviewId);
        if (action === "review-toggle-preview") {
          state.reviewPreviewItemId = state.reviewPreviewItemId === reviewId ? null : reviewId;
          renderReviewQueue();
          return;
        }
        if (!item) {
          setNotice($("#review-message"), "Review item not found.", true);
          return;
        }
        if (action === "review-accept") {
          await runReviewQueueAction("accept", [reviewId], "Accepting review item...");
          return;
        }
        if (action === "review-reject") {
          await runReviewQueueAction("reject", [reviewId], "Rejecting review item...");
          return;
        }
        if (action === "review-accept-artist") {
          const artistKey = String(item.artist || item.album_artist || "").trim().toLowerCase();
          const ids = (state.reviewItems || [])
            .filter((entry) => String(entry.artist || entry.album_artist || "").trim().toLowerCase() === artistKey)
            .map((entry) => String(entry.id || "").trim())
            .filter(Boolean);
          await runReviewQueueAction("accept", ids, "Accepting pending items from this artist...");
          return;
        }
        if (action === "review-accept-album") {
          const artistKey = String(item.artist || item.album_artist || "").trim().toLowerCase();
          const albumKey = String(item.album || "").trim().toLowerCase();
          const ids = (state.reviewItems || [])
            .filter((entry) =>
              String(entry.artist || entry.album_artist || "").trim().toLowerCase() === artistKey &&
              String(entry.album || "").trim().toLowerCase() === albumKey
            )
            .map((entry) => String(entry.id || "").trim())
            .filter(Boolean);
          await runReviewQueueAction("accept", ids, "Accepting pending items from this album...");
          return;
        }
      }
      const checkbox = event.target.closest('input[data-action="review-select"]');
      if (checkbox) {
        const reviewId = String(checkbox.dataset.reviewId || "").trim();
        if (!reviewId) return;
        if (checkbox.checked) {
          state.reviewSelectedIds.add(reviewId);
        } else {
          state.reviewSelectedIds.delete(reviewId);
        }
        updateReviewToolbarState();
      }
    });
  }

  $("#add-account").addEventListener("click", () => addAccountRow("", {}));
  $("#add-playlist").addEventListener("click", () => addPlaylistRow({}));

  $("#status-cancel-active").addEventListener("click", async () => {
    await runStatusQueueAction({
      buttonId: "#status-cancel-active",
      endpoint: "/api/download_jobs/cancel_active",
      confirmText: "Cancel all active jobs? Queued items will remain in place.",
      progressText: "Cancelling active jobs...",
      successText: (data) => {
        const cancelled = Number.isFinite(Number(data?.cancelled)) ? Number(data.cancelled) : 0;
        return `Cancelled ${cancelled} active job${cancelled === 1 ? "" : "s"}.`;
      },
      errorPrefix: "Failed to cancel active jobs",
    });
  });
  $("#status-recover-stale").addEventListener("click", async () => {
    await runStatusQueueAction({
      buttonId: "#status-recover-stale",
      endpoint: "/api/download_jobs/recover_stale",
      confirmText: "Recover stale jobs and place them back into the queue?",
      progressText: "Recovering stale jobs...",
      successText: (data) => {
        const recovered = Number.isFinite(Number(data?.recovered)) ? Number(data.recovered) : 0;
        return `Recovered ${recovered} stale job${recovered === 1 ? "" : "s"}.`;
      },
      errorPrefix: "Failed to recover stale jobs",
    });
  });
  $("#status-clear-failed").addEventListener("click", async () => {
    await runStatusQueueAction({
      buttonId: "#status-clear-failed",
      endpoint: "/api/download_jobs/clear_failed",
      confirmText: "Clear all failed and cancelled jobs from the queue table?",
      progressText: "Clearing failed jobs...",
      successText: (data) => {
        const deleted = Number.isFinite(Number(data?.deleted)) ? Number(data.deleted) : 0;
        return `Cleared ${deleted} failed/cancelled job${deleted === 1 ? "" : "s"}.`;
      },
      errorPrefix: "Failed to clear failed jobs",
    });
  });
  $("#status-clear-queue").addEventListener("click", async () => {
    await runStatusQueueAction({
      buttonId: "#status-clear-queue",
      endpoint: "/api/download_jobs/clear_queue",
      confirmText: "Clear the pending queue? This removes queued and active jobs but keeps completed history.",
      progressText: "Clearing pending queue...",
      successText: (data) => {
        const deleted = Number.isFinite(Number(data?.deleted)) ? Number(data.deleted) : 0;
        return `Cleared ${deleted} queued/active job${deleted === 1 ? "" : "s"}.`;
      },
      errorPrefix: "Failed to clear queue",
    });
  });
  $("#toggle-theme").addEventListener("click", () => {
    const next = resolveTheme() === "light" ? "dark" : "light";
    applyTheme(next);
  });

  document.addEventListener("focusin", (event) => {
    const tag = (event.target && event.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
      state.inputFocused = true;
      updatePollingState();
    }
  });
  document.addEventListener("focusout", (event) => {
    const tag = (event.target && event.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") {
      state.inputFocused = false;
      updatePollingState();
    }
  });
  document.addEventListener("click", clearConfigNotice);
  document.addEventListener("input", clearConfigNotice);

  const settingsAdvancedToggle = $("#settings-advanced-mode");
  if (settingsAdvancedToggle) {
    settingsAdvancedToggle.addEventListener("change", () => {
      applySettingsAdvancedMode(settingsAdvancedToggle.checked, { persist: false });
    });
    settingsAdvancedToggle.checked = false;
    applySettingsAdvancedMode(false, { persist: false });
  }

  const settingsNav = $("#settings-nav");
  if (settingsNav) {
    settingsNav.addEventListener("click", (event) => {
      const link = event.target.closest(".settings-nav-link");
      if (!link) return;
      event.preventDefault();
      const href = String(link.getAttribute("href") || "");
      const targetId = href.startsWith("#") ? href.slice(1) : "";
      if (targetId) {
        setActiveSettingsSection(targetId, { jump: false, smooth: false });
        if (window.location.hash !== `#${targetId}`) {
          history.replaceState(null, "", `#${targetId}`);
        }
      }
    });
  }

  const settingsSelect = $("#settings-section-select");
  if (settingsSelect) {
    settingsSelect.addEventListener("change", () => {
      const targetId = String(settingsSelect.value || "").trim();
      if (!targetId) return;
      setActiveSettingsSection(targetId, { jump: false, smooth: false });
      if (window.location.hash !== `#${targetId}`) {
        history.replaceState(null, "", `#${targetId}`);
      }
    });
  }

  window.addEventListener("resize", () => {
    syncSettingsMainWidthLock();
  });

  const configPanel = $("#config-panel");
  if (configPanel) {
    configPanel.addEventListener("input", () => {
      if (state.suppressDirty) {
        return;
      }
      state.configDirty = true;
      updatePollingState();
    });
    if (typeof ResizeObserver === "function") {
      const layout = configPanel.querySelector(".settings-layout");
      if (layout) {
        if (state.settingsLayoutObserver) {
          state.settingsLayoutObserver.disconnect();
        }
        state.settingsLayoutObserver = new ResizeObserver(() => {
          syncSettingsMainWidthLock();
        });
        state.settingsLayoutObserver.observe(layout);
        const sidebar = layout.querySelector(".settings-sidebar");
        if (sidebar) {
          state.settingsLayoutObserver.observe(sidebar);
        }
        const main = layout.querySelector(".settings-main");
        if (main) {
          state.settingsLayoutObserver.observe(main);
        }
      }
    }
  }

  const watcherEnabledToggle = $("#cfg-watcher-enabled");
  if (watcherEnabledToggle) {
    watcherEnabledToggle.addEventListener("change", syncConfigSectionCollapsedStates);
  }
  const musicMetaEnabledToggle = $("#cfg-music-meta-enabled");
  if (musicMetaEnabledToggle) {
    musicMetaEnabledToggle.addEventListener("change", syncConfigSectionCollapsedStates);
  }
  const spotifyEnabledToggle = $("#spotify-enabled");
  if (spotifyEnabledToggle) {
    spotifyEnabledToggle.addEventListener("change", syncConfigSectionCollapsedStates);
  }
  syncConfigSectionCollapsedStates();
  applyHomeDefaultVideoFormat({ force: true });
  updateMusicModeFormatControl();
  syncSettingsMainWidthLock();
  setActiveSettingsSection(state.settingsActiveSectionId || "settings-core", { jump: false, smooth: false });

}

async function init() {
  window.addEventListener("spotify-oauth-complete", () => {
    setNotice(
      $("#home-results-detail"),
      "Spotify connected successfully. Initial sync has started.",
      false
    );
  });
  applyTheme(resolveTheme());
  bindEvents();
  // Home default: Video mode unless user set a persisted mode.
  setHomeMediaMode(loadHomeMediaModePreference(), { persist: false, clearResultsOnDisable: false });
  setHomeDeliveryMode(getHomeDeliveryMode());
  setupNavActions();
  await loadPaths();
  const initialPage = (window.location.hash || "#home").replace("#", "");
  setPage(initialPage || "home");
  window.addEventListener("hashchange", () => {
    const next = (window.location.hash || "#home").replace("#", "");
    setPage(next || "home");
  });
  setupTimers();
  const logsAuto = $("#logs-auto");
  if (logsAuto) {
    logsAuto.checked = true;
    logsAuto.disabled = false;
  }
}

window.addEventListener("DOMContentLoaded", init);

// Add Enter key handler for home search input to trigger "Search Only"
document.addEventListener("DOMContentLoaded", () => {
  const homeSearchInput = $("#home-search-input");
  if (homeSearchInput) {
    homeSearchInput.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        event.preventDefault();
        const searchOnlyBtn = $("#home-search-only");
        if (searchOnlyBtn && !searchOnlyBtn.disabled) {
          searchOnlyBtn.click();
        }
      }
    });
  }
});


(function ensureHomeClass() {
  const homeSection = document.querySelector('section[data-page="home"]');
  if (!homeSection) return;

  const observer = new MutationObserver(() => {
    const visible = !homeSection.classList.contains("page-hidden");
    document.body.classList.toggle("home-page", visible);
    if (visible) {
      document.body.dataset.page = "home";
    } else if (state.currentPage) {
      document.body.dataset.page = state.currentPage;
    }
  });

  observer.observe(homeSection, { attributes: true, attributeFilter: ["class"] });

  // initial state
  const visible = !homeSection.classList.contains("page-hidden");
  document.body.classList.toggle("home-page", visible);
  if (visible) {
    document.body.dataset.page = "home";
  } else if (state.currentPage) {
    document.body.dataset.page = state.currentPage;
  }
})();
