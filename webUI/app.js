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
  appSidebarCollapsed: false,
  currentPage: "home",
  lastMusicMode: "music",
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
  homeAlbumCoverInFlight: {},
  homeArtistCoverCache: {},
  homeGenreCoverCache: {},
  homeMusicRenderToken: 0,
  homeMusicResultMap: {},
  homeMusicCurrentView: null,
  homeMusicViewStack: [],
  musicAdvancedOpen: false,
  musicResultsSort: "recommended",
  musicCardSize: 180,
  musicHideSuggestedGenres: false,
  musicHiddenGenres: [],
  musicHiddenArtists: [],
  musicGenreBrowseCache: {},
  musicGenreArtistsCache: {},
  musicArtistAlbumsCache: {},
  musicArtistAlbumsInFlight: {},
  musicPreferences: {
    favorite_genres: [],
    favorite_artists: [],
  },
  homeRequestContext: {},
  homeVideoSort: "best_match",
  homeVideoCardSize: 220,
  homeVideoResultsLimit: 24,
  homeBestScores: {},
  homeCandidateCache: {},
  homeCandidatesByItem: {},
  homeCandidatesLoading: {},
  homeCandidateRefreshPending: {},
  homeCandidateData: {},
  homeResultItems: [],
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
  arrMode: "movies",
  arrResults: [],
  arrSearchQuery: "",
  arrSearchYear: "",
  arrFiltersOpen: false,
  arrSearchContext: "search",
  arrSort: "best_match",
  arrCardSize: 260,
  arrExpandedIds: new Set(),
  arrDetailsItemId: null,
  arrCastCache: {},
  arrGenres: {
    movie: [],
    tv: [],
  },
  arrGenreArtworkCache: {
    movie: {},
    tv: {},
  },
  arrGenreRenderToken: 0,
  arrActiveGenre: null,
  arrGenreBrowseCache: {},
  arrEditorial: {
    movie: { shelves: [] },
    tv: { shelves: [] },
  },
  arrPersonTitlesCache: {},
  arrConnectionStatus: {
    radarr: { configured: false, reachable: false, message: "Radarr is not configured" },
    sonarr: { configured: false, reachable: false, message: "Sonarr is not configured" },
  },
  arrStatusPollTimer: null,
  setupStatus: null,
  setupWizard: null,
  servicesHealth: null,
  lastAutoConfigureResults: {},
  adminSecurity: {
    admin_pin_enabled: false,
    admin_pin_session_minutes: 30,
    session_valid: false,
  },
  adminPinToken: "",
  playerView: "home",
  playerLibrary: [],
  playerLibrarySummary: { artists: [], albums: [], tracks: [] },
  playerLibraryMode: "albums",
  playerSelectedArtistKey: "",
  playerSelectedAlbumKey: "",
  playerStations: [],
  playerCommunityCache: [],
  playerPlaylists: [],
  playerSelectedPlaylistId: null,
  playerSelectedPlaylistItems: [],
  playerQueue: [],
  playerActiveStationId: null,
  playerActiveStationRuntime: null,
  playerStationPrimingTimer: null,
  playerHistory: [],
  playerMissingHistory: [],
  playerCurrent: null,
  playerCurrentHasVideo: false,
  playerVideoVisible: false,
  playerYT: null,           // active YT.Player instance, or null when using <audio>
  playerYTReady: false,     // IFrame API script loaded and YT.Player can be instantiated
  playerYTProgressTimer: null,
  playerShuffle: false,
  playerRepeatMode: "off",
  playerProgressDragging: false,
  musicSection: "browse",
  homeSection: "search",
  moviesTvSection: "search",
  musicPlayerExpanded: false,
  musicLibraryMode: "albums",
  musicLibraryLoaded: false,
  videoLibraryItems: [],
  videoLibraryLoaded: false,
  libraryModalPayload: null,
  settingsActiveSectionId: "settings-core",
  settingsLayoutObserver: null,
  logsStickToBottom: true,
  startupSetupPromptPending: false,
  startupSetupPromptDismissedForSession: false,
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
  mediaType: "",
  streamUrl: "",
  itemId: "",
  candidateId: "",
  directUrl: "",
  description: "",
  postedText: "",
  durationText: "",
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
const APP_SIDEBAR_COLLAPSED_KEY = "retreivr.app.sidebar_collapsed";
const ADMIN_PIN_TOKEN_KEY = "retreivr.admin.pin.token";
const HOME_MUSIC_DEBUG_KEY = "retreivr.debug.music";
const MUSIC_RECENT_SEARCHES_KEY = "retreivr.music.recent_searches.v1";
const HOME_ALBUM_COVER_CACHE_KEY = "retreivr.home.album_cover_cache.v1";
const HOME_ARTIST_COVER_CACHE_KEY = "retreivr.home.artist_cover_cache.v1";
const HOME_GENRE_COVER_CACHE_KEY = "retreivr.home.genre_cover_cache.v2";
const MUSIC_ARTWORK_CACHE_REFRESH_MS = 7 * 24 * 60 * 60 * 1000;
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
const DEFAULT_MUSIC_GENRES = [
  "Rock",
  "Pop",
  "Hip Hop",
  "Jazz",
  "Electronic",
  "Metal",
  "Country",
  "Folk",
  "R&B",
  "Classical",
  "Christian",
  "Indie",
];
const MUSIC_GENRE_INTENT_MAP = {
  rock: { label: "Rock", aliases: ["rock", "alternative rock", "hard rock", "classic rock", "pop rock", "indie rock"] },
  pop: { label: "Pop", aliases: ["pop", "dance-pop", "synth-pop", "electropop", "teen pop"] },
  "hip hop": { label: "Hip Hop", aliases: ["hip hop", "hip-hop", "rap", "trap", "conscious hip hop", "southern hip hop"] },
  jazz: { label: "Jazz", aliases: ["jazz", "smooth jazz", "vocal jazz", "jazz fusion", "bebop"] },
  electronic: { label: "Electronic", aliases: ["electronic", "electronica", "edm", "house", "techno", "ambient", "downtempo", "trance"] },
  metal: { label: "Metal", aliases: ["metal", "heavy metal", "thrash metal", "death metal", "black metal", "metalcore"] },
  country: { label: "Country", aliases: ["country", "contemporary country", "alt-country", "country pop", "americana"] },
  folk: { label: "Folk", aliases: ["folk", "indie folk", "folk rock", "singer-songwriter", "acoustic folk"] },
  "r&b": { label: "R&B", aliases: ["r&b", "rnb", "rhythm and blues", "neo soul", "contemporary r&b", "soul"] },
  classical: { label: "Classical", aliases: ["classical", "orchestral", "opera", "chamber music", "instrumental classical"] },
  "contemporary christian": { label: "Christian", aliases: ["contemporary christian", "ccm", "christian", "christian pop", "christian rock", "worship"] },
  indie: { label: "Indie", aliases: ["indie", "indie pop", "indie rock", "alternative", "lo-fi", "dream pop"] },
};
const ARR_POPULARITY_FRESH_THRESHOLD = 25;
const UI_CARD_SIZE_MIN = 120;
const UI_CARD_SIZE_MAX = 320;
const UI_HOME_VIDEO_CARD_SIZE_MIN = 220;
const UI_HOME_VIDEO_CARD_SIZE_MAX = 420;
const UI_DEFAULTS = {
  home_video_card_size: 220,
  home_video_sort: "best_match",
  home_video_results_limit: 24,
  music_card_size: 180,
  music_sort: "recommended",
  music_hide_suggested_genres: false,
  music_hidden_genres: [],
  music_hidden_artists: [],
  movies_tv_card_size: 260,
  movies_tv_sort: "best_match",
};
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
  "Playlist URL detected. Use Search to preview matches, then download the items you want.";
const HOME_HOVER_PREVIEW_DELAY_MS = 800;
const VIDEO_SOURCE_PREFERENCE_KEY = "retreivr.video_sources";
const VIDEO_RECENT_SEARCHES_KEY = "retreivr.video_recent_searches";
const MUSIC_HEADER_MODE_KEY = "retreivr.music_header_mode";

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

function formatOpenSourceLabel(source) {
  const sourceLabel = formatSourceLabel(source) || "Source";
  return `Open in ${sourceLabel}`;
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
  const persistedChecked = new Set(loadVideoSourcePreferences());
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
    input.checked = existingChecked.size
      ? existingChecked.has(source)
      : persistedChecked.size
        ? persistedChecked.has(source)
        : source === "youtube" || index === 0;
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
  if (cleanPage === "music-player") {
    return "music";
  }
  if (cleanPage === "settings" || String(cleanPage).startsWith("settings-")) {
    return "config";
  }
  if (["setup", "connections", "services", "status", "info"].includes(cleanPage)) {
    return "config";
  }
  if (cleanPage === "search" || cleanPage === "advanced") {
    return "info";
  }
  if (["downloads", "history", "logs"].includes(cleanPage)) {
    return "status";
  }
  if (cleanPage === "video") {
    return "video";
  }
  return cleanPage;
}

function loadVideoSourcePreferences() {
  try {
    const raw = localStorage.getItem(VIDEO_SOURCE_PREFERENCE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.map((value) => String(value || "").trim()).filter(Boolean) : [];
  } catch (_err) {
    return [];
  }
}

function persistVideoSourcePreferences(values) {
  try {
    localStorage.setItem(VIDEO_SOURCE_PREFERENCE_KEY, JSON.stringify(Array.isArray(values) ? values : []));
  } catch (_err) {
    // ignore localStorage failures
  }
}

function loadRecentVideoSearches() {
  try {
    const raw = localStorage.getItem(VIDEO_RECENT_SEARCHES_KEY);
    const parsed = JSON.parse(raw || "[]");
    return Array.isArray(parsed)
      ? parsed.map((value) => String(value || "").trim()).filter(Boolean).slice(0, 8)
      : [];
  } catch (_err) {
    return [];
  }
}

function persistRecentVideoSearch(query) {
  const text = String(query || "").trim();
  if (!text) return;
  const next = [text, ...loadRecentVideoSearches().filter((entry) => entry.toLowerCase() !== text.toLowerCase())].slice(0, 8);
  try {
    localStorage.setItem(VIDEO_RECENT_SEARCHES_KEY, JSON.stringify(next));
  } catch (_err) {
    // ignore localStorage failures
  }
}

function loadRecentMusicSearches() {
  try {
    const raw = localStorage.getItem(MUSIC_RECENT_SEARCHES_KEY);
    const parsed = JSON.parse(raw || "[]");
    return Array.isArray(parsed)
      ? parsed.map((value) => String(value || "").trim()).filter(Boolean).slice(0, 8)
      : [];
  } catch (_err) {
    return [];
  }
}

function persistRecentMusicSearch(query) {
  const text = String(query || "").trim();
  if (!text) return;
  const next = [text, ...loadRecentMusicSearches().filter((entry) => entry.toLowerCase() !== text.toLowerCase())].slice(0, 8);
  try {
    localStorage.setItem(MUSIC_RECENT_SEARCHES_KEY, JSON.stringify(next));
  } catch (_err) {
    // ignore localStorage failures
  }
}

function syncTopbarSubbarVisibility() {
  const host = $("#topbar-subbar-host");
  if (!host) return;
  const hasVisibleChild = Array.from(host.children).some((child) => !child.classList.contains("hidden"));
  host.classList.toggle("hidden", !hasVisibleChild);
}

function clearTopbarHosts() {
  const searchHost = $("#topbar-search-host");
  const subbarHost = $("#topbar-subbar-host");
  const parkingHost = $("#topbar-node-parking");
  if (searchHost && parkingHost) {
    while (searchHost.firstChild) {
      parkingHost.appendChild(searchHost.firstChild);
    }
  } else if (searchHost) {
    searchHost.textContent = "";
  }
  if (subbarHost && parkingHost) {
    while (subbarHost.firstChild) {
      parkingHost.appendChild(subbarHost.firstChild);
    }
    subbarHost.classList.add("hidden");
  } else if (subbarHost) {
    subbarHost.textContent = "";
    subbarHost.classList.add("hidden");
  }
}

function mountTopbarForPage(page) {
  clearTopbarHosts();
  const searchHost = $("#topbar-search-host");
  const subbarHost = $("#topbar-subbar-host");
  if (!searchHost || !subbarHost) return;
  if (page === "video") {
    const searchBar = $("#standard-search-container");
    const advancedPanel = $("#home-advanced-panel");
    if (searchBar) searchHost.appendChild(searchBar);
    if (advancedPanel) subbarHost.appendChild(advancedPanel);
    syncTopbarSubbarVisibility();
    return;
  }
  if (page === "music") {
    const musicHeader = $("#music-header-search");
    const musicAdvanced = $("#music-mode-console");
    if (musicHeader) searchHost.appendChild(musicHeader);
    if (musicAdvanced) subbarHost.appendChild(musicAdvanced);
    syncTopbarSubbarVisibility();
    return;
  }
  if (page === "movies-tv") {
    if (!isTmdbConfigured()) {
      syncTopbarSubbarVisibility();
      return;
    }
    const searchRow = getMoviesTvSearchRowEl();
    const filtersPanel = getMoviesTvFiltersPanelEl();
    if (searchRow) searchHost.appendChild(searchRow);
    if (filtersPanel) subbarHost.appendChild(filtersPanel);
    syncTopbarSubbarVisibility();
    return;
  }
}

function getMoviesTvSearchRowEl() {
  return document.querySelector(".movies-tv-search-row");
}

function getMoviesTvFiltersPanelEl() {
  return $("#movies-tv-filters-panel");
}

function setNotice(el, message, isError = false) {
  if (!el) return;
  const text = String(message || "");
  el.textContent = text;
  el.style.color = isError ? "#ff7b7b" : "#59b0ff";
  el.classList.toggle("hidden", !text.trim());
}

function getMusicPageMessageEl() {
  if (state.currentPage === "music" && state.musicSection === "browse") {
    return $("#music-page-message");
  }
  return $("#music-player-message") || $("#music-page-message");
}

function setMusicPageNotice(message, isError = false) {
  const el = getMusicPageMessageEl();
  const region = $("#music-page-message-region");
  const hasText = !!String(message || "").trim();
  if (el) {
    el.classList.toggle("music-error-notice", !!isError && hasText);
  }
  if (state.currentPage === "music" && state.musicSection === "browse" && !isError) {
    setNotice(el, "", false);
    if (region) {
      region.classList.add("hidden");
    }
    return;
  }
  setNotice(el, message, isError);
  if (region) {
    region.classList.toggle("hidden", !hasText || !isError);
  }
}

function getMusicToolbarSlot() {
  return $("#music-toolbar-slot");
}

function getMusicNavSlot() {
  return $("#music-nav-slot");
}

function focusMusicResults() {
  const results = $("#music-results-container");
  if (!results) return;
  results.scrollIntoView({ behavior: "smooth", block: "start" });
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

function runPrioritizedThumbnailJobs(jobs, renderToken, options = {}) {
  const tasks = Array.isArray(jobs) ? jobs.filter((job) => typeof job === "function") : [];
  if (!tasks.length) {
    return;
  }
  const normalizedOptions = typeof options === "number"
    ? { immediateConcurrency: options }
    : (options && typeof options === "object" ? options : {});
  const visibleCount = Math.max(0, Number.parseInt(normalizedOptions.visibleCount, 10) || 12);
  const immediateTasks = tasks.slice(0, visibleCount || tasks.length);
  const deferredTasks = tasks.slice(immediateTasks.length);
  const immediateConcurrency = Math.max(
    1,
    Math.min(Number.parseInt(normalizedOptions.immediateConcurrency, 10) || 10, immediateTasks.length || tasks.length)
  );
  const deferredConcurrency = Math.max(
    1,
    Math.min(Number.parseInt(normalizedOptions.deferredConcurrency, 10) || 4, deferredTasks.length || 1)
  );
  const deferredDelayMs = Math.max(0, Number.parseInt(normalizedOptions.deferredDelayMs, 10) || 80);

  const runTaskQueue = (queue, concurrency) => {
    if (!queue.length) return;
    let nextIndex = 0;
    const worker = async () => {
      while (true) {
        const index = nextIndex;
        nextIndex += 1;
        if (index >= queue.length) {
          return;
        }
        try {
          await queue[index](renderToken);
        } catch (_err) {
          // Thumbnail hydration is best-effort only.
        }
      }
    };
    for (let i = 0; i < concurrency; i += 1) {
      worker();
    }
  };

  runTaskQueue(immediateTasks, immediateConcurrency);
  if (!deferredTasks.length) {
    return;
  }
  window.setTimeout(() => {
    if (state.homeMusicRenderToken !== renderToken) return;
    runTaskQueue(deferredTasks, deferredConcurrency);
  }, deferredDelayMs);
}

function prefetchMusicAlbumsForArtists(artists = [], { limit = 6 } = {}) {
  const items = Array.isArray(artists) ? artists.slice(0, Math.max(0, Number(limit) || 0)) : [];
  items.forEach((artist) => {
    fetchMusicAlbumsByArtist(artist).catch(() => {});
  });
}

function scheduleAlbumCardArtworkLoads(cards = [], candidates = [], { visibleCount = 10 } = {}) {
  const work = [];
  const total = Math.min(cards.length, candidates.length);
  for (let index = 0; index < total; index += 1) {
    const candidate = candidates[index];
    const card = cards[index];
    const cover = card?.querySelector?.(".album-cover");
    const releaseGroupId = String(candidate?.release_group_id || "").trim();
    if (!cover || !releaseGroupId) continue;
    work.push(async () => {
      const coverUrl = await fetchHomeAlbumCoverUrl(releaseGroupId);
      if (!coverUrl) return;
      cover.src = coverUrl;
      cover.style.display = "block";
    });
  }
  runPrioritizedThumbnailJobs(work, state.homeMusicRenderToken, {
    visibleCount,
    immediateConcurrency: Math.min(10, Math.max(4, visibleCount)),
    deferredConcurrency: 4,
    deferredDelayMs: 60,
  });
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
  placeholder.setAttribute("aria-hidden", "true");

  img.addEventListener("load", () => {
    shell.classList.remove("loading", "no-art");
    shell.classList.add("loaded");
  });
  img.addEventListener("error", () => {
    shell.classList.remove("loading", "loaded");
    shell.classList.add("no-art");
    img.removeAttribute("src");
  });

  const setLoading = () => {
    shell.classList.remove("loaded", "no-art");
    shell.classList.add("loading");
  };

  const setNoArt = () => {
    shell.classList.remove("loading", "loaded");
    shell.classList.add("no-art");
    img.removeAttribute("src");
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

function readSessionObject(key) {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_err) {
    return {};
  }
}

function writeSessionObject(key, value) {
  try {
    sessionStorage.setItem(key, JSON.stringify(value));
  } catch (_err) {
    // ignore storage quota/privacy failures
  }
}

function getCachedAlbumCoverUrl(key) {
  const normalizedKey = String(key || "").trim();
  if (!normalizedKey) return null;
  const memory = state.homeAlbumCoverCache[normalizedKey];
  if (typeof memory === "string" && memory) {
    return memory;
  }
  const sessionCache = readSessionObject(HOME_ALBUM_COVER_CACHE_KEY);
  const cached = normalizeArtworkUrl(sessionCache[normalizedKey]);
  if (cached) {
    state.homeAlbumCoverCache[normalizedKey] = cached;
    return cached;
  }
  return null;
}

function setCachedAlbumCoverUrl(key, url) {
  const normalizedKey = String(key || "").trim();
  const normalizedUrl = normalizeArtworkUrl(url);
  if (!normalizedKey || !normalizedUrl) return;
  state.homeAlbumCoverCache[normalizedKey] = normalizedUrl;
  const sessionCache = readSessionObject(HOME_ALBUM_COVER_CACHE_KEY);
  sessionCache[normalizedKey] = normalizedUrl;
  writeSessionObject(HOME_ALBUM_COVER_CACHE_KEY, sessionCache);
}

function getCachedArtistCoverUrl(key) {
  const normalizedKey = String(key || "").trim();
  if (!normalizedKey) return null;
  const memory = state.homeArtistCoverCache[normalizedKey];
  if (typeof memory === "string" && memory) {
    return memory;
  }
  const sessionCache = readSessionObject(HOME_ARTIST_COVER_CACHE_KEY);
  const cached = normalizeArtworkUrl(sessionCache[normalizedKey]);
  if (cached) {
    state.homeArtistCoverCache[normalizedKey] = cached;
    return cached;
  }
  return null;
}

function setCachedArtistCoverUrl(key, url) {
  const normalizedKey = String(key || "").trim();
  const normalizedUrl = normalizeArtworkUrl(url);
  if (!normalizedKey || !normalizedUrl) return;
  state.homeArtistCoverCache[normalizedKey] = normalizedUrl;
  const sessionCache = readSessionObject(HOME_ARTIST_COVER_CACHE_KEY);
  sessionCache[normalizedKey] = normalizedUrl;
  writeSessionObject(HOME_ARTIST_COVER_CACHE_KEY, sessionCache);
}

function normalizeGenreArtworkSet(value) {
  const rawValues = Array.isArray(value) ? value : (typeof value === "string" ? [value] : []);
  const urls = [];
  const seen = new Set();
  rawValues.forEach((entry) => {
    const normalized = normalizeArtworkUrl(entry);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    urls.push(normalized);
  });
  return urls.slice(0, 4);
}

function getCachedGenreCoverUrls(key) {
  const normalizedKey = String(key || "").trim().toLowerCase();
  if (!normalizedKey) return [];
  const memory = state.homeGenreCoverCache[normalizedKey];
  if (Array.isArray(memory) && memory.length) {
    return memory;
  }
  const sessionCache = readSessionObject(HOME_GENRE_COVER_CACHE_KEY);
  const cached = normalizeGenreArtworkSet(sessionCache[normalizedKey]);
  if (cached.length) {
    state.homeGenreCoverCache[normalizedKey] = cached;
    return cached;
  }
  return [];
}

function setCachedGenreCoverUrls(key, urls) {
  const normalizedKey = String(key || "").trim().toLowerCase();
  const normalizedUrls = normalizeGenreArtworkSet(urls);
  if (!normalizedKey || !normalizedUrls.length) return;
  state.homeGenreCoverCache[normalizedKey] = normalizedUrls;
  const sessionCache = readSessionObject(HOME_GENRE_COVER_CACHE_KEY);
  sessionCache[normalizedKey] = normalizedUrls;
  writeSessionObject(HOME_GENRE_COVER_CACHE_KEY, sessionCache);
}

function isArtworkCacheStale(updatedAt) {
  const ts = Number(updatedAt || 0);
  if (!Number.isFinite(ts) || ts <= 0) return true;
  return (Date.now() - (ts * 1000)) > MUSIC_ARTWORK_CACHE_REFRESH_MS;
}

async function fetchPersistentArtistCoverEntry(key) {
  const normalizedKey = String(key || "").trim();
  if (!normalizedKey) return null;
  try {
    const payload = await fetchJson(`/api/music/artist/art/${encodeURIComponent(normalizedKey)}`);
    const coverUrl = normalizeArtworkUrl(payload?.cover_url);
    if (!coverUrl) return null;
    return {
      coverUrl,
      updatedAt: Number(payload?.updated_at || 0),
    };
  } catch {
    return null;
  }
}

async function persistArtistCoverEntry(key, coverUrl) {
  const normalizedKey = String(key || "").trim();
  const normalizedUrl = normalizeArtworkUrl(coverUrl);
  if (!normalizedKey || !normalizedUrl) return;
  try {
    await fetchJson(`/api/music/artist/art/${encodeURIComponent(normalizedKey)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cover_url: normalizedUrl }),
    });
  } catch {
    // best-effort only
  }
}

async function fetchPersistentGenreCoverEntry(key) {
  const normalizedKey = String(key || "").trim().toLowerCase();
  if (!normalizedKey) return null;
  try {
    const payload = await fetchJson(`/api/music/genre/art/${encodeURIComponent(normalizedKey)}`);
    const coverUrls = normalizeGenreArtworkSet(payload?.cover_urls);
    if (!coverUrls.length) return null;
    return {
      coverUrls,
      updatedAt: Number(payload?.updated_at || 0),
    };
  } catch {
    return null;
  }
}

async function persistGenreCoverEntry(key, coverUrls) {
  const normalizedKey = String(key || "").trim().toLowerCase();
  const normalizedUrls = normalizeGenreArtworkSet(coverUrls);
  if (!normalizedKey || !normalizedUrls.length) return;
  try {
    await fetchJson(`/api/music/genre/art/${encodeURIComponent(normalizedKey)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cover_urls: normalizedUrls }),
    });
  } catch {
    // best-effort only
  }
}

function getCachedArrGenreCoverUrls(kind, genreId) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  const normalizedId = String(genreId || "").trim();
  const bucket = state.arrGenreArtworkCache[normalizedKind];
  if (!bucket || !normalizedId) return [];
  return normalizeGenreArtworkSet(bucket[normalizedId]);
}

function setCachedArrGenreCoverUrls(kind, genreId, coverUrls) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  const normalizedId = String(genreId || "").trim();
  const normalizedUrls = normalizeGenreArtworkSet(coverUrls);
  if (!normalizedKind || !normalizedId || !normalizedUrls.length) return;
  if (!state.arrGenreArtworkCache[normalizedKind]) {
    state.arrGenreArtworkCache[normalizedKind] = {};
  }
  state.arrGenreArtworkCache[normalizedKind][normalizedId] = normalizedUrls;
}

async function fetchPersistentArrGenreCoverEntry(kind, genreId) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  const normalizedId = String(genreId || "").trim();
  if (!normalizedKind || !normalizedId) return null;
  try {
    const payload = await fetchJson(`/api/arr/genre/art/${encodeURIComponent(normalizedKind)}/${encodeURIComponent(normalizedId)}`);
    const coverUrls = normalizeGenreArtworkSet(payload?.cover_urls);
    if (!coverUrls.length) return null;
    return {
      coverUrls,
      updatedAt: Number(payload?.updated_at || 0),
    };
  } catch {
    return null;
  }
}

async function persistArrGenreCoverEntry(kind, genreId, coverUrls) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  const normalizedId = String(genreId || "").trim();
  const normalizedUrls = normalizeGenreArtworkSet(coverUrls);
  if (!normalizedKind || !normalizedId || !normalizedUrls.length) return;
  try {
    await fetchJson(`/api/arr/genre/art/${encodeURIComponent(normalizedKind)}/${encodeURIComponent(normalizedId)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cover_urls: normalizedUrls }),
    });
  } catch {
    // best-effort only
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

function getMusicGenreIntentDefinition(genre) {
  const raw = String(genre || "").trim();
  if (!raw) {
    return { label: "", aliases: [] };
  }
  const key = raw.toLowerCase();
  const direct = MUSIC_GENRE_INTENT_MAP[key];
  if (direct) {
    return {
      label: direct.label,
      aliases: [...direct.aliases],
    };
  }
  for (const entry of Object.values(MUSIC_GENRE_INTENT_MAP)) {
    if (entry.aliases.some((alias) => String(alias || "").trim().toLowerCase() === key)) {
      return {
        label: entry.label,
        aliases: [...entry.aliases],
      };
    }
  }
  return { label: raw, aliases: [raw] };
}

function normalizeMusicGenreIntent(genre) {
  return getMusicGenreIntentDefinition(genre).label;
}

function getMusicGenreIntentAliases(genre) {
  const definition = getMusicGenreIntentDefinition(genre);
  const seen = new Set();
  return definition.aliases
    .map((value) => String(value || "").trim())
    .filter((value) => {
      const key = value.toLowerCase();
      if (!value || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function getMusicGenreArtistsCacheKey(genre, limit = 24) {
  return `${normalizeMusicGenreIntent(genre).toLowerCase()}::${Math.max(1, Number(limit || 24))}`;
}

function normalizeHiddenMusicGenres(values) {
  const seen = new Set();
  return (Array.isArray(values) ? values : [])
    .map((value) => normalizeMusicGenreIntent(value))
    .filter((value) => {
      const key = String(value || "").trim().toLowerCase();
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function normalizeHiddenMusicArtists(values) {
  const seen = new Set();
  return (Array.isArray(values) ? values : [])
    .map((value) => {
      if (!value || typeof value !== "object") return null;
      const name = String(value.name || "").trim();
      const artistMbid = String(value.artist_mbid || "").trim() || null;
      const key = (artistMbid || name).trim().toLowerCase();
      if (!name || !key || seen.has(key)) return null;
      seen.add(key);
      return { name, artist_mbid: artistMbid };
    })
    .filter(Boolean);
}

function getHiddenMusicArtistKey(artist = {}) {
  return String(artist?.artist_mbid || artist?.name || "").trim().toLowerCase();
}

function isMusicArtistHidden(artist = {}) {
  const key = getHiddenMusicArtistKey(artist);
  return !!key && state.musicHiddenArtists.some((entry) => getHiddenMusicArtistKey(entry) === key);
}

function isBrowseSuppressedArtist(artist = {}) {
  const name = String(artist?.name || "").trim().toLowerCase();
  return name === "various artists";
}

function isMusicGenreHidden(genre) {
  const normalized = normalizeMusicGenreIntent(genre).toLowerCase();
  return state.musicHiddenGenres.some((entry) => String(entry || "").trim().toLowerCase() === normalized);
}

function getCachedMusicGenreArtists(genre, limit = 24) {
  const cacheKey = getMusicGenreArtistsCacheKey(genre, limit);
  const cached = state.musicGenreArtistsCache[cacheKey];
  return Array.isArray(cached) ? cached.map((item) => ({ ...item })) : null;
}

function setCachedMusicGenreArtists(genre, artists, limit = 24) {
  const cacheKey = getMusicGenreArtistsCacheKey(genre, limit);
  state.musicGenreArtistsCache[cacheKey] = Array.isArray(artists)
    ? artists.map((item) => ({ ...item }))
    : [];
}

async function fetchArtistsForGenreIntent(genre, { limit = 24 } = {}) {
  const cached = getCachedMusicGenreArtists(genre, limit);
  if (cached && cached.length) {
    return cached;
  }
  const payload = await fetchJson(`/api/music/genres/${encodeURIComponent(normalizeMusicGenreIntent(genre))}/artists?limit=${Math.max(1, Number(limit || 24))}`);
  const limited = Array.isArray(payload?.artists) ? payload.artists.map((item) => ({ ...item })) : [];
  setCachedMusicGenreArtists(genre, limited, limit);
  return limited;
}

function warmMusicGenreArtistCaches(genres = [], { limit = 24 } = {}) {
  const uniqueGenres = [...new Set((Array.isArray(genres) ? genres : [])
    .map((value) => normalizeMusicGenreIntent(value))
    .filter(Boolean))];
  uniqueGenres.forEach((genre) => {
    const cached = getCachedMusicGenreArtists(genre, limit);
    const browseCacheKey = getMusicGenreBrowseCacheKey(genre);
    if (cached && cached.length) {
      if (!state.musicGenreBrowseCache[browseCacheKey]) {
        state.musicGenreBrowseCache[browseCacheKey] = { artists: cached };
      }
      return;
    }
    fetchArtistsForGenreIntent(genre, { limit })
      .then((artists) => {
        if (!Array.isArray(artists) || !artists.length || state.musicGenreBrowseCache[browseCacheKey]) return;
        state.musicGenreBrowseCache[browseCacheKey] = { artists };
      })
      .catch(() => {});
  });
}

function scheduleMusicGenrePrewarm({ limit = 18 } = {}) {
  const favoriteGenres = Array.isArray(state.musicPreferences?.favorite_genres)
    ? state.musicPreferences.favorite_genres
    : [];
  const hiddenGenreKeys = new Set(
    normalizeHiddenMusicGenres(state.musicHiddenGenres).map((genre) => genre.toLowerCase())
  );
  const visibleDefaults = DEFAULT_MUSIC_GENRES.filter(
    (genre) => !hiddenGenreKeys.has(normalizeMusicGenreIntent(genre).toLowerCase())
  );
  const targets = [...favoriteGenres.slice(0, 5), ...visibleDefaults.slice(0, 8)];
  if (!targets.length) return;
  setTimeout(() => warmMusicGenreArtistCaches(targets, { limit }), 0);
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
  const requested = String(page || "").split("?")[0] || page;
  const allowed = new Set(["home", "video", "music", "movies-tv", "review", "config"]);
  const target = allowed.has(normalized) ? normalized : "home";
  if (target !== "home") {
    setHomeSetupOverlayOpen(false);
  }
  state.currentPage = target;
  mountTopbarForPage(target);
  if (target === "music") {
    mountMusicPageNodes();
  } else if (target === "video") {
    mountHomePageNodes();
  }
  if (target === "video" || target === "music") {
    if (target === "video") {
      setHomeMediaMode("video", { persist: false, clearResultsOnDisable: false });
      loadVideoLibrarySection().catch(() => {});
      setHomeSection(state.homeSection || "search");
    } else if ((state.homeMediaMode || "video") === "video") {
      setHomeMediaMode(state.lastMusicMode || "music", { persist: false, clearResultsOnDisable: false });
    }
    if (target === "music") {
      loadMusicLibrarySection().catch(() => {});
      const requestedMusicSection = requested === "music-player" ? "player" : (state.musicSection || "browse");
      setMusicSection(requestedMusicSection);
    }
    if (state.homeSearchRequestId) {
      startHomeResultPolling(state.homeSearchRequestId);
    }
    setHomeSearchActive(Boolean(state.homeSearchRequestId || state.homeDirectPreview));
    updateHomeViewAdvancedLink();
    refreshReviewQueue();
  } else {
    stopHomeResultPolling();
    updateHomeViewAdvancedLink();
    closeMusicPlayerModal();
  }
  if (target !== "movies-tv") {
    stopArrStatusPolling();
  }
  document.body.classList.remove("nav-open");
  document.body.classList.toggle("home-page", target === "video" || target === "music");
  document.body.classList.toggle("launcher-page", target === "home");
  document.body.classList.toggle("video-page", target === "video");
  document.body.classList.toggle("music-page", target === "music");
  if (target !== "video" && target !== "music") {
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
  } else if (target === "movies-tv") {
    renderMoviesTvSetupGate();
    mountTopbarForPage("movies-tv");
    refreshArrConnectionStatus({ quiet: true }).catch(() => {});
    loadArrGenres().catch(() => {});
    renderArrResults();
    setMoviesTvSection(state.moviesTvSection || "search");
    startArrStatusPolling();
  } else if (target === "config") {
    mountSettingsSubpages();
    ensureSettingsLayoutVisible();
    const sectionHash = String(window.location.hash || "").replace("#", "");
    const sectionFromPage = ({
      setup: "settings-guided-setup",
      connections: "settings-connections-hub",
      services: "settings-services-hub",
      status: "settings-status-hub",
      logs: "settings-logs-hub",
      info: "settings-info-hub",
    })[requested];
    const nextSettingsSection = sectionHash.startsWith("settings-")
      ? sectionHash
      : (sectionFromPage || state.settingsActiveSectionId || getDefaultSettingsSectionId());
    showSettingsPage(nextSettingsSection, { jump: false, smooth: false });
    initializeSettingsPageData(nextSettingsSection);
  }
  if (target === "home") {
    renderHomeLauncher();
    requestAnimationFrame(() => maybeShowStartupSetupPrompt());
  }
}

function mountMusicPageNodes() {
  return;
}

function mountHomePageNodes() {
  const shell = $("#home-search-surface");
  const messageEl = $("#home-search-message");
  const reviewEl = $("#home-review-alert");
  if (shell && messageEl && messageEl.parentElement !== shell) shell.appendChild(messageEl);
  if (shell && reviewEl && reviewEl.parentElement !== shell) shell.appendChild(reviewEl);
}

function isInitialSetupComplete() {
  const completedModules = Array.isArray(state.config?.setup?.completed_modules)
    ? state.config.setup.completed_modules
    : [];
  return completedModules.length > 0;
}

function getDefaultSettingsSectionId() {
  return isInitialSetupComplete() ? "settings-core" : "settings-guided-setup";
}

function renderHomeLauncher() {
  const booksTile = $("#home-launcher-books");
  const setupTile = $("#home-launcher-setup");
  if (!booksTile && !setupTile) return;
  const readarrCfg = (state.config?.arr?.readarr && typeof state.config.arr.readarr === "object")
    ? state.config.arr.readarr
    : {};
  const stack = (state.config?.setup?.stack && typeof state.config.setup.stack === "object")
    ? state.config.setup.stack
    : {};
  const enabled = !!(stack.enable_readarr || readarrCfg.base_url || readarrCfg.api_key);
  if (booksTile) {
    booksTile.classList.toggle("hidden", !enabled);
  }
  if (setupTile) {
    setupTile.classList.toggle("hidden", isInitialSetupComplete());
  }
}

function setHomeSection(section) {
  const normalized = String(section || "search").trim() || "search";
  state.homeSection = normalized;
  $$("[data-home-section]").forEach((button) => {
    button.classList.toggle("active", button.dataset.homeSection === normalized);
  });
  const searchSurface = $("#home-search-surface");
  const librarySurface = $("#home-library-surface");
  const showSearch = normalized === "search";
  if (searchSurface) {
    searchSurface.classList.toggle("hidden", !showSearch);
    searchSurface.classList.toggle("active", showSearch);
  }
  if (librarySurface) {
    librarySurface.classList.toggle("hidden", showSearch);
    librarySurface.classList.toggle("active", !showSearch);
  }
  const discovery = $("#video-discovery-default");
  if (discovery) {
    const resultsVisible = !$("#home-results")?.classList.contains("hidden");
    const showDiscovery = showSearch && !resultsVisible && !state.homeSearchRequestId;
    discovery.classList.toggle("hidden", !showDiscovery);
  }
}

function mountVideoToolbar() {
  return;
}

function setMoviesTvSection(section) {
  const normalized = String(section || "search").trim() || "search";
  if (!isTmdbConfigured()) {
    state.moviesTvSection = "search";
    applyMoviesTvWorkspaceState();
    return;
  }
  state.moviesTvSection = normalized;
  applyMoviesTvWorkspaceState();
}

function getMoviesTvRecentYears() {
  const currentYear = new Date().getFullYear();
  return Array.from({ length: 6 }, (_, index) => String(currentYear - index));
}

function renderMoviesTvYearChips() {
  const container = $("#movies-tv-year-chips");
  if (!container) return;
  const selectedYear = String(state.arrSearchYear || "").trim();
  const years = ["", ...getMoviesTvRecentYears()];
  container.innerHTML = years.map((year) => {
    const label = year || "Any year";
    const active = selectedYear === year;
    return `
      <button
        type="button"
        class="button ghost small movies-tv-year-chip ${active ? "active" : ""}"
        data-arr-year="${escapeAttr(year)}"
        aria-pressed="${active ? "true" : "false"}"
      >${escapeHtml(label)}</button>
    `;
  }).join("");
}

function setMoviesTvFiltersOpen(open) {
  state.arrFiltersOpen = !!open;
  const panel = $("#movies-tv-filters-panel");
  const toggle = $("#movies-tv-filters-toggle");
  if (panel) {
    panel.classList.toggle("hidden", !state.arrFiltersOpen);
  }
  if (toggle) {
    toggle.setAttribute("aria-expanded", state.arrFiltersOpen ? "true" : "false");
  }
  syncTopbarSubbarVisibility();
}

function setMoviesTvSearchYear(year) {
  const normalized = /^\d{4}$/.test(String(year || "").trim()) ? String(year).trim() : "";
  state.arrSearchYear = normalized;
  renderMoviesTvYearChips();
}

function applyMoviesTvWorkspaceState() {
  const configured = isTmdbConfigured();
  const searchRow = getMoviesTvSearchRowEl();
  const sectionToggle = $("#movies-tv-panel .movies-tv-section-toggle");
  const messageRegion = $("#movies-tv-message-region");
  const setupView = $("#movies-tv-setup-view");
  const setupLanding = $("#movies-tv-setup-landing");
  const discoveryView = $("#movies-tv-discovery-view");
  const genresView = $("#movies-tv-genres-view");
  const resultsView = $("#movies-tv-results-view");
  const filtersPanel = getMoviesTvFiltersPanelEl();
  const section = String(state.moviesTvSection || "search").trim() || "search";
  const hasResults = Array.isArray(state.arrResults) && state.arrResults.length > 0;

  if (searchRow) searchRow.classList.toggle("hidden", !configured);
  if (sectionToggle) sectionToggle.classList.toggle("hidden", !configured);
  if (messageRegion) messageRegion.classList.toggle("hidden", !configured);
  if (filtersPanel && !configured) filtersPanel.classList.add("hidden");

  if (!configured) {
    state.moviesTvSection = "search";
    if (setupView) setupView.classList.remove("hidden");
    if (setupLanding) setupLanding.classList.remove("hidden");
    if (discoveryView) discoveryView.classList.add("hidden");
    if (genresView) genresView.classList.add("hidden");
    if (resultsView) resultsView.classList.add("hidden");
    $$("[data-movies-tv-section]").forEach((button) => {
      button.classList.toggle("active", button.dataset.moviesTvSection === "search");
    });
    return;
  }

  if (setupView) setupView.classList.add("hidden");
  if (setupLanding) setupLanding.classList.add("hidden");
  $$("[data-movies-tv-section]").forEach((button) => {
    button.classList.toggle("active", button.dataset.moviesTvSection === section);
  });

  if (section === "genres") {
    if (discoveryView) discoveryView.classList.add("hidden");
    if (genresView) genresView.classList.remove("hidden");
    if (resultsView) resultsView.classList.add("hidden");
    return;
  }

  if (hasResults) {
    if (discoveryView) discoveryView.classList.add("hidden");
    if (genresView) genresView.classList.add("hidden");
    if (resultsView) resultsView.classList.remove("hidden");
    return;
  }

  if (discoveryView) discoveryView.classList.remove("hidden");
  if (genresView) genresView.classList.add("hidden");
  if (resultsView) resultsView.classList.add("hidden");
}

function movePanelChildren(panelId, slotId) {
  const panel = document.getElementById(panelId);
  const slot = document.getElementById(slotId);
  if (!panel || !slot) return;
  if (slot.dataset.mounted === "1") return;
  const children = Array.from(panel.children);
  children.forEach((child) => {
    slot.appendChild(child);
  });
  panel.classList.add("hidden");
  slot.dataset.mounted = "1";
}

function moveSlotChildren(sourceId, targetId) {
  const source = document.getElementById(sourceId);
  const target = document.getElementById(targetId);
  if (!source || !target || source === target) return;
  const children = Array.from(source.children);
  children.forEach((child) => {
    target.appendChild(child);
  });
}

function mountGuidedSetupHost(slotId) {
  const targetId = String(slotId || "settings-setup-slot");
  const target = document.getElementById(targetId);
  if (!target) return;
  const currentParent = $("#setup-wizard")?.parentElement;
  if (currentParent && currentParent.id === targetId) {
    return;
  }
  const sourceId = ["settings-setup-slot", "home-setup-overlay-slot", "setup-panel"].find((id) => {
    const node = document.getElementById(id);
    return node && node !== target && node.querySelector("#setup-wizard");
  });
  if (sourceId) {
    moveSlotChildren(sourceId, targetId);
  }
}

function mountSettingsSubpages() {
  movePanelChildren("setup-panel", "settings-setup-slot");
  movePanelChildren("connections-panel", "settings-connections-slot");
  movePanelChildren("services-panel", "settings-services-slot");
  ["status-panel", "search-queue-panel", "downloads-panel"].forEach((id) => {
    movePanelChildren(id, "settings-status-slot");
  });
  movePanelChildren("logs-panel", "settings-logs-slot");
  ["metrics-panel", "search-items-panel", "search-requests-panel"].forEach((id) => {
    movePanelChildren(id, "settings-info-slot");
  });
  mountGuidedSetupHost("settings-setup-slot");
}

function setMusicSection(section) {
  const requested = String(section || "browse").trim() || "browse";
  const normalized = requested === "search" || requested === "home" ? "browse" : requested;
  state.musicSection = normalized;
  $$(".music-app-nav").forEach((button) => {
    button.classList.toggle("active", button.dataset.musicSection === normalized);
  });
  const browseView = $("#music-browse-view");
  const libraryView = $("#music-library-view");
  const playerView = $("#music-player-view");
  const showPlayerHost = normalized === "favorites" || normalized === "playlists" || normalized === "player" || normalized === "radio";
  if (browseView) {
    browseView.classList.toggle("hidden", normalized !== "browse");
    browseView.classList.toggle("active", normalized === "browse");
  }
  if (libraryView) {
    libraryView.classList.toggle("hidden", normalized !== "library");
    libraryView.classList.toggle("active", normalized === "library");
  }
  if (playerView) {
    playerView.classList.toggle("hidden", !showPlayerHost);
    playerView.classList.toggle("active", showPlayerHost);
    playerView.classList.toggle("music-player-full", normalized === "player");
  }
  const messageRegion = $("#music-page-message-region");
  const reviewRegion = $("#music-page-review-region");
  const toolbarSlot = getMusicToolbarSlot();
  const navSlot = getMusicNavSlot();
  if (messageRegion) {
    messageRegion.classList.toggle("hidden", normalized !== "browse");
  }
  if (reviewRegion) {
    reviewRegion.classList.toggle("hidden", normalized !== "browse");
  }
  if (toolbarSlot && normalized !== "browse") {
    toolbarSlot.innerHTML = "";
  }
  if (navSlot && normalized !== "browse") {
    navSlot.innerHTML = "";
  }
  if (normalized === "browse") {
    renderMusicLanding();
  }
  const playerViewMap = {
    radio: "radio",
    playlists: "playlists",
    player: "queue",
    favorites: "favorites",
  };
  if (showPlayerHost) {
    const nextPlayerView = playerViewMap[normalized] || "library";
    setMusicPlayerView(nextPlayerView);
    loadMusicPlayerView().catch(() => {});
  }
  syncMusicPlayerVideoShell();
  syncBottomPlayerShell();
}

function syncBottomPlayerShell() {
  const shell = $("#music-bottom-player");
  const titleEl = $("#music-bottom-player-title");
  const metaEl = $("#music-bottom-player-meta");
  const artEl = $("#music-bottom-player-art");
  const toggleButton = $("#music-bottom-player-toggle");
  const prevButton = $("#music-bottom-player-prev");
  const nextButton = $("#music-bottom-player-next");
  const queueButton = $("#music-bottom-player-queue");
  const statusEl = $("#music-bottom-player-status");
  const audio = $("#music-player-audio");
  if (!shell || !titleEl || !metaEl || !artEl || !toggleButton) return;
  const current = state.playerCurrent || {};
  const audioHasSource = !!(audio && (audio.currentSrc || audio.src));
  const hasTrack = !!(current.stream_url && audioHasSource) || !!(current.video_id && activePlayerIsYT());
  const shouldHide = !hasTrack || (state.currentPage === "music" && state.musicSection === "player");
  shell.classList.toggle("hidden", shouldHide);
  titleEl.textContent = String(current.title || "Nothing playing");
  metaEl.textContent = [current.artist, current.album, current.kind].filter(Boolean).join(" • ") || "Choose a track from your library or start a station.";
  if (statusEl) {
    const stationBits = [];
    if (Number(state.playerActiveStationId || 0)) {
      stationBits.push(`<span class="music-status-badge is-queued">Radio</span>`);
      if (state.playerCurrentHasVideo) {
        stationBits.push(`<span class="music-status-badge">Video Ready</span>`);
      }
    }
    const badges = `${stationBits.join("")}${buildMusicStatusBadges(current, { queueOnly: true })}`;
    statusEl.innerHTML = badges;
    statusEl.classList.toggle("hidden", !badges);
  }
  artEl.src = getMusicLibraryArtworkUrl(current);
  toggleButton.textContent = activePlayerIsPaused() ? "Play" : "Pause";
  if (prevButton) prevButton.disabled = !hasTrack;
  if (nextButton) nextButton.disabled = !hasTrack;
  if (queueButton) {
    queueButton.disabled = !hasTrack;
    queueButton.textContent = `Queue ${Array.isArray(state.playerQueue) && state.playerQueue.length ? `(${state.playerQueue.length})` : ""}`.trim();
  }
}

function syncMusicStationCreateForm() {
  const seedTypeEl = $("#music-player-station-seed-type");
  const seedValueEl = $("#music-player-station-seed-value");
  const modeEl = $("#music-player-station-mode");
  if (!seedTypeEl || !seedValueEl || !modeEl) return;
  const seedType = String(seedTypeEl.value || "artist").trim();
  if (seedType === "favorites") {
    seedValueEl.value = "";
    seedValueEl.placeholder = "No seed needed";
    seedValueEl.disabled = true;
    modeEl.value = "mix";
    modeEl.disabled = true;
  } else {
    seedValueEl.disabled = false;
    modeEl.disabled = false;
    seedValueEl.placeholder = seedType === "genre" ? "Genre name" : seedType === "album" ? "Album name" : "Artist name";
    if (!modeEl.value) {
      modeEl.value = "mix";
    }
  }
}

function openMusicPlayerModal() {
  const modal = $("#music-player-modal");
  const slot = $("#music-player-modal-slot");
  const playerMain = $("#music-player-view .music-player-main");
  if (modal && slot && playerMain) {
    slot.appendChild(playerMain);
    modal.classList.remove("hidden");
    state.musicPlayerExpanded = true;
  }
}

function closeMusicPlayerModal() {
  const modal = $("#music-player-modal");
  const inlineSlot = $("#music-player-view");
  const playerMain = $("#music-player-modal-slot .music-player-main");
  if (inlineSlot && playerMain) {
    inlineSlot.appendChild(playerMain);
  }
  if (modal) {
    modal.classList.add("hidden");
  }
  state.musicPlayerExpanded = false;
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("`", "&#96;");
}

function buildSetupWizardDraft() {
  const cfg = state.config && typeof state.config === "object" ? state.config : {};
  const stack = state.setupStatus?.stack || cfg?.setup?.stack || {};
  const serviceManagement = state.setupStatus?.service_management || cfg?.setup?.service_management || {};
  const managedStack = state.setupStatus?.managed_stack || cfg?.setup?.managed_stack || {};
  const existingStack = state.setupStatus?.existing_stack || cfg?.setup?.existing_stack || {};
  const arr = cfg.arr && typeof cfg.arr === "object" ? cfg.arr : {};
  const vpn = arr.vpn && typeof arr.vpn === "object" ? arr.vpn : {};
  const jellyfin = arr.jellyfin && typeof arr.jellyfin === "object" ? arr.jellyfin : {};
  const telegram = cfg.telegram && typeof cfg.telegram === "object" ? cfg.telegram : {};
  const hasTmdb = !!arr.tmdb_api_key;
  const hasYoutube = !!cfg.yt_dlp_cookies;
  const hasTelegram = !!(telegram.bot_token || telegram.chat_id);
  const hasVpn = !!vpn.enabled;
  const hasJellyfin = !!(stack.enable_jellyfin || jellyfin.base_url || jellyfin.api_key);
  const hasArr = !!stack.enable_arr_stack;
  const existingServices = existingStack.services && typeof existingStack.services === "object" ? existingStack.services : {};
  const managedFeatures = managedStack.enabled_features && typeof managedStack.enabled_features === "object" ? managedStack.enabled_features : {};
  return {
    arr_setup_mode: String(serviceManagement.mode || (hasArr ? "managed" : "none")) || "none",
    direct_manage: String(serviceManagement.apply_mode || "manual") === "direct" || !!managedStack.direct_manage_requested,
    managed_movies: !!managedFeatures.movies || !!stack.enable_radarr,
    managed_tv: !!managedFeatures.tv || !!stack.enable_sonarr,
    managed_books: !!managedFeatures.books || !!stack.enable_readarr,
    managed_subtitles: !!managedFeatures.subtitles || !!stack.enable_bazarr,
    managed_downloader: !!managedFeatures.downloader || !!stack.enable_qbittorrent,
    managed_vpn: !!managedFeatures.vpn || hasVpn,
    managed_jellyfin: !!managedFeatures.jellyfin || hasJellyfin,
    enable_radarr: !!stack.enable_radarr,
    enable_sonarr: !!stack.enable_sonarr,
    enable_readarr: !!stack.enable_readarr,
    enable_prowlarr: !!stack.enable_prowlarr,
    enable_bazarr: !!stack.enable_bazarr,
    enable_qbittorrent: !!stack.enable_qbittorrent,
    enable_vpn: hasVpn ? true : null,
    enable_jellyfin: hasJellyfin ? true : null,
    wants_tmdb: hasTmdb ? true : null,
    wants_youtube: hasYoutube ? true : null,
    wants_telegram: hasTelegram ? true : null,
    env_path: String(stack.env_path || ".env"),
    media_root: String(stack.media_root || "./media"),
    movies_root: String(stack.movies_root || "./media/movies"),
    tv_root: String(stack.tv_root || "./media/tv"),
    downloads_root: String(stack.downloads_root || "./downloads"),
    books_root: String(stack.books_root || "./media/books"),
    tmdb_api_key: String(arr.tmdb_api_key || ""),
    yt_dlp_cookies: String(cfg.yt_dlp_cookies || ""),
    telegram_bot_token: String(telegram.bot_token || ""),
    telegram_chat_id: String(telegram.chat_id || ""),
    vpn_provider: String(vpn.provider || "gluetun"),
    vpn_control_url: String(vpn.control_url || ""),
    vpn_openvpn_user: "",
    vpn_openvpn_password: "",
    vpn_wireguard_private_key: "",
    vpn_server_countries: "",
    vpn_route_qbittorrent: vpn.route_qbittorrent !== false,
    vpn_route_prowlarr: !!vpn.route_prowlarr,
    vpn_route_retreivr: !!vpn.route_retreivr,
    jellyfin_base_url: String(jellyfin.base_url || ""),
    jellyfin_api_key: String(jellyfin.api_key || ""),
    existing_radarr_enabled: !!existingServices.radarr?.configured || !!arr.radarr?.base_url,
    existing_sonarr_enabled: !!existingServices.sonarr?.configured || !!arr.sonarr?.base_url,
    existing_readarr_enabled: !!existingServices.readarr?.configured || !!arr.readarr?.base_url,
    existing_prowlarr_enabled: !!existingServices.prowlarr?.configured || !!arr.prowlarr?.base_url,
    existing_bazarr_enabled: !!existingServices.bazarr?.configured || !!arr.bazarr?.base_url,
    existing_qbittorrent_enabled: !!existingServices.qbittorrent?.configured || !!arr.qbittorrent?.base_url,
    existing_jellyfin_enabled: !!existingServices.jellyfin?.configured || !!arr.jellyfin?.base_url,
    existing_radarr_base_url: String(arr.radarr?.base_url || ""),
    existing_sonarr_base_url: String(arr.sonarr?.base_url || ""),
    existing_readarr_base_url: String(arr.readarr?.base_url || ""),
    existing_prowlarr_base_url: String(arr.prowlarr?.base_url || ""),
    existing_bazarr_base_url: String(arr.bazarr?.base_url || ""),
    existing_qbittorrent_base_url: String(arr.qbittorrent?.base_url || ""),
    existing_qbittorrent_username: String(arr.qbittorrent?.username || ""),
    existing_qbittorrent_password: String(arr.qbittorrent?.password || ""),
    existing_jellyfin_base_url: String(arr.jellyfin?.base_url || ""),
    existing_jellyfin_api_key: String(arr.jellyfin?.api_key || ""),
  };
}

function ensureSetupWizardState() {
  if (!state.setupWizard || typeof state.setupWizard !== "object") {
    state.setupWizard = {
      stepIndex: 0,
      draft: buildSetupWizardDraft(),
      jellyfinDiscovery: null,
      jellyfinDiscoveryLoading: false,
      jellyfinDiscoveryError: "",
    };
    return;
  }
  if (!state.setupWizard.draft) {
    state.setupWizard.draft = buildSetupWizardDraft();
  }
  if (!Number.isInteger(state.setupWizard.stepIndex) || state.setupWizard.stepIndex < 0) {
    state.setupWizard.stepIndex = 0;
  }
  if (!Array.isArray(state.setupWizard.jellyfinDiscovery)) {
    state.setupWizard.jellyfinDiscovery = [];
  }
  if (typeof state.setupWizard.jellyfinDiscoveryLoading !== "boolean") {
    state.setupWizard.jellyfinDiscoveryLoading = false;
  }
  if (typeof state.setupWizard.jellyfinDiscoveryError !== "string") {
    state.setupWizard.jellyfinDiscoveryError = "";
  }
}

function getSetupWizardSteps() {
  const draft = state.setupWizard?.draft || buildSetupWizardDraft();
  const steps = [
    {
      id: "arr-mode",
      title: "Do you want help setting up movie, show, and book automation?",
      subtitle: "Retreivr can set up a new managed stack for you, connect to tools you already use, or skip this for now.",
      required: false,
    },
  ];
  if (draft.arr_setup_mode === "managed") {
    steps.push({
      id: "arr-managed-features",
      title: "What should Retreivr set up for you?",
      subtitle: "Choose what you want to enable. Retreivr handles the internal connections for the managed setup path.",
      required: false,
    });
    steps.push({
      id: "arr-managed-control",
      title: "Should Retreivr manage these services directly on this device?",
      subtitle: "If you allow it, Retreivr can use its built-in helper to start the managed services and continue setup automatically after restart.",
      required: false,
    });
  }
  if (draft.arr_setup_mode === "existing") {
    steps.push({
      id: "arr-existing-services",
      title: "Which services do you already have?",
      subtitle: "Choose only the apps you want Retreivr to connect to. You only need to enter their app addresses on this path.",
      required: false,
    });
  }
  steps.push(
    {
      id: "tmdb",
      title: "Do you want movies and TV discovery inside Retreivr?",
      subtitle: "A free TMDb key unlocks browsing by title, cast, genre, year, and trailers inside Retreivr.",
      required: false,
    },
    {
      id: "vpn",
      title: "Do you want guided VPN setup?",
      subtitle: "This is most useful if you plan to use download tools such as qBittorrent. Retreivr will help you choose what should use the VPN.",
      required: false,
    },
    {
      id: "youtube",
      title: "Do you want improved YouTube access?",
      subtitle: "You can add browser cookies for harder-to-access videos and follow a guided path for optional YouTube API setup later.",
      required: false,
    },
    {
      id: "telegram",
      title: "Do you want Telegram notifications?",
      subtitle: "This lets Retreivr send download updates and review alerts to your phone or desktop through Telegram.",
      required: false,
    },
    {
      id: "jellyfin",
      title: "Do you want to connect Jellyfin too?",
      subtitle: "Jellyfin is optional. Retreivr can still acquire and organize media without it, but you can connect it now if you want playback in the same ecosystem.",
      required: false,
    },
    {
      id: "paths",
      title: "Where should Retreivr keep your files?",
      subtitle: "Choose the folders Retreivr will use for your media, downloads, and books. You can change them later if needed.",
      required: true,
    },
    {
      id: "review",
      title: "Review your setup choices",
      subtitle: "Retreivr will save what you chose, prepare the stack settings it manages, and show you the exact restart step to run next.",
      required: true,
    }
  );
  return steps;
}

function getSetupWizardStepState(step, draft = state.setupWizard?.draft || buildSetupWizardDraft()) {
  const status = { tone: "pending", label: "Needs attention", message: "Finish this step before moving on." };
  if (!step) return status;
  const nonEmpty = (value) => String(value || "").trim().length > 0;
  if (step.id === "arr-mode") {
    if (draft.arr_setup_mode === "managed") return { tone: "success", label: "Managed setup", message: "Retreivr will prepare a bundled automation stack for you." };
    if (draft.arr_setup_mode === "existing") return { tone: "success", label: "Existing services", message: "Retreivr will connect to tools you already run." };
    if (draft.arr_setup_mode === "none") return { tone: "skipped", label: "Skipped", message: "Retreivr-only mode stays active. You can add automation later." };
    return { tone: "pending", label: "Choose one", message: "Choose whether Retreivr should set up a new stack, connect an existing one, or skip this." };
  }
  if (step.id === "arr-managed-features") {
    const enabled = [
      draft.managed_movies,
      draft.managed_tv,
      draft.managed_books,
      draft.managed_subtitles,
      draft.managed_downloader,
      draft.managed_vpn,
      draft.managed_jellyfin,
    ].filter(Boolean).length;
    if (!enabled) return { tone: "warning", label: "Choose features", message: "Pick at least one managed feature for Retreivr to set up." };
    return { tone: "success", label: `${enabled} selected`, message: "Your managed setup choices are ready." };
  }
  if (step.id === "arr-managed-control") {
    if (draft.direct_manage === true) return { tone: "success", label: "Direct manage", message: "Retreivr will try to continue setup automatically after restart." };
    if (draft.direct_manage === false) return { tone: "success", label: "Manual apply", message: "Retreivr will prepare a single restart/apply step for you." };
    return { tone: "pending", label: "Choose one", message: "Choose whether Retreivr should manage the bundled services directly on this device." };
  }
  if (step.id === "arr-existing-services") {
    const enabled = [
      draft.existing_radarr_enabled,
      draft.existing_sonarr_enabled,
      draft.existing_readarr_enabled,
      draft.existing_prowlarr_enabled,
      draft.existing_bazarr_enabled,
      draft.existing_qbittorrent_enabled,
      draft.existing_jellyfin_enabled,
    ].filter(Boolean).length;
    if (!enabled) return { tone: "warning", label: "Choose services", message: "Pick at least one existing service to connect." };
    return { tone: "success", label: `${enabled} selected`, message: "Retreivr will probe the services you selected and connect what it can." };
  }
  if (step.id === "tmdb") {
    if (draft.wants_tmdb === false) return { tone: "skipped", label: "Skipped", message: "Movies & TV discovery will stay limited until you add a TMDb key later." };
    if (draft.wants_tmdb === true && nonEmpty(draft.tmdb_api_key)) return { tone: "success", label: "Ready", message: "TMDb is ready to unlock title, cast, genre, and trailer browsing." };
    if (draft.wants_tmdb === true) return { tone: "warning", label: "Key needed", message: "Paste your TMDb key to finish this step." };
    return { tone: "pending", label: "Choose one", message: "Decide whether you want movie and TV discovery inside Retreivr." };
  }
  if (step.id === "vpn") {
    if (draft.enable_vpn === false) return { tone: "skipped", label: "Skipped", message: "No VPN guidance will be added right now." };
    if (draft.enable_vpn === true && nonEmpty(draft.vpn_provider)) return { tone: "success", label: "Ready", message: "VPN guidance is in place. You can still adjust routed apps later." };
    if (draft.enable_vpn === true) return { tone: "warning", label: "More info needed", message: "Choose a VPN provider name before moving on." };
    return { tone: "pending", label: "Choose one", message: "Decide whether you want Retreivr to guide your VPN setup." };
  }
  if (step.id === "youtube") {
    if (draft.wants_youtube === false) return { tone: "skipped", label: "Skipped", message: "You can add better YouTube access later if normal browsing works well enough now." };
    if (draft.wants_youtube === true && nonEmpty(draft.yt_dlp_cookies)) return { tone: "success", label: "Ready", message: "Retreivr has a cookies file location for tougher YouTube downloads." };
    if (draft.wants_youtube === true) return { tone: "warning", label: "File needed", message: "Choose a cookies.txt file location to finish this step." };
    return { tone: "pending", label: "Choose one", message: "Decide whether you want the extra YouTube access setup now." };
  }
  if (step.id === "telegram") {
    if (draft.wants_telegram === false) return { tone: "skipped", label: "Skipped", message: "Telegram notifications will stay off for now." };
    if (draft.wants_telegram === true && nonEmpty(draft.telegram_bot_token) && nonEmpty(draft.telegram_chat_id)) return { tone: "success", label: "Ready", message: "Telegram notifications are configured and ready to test later." };
    if (draft.wants_telegram === true) return { tone: "warning", label: "More info needed", message: "Paste both your bot token and chat ID to finish this step." };
    return { tone: "pending", label: "Choose one", message: "Decide whether you want notifications from Retreivr." };
  }
  if (step.id === "jellyfin") {
    if (draft.arr_setup_mode === "managed") {
      if (draft.managed_jellyfin === false) return { tone: "skipped", label: "Skipped", message: "Jellyfin will stay out of the managed setup for now." };
      if (draft.managed_jellyfin === true) return { tone: "success", label: "Included", message: "Jellyfin will be included in the managed stack if available." };
      return { tone: "pending", label: "Choose one", message: "Decide whether you want Jellyfin included too." };
    }
    if (draft.enable_jellyfin === false) return { tone: "skipped", label: "Skipped", message: "Jellyfin will stay out of your setup for now." };
    if (draft.enable_jellyfin === true && nonEmpty(draft.jellyfin_base_url)) return { tone: "success", label: "Ready", message: "Retreivr has enough Jellyfin info to guide the rest later." };
    if (draft.enable_jellyfin === true) return { tone: "warning", label: "Address needed", message: "Add your Jellyfin app address to finish this step." };
    return { tone: "pending", label: "Choose one", message: "Decide whether you want to connect Jellyfin right now." };
  }
  if (step.id === "paths") {
    const required = [draft.media_root, draft.movies_root, draft.tv_root, draft.downloads_root, draft.books_root].every(nonEmpty);
    return required
      ? { tone: "success", label: "Ready", message: "Your file locations are set." }
      : { tone: "warning", label: "Paths needed", message: "Add all file locations before moving on." };
  }
  if (step.id === "review") {
    return { tone: "success", label: "Ready", message: "You can save your choices now and prepare the next restart step." };
  }
  return status;
}

function validateSetupWizardStep(step, draft = state.setupWizard?.draft || buildSetupWizardDraft()) {
  if (!step) return null;
  const nonEmpty = (value) => String(value || "").trim().length > 0;
  if (step.id === "arr-mode" && !["managed", "existing", "none"].includes(String(draft.arr_setup_mode || ""))) return "Choose one of the setup options before moving on.";
  if (step.id === "arr-managed-features") {
    const enabled = [
      draft.managed_movies,
      draft.managed_tv,
      draft.managed_books,
      draft.managed_subtitles,
      draft.managed_downloader,
      draft.managed_vpn,
      draft.managed_jellyfin,
    ].some(Boolean);
    if (!enabled) return "Choose at least one managed feature to continue.";
  }
  if (step.id === "arr-managed-control" && typeof draft.direct_manage !== "boolean") {
    return "Choose whether Retreivr should manage the bundled services directly on this device.";
  }
  if (step.id === "arr-existing-services") {
    const enabled = [
      draft.existing_radarr_enabled,
      draft.existing_sonarr_enabled,
      draft.existing_readarr_enabled,
      draft.existing_prowlarr_enabled,
      draft.existing_bazarr_enabled,
      draft.existing_qbittorrent_enabled,
      draft.existing_jellyfin_enabled,
    ].some(Boolean);
    if (!enabled) return "Choose at least one existing service to connect.";
    if (draft.existing_radarr_enabled && !nonEmpty(draft.existing_radarr_base_url)) return "Add your Radarr app address to continue.";
    if (draft.existing_sonarr_enabled && !nonEmpty(draft.existing_sonarr_base_url)) return "Add your Sonarr app address to continue.";
    if (draft.existing_readarr_enabled && !nonEmpty(draft.existing_readarr_base_url)) return "Add your Readarr app address to continue.";
    if (draft.existing_prowlarr_enabled && !nonEmpty(draft.existing_prowlarr_base_url)) return "Add your Prowlarr app address to continue.";
    if (draft.existing_bazarr_enabled && !nonEmpty(draft.existing_bazarr_base_url)) return "Add your Bazarr app address to continue.";
    if (draft.existing_qbittorrent_enabled && (!nonEmpty(draft.existing_qbittorrent_base_url) || !nonEmpty(draft.existing_qbittorrent_username) || !nonEmpty(draft.existing_qbittorrent_password))) {
      return "Add the qBittorrent address, username, and password to continue.";
    }
    if (draft.existing_jellyfin_enabled && !nonEmpty(draft.existing_jellyfin_base_url)) return "Add your Jellyfin app address to continue.";
  }
  if (step.id === "tmdb") {
    if (draft.wants_tmdb == null) return "Choose Yes or No before moving on.";
    if (draft.wants_tmdb === true && !nonEmpty(draft.tmdb_api_key)) return "Paste your TMDb key to continue.";
  }
  if (step.id === "vpn") {
    if (draft.enable_vpn == null) return "Choose Yes or No before moving on.";
    if (draft.enable_vpn === true && !nonEmpty(draft.vpn_provider)) return "Add a VPN provider name to continue.";
  }
  if (step.id === "youtube") {
    if (draft.wants_youtube == null) return "Choose Yes or No before moving on.";
    if (draft.wants_youtube === true && !nonEmpty(draft.yt_dlp_cookies)) return "Choose a cookies.txt file location to continue.";
  }
  if (step.id === "telegram") {
    if (draft.wants_telegram == null) return "Choose Yes or No before moving on.";
    if (draft.wants_telegram === true && (!nonEmpty(draft.telegram_bot_token) || !nonEmpty(draft.telegram_chat_id))) {
      return "Add both the Telegram bot token and the chat ID to continue.";
    }
  }
  if (step.id === "jellyfin") {
    if (draft.arr_setup_mode === "managed") {
      if (draft.managed_jellyfin == null) return "Choose Yes or No before moving on.";
      return null;
    }
    if (draft.enable_jellyfin == null) return "Choose Yes or No before moving on.";
    if (draft.enable_jellyfin === true && !nonEmpty(draft.jellyfin_base_url)) return "Add your Jellyfin app address to continue.";
  }
  if (step.id === "paths") {
    const required = [
      ["Media folder", draft.media_root],
      ["Movies folder", draft.movies_root],
      ["TV folder", draft.tv_root],
      ["Downloads folder", draft.downloads_root],
      ["Books folder", draft.books_root],
    ];
    const missing = required.find(([, value]) => !nonEmpty(value));
    if (missing) return `${missing[0]} is required before moving on.`;
  }
  return null;
}

function syncSetupWizardToLegacyFields() {
  const draft = state.setupWizard?.draft;
  if (!draft) return;
  draft.enable_arr_stack = draft.arr_setup_mode === "managed";
  draft.enable_radarr = !!draft.managed_movies;
  draft.enable_sonarr = !!draft.managed_tv;
  draft.enable_readarr = !!draft.managed_books;
  draft.enable_prowlarr = !!(draft.managed_movies || draft.managed_tv || draft.managed_books);
  draft.enable_bazarr = !!draft.managed_subtitles;
  draft.enable_qbittorrent = !!draft.managed_downloader;
  draft.enable_vpn = !!draft.managed_vpn;
  draft.enable_jellyfin = !!draft.managed_jellyfin;
  draft.enable_hostctl = !!draft.direct_manage;
  const mappings = [
    ["setup-enable-arr-stack", !!draft.enable_arr_stack, "checked"],
    ["setup-enable-radarr", !!draft.enable_radarr, "checked"],
    ["setup-enable-sonarr", !!draft.enable_sonarr, "checked"],
    ["setup-enable-readarr", !!draft.enable_readarr, "checked"],
    ["setup-enable-prowlarr", !!draft.enable_prowlarr, "checked"],
    ["setup-enable-bazarr", !!draft.enable_bazarr, "checked"],
    ["setup-enable-qbittorrent", !!draft.enable_qbittorrent, "checked"],
    ["setup-enable-vpn", !!draft.enable_vpn, "checked"],
    ["setup-enable-jellyfin", !!draft.enable_jellyfin, "checked"],
    ["setup-env-path", draft.env_path || ".env", "value"],
    ["setup-media-root", draft.media_root || "./media", "value"],
    ["setup-movies-root", draft.movies_root || "./media/movies", "value"],
    ["setup-tv-root", draft.tv_root || "./media/tv", "value"],
    ["setup-downloads-root", draft.downloads_root || "./downloads", "value"],
    ["setup-books-root", draft.books_root || "./media/books", "value"],
  ];
  mappings.forEach(([id, value, mode]) => {
    const el = $(`#${id}`);
    if (!el) return;
    if (mode === "checked") {
      el.checked = !!value;
    } else {
      el.value = String(value || "");
    }
  });
}

async function saveSetupWizardConfig() {
  const draft = state.setupWizard?.draft;
  if (!draft) return;
  if (!(await ensureAdminPinSession())) {
    throw new Error("Admin PIN unlock is required before saving guided setup.");
  }
  const base = state.config ? JSON.parse(JSON.stringify(state.config)) : {};
  base.arr = (base.arr && typeof base.arr === "object") ? { ...base.arr } : {};
  base.arr.vpn = (base.arr.vpn && typeof base.arr.vpn === "object") ? { ...base.arr.vpn } : {};
  base.arr.jellyfin = (base.arr.jellyfin && typeof base.arr.jellyfin === "object") ? { ...base.arr.jellyfin } : {};
  base.telegram = (base.telegram && typeof base.telegram === "object") ? { ...base.telegram } : {};
  base.arr.tmdb_api_key = String(draft.tmdb_api_key || "").trim();
  base.yt_dlp_cookies = String(draft.yt_dlp_cookies || "").trim();
  base.telegram.bot_token = String(draft.telegram_bot_token || "").trim();
  base.telegram.chat_id = String(draft.telegram_chat_id || "").trim();
  base.arr.vpn.enabled = !!(draft.arr_setup_mode === "managed" ? draft.managed_vpn : draft.enable_vpn);
  base.arr.vpn.provider = String(draft.vpn_provider || "gluetun").trim() || "gluetun";
  base.arr.vpn.control_url = String(draft.vpn_control_url || "").trim();
  base.arr.vpn.route_qbittorrent = !!draft.vpn_route_qbittorrent;
  base.arr.vpn.route_prowlarr = !!draft.vpn_route_prowlarr;
  base.arr.vpn.route_retreivr = !!draft.vpn_route_retreivr;
  base.arr.jellyfin.base_url = String(draft.jellyfin_base_url || "").trim();
  base.arr.jellyfin.api_key = String(draft.jellyfin_api_key || "").trim();
  await fetchJson("/api/config", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(base),
  });
  state.config = base;

  if (draft.arr_setup_mode === "managed") {
    await fetchJson("/api/setup/managed/plan", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        movies: !!draft.managed_movies,
        tv: !!draft.managed_tv,
        books: !!draft.managed_books,
        subtitles: !!draft.managed_subtitles,
        downloader: !!draft.managed_downloader,
        vpn: !!draft.managed_vpn,
        jellyfin: !!draft.managed_jellyfin,
        direct_manage: !!draft.direct_manage,
        env_path: String(draft.env_path || ".env"),
        media_root: String(draft.media_root || "./media"),
        movies_root: String(draft.movies_root || "./media/movies"),
        tv_root: String(draft.tv_root || "./media/tv"),
        downloads_root: String(draft.downloads_root || "./downloads"),
        books_root: String(draft.books_root || "./media/books"),
        vpn_provider: String(draft.vpn_provider || "gluetun"),
        vpn_openvpn_user: String(draft.vpn_openvpn_user || ""),
        vpn_openvpn_password: String(draft.vpn_openvpn_password || ""),
        vpn_wireguard_private_key: String(draft.vpn_wireguard_private_key || ""),
        vpn_server_countries: String(draft.vpn_server_countries || ""),
      }),
    });
  } else if (draft.arr_setup_mode === "existing") {
    await fetchJson("/api/setup/existing/discover", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        radarr: { enabled: !!draft.existing_radarr_enabled, base_url: draft.existing_radarr_base_url, api_key: "" },
        sonarr: { enabled: !!draft.existing_sonarr_enabled, base_url: draft.existing_sonarr_base_url, api_key: "" },
        readarr: { enabled: !!draft.existing_readarr_enabled, base_url: draft.existing_readarr_base_url, api_key: "" },
        prowlarr: { enabled: !!draft.existing_prowlarr_enabled, base_url: draft.existing_prowlarr_base_url, api_key: "" },
        bazarr: { enabled: !!draft.existing_bazarr_enabled, base_url: draft.existing_bazarr_base_url, api_key: "" },
        qbittorrent: { enabled: !!draft.existing_qbittorrent_enabled, base_url: draft.existing_qbittorrent_base_url, username: draft.existing_qbittorrent_username, password: draft.existing_qbittorrent_password },
        jellyfin: { enabled: !!draft.existing_jellyfin_enabled, base_url: draft.existing_jellyfin_base_url, api_key: draft.existing_jellyfin_api_key },
      }),
    });
  }
  await refreshSetupStatus();
}

function buildSetupWizardSummaryRows() {
  const draft = state.setupWizard?.draft || buildSetupWizardDraft();
  const modeLabel = draft.arr_setup_mode === "managed"
    ? "Managed stack"
    : draft.arr_setup_mode === "existing"
      ? "Existing services"
      : "Retreivr only";
  const enabledServices = draft.arr_setup_mode === "managed"
    ? [
        draft.managed_movies ? "Movies" : null,
        draft.managed_tv ? "TV" : null,
        draft.managed_books ? "Books" : null,
        draft.managed_subtitles ? "Subtitles" : null,
        draft.managed_downloader ? "Downloader" : null,
        draft.managed_vpn ? "VPN" : null,
        draft.managed_jellyfin ? "Jellyfin" : null,
      ].filter(Boolean).join(", ") || "None selected"
    : draft.arr_setup_mode === "existing"
      ? [
          draft.existing_radarr_enabled ? "Radarr" : null,
          draft.existing_sonarr_enabled ? "Sonarr" : null,
          draft.existing_readarr_enabled ? "Readarr" : null,
          draft.existing_prowlarr_enabled ? "Prowlarr" : null,
          draft.existing_bazarr_enabled ? "Bazarr" : null,
          draft.existing_qbittorrent_enabled ? "qBittorrent" : null,
          draft.existing_jellyfin_enabled ? "Jellyfin" : null,
        ].filter(Boolean).join(", ") || "None selected"
      : "Retreivr only";
  return [
    ["Mode", modeLabel],
    ["Services", enabledServices],
    ["Apply", draft.arr_setup_mode === "managed" ? (draft.direct_manage ? "Retreivr-managed after restart" : "One restart step, then resume") : "Manual / existing"],
    ["TMDb", draft.tmdb_api_key ? "Configured" : "Not configured"],
    ["YouTube", draft.yt_dlp_cookies ? "Cookie path added" : "No cookie path"],
    ["Telegram", draft.telegram_bot_token && draft.telegram_chat_id ? "Configured" : "Not configured"],
    ["Media root", draft.media_root || "./media"],
    ["Movies root", draft.movies_root || "./media/movies"],
    ["TV root", draft.tv_root || "./media/tv"],
    ["Downloads root", draft.downloads_root || "./downloads"],
    ["Books root", draft.books_root || "./media/books"],
  ];
}

function renderSetupWizard() {
  const wizardEl = $("#setup-wizard");
  if (!wizardEl) return;
  ensureSetupWizardState();
  const steps = getSetupWizardSteps();
  const stepIndex = Math.max(0, Math.min(steps.length - 1, Number(state.setupWizard.stepIndex || 0)));
  state.setupWizard.stepIndex = stepIndex;
  const step = steps[stepIndex];
  const draft = state.setupWizard.draft;
  const modules = state.setupStatus?.modules || {};
  const progressPct = steps.length > 1 ? Math.round(((stepIndex + 1) / steps.length) * 100) : 100;
  const moduleEntries = Object.entries(modules).slice(0, 5);
  const stepState = getSetupWizardStepState(step, draft);
  const feedback = state.setupWizard?.feedback && state.setupWizard.feedback.stepId === step.id
    ? state.setupWizard.feedback
    : null;
  const messageText = feedback?.text || stepState.message;
  const messageTone = feedback?.tone || stepState.tone;

  let body = "";
  if (step.id === "arr-mode") {
    body = `
      <div class="setup-wizard-note">
        Retreivr can stay simple, set up a new managed automation stack for you, or connect to services you already run. On the managed path, Retreivr handles the internal app-to-app connections for you.
      </div>
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.arr_setup_mode === "managed" ? "active" : ""}" data-setup-choice="arr_setup_mode" data-value="managed">Set up a new managed stack</button>
        <button type="button" class="button setup-wizard-choice ${draft.arr_setup_mode === "existing" ? "active" : ""}" data-setup-choice="arr_setup_mode" data-value="existing">Connect my existing services</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.arr_setup_mode === "none" ? "active" : ""}" data-setup-choice="arr_setup_mode" data-value="none">Skip for now</button>
      </div>
      <div class="setup-wizard-step-list">
        <div>Managed stack: Retreivr prepares the bundled tools and their internal connections for you.</div>
        <div>Existing services: Retreivr asks only for the app addresses you already use.</div>
        <div>You can still browse movies and TV with only a TMDb key, even if you skip the extra tools.</div>
      </div>
    `;
  } else if (step.id === "arr-managed-features") {
    body = `
      <div class="setup-wizard-note">Choose what you want Retreivr to set up. You only need to choose features here, not internal app addresses or app-to-app API keys.</div>
      <div class="setup-wizard-chip-grid">
        <button type="button" class="button ${draft.managed_movies ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_movies">Movies</button>
        <button type="button" class="button ${draft.managed_tv ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_tv">TV</button>
        <button type="button" class="button ${draft.managed_books ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_books">Books</button>
        <button type="button" class="button ${draft.managed_subtitles ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_subtitles">Subtitles</button>
        <button type="button" class="button ${draft.managed_downloader ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_downloader">Downloader</button>
        <button type="button" class="button ${draft.managed_vpn ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_vpn">VPN</button>
        <button type="button" class="button ${draft.managed_jellyfin ? "setup-wizard-choice active" : "ghost"}" data-setup-toggle-choice="managed_jellyfin">Jellyfin</button>
      </div>
      <div class="setup-wizard-step-list">
        <div>Movies and TV turn on the matching automation apps.</div>
        <div>Downloader adds qBittorrent for torrent-based downloads.</div>
        <div>VPN is recommended when you use the downloader.</div>
        <div>Subtitles adds Bazarr on top of movie/show automation.</div>
      </div>
    `;
  } else if (step.id === "arr-managed-control") {
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.direct_manage === true ? "active" : ""}" data-setup-choice="direct_manage" data-value="true">Yes, let Retreivr manage this setup</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.direct_manage === false ? "active" : ""}" data-setup-choice="direct_manage" data-value="false">No, just prepare a single restart step</button>
      </div>
      <div class="setup-wizard-note">If you allow direct management, Retreivr uses its bundled helper to continue setup after restart. If not, Retreivr still prepares everything and shows one exact restart step.</div>
    `;
  } else if (step.id === "arr-existing-services") {
    body = `
      <div class="setup-wizard-note">Choose the services you already run and add only their app addresses here. Retreivr will probe them and link what it can automatically.</div>
      <div class="setup-wizard-fields two">
        <label class="field checkbox"><input data-setup-toggle="existing_radarr_enabled" type="checkbox" ${draft.existing_radarr_enabled ? "checked" : ""}><span>Radarr</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_sonarr_enabled" type="checkbox" ${draft.existing_sonarr_enabled ? "checked" : ""}><span>Sonarr</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_readarr_enabled" type="checkbox" ${draft.existing_readarr_enabled ? "checked" : ""}><span>Readarr</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_prowlarr_enabled" type="checkbox" ${draft.existing_prowlarr_enabled ? "checked" : ""}><span>Prowlarr</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_bazarr_enabled" type="checkbox" ${draft.existing_bazarr_enabled ? "checked" : ""}><span>Bazarr</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_qbittorrent_enabled" type="checkbox" ${draft.existing_qbittorrent_enabled ? "checked" : ""}><span>qBittorrent</span></label>
        <label class="field checkbox"><input data-setup-toggle="existing_jellyfin_enabled" type="checkbox" ${draft.existing_jellyfin_enabled ? "checked" : ""}><span>Jellyfin</span></label>
      </div>
      <div class="setup-wizard-fields two">
        ${draft.existing_radarr_enabled ? `<label class="field full"><span>Radarr app address</span><input data-setup-input="existing_radarr_base_url" type="text" value="${escapeAttr(draft.existing_radarr_base_url || "")}" placeholder="For example: http://radarr:7878"></label>` : ""}
        ${draft.existing_sonarr_enabled ? `<label class="field full"><span>Sonarr app address</span><input data-setup-input="existing_sonarr_base_url" type="text" value="${escapeAttr(draft.existing_sonarr_base_url || "")}" placeholder="For example: http://sonarr:8989"></label>` : ""}
        ${draft.existing_readarr_enabled ? `<label class="field full"><span>Readarr app address</span><input data-setup-input="existing_readarr_base_url" type="text" value="${escapeAttr(draft.existing_readarr_base_url || "")}" placeholder="For example: http://readarr:8787"></label>` : ""}
        ${draft.existing_prowlarr_enabled ? `<label class="field full"><span>Prowlarr app address</span><input data-setup-input="existing_prowlarr_base_url" type="text" value="${escapeAttr(draft.existing_prowlarr_base_url || "")}" placeholder="For example: http://prowlarr:9696"></label>` : ""}
        ${draft.existing_bazarr_enabled ? `<label class="field full"><span>Bazarr app address</span><input data-setup-input="existing_bazarr_base_url" type="text" value="${escapeAttr(draft.existing_bazarr_base_url || "")}" placeholder="For example: http://bazarr:6767"></label>` : ""}
        ${draft.existing_qbittorrent_enabled ? `<label class="field full"><span>qBittorrent app address</span><input data-setup-input="existing_qbittorrent_base_url" type="text" value="${escapeAttr(draft.existing_qbittorrent_base_url || "")}" placeholder="For example: http://qbittorrent:8080"></label>` : ""}
        ${draft.existing_qbittorrent_enabled ? `<label class="field full"><span>qBittorrent username</span><input data-setup-input="existing_qbittorrent_username" type="text" value="${escapeAttr(draft.existing_qbittorrent_username || "")}" placeholder="Username"></label>` : ""}
        ${draft.existing_qbittorrent_enabled ? `<label class="field full"><span>qBittorrent password</span><input data-setup-input="existing_qbittorrent_password" type="password" value="${escapeAttr(draft.existing_qbittorrent_password || "")}" placeholder="Password"></label>` : ""}
        ${draft.existing_jellyfin_enabled ? `<label class="field full"><span>Jellyfin app address</span><input data-setup-input="existing_jellyfin_base_url" type="text" value="${escapeAttr(draft.existing_jellyfin_base_url || "")}" placeholder="For example: http://jellyfin:8096"></label>` : ""}
        ${draft.existing_jellyfin_enabled ? `<label class="field full"><span>Jellyfin API key</span><input data-setup-input="existing_jellyfin_api_key" type="password" value="${escapeAttr(draft.existing_jellyfin_api_key || "")}" placeholder="Only if you already use one"></label>` : ""}
      </div>
    `;
  } else if (step.id === "tmdb") {
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.wants_tmdb === true ? "active" : ""}" data-setup-choice="wants_tmdb" data-value="true">Yes, I want this</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.wants_tmdb === false ? "active" : ""}" data-setup-choice="wants_tmdb" data-value="false">No, skip for now</button>
      </div>
      ${draft.wants_tmdb === true ? `
        <div class="setup-wizard-helper-grid">
          <a class="setup-wizard-helper-card" href="https://www.themoviedb.org/signup" target="_blank" rel="noreferrer">
            <strong>Create a free TMDb account</strong>
            <span>Start here if you do not already have one.</span>
          </a>
          <a class="setup-wizard-helper-card" href="https://www.themoviedb.org/settings/api" target="_blank" rel="noreferrer">
            <strong>Open the TMDb API page</strong>
            <span>Create or copy your TMDb API key, then paste it below.</span>
          </a>
        </div>
        <div class="setup-wizard-fields">
          <label class="field full">
            <span>TMDb API key</span>
            <input data-setup-input="tmdb_api_key" type="password" value="${escapeAttr(draft.tmdb_api_key || "")}" placeholder="Paste your TMDb API key">
          </label>
        </div>
        <div class="setup-wizard-step-list">
          <div>1. Create a free TMDb account if you do not already have one.</div>
          <div>2. Open the API page and create a key.</div>
          <div>3. Paste the key here to unlock movies and TV discovery.</div>
        </div>
      ` : ""}
    `;
  } else if (step.id === "vpn") {
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.enable_vpn === true ? "active" : ""}" data-setup-choice="enable_vpn" data-value="true">Yes, I want this</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.enable_vpn === false ? "active" : ""}" data-setup-choice="enable_vpn" data-value="false">No, skip for now</button>
      </div>
      ${draft.enable_vpn === true ? `
        <div class="setup-wizard-note">Most people only need the VPN for the download tool itself. Retreivr can keep the rest of the system simple unless you choose otherwise.</div>
        <div class="setup-wizard-fields two">
          <label class="field full">
            <span>Provider</span>
            <input data-setup-input="vpn_provider" type="text" value="${escapeAttr(draft.vpn_provider || "gluetun")}" placeholder="For example: gluetun">
          </label>
          <label class="field full">
            <span>VPN status address</span>
            <input data-setup-input="vpn_control_url" type="text" value="${escapeAttr(draft.vpn_control_url || "")}" placeholder="Optional: where Retreivr can check your VPN status">
          </label>
        </div>
        ${draft.arr_setup_mode === "managed" ? `
          <div class="setup-wizard-fields two">
            <label class="field full">
              <span>OpenVPN username</span>
              <input data-setup-input="vpn_openvpn_user" type="text" value="${escapeAttr(draft.vpn_openvpn_user || "")}" placeholder="Only if your provider uses OpenVPN">
            </label>
            <label class="field full">
              <span>OpenVPN password</span>
              <input data-setup-input="vpn_openvpn_password" type="password" value="${escapeAttr(draft.vpn_openvpn_password || "")}" placeholder="Only if your provider uses OpenVPN">
            </label>
            <label class="field full">
              <span>WireGuard private key</span>
              <input data-setup-input="vpn_wireguard_private_key" type="password" value="${escapeAttr(draft.vpn_wireguard_private_key || "")}" placeholder="Only if your provider uses WireGuard">
            </label>
            <label class="field full">
              <span>Preferred countries</span>
              <input data-setup-input="vpn_server_countries" type="text" value="${escapeAttr(draft.vpn_server_countries || "")}" placeholder="Optional, comma-separated">
            </label>
          </div>
        ` : ""}
        <div class="setup-wizard-fields">
          <label class="field checkbox"><input data-setup-toggle="vpn_route_qbittorrent" type="checkbox" ${draft.vpn_route_qbittorrent ? "checked" : ""}><span>Route qBittorrent through VPN</span></label>
          <label class="field checkbox"><input data-setup-toggle="vpn_route_prowlarr" type="checkbox" ${draft.vpn_route_prowlarr ? "checked" : ""}><span>Route Prowlarr through VPN</span></label>
          <label class="field checkbox"><input data-setup-toggle="vpn_route_retreivr" type="checkbox" ${draft.vpn_route_retreivr ? "checked" : ""}><span>Route Retreivr through VPN</span></label>
        </div>
      ` : ""}
    `;
  } else if (step.id === "youtube") {
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.wants_youtube === true ? "active" : ""}" data-setup-choice="wants_youtube" data-value="true">Yes, I want this</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.wants_youtube === false ? "active" : ""}" data-setup-choice="wants_youtube" data-value="false">No, skip for now</button>
      </div>
      ${draft.wants_youtube === true ? `
        <div class="setup-wizard-helper-grid">
          <a class="setup-wizard-helper-card" href="https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp" target="_blank" rel="noreferrer">
            <strong>How to export a cookies.txt file</strong>
            <span>Open a simple guide for exporting browser cookies when you need them.</span>
          </a>
          <a class="setup-wizard-helper-card" href="https://console.cloud.google.com/apis/library/youtube.googleapis.com" target="_blank" rel="noreferrer">
            <strong>Optional: YouTube API guide</strong>
            <span>If you want API-based setup later, this is the official starting point.</span>
          </a>
        </div>
        <div class="setup-wizard-fields">
          <label class="field full">
            <span>cookies.txt file</span>
            <div class="row path-picker">
              <input data-setup-input="yt_dlp_cookies" type="text" value="${escapeAttr(draft.yt_dlp_cookies || "")}" placeholder="Choose your cookies.txt file">
              <button type="button" class="button ghost small" data-setup-browse="yt_dlp_cookies">Browse</button>
            </div>
          </label>
        </div>
        <div class="setup-wizard-note">If your normal searches/downloads already work, you can skip this for now. Add a cookies file when you need more reliable authenticated YouTube access.</div>
      ` : ""}
    `;
  } else if (step.id === "telegram") {
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${draft.wants_telegram === true ? "active" : ""}" data-setup-choice="wants_telegram" data-value="true">Yes, I want this</button>
        <button type="button" class="button ghost setup-wizard-choice ${draft.wants_telegram === false ? "active" : ""}" data-setup-choice="wants_telegram" data-value="false">No, skip for now</button>
      </div>
      ${draft.wants_telegram === true ? `
        <div class="setup-wizard-helper-grid">
          <a class="setup-wizard-helper-card" href="https://t.me/BotFather" target="_blank" rel="noreferrer">
            <strong>Open BotFather</strong>
            <span>Create a bot and copy its token from Telegram.</span>
          </a>
          <a class="setup-wizard-helper-card" href="https://t.me/userinfobot" target="_blank" rel="noreferrer">
            <strong>Find your chat ID</strong>
            <span>Open a Telegram helper bot to look up your chat ID.</span>
          </a>
        </div>
        <div class="setup-wizard-fields two">
          <label class="field full">
            <span>Bot token</span>
            <input data-setup-input="telegram_bot_token" type="password" value="${escapeAttr(draft.telegram_bot_token || "")}" placeholder="Telegram bot token">
          </label>
          <label class="field full">
            <span>Chat ID</span>
            <input data-setup-input="telegram_chat_id" type="text" value="${escapeAttr(draft.telegram_chat_id || "")}" placeholder="Telegram chat ID">
          </label>
        </div>
        <div class="setup-wizard-note">Skip this if you do not want notifications. You can always add it later without affecting core acquisition behavior.</div>
      ` : ""}
    `;
  } else if (step.id === "jellyfin") {
    const jellyfinEnabledChoice = (draft.arr_setup_mode === "managed" ? draft.managed_jellyfin : draft.enable_jellyfin) === true;
    const jellyfinSuggestions = Array.isArray(state.setupWizard?.jellyfinDiscovery) ? state.setupWizard.jellyfinDiscovery : [];
    const jellyfinDiscoveryError = String(state.setupWizard?.jellyfinDiscoveryError || "").trim();
    const jellyfinDiscoveryLoading = !!state.setupWizard?.jellyfinDiscoveryLoading;
    body = `
      <div class="setup-wizard-choice-row">
        <button type="button" class="button setup-wizard-choice ${jellyfinEnabledChoice ? "active" : ""}" data-setup-choice="${draft.arr_setup_mode === "managed" ? "managed_jellyfin" : "enable_jellyfin"}" data-value="true">Yes, I want this</button>
        <button type="button" class="button ghost setup-wizard-choice ${(draft.arr_setup_mode === "managed" ? draft.managed_jellyfin : draft.enable_jellyfin) === false ? "active" : ""}" data-setup-choice="${draft.arr_setup_mode === "managed" ? "managed_jellyfin" : "enable_jellyfin"}" data-value="false">No, skip for now</button>
      </div>
      ${jellyfinEnabledChoice ? `
        <div class="setup-wizard-fields two">
          <label class="field full">
            <span>${draft.arr_setup_mode === "managed" ? "Retreivr-managed Jellyfin" : "Find your Jellyfin app"}</span>
            ${draft.arr_setup_mode === "managed"
              ? `<div class="setup-wizard-note">Retreivr will include Jellyfin in the managed setup and use the built-in app address automatically. You do not need to enter any internal address for this path.</div>`
              : `
                <div class="setup-wizard-link-grid">
                  <button type="button" class="button ghost" data-setup-action="discover-jellyfin">${jellyfinDiscoveryLoading ? "Looking for Jellyfin..." : "Find Jellyfin on this network"}</button>
                </div>
                ${jellyfinSuggestions.length ? `
                  <div class="setup-wizard-note">We found these likely Jellyfin app addresses. Pick one to fill it in automatically.</div>
                  <div class="setup-wizard-chip-grid">
                    ${jellyfinSuggestions.map((item) => `
                      <button type="button" class="button ghost setup-wizard-suggestion" data-setup-fill="jellyfin_base_url" data-value="${escapeAttr(item.url || "")}">
                        ${escapeHtml(item.label || item.url || "")}
                      </button>
                    `).join("")}
                  </div>
                ` : ""}
                ${jellyfinDiscoveryError ? `<div class="setup-wizard-note warning">${escapeHtml(jellyfinDiscoveryError)}</div>` : `<div class="setup-wizard-note">Retreivr can try common Jellyfin addresses for you first. If nothing is found, enter it manually below.</div>`}
                <input data-setup-input="jellyfin_base_url" type="text" value="${escapeAttr(draft.jellyfin_base_url || "")}" placeholder="For example: http://jellyfin:8096">
              `}
          </label>
          ${draft.arr_setup_mode === "managed" ? "" : `
            <label class="field full">
              <span>Optional admin API key</span>
              <input data-setup-input="jellyfin_api_key" type="password" value="${escapeAttr(draft.jellyfin_api_key || "")}" placeholder="Only if you already use one">
            </label>
          `}
        </div>
      ` : ""}
    `;
  } else if (step.id === "paths") {
    body = `
      <div class="setup-wizard-fields two">
        <label class="field full"><span>App settings file</span><input data-setup-input="env_path" type="text" value="${escapeAttr(draft.env_path || ".env")}"></label>
        <label class="field full"><span>Main media folder</span><input data-setup-input="media_root" type="text" value="${escapeAttr(draft.media_root || "./media")}"></label>
        <label class="field full"><span>Movies folder</span><input data-setup-input="movies_root" type="text" value="${escapeAttr(draft.movies_root || "./media/movies")}"></label>
        <label class="field full"><span>TV folder</span><input data-setup-input="tv_root" type="text" value="${escapeAttr(draft.tv_root || "./media/tv")}"></label>
        <label class="field full"><span>Downloads folder</span><input data-setup-input="downloads_root" type="text" value="${escapeAttr(draft.downloads_root || "./downloads")}"></label>
        <label class="field full"><span>Books folder</span><input data-setup-input="books_root" type="text" value="${escapeAttr(draft.books_root || "./media/books")}"></label>
      </div>
    `;
  } else if (step.id === "review") {
    body = `
      <div class="setup-wizard-summary-grid">
        ${buildSetupWizardSummaryRows().map(([label, value]) => `
          <div class="setup-wizard-summary-row">
            <strong>${escapeHtml(label)}</strong>
            <span class="meta">${escapeHtml(String(value || ""))}</span>
          </div>
        `).join("")}
      </div>
      <div class="setup-wizard-note">Save your choices first. When you are ready, let Retreivr prepare the stack settings it manages and show you the exact restart step to run next.</div>
    `;
  }

  wizardEl.innerHTML = `
    <div class="setup-wizard-progress">
      <div class="setup-wizard-progress-copy">
        <div class="setup-wizard-kicker">Guided setup</div>
        <div class="meta">Step ${stepIndex + 1} of ${steps.length}</div>
      </div>
      <div class="setup-wizard-progress-bar"><span style="width:${progressPct}%"></span></div>
    </div>
    <div class="setup-wizard-stage">
      <div class="setup-wizard-card">
        <div class="setup-wizard-card-top">
          <div class="setup-wizard-kicker">${escapeHtml(step.required ? "Required step" : "Optional step")}</div>
          <span class="setup-wizard-step-state is-${escapeAttr(messageTone)}">${escapeHtml(stepState.label)}</span>
        </div>
        <h2 class="setup-wizard-title">${escapeHtml(step.title)}</h2>
        <div class="setup-wizard-lead">${escapeHtml(step.subtitle)}</div>
        ${body}
        <div id="setup-wizard-message" class="notice ${messageTone === "warning" || messageTone === "error" ? "error" : ""}" role="status">${escapeHtml(messageText)}</div>
        <div class="setup-wizard-actions">
          <div class="setup-wizard-actions-group">
            <button type="button" class="button ghost" data-setup-nav="back" ${stepIndex === 0 ? "disabled" : ""}>Back</button>
            <button type="button" class="button ${stepIndex === steps.length - 1 ? "ghost" : "primary"}" data-setup-nav="next">${stepIndex === steps.length - 1 ? "Review complete" : "Next"}</button>
          </div>
          <div class="setup-wizard-actions-group">
            <button type="button" class="button ghost" data-setup-action="start-over">Start Over</button>
            <button type="button" class="button ghost" data-setup-action="save-progress">Save Progress</button>
            <button type="button" class="button primary" data-setup-action="apply-env">Prepare Setup</button>
          </div>
        </div>
      </div>
      <div class="setup-wizard-sidecard">
        <div class="group-title">Current Setup Status</div>
        <div class="setup-wizard-status-list">
          ${moduleEntries.map(([key, module]) => `
            <div class="setup-wizard-status-item">
              <div class="setup-wizard-status-top">
                <strong>${escapeHtml(module.title || key)}</strong>
                <span class="setup-status-chip is-${escapeAttr(String(module.status || "pending").replace(/[^a-z_]/gi, "_").toLowerCase())}">${escapeHtml(module.status || "pending")}</span>
              </div>
              <div class="meta">${escapeHtml(module.summary || "")}</div>
            </div>
          `).join("")}
        </div>
        <div class="setup-wizard-note">
          Retreivr-only stays the default until you explicitly save your setup choices and prepare the restart step.
        </div>
      </div>
    </div>
  `;
}

async function refreshSetupStatus() {
  const modulesEl = $("#setup-modules");
  const helperEl = $("#setup-command-helper");
  const summaryEl = $("#setup-command-summary");
  const servicesSummary = $("#services-profile-summary");
  const servicesList = $("#services-list");
  if (modulesEl) {
    modulesEl.innerHTML = `<div class="home-results-empty">Loading setup status…</div>`;
  }
  try {
    const payload = await fetchJson("/api/setup/status");
    state.setupStatus = payload;
    state.adminSecurity = payload?.security || state.adminSecurity;
    ensureSetupWizardState();
    const modules = payload?.modules || {};
    const stack = payload?.stack || {};
    if ($("#setup-enable-arr-stack")) $("#setup-enable-arr-stack").checked = !!stack.enable_arr_stack;
    if ($("#setup-enable-radarr")) $("#setup-enable-radarr").checked = !!stack.enable_radarr;
    if ($("#setup-enable-sonarr")) $("#setup-enable-sonarr").checked = !!stack.enable_sonarr;
    if ($("#setup-enable-readarr")) $("#setup-enable-readarr").checked = !!stack.enable_readarr;
    if ($("#setup-enable-prowlarr")) $("#setup-enable-prowlarr").checked = !!stack.enable_prowlarr;
    if ($("#setup-enable-bazarr")) $("#setup-enable-bazarr").checked = !!stack.enable_bazarr;
    if ($("#setup-enable-qbittorrent")) $("#setup-enable-qbittorrent").checked = !!stack.enable_qbittorrent;
    if ($("#setup-enable-vpn")) $("#setup-enable-vpn").checked = !!stack.enable_vpn;
    if ($("#setup-enable-jellyfin")) $("#setup-enable-jellyfin").checked = !!stack.enable_jellyfin;
    if ($("#setup-env-path")) $("#setup-env-path").value = stack.env_path || ".env";
    if ($("#setup-media-root")) $("#setup-media-root").value = stack.media_root || "./media";
    if ($("#setup-movies-root")) $("#setup-movies-root").value = stack.movies_root || "./media/movies";
    if ($("#setup-tv-root")) $("#setup-tv-root").value = stack.tv_root || "./media/tv";
    if ($("#setup-downloads-root")) $("#setup-downloads-root").value = stack.downloads_root || "./downloads";
    if ($("#setup-books-root")) $("#setup-books-root").value = stack.books_root || "./media/books";
    if (!state.setupWizard?.draft || !state.setupWizard?.draft.__hydrated) {
      state.setupWizard = {
        stepIndex: state.setupWizard?.stepIndex || 0,
        draft: {
          ...buildSetupWizardDraft(),
          __hydrated: true,
        },
      };
    }
    renderSetupWizard();
    if (modulesEl) {
      modulesEl.innerHTML = Object.entries(modules).map(([key, module]) => `
        <div class="group setup-module-card">
          <div class="group-title">${escapeHtml(module.title || key)}</div>
          <div class="setup-status-chip is-${escapeAttr(String(module.status || "pending").replace(/[^a-z_]/gi, "_").toLowerCase())}">${escapeHtml(module.status || "pending")}${module.required ? " • Required" : " • Optional"}</div>
          <div class="meta">${escapeHtml(module.summary || "")}</div>
          <div class="setup-module-summary">
            ${module.complete ? `<div class="meta">Complete</div>` : `<div class="meta">Awaiting input or enablement</div>`}
          </div>
        </div>
      `).join("");
    }
    const profiles = Array.isArray(stack.compose_profiles) ? stack.compose_profiles : [];
    const command = String(stack.compose_command || "").trim();
    if (helperEl) {
      const helperText = stack.restart_required
        ? `Restart required after apply. Run: ${command}`
        : (command ? `Managed stack is saved. Apply command: ${command}` : "Retreivr-only mode is active. Save stack choices, then apply when you are ready.");
      setNotice(helperEl, helperText, false);
    }
    if (summaryEl) {
      const pathRows = [
        ["Env file", stack.env_path || ".env"],
        ["Media root", stack.media_root || "./media"],
        ["Movies root", stack.movies_root || "./media/movies"],
        ["TV root", stack.tv_root || "./media/tv"],
        ["Downloads root", stack.downloads_root || "./downloads"],
        ["Books root", stack.books_root || "./media/books"],
      ];
      const vpnPolicy = stack.vpn_policy || {};
      const routeSummary = [
        vpnPolicy.route_qbittorrent ? "qBittorrent" : null,
        vpnPolicy.route_prowlarr ? "Prowlarr" : null,
        vpnPolicy.route_retreivr ? "Retreivr" : null,
      ].filter(Boolean).join(", ") || "No services selected";
      summaryEl.innerHTML = `
        <div class="group setup-command-card">
          <div class="group-title">Apply Summary</div>
          <div class="meta">${profiles.length ? `Profiles: ${escapeHtml(profiles.join(", "))}` : "Profiles: none (Retreivr-only mode)"}</div>
          <div class="meta">${stack.restart_required ? "Restart required after the last managed stack change." : "No pending restart requirement recorded."}</div>
          ${stack.last_applied_at ? `<div class="meta">Last applied: ${escapeHtml(stack.last_applied_at)}</div>` : `<div class="meta">Last applied: never</div>`}
          ${stack.last_applied_env_path ? `<div class="meta">Managed env target: ${escapeHtml(stack.last_applied_env_path)}</div>` : ""}
          <div class="meta">VPN policy: ${vpnPolicy.enabled ? `enabled via ${escapeHtml(vpnPolicy.provider || "gluetun")}` : "disabled"} • Routes: ${escapeHtml(routeSummary)}</div>
          <code class="setup-command-code">${escapeHtml(command || "docker compose up -d")}</code>
        </div>
        <div class="group setup-command-card">
          <div class="group-title">Managed Paths</div>
          <div class="setup-path-summary">
            ${pathRows.map(([label, value]) => `<div><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</div>`).join("")}
          </div>
        </div>
      `;
    }
    if (servicesSummary) {
      servicesSummary.textContent = profiles.length
        ? `Active optional profiles: ${profiles.join(", ")}`
        : "No optional services enabled. Retreivr-only mode is active.";
    }
    if (servicesList) {
      const serviceLabels = [
        ["enable_radarr", "Radarr"],
        ["enable_sonarr", "Sonarr"],
        ["enable_readarr", "Readarr"],
        ["enable_prowlarr", "Prowlarr"],
        ["enable_bazarr", "Bazarr"],
        ["enable_qbittorrent", "qBittorrent"],
        ["enable_vpn", "VPN / Gluetun"],
        ["enable_jellyfin", "Jellyfin"],
      ];
      servicesList.innerHTML = serviceLabels.map(([key, label]) => `
        <div class="group setup-module-card">
          <div class="group-title">${escapeHtml(label)}</div>
          <div class="meta">${stack[key] ? "Enabled in managed stack" : "Disabled by default"}</div>
        </div>
      `).join("");
    }
  } catch (err) {
    if (modulesEl) {
      modulesEl.innerHTML = `<div class="home-results-empty">Setup status failed: ${escapeHtml(toUserErrorMessage(err))}</div>`;
    }
    if (helperEl) {
      setNotice(helperEl, `Setup status failed: ${toUserErrorMessage(err)}`, true);
    }
  }
}

function collectSetupStackPayload() {
  return {
    enable_arr_stack: !!$("#setup-enable-arr-stack")?.checked,
    enable_radarr: !!$("#setup-enable-radarr")?.checked,
    enable_sonarr: !!$("#setup-enable-sonarr")?.checked,
    enable_readarr: !!$("#setup-enable-readarr")?.checked,
    enable_prowlarr: !!$("#setup-enable-prowlarr")?.checked,
    enable_bazarr: !!$("#setup-enable-bazarr")?.checked,
    enable_qbittorrent: !!$("#setup-enable-qbittorrent")?.checked,
    enable_vpn: !!$("#setup-enable-vpn")?.checked,
    enable_jellyfin: !!$("#setup-enable-jellyfin")?.checked,
    env_path: String($("#setup-env-path")?.value || ".env").trim() || ".env",
    media_root: String($("#setup-media-root")?.value || "./media").trim() || "./media",
    movies_root: String($("#setup-movies-root")?.value || "./media/movies").trim() || "./media/movies",
    tv_root: String($("#setup-tv-root")?.value || "./media/tv").trim() || "./media/tv",
    downloads_root: String($("#setup-downloads-root")?.value || "./downloads").trim() || "./downloads",
    books_root: String($("#setup-books-root")?.value || "./media/books").trim() || "./media/books",
  };
}

async function saveSetupStack() {
  const helperEl = $("#setup-command-helper");
  if (!(await ensureAdminPinSession())) return;
  setNotice(helperEl, "Saving stack choices…", false);
  const payload = await fetchJson("/api/setup/stack", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(collectSetupStackPayload()),
  });
  state.setupStatus = payload;
  await refreshSetupStatus();
  setNotice(helperEl, `Saved. Restart is not required until you click "Write Managed Env". ${payload?.stack?.compose_command || ""}`.trim(), false);
}

async function applySetupStack() {
  const helperEl = $("#setup-command-helper");
  const summaryEl = $("#setup-command-summary");
  if (!(await ensureAdminPinSession())) return;
  setNotice(helperEl, "Writing managed env block…", false);
  await saveSetupStack();
  const payload = await fetchJson("/api/setup/apply-stack", { method: "POST" });
  setNotice(helperEl, `Managed env written to ${payload.env_path}. Restart required: ${payload.compose_command}`, false);
  if (summaryEl) {
    const services = Array.isArray(payload.enabled_services) && payload.enabled_services.length
      ? payload.enabled_services.join(", ")
      : "Retreivr only";
    summaryEl.innerHTML = `
      <div class="group setup-command-card">
        <div class="group-title">Apply Ready</div>
        <div class="meta"><strong>Enabled services:</strong> ${escapeHtml(services)}</div>
        <div class="meta"><strong>Profiles:</strong> ${escapeHtml((payload.profiles || []).join(", ") || "none")}</div>
        <div class="meta"><strong>Managed env:</strong> ${escapeHtml(payload.env_path || ".env")}</div>
        <code class="setup-command-code">${escapeHtml(payload.compose_command || "docker compose up -d")}</code>
      </div>
    `;
  }
}

function updateSetupWizardDraftField(key, value) {
  ensureSetupWizardState();
  state.setupWizard.draft[key] = value;
  state.setupWizard.feedback = null;
  if (key === "managed_jellyfin" || key === "enable_jellyfin" || key === "existing_jellyfin_enabled") {
    state.setupWizard.jellyfinDiscoveryError = "";
    if (value !== true) {
      state.setupWizard.jellyfinDiscovery = [];
      state.setupWizard.jellyfinDiscoveryLoading = false;
    }
  }
  if (key === "arr_setup_mode") {
    if (value === "none") {
      state.setupWizard.draft.managed_movies = false;
      state.setupWizard.draft.managed_tv = false;
      state.setupWizard.draft.managed_books = false;
      state.setupWizard.draft.managed_subtitles = false;
      state.setupWizard.draft.managed_downloader = false;
      state.setupWizard.draft.managed_vpn = false;
      state.setupWizard.draft.managed_jellyfin = false;
      state.setupWizard.draft.direct_manage = false;
    }
    state.setupWizard.jellyfinDiscovery = [];
    state.setupWizard.jellyfinDiscoveryLoading = false;
    state.setupWizard.jellyfinDiscoveryError = "";
  }
  if (key === "managed_downloader" && !value) {
    state.setupWizard.draft.managed_vpn = false;
  }
}

function advanceSetupWizardStep(direction = 1) {
  ensureSetupWizardState();
  const steps = getSetupWizardSteps();
  const currentIndex = Math.max(0, Number(state.setupWizard.stepIndex || 0));
  const delta = Number.isFinite(Number(direction)) ? Number(direction) : 1;
  state.setupWizard.stepIndex = Math.max(0, Math.min(steps.length - 1, currentIndex + delta));
}

function resetSetupWizardDraft() {
  state.setupWizard = {
    stepIndex: 0,
    draft: buildSetupWizardDraft(),
    jellyfinDiscovery: [],
    jellyfinDiscoveryLoading: false,
    jellyfinDiscoveryError: "",
  };
  syncSetupWizardToLegacyFields();
}

async function discoverJellyfinForSetup() {
  ensureSetupWizardState();
  state.setupWizard.jellyfinDiscoveryLoading = true;
  state.setupWizard.jellyfinDiscoveryError = "";
  state.setupWizard.jellyfinDiscovery = [];
  renderSetupWizard();
  try {
    const payload = await fetchJson("/api/setup/jellyfin/discover");
    state.setupWizard.jellyfinDiscovery = Array.isArray(payload?.suggestions) ? payload.suggestions : [];
    if (!state.setupWizard.jellyfinDiscovery.length) {
      state.setupWizard.jellyfinDiscoveryError = "No Jellyfin app was found automatically. You can still enter the address yourself.";
    }
  } catch (err) {
    state.setupWizard.jellyfinDiscoveryError = toUserErrorMessage(err, "Jellyfin discovery failed");
  } finally {
    state.setupWizard.jellyfinDiscoveryLoading = false;
    renderSetupWizard();
  }
}

async function saveSetupWizardProgress() {
  syncSetupWizardToLegacyFields();
  await saveSetupWizardConfig();
  if (state.setupWizard?.draft?.arr_setup_mode === "none") {
    await saveSetupStack();
  }
  if (state.setupWizard?.draft) {
    state.setupWizard.draft.__hydrated = true;
  }
  if (state.setupWizard) {
    const step = getSetupWizardSteps()[Math.max(0, Number(state.setupWizard.stepIndex || 0))];
    state.setupWizard.feedback = {
      stepId: step?.id || "",
      tone: "success",
      text: "Saved. You can keep going or come back later.",
    };
  }
}

async function applySetupWizardEnv() {
  syncSetupWizardToLegacyFields();
  await saveSetupWizardConfig();
  const mode = state.setupWizard?.draft?.arr_setup_mode || "none";
  if (mode === "managed") {
    await fetchJson("/api/setup/managed/apply", { method: "POST" });
    await refreshSetupStatus();
  } else if (mode === "existing") {
    await fetchJson("/api/setup/existing/connect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        radarr: { enabled: !!state.setupWizard.draft.existing_radarr_enabled, base_url: state.setupWizard.draft.existing_radarr_base_url, api_key: "" },
        sonarr: { enabled: !!state.setupWizard.draft.existing_sonarr_enabled, base_url: state.setupWizard.draft.existing_sonarr_base_url, api_key: "" },
        readarr: { enabled: !!state.setupWizard.draft.existing_readarr_enabled, base_url: state.setupWizard.draft.existing_readarr_base_url, api_key: "" },
        prowlarr: { enabled: !!state.setupWizard.draft.existing_prowlarr_enabled, base_url: state.setupWizard.draft.existing_prowlarr_base_url, api_key: "" },
        bazarr: { enabled: !!state.setupWizard.draft.existing_bazarr_enabled, base_url: state.setupWizard.draft.existing_bazarr_base_url, api_key: "" },
        qbittorrent: { enabled: !!state.setupWizard.draft.existing_qbittorrent_enabled, base_url: state.setupWizard.draft.existing_qbittorrent_base_url, username: state.setupWizard.draft.existing_qbittorrent_username, password: state.setupWizard.draft.existing_qbittorrent_password },
        jellyfin: { enabled: !!state.setupWizard.draft.existing_jellyfin_enabled, base_url: state.setupWizard.draft.existing_jellyfin_base_url, api_key: state.setupWizard.draft.existing_jellyfin_api_key },
      }),
    });
    await refreshSetupStatus();
  } else {
    await applySetupStack();
  }
  if (state.setupWizard?.draft) {
    state.setupWizard.draft.__hydrated = true;
  }
}

async function saveAdminPinSettings() {
  const enabled = !!$("#cfg-security-admin-pin-enabled")?.checked;
  const currentPin = String($("#cfg-security-admin-pin-current")?.value || "").trim();
  const newPin = String($("#cfg-security-admin-pin-new")?.value || "").trim();
  const confirmPin = String($("#cfg-security-admin-pin-confirm")?.value || "").trim();
  const sessionMinutes = Number.parseInt(String($("#cfg-security-admin-pin-session-minutes")?.value || "30").trim(), 10) || 30;
  const statusEl = $("#security-admin-pin-status");
  const existingEnabled = !!state.adminSecurity?.admin_pin_enabled;
  if (enabled && !existingEnabled && !newPin) {
    if (statusEl) {
      statusEl.textContent = "Set a new PIN to enable admin protection.";
      statusEl.classList.add("warning");
    }
    return;
  }
  if (newPin && newPin.length < 4) {
    if (statusEl) {
      statusEl.textContent = "PIN should be at least 4 digits or characters.";
      statusEl.classList.add("warning");
    }
    return;
  }
  if (newPin && newPin !== confirmPin) {
    if (statusEl) {
      statusEl.textContent = "New PIN and confirmation do not match.";
      statusEl.classList.add("warning");
    }
    return;
  }
  if (!enabled && !existingEnabled) {
    if (statusEl) {
      statusEl.textContent = "Admin PIN is already disabled.";
      statusEl.classList.remove("warning");
    }
    return;
  }
  if (statusEl) {
    statusEl.textContent = "Saving admin PIN settings…";
    statusEl.classList.remove("warning");
  }
  try {
    const payload = await fetchJson("/api/admin/pin/configure", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        admin_pin_enabled: enabled,
        current_pin: currentPin,
        new_pin: newPin,
        admin_pin_session_minutes: sessionMinutes,
      }),
    });
    state.adminPinToken = String(payload?.token || state.adminPinToken || "");
    if (state.adminPinToken) {
      localStorage.setItem(ADMIN_PIN_TOKEN_KEY, state.adminPinToken);
    } else {
      localStorage.removeItem(ADMIN_PIN_TOKEN_KEY);
    }
    await refreshAdminSecurityStatus();
    if ($("#cfg-security-admin-pin-current")) $("#cfg-security-admin-pin-current").value = "";
    if ($("#cfg-security-admin-pin-new")) $("#cfg-security-admin-pin-new").value = "";
    if ($("#cfg-security-admin-pin-confirm")) $("#cfg-security-admin-pin-confirm").value = "";
    if (statusEl) {
      statusEl.textContent = enabled ? "Admin PIN settings saved." : "Admin PIN disabled.";
      statusEl.classList.remove("warning");
    }
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = `Could not save admin PIN settings: ${toUserErrorMessage(err)}`;
      statusEl.classList.add("warning");
    }
  }
}

async function refreshConnectionsStatus() {
  const grid = $("#connections-grid");
  const messageEl = $("#connections-message");
  if (grid) {
    grid.innerHTML = `<div class="home-results-empty">Checking service connections…</div>`;
  }
  try {
    const payload = await fetchJson("/api/services/health");
    state.servicesHealth = payload?.services || {};
    if (grid) {
      grid.innerHTML = Object.entries(state.servicesHealth).map(([name, entry]) => `
        <div class="group setup-module-card">
          <div class="group-title">${escapeHtml(name)}</div>
          <div class="setup-status-chip is-${entry.reachable ? "verified" : (entry.configured ? "needs_attention" : "optional")}">${entry.reachable ? "Reachable" : (entry.configured ? "Needs attention" : "Not configured")}</div>
          <div class="connection-detail-list">
            <div class="meta">${entry.configured ? "Configured in Retreivr" : "Not configured in Retreivr"} • ${escapeHtml(entry.status || "unknown")}</div>
            ${entry.base_url ? `<div class="meta">Base URL: ${escapeHtml(entry.base_url)}</div>` : ""}
            ${entry.target_path ? `<div class="meta">Target path: ${escapeHtml(entry.target_path)}</div>` : ""}
            ${entry.download_root ? `<div class="meta">Download root: ${escapeHtml(entry.download_root)}</div>` : ""}
            ${state.lastAutoConfigureResults?.[name]?.status ? `<div class="meta">Last auto-config: ${escapeHtml(state.lastAutoConfigureResults[name].status)}</div>` : ""}
            ${state.lastAutoConfigureResults?.[name]?.message ? `<div class="meta">${escapeHtml(state.lastAutoConfigureResults[name].message)}</div>` : ""}
            ${Array.isArray(state.lastAutoConfigureResults?.[name]?.actions) && state.lastAutoConfigureResults[name].actions.length ? `<div class="meta">Actions: ${escapeHtml(state.lastAutoConfigureResults[name].actions.join(", "))}</div>` : ""}
          </div>
          ${entry.external_ip ? `<div class="meta">External IP: ${escapeHtml(entry.external_ip)}</div>` : ""}
          ${entry.provider ? `<div class="meta">Provider: ${escapeHtml(entry.provider)}</div>` : ""}
          ${entry.expected_routes ? `<div class="meta">Routes: ${escapeHtml(Object.entries(entry.expected_routes).filter(([, enabled]) => !!enabled).map(([service]) => service).join(", ") || "none")}</div>` : ""}
          ${typeof entry.kill_switch_expected === "boolean" ? `<div class="meta">Kill switch expected: ${entry.kill_switch_expected ? "Yes" : "No"}</div>` : ""}
        </div>
      `).join("");
    }
    if (messageEl) {
      setNotice(messageEl, "Connections refreshed.", false);
    }
  } catch (err) {
    if (grid) {
      grid.innerHTML = `<div class="home-results-empty">Connection check failed: ${escapeHtml(toUserErrorMessage(err))}</div>`;
    }
    if (messageEl) {
      setNotice(messageEl, `Connection check failed: ${toUserErrorMessage(err)}`, true);
    }
  }
}

async function autoConfigureConnections() {
  const messageEl = $("#connections-message");
  if (!(await ensureAdminPinSession())) return;
  setNotice(messageEl, "Applying best-effort ARR and qBittorrent configuration…", false);
  try {
    const payload = await fetchJson("/api/services/autoconfigure", { method: "POST" });
    const services = payload?.services || {};
    state.lastAutoConfigureResults = services || {};
    const configured = Object.entries(services).filter(([, item]) => item?.status === "configured" || item?.status === "updated" || item?.status === "created" || item?.status === "connected").length;
    const attention = Object.entries(services).filter(([, item]) => item?.status === "needs_attention").length;
    const summary = attention
      ? `Configured ${configured} services. ${attention} service${attention === 1 ? "" : "s"} still need attention.`
      : `Configured ${configured} services successfully.`;
    setNotice(messageEl, summary, false);
    await refreshConnectionsStatus();
  } catch (err) {
    setNotice(messageEl, `Auto configure failed: ${toUserErrorMessage(err)}`, true);
  }
}

function setMusicPlayerView(view) {
  state.playerView = view;
  $$(".music-player-nav").forEach((button) => {
    button.classList.toggle("active", button.dataset.playerView === view);
  });
  $$(".music-player-view").forEach((section) => {
    section.classList.toggle("hidden", section.id !== `music-player-${view}`);
  });
}

function getMusicPlayerSelectedArtist() {
  const artists = Array.isArray(state.playerLibrarySummary?.artists) ? state.playerLibrarySummary.artists : [];
  const selectedKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  return artists.find((entry) => String(entry.artist_key || "").trim().toLowerCase() === selectedKey) || null;
}

function getMusicPlayerSelectedAlbum() {
  const albums = Array.isArray(state.playerLibrarySummary?.albums) ? state.playerLibrarySummary.albums : [];
  const selectedArtistKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  const selectedAlbumKey = String(state.playerSelectedAlbumKey || "").trim().toLowerCase();
  return albums.find((entry) =>
    String(entry.artist_key || "").trim().toLowerCase() === selectedArtistKey &&
    String(entry.album_key || "").trim().toLowerCase() === selectedAlbumKey
  ) || null;
}

function getMusicPlayerFilteredTracks() {
  const tracks = Array.isArray(state.playerLibrarySummary?.tracks) ? state.playerLibrarySummary.tracks : [];
  const selectedArtistKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  const selectedAlbumKey = String(state.playerSelectedAlbumKey || "").trim().toLowerCase();
  return tracks.filter((entry) => {
    if (selectedArtistKey && String(entry.artist_key || "").trim().toLowerCase() !== selectedArtistKey) {
      return false;
    }
    if (selectedAlbumKey && String(entry.album_key || "").trim().toLowerCase() !== selectedAlbumKey) {
      return false;
    }
    return true;
  });
}

function getMusicPlayerPlaylistSummary() {
  const playlists = Array.isArray(state.playerPlaylists) ? state.playerPlaylists : [];
  const selectedId = Number(state.playerSelectedPlaylistId || 0);
  const selected = playlists.find((entry) => Number(entry.id || 0) === selectedId) || null;
  const items = Array.isArray(state.playerSelectedPlaylistItems) ? state.playerSelectedPlaylistItems : [];
  return { playlists, selected, items };
}

function renderMusicPlayerPlaylistsPanel() {
  const { playlists, selected, items } = getMusicPlayerPlaylistSummary();
  const playlistCards = playlists.length ? playlists.map((playlist) => `
    <button
      class="music-player-playlist-card${Number(playlist.id || 0) === Number(selected?.id || 0) ? " is-selected" : ""}"
      type="button"
      data-action="player-open-playlist"
      data-playlist-id="${escapeAttr(playlist.id)}"
    >
      <span class="music-player-track-title">${escapeHtml(playlist.name || "Playlist")}</span>
      <span class="music-player-track-meta">${escapeHtml(`${playlist.item_count || 0} tracks`)}</span>
    </button>
  `).join("") : `<div class="home-results-empty">No playlists yet.</div>`;
  const selectedMarkup = selected ? `
    <div class="group music-player-playlist-detail">
      <div class="panel-header-row compact">
        <div>
          <div class="group-title">${escapeHtml(selected.name || "Playlist")}</div>
          <div class="meta">${escapeHtml(`${items.length} saved track${items.length === 1 ? "" : "s"}`)}</div>
        </div>
        <div class="row compact">
          <button class="button ghost small" type="button" data-action="player-play-playlist" data-playlist-id="${escapeAttr(selected.id)}">Play Playlist</button>
          <button class="button ghost small" type="button" data-action="player-delete-playlist" data-playlist-id="${escapeAttr(selected.id)}">Delete Playlist</button>
        </div>
      </div>
      <div class="music-player-playlist-items">
        ${items.length ? items.map((item) => `
          <div class="music-player-playlist-item">
            <button
              class="music-player-track"
              type="button"
              data-action="player-play"
              data-stream-url="${escapeAttr(item.stream_url || "")}"
              data-title="${escapeAttr(item.title || "")}"
              data-artist="${escapeAttr(item.artist || "")}"
              data-album="${escapeAttr(item.album || "")}"
              data-local-path="${escapeAttr(item.local_path || "")}"
              data-source-kind="${escapeAttr(item.source_kind || "local")}"
            >
              <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
              <span class="music-player-track-meta">${escapeHtml([item.artist, item.album].filter(Boolean).join(" • "))}</span>
            </button>
            <div class="music-player-inline-actions">
              <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">Play Next</button>
              <button class="button ghost small" type="button" data-action="player-remove-playlist-item" data-playlist-id="${escapeAttr(selected.id)}" data-item-id="${escapeAttr(item.id)}">Remove</button>
            </div>
          </div>
        `).join("") : `<div class="home-results-empty">Add tracks from the library to start this playlist.</div>`}
      </div>
    </div>
  ` : `<div class="group music-player-playlist-detail"><div class="home-results-empty">Select a playlist to see its tracks.</div></div>`;
  return `
    <div class="group">
      <div class="group-title">Playlists</div>
      <div class="music-player-playlist-create">
        <input id="music-player-playlist-name" type="text" placeholder="New playlist name">
        <button id="music-player-create-playlist" class="button primary" type="button">Create Playlist</button>
      </div>
    </div>
    <div class="music-player-playlist-list">${playlistCards}</div>
    ${selectedMarkup}
  `;
}

function renderMusicPlayerPlaylistsView() {
  const playlistsEl = $("#music-player-playlists");
  if (!playlistsEl) return;
  playlistsEl.innerHTML = renderMusicPlayerPlaylistsPanel();
}

function renderMusicPlayerHome() {
  const homeEl = $("#music-player-home");
  if (!homeEl) return;
  const summary = state.playerLibrarySummary || { artists: [], albums: [], tracks: [] };
  const albums = Array.isArray(summary.albums) ? summary.albums : [];
  const recentTracks = Array.isArray(state.playerHistory) ? state.playerHistory.filter((item) => !item?.is_missing_local).slice(0, 6) : [];
  const recentlyAdded = albums.slice(0, 8);
  const favoriteArtists = Array.isArray(state.musicPreferences?.favorite_artists) ? state.musicPreferences.favorite_artists : [];
  const favoriteArtistRows = favoriteArtists.slice(0, 8).map((artist) => {
    const artistKey = String((artist?.name || "")).trim().toLowerCase();
    const downloadedArtist = (Array.isArray(summary.artists) ? summary.artists : []).find((entry) => String(entry.artist_key || "").trim().toLowerCase() === artistKey) || null;
    return {
      name: artist?.name || "Favorite Artist",
      artist_key: artistKey,
      artwork_url: getMusicLibraryArtworkUrl(downloadedArtist || artist),
      downloaded: !!downloadedArtist,
      album_count: Number(downloadedArtist?.album_count || 0),
      track_count: Number(downloadedArtist?.track_count || 0),
    };
  });
  const newFromFavorites = recentlyAdded.filter((album) => isMusicArtistFavorited(String(album.artist || ""), String(album.artist_key || ""))).slice(0, 8);
  homeEl.innerHTML = `
    <div class="music-home-layout">
      <section class="group">
        <div class="panel-header-row compact">
          <div>
            <div class="group-title">Continue Listening</div>
            <div class="meta">Pick up where you left off from your local library.</div>
          </div>
        </div>
        <div class="music-player-track-list">
          ${recentTracks.length ? recentTracks.map((item) => `
            <article class="music-player-track-row music-player-track-row-rich">
              <div class="music-player-browser-card-art music-player-track-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Track")}" loading="lazy">
              </div>
              <button class="music-player-track" type="button" data-action="player-play" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">
                <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
                <span class="music-player-track-meta">${escapeHtml([item.artist, item.played_at].filter(Boolean).join(" • "))}</span>
              </button>
              <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">Play Next</button>
            </article>
          `).join("") : `<div class="home-results-empty">Start playing something from Library or Radio to build your listening history.</div>`}
        </div>
      </section>
      <section class="group">
        <div class="panel-header-row compact">
          <div>
            <div class="group-title">Recently Added</div>
            <div class="meta">Newest downloaded albums in your library.</div>
          </div>
          <button class="button ghost small" type="button" data-action="music-go-library">Open Library</button>
        </div>
        <div class="music-player-browser-grid">
          ${recentlyAdded.length ? recentlyAdded.map((album) => `
            <article class="music-player-browser-card music-player-browser-card-rich">
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(album))}" alt="${escapeAttr(album.album || "Album")}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(album.album || "Album")}</span>
                <span class="music-player-track-meta">${escapeHtml([album.artist, `${album.track_count || 0} tracks`].filter(Boolean).join(" • "))}</span>
                <div class="music-status-row">${buildMusicStatusBadges(album)}</div>
              </div>
              <div class="music-player-browser-card-actions">
                <button class="button ghost small" type="button" data-action="player-open-album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">Open Album</button>
              </div>
            </article>
          `).join("") : `<div class="home-results-empty">Downloaded albums will appear here.</div>`}
        </div>
      </section>
      <section class="group">
        <div class="panel-header-row compact">
          <div>
            <div class="group-title">New From Favorite Artists</div>
            <div class="meta">Downloaded albums from artists you have favorited.</div>
          </div>
        </div>
        <div class="music-player-browser-grid">
          ${newFromFavorites.length ? newFromFavorites.map((album) => `
            <article class="music-player-browser-card music-player-browser-card-rich">
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(album))}" alt="${escapeAttr(album.album || "Album")}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(album.album || "Album")}</span>
                <span class="music-player-track-meta">${escapeHtml([album.artist, `${album.track_count || 0} tracks`].filter(Boolean).join(" • "))}</span>
                <div class="music-status-row">${buildMusicStatusBadges(album)}</div>
              </div>
              <div class="music-player-browser-card-actions">
                <button class="button ghost small" type="button" data-action="player-open-album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">Open Album</button>
              </div>
            </article>
          `).join("") : `<div class="home-results-empty">Favorite more artists or download more albums to fill this section.</div>`}
        </div>
      </section>
      <section class="group">
        <div class="panel-header-row compact">
          <div>
            <div class="group-title">Favorites</div>
            <div class="meta">Favorites are your taste profile. They are not always downloaded.</div>
          </div>
          <button class="button ghost small" type="button" data-action="music-go-favorites">Open Favorites</button>
        </div>
        <div class="music-player-browser-grid">
          ${favoriteArtistRows.length ? favoriteArtistRows.map((artist) => `
            <article class="music-player-browser-card music-player-browser-card-rich">
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(artist.artwork_url || "assets/no_artwork.png")}" alt="${escapeAttr(artist.name || "Favorite Artist")}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(artist.name || "Favorite Artist")}</span>
                <span class="music-player-track-meta">${artist.downloaded ? escapeHtml(`${artist.album_count} albums • ${artist.track_count} tracks downloaded`) : "Favorited only • Not downloaded yet"}</span>
                <div class="music-status-row">
                  <span class="music-status-badge is-favorited">Favorited</span>
                  ${artist.downloaded ? `<span class="music-status-badge is-downloaded">Downloaded</span>` : `<span class="music-status-badge">Not Downloaded</span>`}
                </div>
              </div>
              <div class="music-player-browser-card-actions">
                ${artist.downloaded ? `<button class="button ghost small" type="button" data-action="player-open-artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">Open Library</button>` : `<button class="button ghost small" type="button" data-action="music-go-search">Find Music</button>`}
              </div>
            </article>
          `).join("") : `<div class="home-results-empty">Favorite artists from Search to build your taste profile here.</div>`}
        </div>
      </section>
    </div>
  `;
}

function renderMusicPlayerLibrary() {
  const libraryEl = $("#music-player-library");
  if (!libraryEl) return;
  const summary = state.playerLibrarySummary || {};
  const artists = Array.isArray(summary.artists) ? summary.artists : [];
  const albums = Array.isArray(summary.albums) ? summary.albums : [];
  const tracks = getMusicPlayerFilteredTracks();
  const selectedArtist = getMusicPlayerSelectedArtist();
  const selectedAlbum = getMusicPlayerSelectedAlbum();
  const selectedPlaylistId = Number(state.playerSelectedPlaylistId || 0);
  const libraryMode = String(state.playerLibraryMode || "artists");
  const rootsLabel = getMusicLibraryRootsLabel();
  let browserMarkup = `<div class="home-results-empty">No local library tracks found yet.</div>`;
  let detailHero = "";

  if (artists.length) {
    if (libraryMode === "artists") {
      browserMarkup = `
        <div class="music-player-browser-grid">
          ${artists.map((artist) => `
            <article class="music-player-browser-card music-player-browser-card-rich" data-artist-key="${escapeAttr(artist.artist_key || "")}">
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(artist))}" alt="${escapeAttr(artist.artist || "Artist")}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(artist.artist || "Unknown Artist")}</span>
                <span class="music-player-track-meta">${escapeHtml(`${artist.album_count || 0} albums • ${artist.track_count || 0} tracks`)}</span>
                <div class="music-status-row">${buildMusicStatusBadges(artist)}</div>
              </div>
              <div class="music-player-browser-card-actions">
                <button class="button ghost small" type="button" data-action="player-open-artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">Browse Albums</button>
                <button class="button ghost small" type="button" data-action="player-play-artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">Play Artist</button>
              </div>
            </article>
          `).join("")}
        </div>
      `;
    } else if (libraryMode === "albums") {
      const filteredAlbums = albums.filter((album) => {
        if (!selectedArtist) return true;
        return String(album.artist_key || "").trim().toLowerCase() === String(selectedArtist.artist_key || "").trim().toLowerCase();
      });
      browserMarkup = filteredAlbums.length ? `
        <div class="music-player-browser-grid">
          ${filteredAlbums.map((album) => `
            <article
              class="music-player-browser-card music-player-browser-card-rich${selectedAlbum && String(selectedAlbum.album_key || "") === String(album.album_key || "") && String(selectedAlbum.artist_key || "") === String(album.artist_key || "") ? " is-selected" : ""}"
              data-artist-key="${escapeAttr(album.artist_key || "")}"
              data-album-key="${escapeAttr(album.album_key || "")}"
            >
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(album))}" alt="${escapeAttr(album.album || "Album")}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(album.album || "Unknown Album")}</span>
                <span class="music-player-track-meta">${escapeHtml([album.artist, `${album.track_count || 0} tracks`].filter(Boolean).join(" • "))}</span>
                <div class="music-status-row">${buildMusicStatusBadges(album)}</div>
              </div>
              <div class="music-player-browser-card-actions">
                <button
                  class="button ghost small"
                  type="button"
                  data-action="player-open-album"
                  data-artist-key="${escapeAttr(album.artist_key || "")}"
                  data-album-key="${escapeAttr(album.album_key || "")}"
                >View Tracks</button>
                <button class="button ghost small" type="button" data-action="player-queue-album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">Queue Album</button>
              </div>
            </article>
          `).join("")}
        </div>
      ` : `<div class="home-results-empty">No albums available for this artist.</div>`;
    } else {
      browserMarkup = tracks.length ? `
        <div class="music-player-track-list">
          ${tracks.map((item) => `
            <article class="music-player-track-row music-player-track-row-rich">
              <div class="music-player-browser-card-art music-player-track-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Track")}" loading="lazy">
              </div>
              <button
                class="music-player-track"
                type="button"
                data-action="player-play"
                data-stream-url="${escapeAttr(item.stream_url || "")}"
                data-title="${escapeAttr(item.title || "")}"
                data-artist="${escapeAttr(item.artist || "")}"
                data-album="${escapeAttr(item.album || "")}"
                data-local-path="${escapeAttr(item.local_path || "")}"
                data-source-kind="${escapeAttr(item.kind || "local")}"
              >
                <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
                <span class="music-player-track-meta">${escapeHtml([item.artist, item.album].filter(Boolean).join(" • "))}</span>
              </button>
              <div class="music-player-inline-actions">
                <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.kind || "local")}">Play Next</button>
                <button class="button ghost small" type="button" data-action="player-queue-track" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.kind || "local")}">Queue</button>
                <button
                  class="button ghost small"
                  type="button"
                  data-action="player-add-to-playlist"
                  data-playlist-id="${escapeAttr(selectedPlaylistId || "")}"
                  data-stream-url="${escapeAttr(item.stream_url || "")}"
                  data-title="${escapeAttr(item.title || "")}"
                  data-artist="${escapeAttr(item.artist || "")}"
                  data-album="${escapeAttr(item.album || "")}"
                  data-local-path="${escapeAttr(item.local_path || "")}"
                  data-source-kind="${escapeAttr(item.kind || "local")}"
                  ${selectedPlaylistId ? "" : "disabled"}
                >Add to Playlist</button>
              </div>
            </article>
          `).join("")}
        </div>
      ` : `<div class="home-results-empty">No tracks available for this selection.</div>`;
    }
  }

  if (selectedArtist && !selectedAlbum) {
    detailHero = `
      <div class="group music-library-detail-hero">
        <div class="music-library-detail-art">
          <img src="${escapeAttr(getMusicLibraryArtworkUrl(selectedArtist))}" alt="${escapeAttr(selectedArtist.artist || "Artist")}" loading="lazy">
        </div>
        <div class="music-library-detail-copy">
          <div class="group-title">${escapeHtml(selectedArtist.artist || "Artist")}</div>
          <div class="meta">${escapeHtml(`${selectedArtist.album_count || 0} albums • ${selectedArtist.track_count || 0} tracks downloaded`)}</div>
          <div class="music-status-row">${buildMusicStatusBadges(selectedArtist)}</div>
          <div class="row compact">
            <button class="button primary small" type="button" data-action="player-play-artist" data-artist-key="${escapeAttr(selectedArtist.artist_key || "")}">Play Artist</button>
            <button class="button ghost small" type="button" data-action="player-shuffle-artist" data-artist-key="${escapeAttr(selectedArtist.artist_key || "")}">Shuffle Artist</button>
          </div>
        </div>
      </div>
    `;
  } else if (selectedAlbum) {
    detailHero = `
      <div class="group music-library-detail-hero">
        <div class="music-library-detail-art">
          <img src="${escapeAttr(getMusicLibraryArtworkUrl(selectedAlbum))}" alt="${escapeAttr(selectedAlbum.album || "Album")}" loading="lazy">
        </div>
        <div class="music-library-detail-copy">
          <div class="group-title">${escapeHtml(selectedAlbum.album || "Album")}</div>
          <div class="meta">${escapeHtml([selectedAlbum.artist, `${selectedAlbum.track_count || 0} tracks downloaded`].filter(Boolean).join(" • "))}</div>
          <div class="music-status-row">${buildMusicStatusBadges(selectedAlbum)}</div>
          <div class="row compact">
            <button class="button primary small" type="button" data-action="player-play-album" data-artist-key="${escapeAttr(selectedAlbum.artist_key || "")}" data-album-key="${escapeAttr(selectedAlbum.album_key || "")}">Play Album</button>
            <button class="button ghost small" type="button" data-action="player-queue-album" data-artist-key="${escapeAttr(selectedAlbum.artist_key || "")}" data-album-key="${escapeAttr(selectedAlbum.album_key || "")}">Queue Album</button>
          </div>
        </div>
      </div>
    `;
  }

  const breadcrumbBits = [];
  if (selectedArtist) {
    breadcrumbBits.push(`<button class="button ghost small" type="button" data-action="player-library-mode" data-library-mode="albums">${escapeHtml(selectedArtist.artist || "Artist")}</button>`);
  }
  if (selectedAlbum) {
    breadcrumbBits.push(`<button class="button ghost small" type="button" data-action="player-library-mode" data-library-mode="tracks">${escapeHtml(selectedAlbum.album || "Album")}</button>`);
  }
  libraryEl.innerHTML = `
    <div class="music-player-library-layout">
      <div class="music-player-library-browser">
        <div class="panel-header-row compact">
          <div>
            <div class="group-title">Downloaded Library</div>
            <div class="meta">${escapeHtml(`${artists.length} artists • ${albums.length} albums • ${Array.isArray(summary.tracks) ? summary.tracks.length : 0} tracks`)}</div>
            <div class="meta">Showing downloaded/local files only${rootsLabel ? ` • ${escapeHtml(rootsLabel)}` : ""}</div>
          </div>
          <div class="music-player-library-modes" role="tablist" aria-label="Music player library sections">
            <button class="button ghost small${libraryMode === "artists" ? " active" : ""}" type="button" data-action="player-library-mode" data-library-mode="artists">Artists</button>
            <button class="button ghost small${libraryMode === "albums" ? " active" : ""}" type="button" data-action="player-library-mode" data-library-mode="albums">Albums</button>
            <button class="button ghost small${libraryMode === "tracks" ? " active" : ""}" type="button" data-action="player-library-mode" data-library-mode="tracks">Tracks</button>
          </div>
        </div>
        <div class="music-player-library-breadcrumbs">
          <button class="button ghost small" type="button" data-action="player-reset-library">All Library</button>
          ${breadcrumbBits.join("")}
        </div>
        ${detailHero}
        ${browserMarkup}
      </div>
      <aside class="music-player-library-sidepanel">
        ${renderMusicPlayerPlaylistsPanel()}
      </aside>
    </div>
  `;
}

function renderMusicPlayerStations() {
  const stationsEl = $("#music-player-stations");
  if (!stationsEl) return;
  const stations = Array.isArray(state.playerStations) ? state.playerStations : [];
  const stationModeLabel = (mode, seedType) => {
    const normalizedMode = String(mode || "mix").trim().toLowerCase();
    if (String(seedType || "").trim().toLowerCase() === "favorites") return "Favorites Mix";
    if (normalizedMode === "essentials") return "Artist Essentials";
    if (normalizedMode === "top_hits") return "Top Hits";
    if (normalizedMode === "latest") return "Latest";
    if (normalizedMode === "deep_cuts") return "Deep Cuts";
    return String(seedType || "").trim().toLowerCase() === "genre" ? "Genre Radio" : "Artist Radio";
  };
  const runtimeTone = (runtime) => {
    const ready = Number(runtime?.ready_count || 0);
    const unresolved = Number(runtime?.unresolved_count || 0);
    if (ready >= 3) return { label: "Ready", className: "is-downloaded" };
    if (ready >= 1 || unresolved > 0) return { label: "Priming", className: "is-queued" };
    return { label: "Needs Matches", className: "is-warning" };
  };
  if (!stations.length) {
    stationsEl.innerHTML = `<div class="home-results-empty">No stations yet — create one above to start your first radio mix.</div>`;
    return;
  }
  stationsEl.innerHTML = `<div class="music-meta-grid music-station-grid">${stations.map((station) => {
    const runtime = station.runtime || {};
    const previewItems = (Array.isArray(station.preview?.items) ? station.preview.items : []);
    const firstArt = previewItems.length ? getMusicLibraryArtworkUrl(previewItems[0]) : "assets/no_artwork.png";
    const tone = runtimeTone(runtime);
    const active = Number(state.playerActiveStationId || 0) === Number(station.id || 0);
    const readyCount = Number(runtime.ready_count || station.preview?.ready_count || 0);
    const localCount = Number(runtime.local_count || station.preview?.local_count || 0);
    const cachedCount = Number(runtime.cached_count || station.preview?.cached_count || 0);
    const needsMatches = tone.label === "Needs Matches";
    const modeLabel = stationModeLabel(station.station_mode, station.seed_type);
    return `
      <article class="home-result-card music-meta-card music-grid-card music-station-grid-card${active ? " is-active" : ""}">
        <div class="music-card-thumb-shell">
          <img src="${escapeAttr(firstArt)}" alt="${escapeAttr(station.name || "Station")}" loading="lazy" style="width:100%;height:100%;object-fit:cover;border-radius:14px;">
          ${active ? `<div class="music-station-active-overlay"><span class="music-station-playing-dot" aria-hidden="true"></span></div>` : ""}
        </div>
        <div class="music-meta-main">
          <div class="home-candidate-title">${escapeHtml(station.name || station.seed_value || "Station")}</div>
          <div class="home-candidate-meta">${escapeHtml(modeLabel)}${station.seed_value ? ` · ${escapeHtml(station.seed_value)}` : ""}</div>
          <div class="music-status-row">
            <span class="music-status-badge ${tone.className}">${escapeHtml(tone.label)}</span>
            ${readyCount > 0 ? `<span class="music-status-badge is-downloaded">${escapeHtml(`${readyCount} ready`)}</span>` : ""}
            ${localCount + cachedCount > 0 ? `<span class="music-status-badge">${escapeHtml(`${localCount}L · ${cachedCount}C`)}</span>` : ""}
          </div>
        </div>
        <div class="home-candidate-action home-candidate-action-primary-stack">
          ${active
            ? `<button class="button ghost small" type="button" disabled>Now Playing</button>`
            : `<button class="button ${needsMatches ? "ghost" : "primary"} small home-candidate-download-primary" type="button" data-action="player-load-station" data-station-id="${escapeAttr(station.id)}">${needsMatches ? "Play Anyway" : "Play Station"}</button>`
          }
          <button class="button ghost small" type="button" data-action="player-prime-station" data-station-id="${escapeAttr(station.id)}">Prime</button>
          <button class="button ghost small" type="button" data-action="player-delete-station" data-station-id="${escapeAttr(station.id)}">Delete</button>
        </div>
      </article>
    `;
  }).join("")}</div>`;
}

function renderMusicPlayerCommunityCache() {
  const container = $("#music-player-community-cache");
  if (!container) return;
  const items = Array.isArray(state.playerCommunityCache) ? state.playerCommunityCache : [];
  container.innerHTML = items.length ? `
    <div class="music-player-browser-grid">
      ${items.map((item) => `
        <article class="music-player-browser-card music-player-browser-card-rich">
          <div class="music-player-browser-card-art">
            <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Cached Match")}" loading="lazy">
          </div>
          <div class="music-player-browser-card-copy">
            <span class="music-player-track-title">${escapeHtml(item.title || "Cached Match")}</span>
            <span class="music-player-track-meta">${escapeHtml([item.artist, item.album, item.source].filter(Boolean).join(" • "))}</span>
            <div class="music-status-row">
              <span class="music-status-badge is-queued">Community Cache</span>
              ${item.verification_status ? `<span class="music-status-badge">${escapeHtml(item.verification_status)}</span>` : ""}
            </div>
          </div>
          <div class="music-player-browser-card-actions">
            <button class="button ghost small" type="button" data-action="player-play" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-source-kind="${escapeAttr(item.kind || "cached")}">Play</button>
            <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-source-kind="${escapeAttr(item.kind || "cached")}">Play Next</button>
          </div>
        </article>
      `).join("")}
    </div>
  ` : `<div class="home-results-empty">Verified community-cache matches will appear here when available.</div>`;
}

function renderMusicPlayerQueue() {
  const queueEl = $("#music-player-queue");
  if (!queueEl) return;
  const queue = Array.isArray(state.playerQueue) ? state.playerQueue : [];
  queueEl.innerHTML = `
    <div class="group">
      <div class="panel-header-row compact">
        <div>
          <div class="group-title">Queue</div>
          <div class="meta">${escapeHtml(`${queue.length} upcoming track${queue.length === 1 ? "" : "s"}`)}</div>
        </div>
        <div class="row compact">
          <button class="button ghost small" type="button" data-action="player-save-queue-playlist" ${queue.length ? "" : "disabled"}>Save Queue as Playlist</button>
          <button class="button ghost small" type="button" data-action="player-clear-queue" ${queue.length ? "" : "disabled"}>Clear Queue</button>
        </div>
      </div>
    </div>
    ${queue.length ? `
      <div class="music-player-track-list">
        ${queue.map((item, index) => `
          <article class="music-player-track-row music-player-track-row-rich${String(state.playerCurrent?.stream_url || "") === String(item.stream_url || "") ? " is-current" : ""}">
            <div class="music-player-browser-card-art music-player-track-art">
              <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Track")}" loading="lazy">
            </div>
            <button class="music-player-track" type="button" data-action="player-play" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.kind || "cached")}">
              <span class="music-player-track-title">${index + 1}. ${escapeHtml(item.title || "Untitled")}</span>
              <span class="music-player-track-meta">${escapeHtml([item.artist, item.album, item.kind].filter(Boolean).join(" • "))}</span>
            </button>
            <div class="music-player-inline-actions">
              <button class="button ghost small" type="button" data-action="player-queue-move-up" data-queue-index="${escapeAttr(index)}" ${index === 0 ? "disabled" : ""}>Up</button>
              <button class="button ghost small" type="button" data-action="player-queue-move-down" data-queue-index="${escapeAttr(index)}" ${index === queue.length - 1 ? "disabled" : ""}>Down</button>
              <button class="button ghost small" type="button" data-action="player-remove-queue-item" data-queue-index="${escapeAttr(index)}">Remove</button>
            </div>
          </article>
        `).join("")}
      </div>
    ` : `<div class="home-results-empty">Queue is empty. Start a station or pick a library track.</div>`}
  `;
}

function syncMusicPlayerVideoShell() {
  const shell = $("#music-player-video-shell");
  const toggle = $("#music-player-video-toggle");
  if (!shell) return;
  // YT items: shell is always "available"; visibility toggled by user or section.
  const hasVideo = activePlayerIsYT() || !!String(state.playerCurrent?.video_embed_url || "").trim();
  const shouldShow = hasVideo && !!state.playerVideoVisible && state.musicSection === "player";
  if (toggle) {
    toggle.classList.toggle("hidden", !hasVideo);
    toggle.textContent = state.playerVideoVisible ? "Hide Video" : "Show Video";
  }
  shell.classList.toggle("hidden", !shouldShow);
  // For non-YT embed-url items (legacy path) keep the frame src in sync.
  if (!activePlayerIsYT()) {
    const frame = $("#music-player-video-frame");
    if (!frame) return;
    const embedUrl = String(state.playerCurrent?.video_embed_url || "").trim();
    if (shouldShow && embedUrl && frame.src !== embedUrl) frame.src = embedUrl;
    else if (!shouldShow && frame.src && !activePlayerIsYT()) frame.src = "";
  }
}

// ── YouTube IFrame player abstraction ────────────────────────────────────────
// Load the IFrame API once; sets state.playerYTReady when the API is callable.
function ensureYouTubeAPILoaded() {
  if (window.YT && window.YT.Player) {
    state.playerYTReady = true;
    return;
  }
  if (document.getElementById("yt-iframe-api-script")) return;
  window.onYouTubeIframeAPIReady = () => {
    state.playerYTReady = true;
  };
  const script = document.createElement("script");
  script.id = "yt-iframe-api-script";
  script.src = "https://www.youtube.com/iframe_api";
  document.head.appendChild(script);
}

function _stopYTProgressTimer() {
  if (state.playerYTProgressTimer) {
    clearInterval(state.playerYTProgressTimer);
    state.playerYTProgressTimer = null;
  }
}

function _startYTProgressTimer() {
  _stopYTProgressTimer();
  state.playerYTProgressTimer = setInterval(() => {
    updateMusicPlayerTransportUI();
    syncBottomPlayerShell();
  }, 500);
}

// Destroy any existing YT.Player and reset the iframe to a clean state.
function destroyYTPlayer() {
  _stopYTProgressTimer();
  if (state.playerYT) {
    try { state.playerYT.destroy(); } catch (_e) {}
    state.playerYT = null;
  }
  const frame = $("#music-player-video-frame");
  if (frame) frame.src = "";
}

// Create a YT.Player for the given videoId. Resolves when the player is ready.
function createYTPlayer(videoId) {
  return new Promise((resolve, reject) => {
    if (!state.playerYTReady || !window.YT || !window.YT.Player) {
      // API not loaded yet — wait up to 8 seconds
      let waited = 0;
      const poll = setInterval(() => {
        waited += 200;
        if (window.YT && window.YT.Player && state.playerYTReady) {
          clearInterval(poll);
          _doCreate();
        } else if (waited >= 8000) {
          clearInterval(poll);
          reject(new Error("YouTube IFrame API failed to load"));
        }
      }, 200);
    } else {
      _doCreate();
    }
    function _doCreate() {
      destroyYTPlayer();
      const frame = $("#music-player-video-frame");
      if (!frame) { reject(new Error("video frame element missing")); return; }
      // Give the iframe a stable id the YT API needs
      frame.id = "music-player-video-frame";
      state.playerYT = new window.YT.Player("music-player-video-frame", {
        videoId,
        playerVars: { autoplay: 1, rel: 0, modestbranding: 1, enablejsapi: 1 },
        events: {
          onReady: (event) => {
            event.target.playVideo();
            _startYTProgressTimer();
            resolve(event.target);
          },
          onStateChange: (event) => {
            const S = window.YT.PlayerState;
            if (event.data === S.ENDED) {
              playNextPlayerItem({ autoAdvance: true }).catch(() => {});
            }
            updateMusicPlayerTransportUI();
            syncBottomPlayerShell();
          },
          onError: (event) => {
            console.warn("[YT] playback error", event.data);
            // skip to next on unplayable video
            playNextPlayerItem({ autoAdvance: true }).catch(() => {});
          },
        },
      });
    }
  });
}

// Active-player helpers — route to YT.Player or <audio> transparently.
function activePlayerIsYT() { return !!state.playerYT; }

function activePlayerPlay() {
  if (activePlayerIsYT()) { try { state.playerYT.playVideo(); } catch (_e) {} return; }
  const a = $("#music-player-audio"); if (a) a.play().catch(() => {});
}
function activePlayerPause() {
  if (activePlayerIsYT()) { try { state.playerYT.pauseVideo(); } catch (_e) {} return; }
  const a = $("#music-player-audio"); if (a) a.pause();
}
function activePlayerIsPaused() {
  if (activePlayerIsYT()) {
    try { return state.playerYT.getPlayerState() !== window.YT?.PlayerState?.PLAYING; } catch (_e) {}
    return true;
  }
  const a = $("#music-player-audio"); return !a || a.paused;
}
function activePlayerCurrentTime() {
  if (activePlayerIsYT()) { try { return state.playerYT.getCurrentTime() || 0; } catch (_e) {} return 0; }
  const a = $("#music-player-audio"); return Number(a?.currentTime) || 0;
}
function activePlayerDuration() {
  if (activePlayerIsYT()) { try { const d = state.playerYT.getDuration(); return d > 0 ? d : 0; } catch (_e) {} return 0; }
  const a = $("#music-player-audio"); return Number.isFinite(a?.duration) ? a.duration : 0;
}
function activePlayerSeek(seconds) {
  if (activePlayerIsYT()) { try { state.playerYT.seekTo(seconds, true); } catch (_e) {} return; }
  const a = $("#music-player-audio"); if (a) a.currentTime = seconds;
}

function stopStationPrimeTimer() {
  if (state.playerStationPrimingTimer) {
    window.clearTimeout(state.playerStationPrimingTimer);
    state.playerStationPrimingTimer = null;
  }
}

function scheduleStationPrime(stationId, delayMs = 1800) {
  stopStationPrimeTimer();
  const normalized = Number(stationId || 0) || null;
  if (!normalized) return;
  state.playerStationPrimingTimer = window.setTimeout(async () => {
    try {
      const payload = await fetchJson(`/api/player/stations/${encodeURIComponent(normalized)}/prime`, { method: "POST" });
      const stations = Array.isArray(state.playerStations) ? state.playerStations.slice() : [];
      const nextStation = payload?.station || null;
      if (nextStation) {
        const index = stations.findIndex((entry) => Number(entry?.id || 0) === normalized);
        if (index >= 0) stations[index] = nextStation;
        else stations.unshift(nextStation);
        state.playerStations = stations;
        renderMusicPlayerStations();
      }
      if (Number(state.playerActiveStationId || 0) === normalized && Array.isArray(payload?.queue)) {
        setPlayerQueue(payload.queue, { preserveCurrent: true });
        state.playerActiveStationRuntime = payload?.runtime || state.playerActiveStationRuntime;
      }
    } catch (_err) {
      // best-effort priming
    } finally {
      state.playerStationPrimingTimer = null;
    }
  }, Math.max(250, Number(delayMs || 0)));
}

function clearActiveStationPlayback() {
  const wasActive = !!state.playerActiveStationId;
  state.playerActiveStationId = null;
  state.playerActiveStationRuntime = null;
  stopStationPrimeTimer();
  // Re-render station cards so the is-active highlight clears immediately when the user
  // plays something outside the station context.
  if (wasActive) renderMusicPlayerStations();
}

function renderMusicPlayerHistory() {
  const recentEl = $("#music-player-recent");
  const favoritesEl = $("#music-player-favorites");
  if (recentEl) {
    const recentItems = Array.isArray(state.playerHistory) ? state.playerHistory.filter((item) => !item?.is_missing_local) : [];
    recentEl.innerHTML = recentItems.length ? `
      <div class="music-player-track-list">
        ${recentItems.map((item) => `
          <article class="music-player-track-row music-player-track-row-rich">
            <div class="music-player-browser-card-art music-player-track-art">
              <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Track")}" loading="lazy">
            </div>
            <button class="music-player-track" type="button" data-action="player-play" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">
              <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
              <span class="music-player-track-meta">${escapeHtml([item.artist, item.played_at].filter(Boolean).join(" • "))}</span>
            </button>
            <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">Play Next</button>
          </article>
        `).join("")}
      </div>
    ` : `<div class="home-results-empty">No recent playback yet.</div>`;
  }
  if (favoritesEl) {
    const favoriteArtists = Array.isArray(state.musicPreferences?.favorite_artists) ? state.musicPreferences.favorite_artists : [];
    favoritesEl.innerHTML = favoriteArtists.length ? `
      <div class="music-player-browser-grid">
        ${favoriteArtists.map((artist) => {
          const artistName = artist.name || artist.artist_name || "Favorite Artist";
          const artistKey = String(artistName).trim().toLowerCase();
          const downloadedArtist = (Array.isArray(state.playerLibrarySummary?.artists) ? state.playerLibrarySummary.artists : []).find((entry) => String(entry.artist_key || "").trim().toLowerCase() === artistKey) || null;
          return `
            <article class="music-player-browser-card music-player-browser-card-rich">
              <div class="music-player-browser-card-art">
                <img src="${escapeAttr(getMusicLibraryArtworkUrl(downloadedArtist || artist))}" alt="${escapeAttr(artistName)}" loading="lazy">
              </div>
              <div class="music-player-browser-card-copy">
                <span class="music-player-track-title">${escapeHtml(artistName)}</span>
                <span class="music-player-track-meta">${downloadedArtist ? escapeHtml(`${downloadedArtist.album_count || 0} albums • ${downloadedArtist.track_count || 0} tracks downloaded`) : "Favorited only • Not downloaded yet"}</span>
                <div class="music-status-row">
                  <span class="music-status-badge is-favorited">Favorited</span>
                  ${downloadedArtist ? `<span class="music-status-badge is-downloaded">Downloaded</span>` : `<span class="music-status-badge">Not Downloaded</span>`}
                </div>
              </div>
              <div class="music-player-browser-card-actions">
                ${downloadedArtist ? `<button class="button ghost small" type="button" data-action="player-open-artist" data-artist-key="${escapeAttr(downloadedArtist.artist_key || "")}">Open Library</button>` : `<button class="button ghost small" type="button" data-action="music-go-search">Find Music</button>`}
              </div>
            </article>
          `;
        }).join("")}
      </div>
    ` : `<div class="home-results-empty">Favorite artists appear here for quick access. Favorites are not the same as downloaded music.</div>`;
  }
}

function formatPlayerTime(seconds) {
  const total = Number.isFinite(Number(seconds)) ? Math.max(0, Math.floor(Number(seconds))) : 0;
  const mins = Math.floor(total / 60);
  const secs = total % 60;
  return `${mins}:${String(secs).padStart(2, "0")}`;
}

function updateMusicPlayerTransportUI() {
  const playPause = $("#music-player-playpause");
  const shuffle = $("#music-player-shuffle");
  const repeat = $("#music-player-repeat");
  const progress = $("#music-player-progress");
  const currentTimeEl = $("#music-player-current-time");
  const durationEl = $("#music-player-duration");
  if (playPause) {
    playPause.textContent = activePlayerIsPaused() ? "Play" : "Pause";
  }
  if (shuffle) {
    shuffle.classList.toggle("active", !!state.playerShuffle);
    shuffle.setAttribute("aria-pressed", state.playerShuffle ? "true" : "false");
  }
  if (repeat) {
    const mode = String(state.playerRepeatMode || "off");
    repeat.textContent = `Repeat: ${mode === "one" ? "One" : mode === "all" ? "All" : "Off"}`;
    repeat.classList.toggle("active", mode !== "off");
    repeat.setAttribute("aria-pressed", mode !== "off" ? "true" : "false");
  }
  const duration = activePlayerDuration();
  const currentTime = activePlayerCurrentTime();
  if (currentTimeEl) currentTimeEl.textContent = formatPlayerTime(currentTime);
  if (durationEl) durationEl.textContent = formatPlayerTime(duration);
  if (progress && !state.playerProgressDragging) {
    progress.value = duration > 0 ? String(Math.round((currentTime / duration) * 1000)) : "0";
  }
}

function normalizePlayableItem(item = {}) {
  if (!item || typeof item !== "object") return null;
  const normalized = {
    ...item,
    id: String(item.id || item.local_path || item.stream_url || item.recording_mbid || "").trim() || null,
    title: String(item.title || item.track || item.name || "").trim() || "Track",
    artist: String(item.artist || item.artist_name || "").trim() || "",
    album: String(item.album || "").trim() || "",
    local_path: String(item.local_path || "").trim() || null,
    stream_url: String(item.stream_url || item.source_url || "").trim() || null,
    recording_mbid: String(item.recording_mbid || "").trim() || null,
    mb_release_id: String(item.mb_release_id || item.release_mbid || "").trim() || null,
    mb_release_group_id: String(item.mb_release_group_id || item.release_group_mbid || "").trim() || null,
    source: String(item.source || "").trim() || null,
    resolved_via: String(item.resolved_via || "").trim() || null,
    artwork_url: String(item.artwork_url || "").trim() || null,
  };
  normalized.video_id = String(item.video_id || extractYouTubeVideoId(normalized.stream_url) || "").trim() || null;
  normalized.kind = String(item.kind || item.source_kind || (normalized.local_path ? "local" : (normalized.video_id ? "youtube" : "unresolved"))).trim() || "unresolved";
  return normalized;
}

function playerItemsMatch(left, right) {
  const a = normalizePlayableItem(left);
  const b = normalizePlayableItem(right);
  if (!a || !b) return false;
  if (a.recording_mbid && b.recording_mbid) return a.recording_mbid === b.recording_mbid;
  if (a.local_path && b.local_path) return a.local_path === b.local_path;
  if (a.video_id && b.video_id) return a.video_id === b.video_id;
  if (a.stream_url && b.stream_url) return a.stream_url === b.stream_url;
  if (a.id && b.id) return a.id === b.id;
  return false;
}

function buildPlayableItemFromDataset(dataset = {}) {
  return normalizePlayableItem({
    id: String(dataset.localPath || dataset.streamUrl || dataset.recordingMbid || "").trim() || null,
    title: String(dataset.title || "").trim(),
    artist: String(dataset.artist || "").trim(),
    album: String(dataset.album || "").trim(),
    local_path: String(dataset.localPath || "").trim() || null,
    stream_url: String(dataset.streamUrl || "").trim() || null,
    video_id: String(dataset.videoId || "").trim() || null,
    source: String(dataset.source || "").trim() || null,
    kind: String(dataset.sourceKind || "").trim() || null,
    recording_mbid: String(dataset.recordingMbid || "").trim() || null,
    mb_release_id: String(dataset.releaseMbid || "").trim() || null,
    mb_release_group_id: String(dataset.releaseGroupMbid || "").trim() || null,
    artwork_url: String(dataset.artworkUrl || "").trim() || null,
  });
}

function buildPlayableItemFromButton(button) {
  return buildPlayableItemFromDataset(button?.dataset || {});
}

function getCurrentQueueIndex() {
  if (!Array.isArray(state.playerQueue) || !state.playerQueue.length || !state.playerCurrent) {
    return -1;
  }
  return state.playerQueue.findIndex((item) => playerItemsMatch(item, state.playerCurrent));
}

function setPlayerQueue(items = [], { preserveCurrent = false } = {}) {
  state.playerQueue = Array.isArray(items) ? items.map((item) => normalizePlayableItem(item)).filter(Boolean) : [];
  if (!preserveCurrent && state.playerQueue.length && state.playerCurrent) {
    const index = getCurrentQueueIndex();
    if (index < 0) {
      state.playerCurrent = null;
    }
  }
  renderMusicPlayerQueue();
}

async function playPlayerQueueIndex(index) {
  if (!Array.isArray(state.playerQueue) || !state.playerQueue.length) return;
  const targetIndex = Number.parseInt(String(index), 10);
  if (!Number.isFinite(targetIndex) || targetIndex < 0 || targetIndex >= state.playerQueue.length) return;
  const item = normalizePlayableItem(state.playerQueue[targetIndex]);
  if (!item) return;
  if (!item.stream_url && !item.video_id && String(item.kind || "") === "unresolved" && item.recording_mbid) {
    // Resolve the stream URL before playing — update queue in place so the UI reflects it.
    const resolved = await resolveRecordingStreamUrl(item.recording_mbid, {
      artist: item.artist,
      track: item.title,
      album: item.album,
      release_group_mbid: item.mb_release_group_id,
    });
    if (resolved?.stream_url || resolved?.video_id) {
      const readyItem = normalizePlayableItem({ ...item, ...resolved, kind: resolved.video_id ? "youtube" : "cached" });
      state.playerQueue[targetIndex] = readyItem;
      await playMusicPlayerItem(readyItem);
      // Pre-resolve the next unresolved item in the background so it's ready when needed.
      _prefetchNextUnresolved(targetIndex + 1);
      return;
    }
    // Could not resolve — skip to next track.
    const nextIndex = targetIndex + 1;
    if (nextIndex < state.playerQueue.length) {
      await playPlayerQueueIndex(nextIndex);
    }
    return;
  }
  await playMusicPlayerItem(item);
  _prefetchNextUnresolved(targetIndex + 1);
}

// Background-resolve the next unresolved queue item so playback can start instantly.
function _prefetchNextUnresolved(fromIndex) {
  const queue = Array.isArray(state.playerQueue) ? state.playerQueue : [];
  for (let i = fromIndex; i < Math.min(fromIndex + 3, queue.length); i++) {
    const item = normalizePlayableItem(queue[i]);
    if (!item?.stream_url && !item?.video_id && String(item?.kind || "") === "unresolved" && item?.recording_mbid) {
      const capturedIndex = i;
      resolveRecordingStreamUrl(item.recording_mbid, {
        artist: item.artist, track: item.title, album: item.album, release_group_mbid: item.mb_release_group_id,
      }).then((resolved) => {
        if (!resolved?.stream_url && !resolved?.video_id) return;
        if (Array.isArray(state.playerQueue) && state.playerQueue[capturedIndex]?.recording_mbid === item.recording_mbid) {
          state.playerQueue[capturedIndex] = normalizePlayableItem({ ...state.playerQueue[capturedIndex], ...resolved, kind: resolved.video_id ? "youtube" : "cached" });
        }
      }).catch(() => {});
      break; // one at a time
    }
  }
}

async function playNextPlayerItem({ autoAdvance = false } = {}) {
  if (!Array.isArray(state.playerQueue) || !state.playerQueue.length) return;
  if (Number(state.playerActiveStationId || 0) && !state.playerShuffle && String(state.playerRepeatMode || "off") !== "one") {
    try {
      const payload = await fetchJson(`/api/player/stations/${encodeURIComponent(state.playerActiveStationId)}/next`, { method: "POST" });
      if (Array.isArray(payload?.queue)) {
        // preserveCurrent: station response replaces the queue but the currently-playing item
        // must not be nulled mid-transition — playMusicPlayerItem sets it explicitly below.
        setPlayerQueue(payload.queue, { preserveCurrent: true });
      }
      state.playerActiveStationRuntime = payload?.runtime || state.playerActiveStationRuntime;
      if (payload?.station) {
        const nextStation = payload.station;
        state.playerStations = (Array.isArray(state.playerStations) ? state.playerStations : []).map((entry) =>
          Number(entry?.id || 0) === Number(nextStation?.id || 0) ? nextStation : entry
        );
        renderMusicPlayerStations();
      }
      if (payload?.current_item?.stream_url || payload?.current_item?.video_id || payload?.current_item?.recording_mbid || payload?.current_item?.local_path) {
        await playMusicPlayerItem(payload.current_item, { preserveStation: true });
        scheduleStationPrime(state.playerActiveStationId, 900);
        return;
      }
      // Station returned no playable next item (needs_matches or empty queue).
      // Fall through to local queue logic rather than going silent — a scheduled prime
      // may recover the station without requiring user interaction.
    } catch (_err) {
      // Network/server error — fall through to local queue behavior below.
    }
  }
  const currentIndex = getCurrentQueueIndex();
  if (state.playerRepeatMode === "one" && autoAdvance && currentIndex >= 0) {
    await playPlayerQueueIndex(currentIndex);
    return;
  }
  if (state.playerShuffle) {
    if (state.playerQueue.length === 1) {
      await playPlayerQueueIndex(0);
      return;
    }
    let nextIndex = currentIndex;
    while (nextIndex === currentIndex) {
      nextIndex = Math.floor(Math.random() * state.playerQueue.length);
    }
    await playPlayerQueueIndex(nextIndex);
    return;
  }
  if (currentIndex < 0) {
    await playPlayerQueueIndex(0);
    return;
  }
  const nextIndex = currentIndex + 1;
  if (nextIndex < state.playerQueue.length) {
    await playPlayerQueueIndex(nextIndex);
    return;
  }
  if (state.playerRepeatMode === "all") {
    await playPlayerQueueIndex(0);
    return;
  }
  if (autoAdvance) {
    clearMusicPlayerCurrentState();
    // If a station is still associated, schedule a background prime so it can refill
    // and recover without requiring manual user interaction.
    if (state.playerActiveStationId) {
      scheduleStationPrime(state.playerActiveStationId, 2500);
    }
  }
}

async function playPreviousPlayerItem() {
  if (!Array.isArray(state.playerQueue) || !state.playerQueue.length) return;
  const audio = $("#music-player-audio");
  if (audio && Number(audio.currentTime || 0) > 4) {
    audio.currentTime = 0;
    updateMusicPlayerTransportUI();
    return;
  }
  const currentIndex = getCurrentQueueIndex();
  if (state.playerShuffle && state.playerQueue.length > 1) {
    let nextIndex = currentIndex;
    while (nextIndex === currentIndex) {
      nextIndex = Math.floor(Math.random() * state.playerQueue.length);
    }
    await playPlayerQueueIndex(nextIndex);
    return;
  }
  if (currentIndex > 0) {
    await playPlayerQueueIndex(currentIndex - 1);
    return;
  }
  if (state.playerRepeatMode === "all" && state.playerQueue.length) {
    await playPlayerQueueIndex(state.playerQueue.length - 1);
  }
}

function togglePlayerShuffle() {
  state.playerShuffle = !state.playerShuffle;
  updateMusicPlayerTransportUI();
}

function cyclePlayerRepeatMode() {
  const current = String(state.playerRepeatMode || "off");
  state.playerRepeatMode = current === "off" ? "all" : current === "all" ? "one" : "off";
  updateMusicPlayerTransportUI();
}

function buildQueueFromTracks(tracks, currentItem) {
  const items = Array.isArray(tracks) ? tracks.map((entry) => normalizePlayableItem(entry)).filter(Boolean) : [];
  if (!items.length) return [];
  const target = normalizePlayableItem(currentItem);
  const currentIndex = target ? items.findIndex((entry) => playerItemsMatch(entry, target)) : -1;
  if (currentIndex <= 0) return items;
  return [...items.slice(currentIndex), ...items.slice(0, currentIndex)];
}

function getPlayerTracksForArtist(artistKey = "") {
  const key = String(artistKey || "").trim().toLowerCase();
  return (Array.isArray(state.playerLibrarySummary?.tracks) ? state.playerLibrarySummary.tracks : []).filter((entry) => String(entry.artist_key || "").trim().toLowerCase() === key);
}

function getPlayerTracksForAlbum(artistKey = "", albumKey = "") {
  const artistMatch = String(artistKey || "").trim().toLowerCase();
  const albumMatch = String(albumKey || "").trim().toLowerCase();
  return (Array.isArray(state.playerLibrarySummary?.tracks) ? state.playerLibrarySummary.tracks : []).filter((entry) =>
    String(entry.artist_key || "").trim().toLowerCase() === artistMatch &&
    String(entry.album_key || "").trim().toLowerCase() === albumMatch
  );
}

async function loadMusicPlayerView() {
  const messageEl = $("#music-player-message");
  if (messageEl) {
    setNotice(messageEl, "Loading music player…", false);
  }
  try {
    const [libraryPayload, librarySummaryPayload, stationsPayload, historyPayload, playlistsPayload, communityPayload] = await Promise.all([
      fetchJson("/api/player/library"),
      fetchJson("/api/player/library/summary"),
      fetchJson("/api/player/stations"),
      fetchJson("/api/player/history"),
      fetchJson("/api/player/playlists"),
      fetchJson("/api/player/community-cache"),
    ]);
    state.playerLibrary = Array.isArray(libraryPayload?.items) ? libraryPayload.items : [];
    state.playerLibrarySummary = (librarySummaryPayload?.summary && typeof librarySummaryPayload.summary === "object")
      ? librarySummaryPayload.summary
      : { artists: [], albums: [], tracks: [] };
    state.playerStations = Array.isArray(stationsPayload?.stations) ? stationsPayload.stations : [];
    if (Number(state.playerActiveStationId || 0) && !state.playerStations.some((entry) => Number(entry?.id || 0) === Number(state.playerActiveStationId || 0))) {
      clearActiveStationPlayback();
    }
    if (Number(state.playerActiveStationId || 0)) {
      const activeStation = state.playerStations.find((entry) => Number(entry?.id || 0) === Number(state.playerActiveStationId || 0)) || null;
      state.playerActiveStationRuntime = activeStation?.runtime || state.playerActiveStationRuntime;
    }
    state.playerCommunityCache = Array.isArray(communityPayload?.items) ? communityPayload.items : [];
    state.playerHistory = Array.isArray(historyPayload?.history) ? historyPayload.history : [];
    state.playerMissingHistory = state.playerHistory.filter((item) => !!item?.is_missing_local);
    state.playerHistory = state.playerHistory.filter((item) => !item?.is_missing_local);
    state.playerPlaylists = Array.isArray(playlistsPayload?.playlists) ? playlistsPayload.playlists : [];
    if (!state.playerSelectedPlaylistId && state.playerPlaylists[0]) {
      state.playerSelectedPlaylistId = Number(state.playerPlaylists[0].id || 0) || null;
    }
    const selectedPlaylist = state.playerPlaylists.find((entry) => Number(entry.id || 0) === Number(state.playerSelectedPlaylistId || 0));
    if (selectedPlaylist) {
      const detailPayload = await fetchJson(`/api/player/playlists/${encodeURIComponent(selectedPlaylist.id)}`);
      state.playerSelectedPlaylistItems = Array.isArray(detailPayload?.items) ? detailPayload.items : [];
    } else {
      state.playerSelectedPlaylistItems = [];
    }
    renderMusicPlayerLibrary();
    renderMusicPlayerPlaylistsView();
    renderMusicPlayerStations();
    renderMusicPlayerCommunityCache();
    renderMusicPlayerQueue();
    renderMusicPlayerHistory();
    setMusicPlayerView(state.playerView || "library");
    syncBottomPlayerShell();
    syncMusicPlayerVideoShell();
    if (messageEl) {
      setNotice(messageEl, "", false);
    }
  } catch (err) {
    if (messageEl) {
      setNotice(messageEl, `Music player failed to load: ${toUserErrorMessage(err)}`, true);
    }
  }
}

async function playMusicPlayerItem(payload, { preserveStation = false } = {}) {
  if (!payload) return;
  payload = normalizePlayableItem(payload);
  if (!payload) return;
  if (!preserveStation) {
    clearActiveStationPlayback();
  }
  if (!payload.stream_url && !payload.video_id && !payload.local_path && payload.recording_mbid) {
    const resolved = await resolveRecordingStreamUrl(payload.recording_mbid, {
      artist: payload.artist,
      track: payload.title,
      album: payload.album,
      release_mbid: payload.mb_release_id,
      release_group_mbid: payload.mb_release_group_id,
    });
    if (resolved?.stream_url || resolved?.video_id) {
      payload = normalizePlayableItem({ ...payload, ...resolved, kind: resolved.video_id ? "youtube" : payload.kind });
    }
  }
  if (!payload.stream_url && payload.local_path) {
    const localStreamUrl = buildLocalPlayerStreamUrl(payload.local_path);
    if (localStreamUrl) {
      payload = normalizePlayableItem({ ...payload, stream_url: localStreamUrl, kind: payload.kind || "local" });
    }
  }

  const videoId = String(payload.video_id || extractYouTubeVideoId(payload.stream_url) || "").trim();
  const isYouTube = !!videoId;

  if (isYouTube) {
    // Stop any audio playback and hand off to YT IFrame player.
    const audio = $("#music-player-audio");
    if (audio) { audio.pause(); audio.removeAttribute("src"); audio.load(); }
    ensureYouTubeAPILoaded();
    try {
      await createYTPlayer(videoId);
    } catch (err) {
      console.warn("[YT] createYTPlayer failed:", err);
      return;
    }
    state.playerCurrentHasVideo = true;
    // Auto-show video when in the main player view; otherwise keep minimized.
    if (state.musicSection === "player") {
      state.playerVideoVisible = true;
    }
  } else {
    // Local file / direct stream — use <audio>.
    const audio = $("#music-player-audio");
    if (!audio || !payload.stream_url) return;
    destroyYTPlayer();
    audio.src = payload.stream_url;
    try { await audio.play(); } catch (_err) {}
    state.playerCurrentHasVideo = !!String(payload.video_embed_url || "").trim();
    if (!state.playerCurrentHasVideo) state.playerVideoVisible = false;
  }

  state.playerCurrent = payload;
  const titleEl = $("#music-player-now-title");
  const metaEl = $("#music-player-now-meta");
  const contextEl = $("#music-player-now-context");
  const nowArt = $("#music-player-now-art");
  if (titleEl) titleEl.textContent = payload.title || "Now Playing";
  if (metaEl) metaEl.textContent = [payload.artist, payload.album].filter(Boolean).join(" • ") || "Playing";
  if (contextEl) {
    const kindLabel = isYouTube ? "Streaming via YouTube"
      : payload.kind === "local" ? "Playing from your library"
      : "Streaming from cached match";
    contextEl.textContent = [kindLabel, payload.source || ""].filter(Boolean).join(" • ");
  }
  if (nowArt) nowArt.src = getMusicLibraryArtworkUrl(payload);

  syncBottomPlayerShell();
  updateMusicPlayerTransportUI();
  syncMusicPlayerVideoShell();

  fetchJson("/api/player/history", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      item_id: payload.id || payload.local_path || payload.stream_url || `yt:${videoId}`,
      title: payload.title,
      artist: payload.artist,
      stream_url: payload.stream_url || (videoId ? `https://www.youtube.com/watch?v=${videoId}` : null),
      local_path: payload.local_path,
      source_kind: isYouTube ? "youtube" : (payload.kind || "local"),
    }),
  }).catch(() => {});
}

function clearMusicPlayerCurrentState() {
  destroyYTPlayer();
  const audio = $("#music-player-audio");
  if (audio) {
    audio.pause();
    audio.removeAttribute("src");
    audio.load();
  }
  state.playerCurrent = null;
  const nowTitle = $("#music-player-now-title");
  const nowMeta = $("#music-player-now-meta");
  const nowContext = $("#music-player-now-context");
  const nowArt = $("#music-player-now-art");
  if (nowTitle) nowTitle.textContent = "Now Playing";
  if (nowMeta) nowMeta.textContent = "Choose a track from your library or start a station.";
  if (nowContext) nowContext.textContent = "";
  if (nowArt) nowArt.src = "assets/no_artwork.png";
  state.playerCurrentHasVideo = false;
  state.playerVideoVisible = false;
  syncMusicPlayerVideoShell();
  updateMusicPlayerTransportUI();
  syncBottomPlayerShell();
}

function setupHeaderScrollVisibility() {
  const topbar = $(".topbar");
  if (!topbar) return;
  let lastY = window.scrollY || 0;
  let ticking = false;
  const onScroll = () => {
    const currentY = Math.max(0, window.scrollY || 0);
    const delta = currentY - lastY;
    if (currentY <= 24) {
      document.body.classList.remove("topbar-hidden");
    } else if (delta > 8) {
      document.body.classList.add("topbar-hidden");
    } else if (delta < -8) {
      document.body.classList.remove("topbar-hidden");
    }
    lastY = currentY;
    ticking = false;
  };
  window.addEventListener("scroll", () => {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(onScroll);
  }, { passive: true });
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

function buildYouTubeHomeHoverEmbedUrl(url) {
  const videoId = extractYouTubeVideoIdFromUrl(url);
  if (!videoId) {
    return null;
  }
  return `https://www.youtube.com/embed/${encodeURIComponent(videoId)}?autoplay=1&mute=1&controls=0&rel=0&modestbranding=1&playsinline=1`;
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

function buildHomeHoverPreviewDescriptor(candidate) {
  if (!candidate || typeof candidate !== "object") {
    return null;
  }
  const source = normalizePreviewSourceKey(candidate.source);
  const url = String(candidate.url || "").trim();
  if (!source || !url || !isValidHttpUrl(url)) {
    return null;
  }
  let embedUrl = null;
  if (source === "youtube" || source === "youtube_music") {
    embedUrl = buildYouTubeHomeHoverEmbedUrl(url);
  } else if (source === "rumble") {
    embedUrl = buildRumbleHomePreviewEmbedUrl(url, candidate);
  }
  if (!embedUrl || !isValidHttpUrl(embedUrl)) {
    return null;
  }
  return {
    source,
    title: String(candidate.title || "").trim() || "Preview",
    embedUrl,
  };
}

function stopHomeArtworkHoverPreview(row) {
  if (!row) return;
  const artwork = row.querySelector(".home-candidate-artwork");
  if (!artwork) return;
  if (row._hoverPreviewTimer) {
    clearTimeout(row._hoverPreviewTimer);
    row._hoverPreviewTimer = null;
  }
  row.classList.remove("hover-preview-active");
  const frame = artwork.querySelector(".home-candidate-hover-preview");
  if (frame) {
    frame.remove();
  }
}

function startHomeArtworkHoverPreview(row, descriptor) {
  if (!row || !descriptor?.embedUrl) return;
  const artwork = row.querySelector(".home-candidate-artwork");
  if (!artwork) return;
  stopHomeArtworkHoverPreview(row);
  row._hoverPreviewTimer = setTimeout(() => {
    row._hoverPreviewTimer = null;
    const frame = document.createElement("iframe");
    frame.className = "home-candidate-hover-preview";
    frame.src = descriptor.embedUrl;
    frame.title = descriptor.title || "Preview";
    frame.setAttribute("allow", "accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share");
    frame.setAttribute("allowfullscreen", "true");
    frame.setAttribute("loading", "eager");
    frame.tabIndex = -1;
    artwork.appendChild(frame);
    row.classList.add("hover-preview-active");
  }, HOME_HOVER_PREVIEW_DELAY_MS);
}

function buildHomePreviewDescriptorFromRow(row) {
  if (!row) return null;
  const previewButton = row.querySelector('button[data-action="home-preview"]');
  if (!previewButton) return null;
  const mediaType = String(previewButton.dataset.previewMediaType || "").trim().toLowerCase();
  const title = String(previewButton.dataset.previewTitle || "").trim() || "Preview";
  const source = String(previewButton.dataset.previewSource || "").trim();
  const description = String(previewButton.dataset.previewDescription || "").trim();
  const postedText = String(previewButton.dataset.previewPostedText || "").trim();
  const durationText = String(previewButton.dataset.previewDurationText || "").trim();
  const itemId = String(previewButton.dataset.previewItemId || "").trim();
  const candidateId = String(previewButton.dataset.previewCandidateId || "").trim();
  const directUrl = String(previewButton.dataset.previewDirectUrl || "").trim();
  if (mediaType === "audio") {
    const streamUrl = String(previewButton.dataset.previewStreamUrl || "").trim();
    if (!streamUrl || !isValidHttpUrl(streamUrl)) return null;
    return {
      mediaType: "audio",
      streamUrl,
      source,
      title,
      description,
      postedText,
      durationText,
      itemId,
      candidateId,
      directUrl,
    };
  }
  const embedUrl = String(previewButton.dataset.previewEmbedUrl || "").trim();
  if (!embedUrl || !isValidHttpUrl(embedUrl)) return null;
  return {
    mediaType: "video",
    embedUrl,
    source,
    title,
    description,
    postedText,
    durationText,
    itemId,
    candidateId,
    directUrl,
  };
}

function openHomePreviewModal(descriptor) {
  if (!descriptor || (!descriptor.embedUrl && !descriptor.streamUrl)) {
    return;
  }
  const modal = $("#home-preview-modal");
  const frame = $("#home-preview-frame");
  const audioWrap = $("#home-preview-audio-wrap");
  const audioEl = $("#home-preview-audio");
  const titleEl = $("#home-preview-title");
  const sourceEl = $("#home-preview-source");
  const downloadButton = $("#home-preview-download");
  const detailsWrap = $("#home-preview-details");
  const detailsMetaEl = $("#home-preview-details-meta");
  const descriptionEl = $("#home-preview-description");
  if (!modal || !frame || !audioWrap || !audioEl || !titleEl || !sourceEl || !downloadButton || !detailsWrap || !detailsMetaEl || !descriptionEl) {
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
  previewState.embedUrl = descriptor.embedUrl || "";
  previewState.mediaType = descriptor.mediaType || "video";
  previewState.streamUrl = descriptor.streamUrl || "";
  previewState.itemId = String(descriptor.itemId || "").trim();
  previewState.candidateId = String(descriptor.candidateId || "").trim();
  previewState.directUrl = String(descriptor.directUrl || "").trim();
  previewState.description = String(descriptor.description || "").trim();
  previewState.postedText = String(descriptor.postedText || "").trim();
  previewState.durationText = String(descriptor.durationText || "").trim();
  titleEl.textContent = descriptor.title || "Preview";
  sourceEl.textContent = `Source: ${sourceLabels[descriptor.source] || descriptor.source || "Unknown"}`;
  const detailMeta = [
    previewState.postedText ? `Posted: ${previewState.postedText}` : "",
    previewState.durationText ? `Duration: ${previewState.durationText}` : "",
  ].filter(Boolean).join(" • ");
  detailsMetaEl.textContent = detailMeta;
  descriptionEl.textContent = previewState.description || "";
  detailsWrap.classList.toggle("hidden", !detailMeta && !previewState.description);
  const canDownload = !!(previewState.directUrl && (previewState.candidateId || previewState.itemId));
  downloadButton.classList.toggle("hidden", previewState.mediaType !== "video" || !canDownload);
  downloadButton.disabled = !canDownload;
  downloadButton.textContent = "Download";
  if (previewState.mediaType === "audio") {
    frame.src = "about:blank";
    frame.closest(".home-preview-frame-wrap")?.classList.add("hidden");
    audioWrap.classList.remove("hidden");
    audioEl.src = descriptor.streamUrl || "";
  } else {
    audioEl.removeAttribute("src");
    audioWrap.classList.add("hidden");
    frame.closest(".home-preview-frame-wrap")?.classList.remove("hidden");
    frame.src = descriptor.embedUrl || "about:blank";
  }
  modal.classList.remove("hidden");
  updatePollingState();
}

function closeHomePreviewModal() {
  const modal = $("#home-preview-modal");
  const frame = $("#home-preview-frame");
  const audioWrap = $("#home-preview-audio-wrap");
  const audioEl = $("#home-preview-audio");
  if (frame) {
    frame.src = "about:blank";
  }
  if (audioEl) {
    try {
      audioEl.pause();
    } catch (_err) {
      // ignore
    }
    audioEl.removeAttribute("src");
    audioEl.load();
  }
  if (audioWrap) {
    audioWrap.classList.add("hidden");
  }
  frame?.closest(".home-preview-frame-wrap")?.classList.remove("hidden");
  if (modal) {
    modal.classList.add("hidden");
  }
  previewState.open = false;
  previewState.source = "";
  previewState.title = "";
  previewState.embedUrl = "";
  previewState.mediaType = "";
  previewState.streamUrl = "";
  previewState.itemId = "";
  previewState.candidateId = "";
  previewState.directUrl = "";
  previewState.description = "";
  previewState.postedText = "";
  previewState.durationText = "";
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
  return !!state.config?.settings_advanced_mode;
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
  const hasVisibleSection = $$(".settings-section[data-settings-section]").some((section) => !section.classList.contains("hidden"));
  if (!hasVisibleSection) {
    const fallbackSection = document.getElementById(allowed[0] || getDefaultSettingsSectionId());
    if (fallbackSection) {
      fallbackSection.classList.remove("hidden");
      fallbackSection.dataset.settingsVisible = "1";
      fallbackSection.setAttribute("aria-hidden", "false");
      state.settingsActiveSectionId = fallbackSection.id;
    }
  }
  syncSettingsMainWidthLock();
  updateSettingsSectionNavState(state.settingsActiveSectionId || nextActive);
  if (jump) {
    const activeId = state.settingsActiveSectionId || nextActive;
    if (activeId === SETTINGS_ALL_SECTION_ID) {
      const firstVisible = $$(".settings-section[data-settings-section]").find((section) => !section.classList.contains("hidden"));
      if (firstVisible) {
        firstVisible.scrollIntoView({ behavior: smooth ? "smooth" : "auto", block: "start" });
      }
      return;
    }
    const activeSection = document.getElementById(activeId);
    if (activeSection) {
      activeSection.scrollIntoView({ behavior: smooth ? "smooth" : "auto", block: "start" });
    }
  }
}

function ensureSettingsLayoutVisible() {
  const panel = $("#config-panel");
  const layout = panel?.querySelector(".settings-layout");
  const sidebar = layout?.querySelector(".settings-sidebar");
  const main = layout?.querySelector(".settings-main");
  if (panel) {
    panel.classList.remove("hidden", "page-hidden");
  }
  if (layout) {
    layout.classList.remove("hidden");
  }
  if (sidebar) {
    sidebar.classList.remove("hidden");
  }
  if (main) {
    main.classList.remove("hidden");
  }
}

function queueSettingsBackgroundRefreshes({ includeDashboard = true } = {}) {
  window.setTimeout(() => {
    refreshConnectionsStatus().catch(() => {});
    refreshSchedule();
    if (!includeDashboard) {
      return;
    }
    refreshStatus();
    refreshMetrics();
    refreshVersion();
    refreshSearchQueue();
    refreshDownloads();
    refreshHistory();
    refreshLogs();
  }, 0);
}

function initializeSettingsPageData(sectionId) {
  const activeSectionId = String(sectionId || "");
  const prioritizeGuidedSetup = activeSectionId === "settings-guided-setup";
  if (!state.config || !state.configDirty) {
    loadConfig().then(async () => {
      ensureSettingsLayoutVisible();
      const sectionHash = String(window.location.hash || "").replace("#", "");
      const resolvedSection = sectionHash.startsWith("settings-")
        ? sectionHash
        : (state.settingsActiveSectionId || getDefaultSettingsSectionId());
      showSettingsPage(resolvedSection, { jump: false, smooth: false });
      refreshSpotifyConfig().catch(() => {});
      if (consumeSpotifyConnectedHashFlag()) {
        await refreshSpotifyConfig().catch(() => {});
        setConfigNotice("Spotify connected successfully.", false, true);
      }
      refreshCommunityPublishStatus().catch(() => {});
      refreshCommunityCacheSyncStatus().catch(() => {});
    }).catch(() => {});
  } else {
    refreshSpotifyConfig().catch(() => {});
    refreshCommunityPublishStatus().catch(() => {});
    refreshCommunityCacheSyncStatus().catch(() => {});
  }
  refreshSetupStatus().catch(() => {});
  queueSettingsBackgroundRefreshes({ includeDashboard: !prioritizeGuidedSetup });
}

function showSettingsPage(sectionId, options = {}) {
  ensureSettingsLayoutVisible();
  mountGuidedSetupHost("settings-setup-slot");
  const allowed = getAllowedSettingsSectionIds();
  if (!allowed.length) {
    return;
  }
  const requested = String(sectionId || "").trim();
  const activeId = requested === SETTINGS_ALL_SECTION_ID
    ? SETTINGS_ALL_SECTION_ID
    : (allowed.includes(requested) ? requested : (allowed[0] || getDefaultSettingsSectionId()));
  setActiveSettingsSection(activeId, options);
}

function setHomeSetupOverlayOpen(open) {
  const overlay = $("#home-setup-overlay");
  if (!overlay) return;
  const shouldOpen = !!open;
  overlay.classList.toggle("hidden", !shouldOpen);
  if (shouldOpen) {
    mountSettingsSubpages();
    mountGuidedSetupHost("home-setup-overlay-slot");
    renderSetupWizard();
    refreshSetupStatus().catch(() => {});
  } else {
    mountGuidedSetupHost("settings-setup-slot");
  }
  updatePollingState();
}

function applySettingsAdvancedMode(enabled, { persist = true } = {}) {
  const normalized = !!enabled;
  const toggle = $("#settings-advanced-mode");
  if (toggle) {
    toggle.checked = normalized;
  }
  if (persist) {
    if (!state.config || typeof state.config !== "object") {
      state.config = {};
    }
    state.config.settings_advanced_mode = normalized;
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
  updateAdvancedPageVisibility();
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

function applyHomeDefaultActiveFormat({ force = false } = {}) {
  if (state.homeMusicMode) {
    updateMusicModeFormatControl();
    return;
  }
  applyHomeDefaultVideoFormat({ force });
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
  if (!topActions) {
    return;
  }
  if (!state.actionButtons) {
    state.actionButtons = Array.from(topActions.children);
  }
  state.actionButtons.forEach((button) => {
    if (button.parentElement !== topActions) {
      topActions.appendChild(button);
    }
  });
}

function updatePollingState() {
  const importModalOpen = !$("#import-progress-modal")?.classList.contains("hidden");
  const homeSetupOverlayOpen = !$("#home-setup-overlay")?.classList.contains("hidden");
  state.pollingPaused = browserState.open || oauthState.open || previewState.open || importModalOpen || homeSetupOverlayOpen || state.configDirty || state.inputFocused;
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

function streamUrl(fileId) {
  return `/api/files/${encodeURIComponent(fileId)}/stream`;
}

const PLAYBACK_PRESETS = {
  none: {
    mode: "none",
    label: "",
    template: "",
  },
  jellyfin: {
    mode: "jellyfin",
    label: "Watch in Jellyfin",
    template: "",
  },
  plex: {
    mode: "plex",
    label: "Watch in Plex",
    template: "",
  },
  vlc: {
    mode: "custom",
    label: "Open in VLC",
    template: "vlc://{stream_url}",
  },
  mpv: {
    mode: "custom",
    label: "Open in MPV",
    template: "mpv://{stream_url}",
  },
  iina: {
    mode: "custom",
    label: "Open in IINA",
    template: "iina://weblink?url={encoded_stream_url}",
  },
};

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
  const nextOptions = { ...options };
  const headers = new Headers(options.headers || {});
  const token = state.adminPinToken || localStorage.getItem(ADMIN_PIN_TOKEN_KEY) || "";
  if (token) {
    headers.set("X-Retreivr-Admin-Token", token);
  }
  nextOptions.headers = headers;
  const response = await fetch(url, nextOptions);
  if (!response.ok) {
    const text = await response.text();
    if (response.status === 403 && text.includes("admin_pin_required")) {
      state.adminPinToken = "";
      state.adminSecurity = { ...(state.adminSecurity || {}), session_valid: false };
      localStorage.removeItem(ADMIN_PIN_TOKEN_KEY);
    }
    throw new Error(`${response.status} ${text}`);
  }
  return response.json();
}

async function refreshAdminSecurityStatus() {
  try {
    const payload = await fetchJson("/api/admin/security/status");
    state.adminSecurity = payload || state.adminSecurity;
    if (!payload?.session_valid) {
      state.adminPinToken = "";
      localStorage.removeItem(ADMIN_PIN_TOKEN_KEY);
    }
    const statusEl = $("#security-admin-pin-status");
    const currentInput = $("#cfg-security-admin-pin-current");
    if ($("#cfg-security-admin-pin-enabled")) $("#cfg-security-admin-pin-enabled").checked = !!payload?.admin_pin_enabled;
    if ($("#cfg-security-admin-pin-session-minutes")) {
      $("#cfg-security-admin-pin-session-minutes").value = Number(payload?.admin_pin_session_minutes || 30);
    }
    if (currentInput) {
      const hasExistingPin = !!payload?.admin_pin_enabled;
      currentInput.disabled = !hasExistingPin;
      currentInput.placeholder = hasExistingPin
        ? "Required to change or disable an existing PIN"
        : "No current PIN yet";
      if (!hasExistingPin) {
        currentInput.value = "";
      }
    }
    if (statusEl) {
      statusEl.textContent = payload?.admin_pin_enabled
        ? (payload.session_valid ? `Admin PIN enabled • unlocked for ${payload.admin_pin_session_minutes || 30} min` : "Admin PIN enabled • locked")
        : "Admin PIN is disabled.";
      statusEl.classList.remove("warning");
    }
  } catch (_err) {
    // ignore
  }
}

function setAdminPinModalOpen(open) {
  const modal = $("#admin-pin-modal");
  if (!modal) return;
  modal.classList.toggle("hidden", !open);
  if (open) {
    const input = $("#admin-pin-input");
    const message = $("#admin-pin-message");
    if (message) message.textContent = "";
    if (input) {
      input.value = "";
      setTimeout(() => input.focus(), 0);
    }
  }
}

function setStartupSetupModalOpen(open) {
  const modal = $("#startup-setup-modal");
  if (!modal) return;
  modal.classList.toggle("hidden", !open);
  if (open) {
    const message = $("#startup-setup-message");
    if (message) message.textContent = "";
  }
  updatePollingState();
}

function shouldShowStartupSetupPrompt() {
  const showOnStartup = !!state.config?.setup?.show_on_startup;
  const completedModules = Array.isArray(state.config?.setup?.completed_modules)
    ? state.config.setup.completed_modules
    : [];
  return showOnStartup && completedModules.length === 0;
}

function maybeShowStartupSetupPrompt() {
  if (state.currentPage !== "home") return;
  if (!state.startupSetupPromptPending || state.startupSetupPromptDismissedForSession) return;
  setStartupSetupModalOpen(true);
}

function loadMusicHeaderModePreference() {
  try {
    const value = localStorage.getItem(MUSIC_HEADER_MODE_KEY);
    return ["auto", "artist", "album", "track"].includes(value || "") ? value : "auto";
  } catch (_err) {
    return "auto";
  }
}

function setMusicHeaderMode(mode, { persist = true } = {}) {
  const normalized = ["auto", "artist", "album", "track"].includes(String(mode || "").trim())
    ? String(mode || "").trim()
    : "auto";
  state.musicHeaderMode = normalized;
  $$("#music-header-mode-toggle [data-mode]").forEach((button) => {
    const active = button.dataset.mode === normalized;
    button.classList.toggle("active", active);
    button.setAttribute("aria-checked", active ? "true" : "false");
  });
  if (persist) {
    try {
      localStorage.setItem(MUSIC_HEADER_MODE_KEY, normalized);
    } catch (_err) {
      // ignore
    }
  }
}

function parseMusicHeaderQuery(rawQuery, mode = "auto") {
  const query = String(rawQuery || "").trim();
  const normalizedMode = ["auto", "artist", "album", "track"].includes(mode) ? mode : "auto";
  if (!query) {
    return { artist: "", album: "", track: "" };
  }
  const parts = query.split(/\s[-–:]\s/).map((part) => part.trim()).filter(Boolean);
  if (normalizedMode === "artist") {
    return { artist: query, album: "", track: "" };
  }
  if (normalizedMode === "album") {
    if (parts.length >= 2) {
      return { artist: parts[0], album: parts.slice(1).join(" - "), track: "" };
    }
    return { artist: query, album: "", track: "" };
  }
  if (normalizedMode === "track") {
    if (parts.length >= 2) {
      return { artist: parts[0], album: "", track: parts.slice(1).join(" - ") };
    }
    return { artist: "", album: "", track: query };
  }
  if (parts.length >= 2) {
    const right = parts.slice(1).join(" - ");
    const likelyTrack = right.split(/\s+/).length <= 5 || /["']/.test(right);
    return {
      artist: parts[0],
      album: likelyTrack ? "" : right,
      track: likelyTrack ? right : "",
    };
  }
  return { artist: query, album: "", track: "" };
}

async function runMusicHeaderSearch() {
  const query = $("#music-header-query")?.value || "";
  const parsed = parseMusicHeaderQuery(query, state.musicHeaderMode || "auto");
  persistRecentMusicSearch(query);
  if ($("#search-artist")) $("#search-artist").value = parsed.artist;
  if ($("#search-album")) $("#search-album").value = parsed.album;
  if ($("#search-track")) $("#search-track").value = parsed.track;
  const container = $("#music-results-container");
  try {
    setMusicSection("browse");
    setMusicPageNotice("Searching music…", false);
    if (container) {
      container.innerHTML = '<div class="home-results-empty">Searching artists, albums, and tracks…</div>';
    }
    await performMusicModeSearch();
  } catch (err) {
    setMusicPageNotice(`Music search failed: ${toUserErrorMessage(err)}`, true);
    if (container) {
      container.innerHTML = '<div class="home-results-empty">Music search failed.</div>';
    }
  }
}

function getMusicTopMatchCandidate({ artists = [], albums = [], tracks = [] } = {}, rawQuery = "") {
  const query = String(rawQuery || "").trim().toLowerCase();
  const scoreTextMatch = (value = "") => {
    const text = String(value || "").trim().toLowerCase();
    if (!query || !text) return 0;
    if (text === query) return 120;
    if (text.startsWith(query)) return 80;
    if (text.includes(query)) return 50;
    return 0;
  };
  const candidates = [];
  artists.forEach((item, index) => {
    candidates.push({
      kind: "artist",
      item,
      index,
      key: String(item?.artist_mbid || item?.name || `artist-${index}`),
      score: scoreTextMatch(item?.name) + (Number(item?.recommended_score) || 0),
    });
  });
  albums.forEach((item, index) => {
    candidates.push({
      kind: "album",
      item,
      index,
      key: String(item?.release_group_mbid || item?.title || `album-${index}`),
      score: Math.max(
        scoreTextMatch(item?.title),
        scoreTextMatch(`${item?.artist || ""} ${item?.title || ""}`)
      ) + (Number(item?.recommended_score) || 0) + 8,
    });
  });
  tracks.forEach((item, index) => {
    candidates.push({
      kind: "track",
      item,
      index,
      key: `${item?.recording_mbid || item?.track || "track"}::${item?.mb_release_id || index}`,
      score: Math.max(
        scoreTextMatch(item?.track),
        scoreTextMatch(`${item?.artist || ""} ${item?.track || ""}`),
        scoreTextMatch(`${item?.artist || ""} ${item?.album || ""} ${item?.track || ""}`)
      ) + (Number(item?.score) || 0) + 12,
    });
  });
  if (!candidates.length) {
    return null;
  }
  candidates.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return a.index - b.index;
  });
  return candidates[0];
}

async function persistSetupStartupPreference(showOnStartup) {
  const nextConfig = state.config && typeof state.config === "object"
    ? JSON.parse(JSON.stringify(state.config))
    : {};
  const setup = (nextConfig.setup && typeof nextConfig.setup === "object") ? { ...nextConfig.setup } : {};
  setup.show_on_startup = !!showOnStartup;
  nextConfig.setup = setup;
  await fetchJson("/api/config", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(nextConfig),
  });
  state.config = nextConfig;
}

async function ensureAdminPinSession() {
  if (!state.adminSecurity?.admin_pin_enabled) {
    return true;
  }
  if (state.adminSecurity?.session_valid && (state.adminPinToken || localStorage.getItem(ADMIN_PIN_TOKEN_KEY))) {
    return true;
  }
  setAdminPinModalOpen(true);
  return false;
}

async function submitAdminPinUnlock() {
  const input = $("#admin-pin-input");
  const messageEl = $("#admin-pin-message");
  const pin = String(input?.value || "").trim();
  if (!pin) {
    if (messageEl) messageEl.textContent = "PIN is required.";
    return false;
  }
  if (messageEl) messageEl.textContent = "Verifying PIN…";
  try {
    const payload = await fetchJson("/api/admin/pin/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pin }),
    });
    state.adminPinToken = String(payload?.token || "");
    if (state.adminPinToken) {
      localStorage.setItem(ADMIN_PIN_TOKEN_KEY, state.adminPinToken);
    }
    await refreshAdminSecurityStatus();
    setAdminPinModalOpen(false);
    return true;
  } catch (err) {
    if (messageEl) messageEl.textContent = toUserErrorMessage(err);
    return false;
  }
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

function setMediaLibraryNotice(element, message, isError = false) {
  if (!element) return;
  if (!message) {
    element.textContent = "";
    element.classList.add("hidden");
    element.classList.remove("warning");
    return;
  }
  element.textContent = message;
  element.classList.remove("hidden");
  element.classList.toggle("warning", !!isError);
}

function getMusicLibrarySelectedArtist() {
  const artists = Array.isArray(state.playerLibrarySummary?.artists) ? state.playerLibrarySummary.artists : [];
  const selectedKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  return artists.find((entry) => String(entry.artist_key || "").trim().toLowerCase() === selectedKey) || null;
}

function getMusicLibrarySelectedAlbum() {
  const albums = Array.isArray(state.playerLibrarySummary?.albums) ? state.playerLibrarySummary.albums : [];
  const selectedArtistKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  const selectedAlbumKey = String(state.playerSelectedAlbumKey || "").trim().toLowerCase();
  return albums.find((entry) =>
    String(entry.artist_key || "").trim().toLowerCase() === selectedArtistKey &&
    String(entry.album_key || "").trim().toLowerCase() === selectedAlbumKey
  ) || null;
}

function getMusicLibraryFilteredAlbums() {
  const albums = Array.isArray(state.playerLibrarySummary?.albums) ? state.playerLibrarySummary.albums : [];
  const selectedArtistKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  if (!selectedArtistKey) {
    return albums;
  }
  return albums.filter((album) => String(album.artist_key || "").trim().toLowerCase() === selectedArtistKey);
}

function getMusicLibraryFilteredTracks() {
  const tracks = Array.isArray(state.playerLibrarySummary?.tracks) ? state.playerLibrarySummary.tracks : [];
  const selectedArtistKey = String(state.playerSelectedArtistKey || "").trim().toLowerCase();
  const selectedAlbumKey = String(state.playerSelectedAlbumKey || "").trim().toLowerCase();
  return tracks.filter((entry) => {
    if (selectedArtistKey && String(entry.artist_key || "").trim().toLowerCase() !== selectedArtistKey) {
      return false;
    }
    if (selectedAlbumKey && String(entry.album_key || "").trim().toLowerCase() !== selectedAlbumKey) {
      return false;
    }
    return true;
  });
}

function getVideoLibrarySourceBadge(item) {
  const source = String(item?.source || "").trim();
  if (!source) return "Video";
  if (source === "youtube_music") return "YouTube Music";
  if (source === "archive_org") return "Archive";
  return formatSourceLabel(source);
}

function getMusicLibraryRootsLabel() {
  const cfg = state.config || {};
  const roots = [];
  const libraryPath = String(cfg?.music?.library_path || "").trim();
  if (libraryPath) {
    roots.push(libraryPath);
  }
  const downloadsMusicRoot = `/downloads/${String(cfg.home_music_download_folder || cfg.music_download_folder || "Music").trim() || "Music"}`;
  roots.push(downloadsMusicRoot);
  return roots.join(" • ");
}

function getMusicLibraryArtworkUrl(item) {
  const localArtwork = String(item?.artwork_url || "").trim();
  if (localArtwork) return localArtwork;
  const albumKey = String(item?.album_key || "").trim().toLowerCase();
  if (albumKey) {
    const tracks = Array.isArray(state.playerLibrarySummary?.tracks) ? state.playerLibrarySummary.tracks : [];
    const matchingTrack = tracks.find((entry) => String(entry?.album_key || "").trim().toLowerCase() === albumKey && String(entry?.artwork_url || "").trim());
    if (matchingTrack?.artwork_url) return String(matchingTrack.artwork_url).trim();
  }
  const artistKey = String(item?.artist_key || "").trim().toLowerCase();
  if (artistKey) {
    const albums = Array.isArray(state.playerLibrarySummary?.albums) ? state.playerLibrarySummary.albums : [];
    const matchingAlbum = albums.find((entry) => String(entry?.artist_key || "").trim().toLowerCase() === artistKey && String(entry?.artwork_url || "").trim());
    if (matchingAlbum?.artwork_url) return String(matchingAlbum.artwork_url).trim();
  }
  return "assets/no_artwork.png";
}

function isMusicArtistFavorited(artistName = "", artistKey = "") {
  if (artistKey && isFavoriteArtist({ artist_mbid: "", name: artistName, artist_name: artistName, artist_key: artistKey })) {
    return true;
  }
  return isFavoriteArtist({ name: artistName, artist_name: artistName });
}

function isMusicArtistDownloaded(artistKey = "") {
  const artists = Array.isArray(state.playerLibrarySummary?.artists) ? state.playerLibrarySummary.artists : [];
  const key = String(artistKey || "").trim().toLowerCase();
  return !!key && artists.some((entry) => String(entry.artist_key || "").trim().toLowerCase() === key);
}

function isMusicAlbumDownloaded(artistKey = "", albumKey = "") {
  const albums = Array.isArray(state.playerLibrarySummary?.albums) ? state.playerLibrarySummary.albums : [];
  const artistMatch = String(artistKey || "").trim().toLowerCase();
  const albumMatch = String(albumKey || "").trim().toLowerCase();
  return !!albumMatch && albums.some((entry) =>
    String(entry.artist_key || "").trim().toLowerCase() === artistMatch &&
    String(entry.album_key || "").trim().toLowerCase() === albumMatch
  );
}

function isPlayerQueueItem(item) {
  const items = Array.isArray(state.playerQueue) ? state.playerQueue : [];
  return items.some((entry) => playerItemsMatch(entry, item));
}

function buildMusicStatusBadges(item, { queueOnly = false } = {}) {
  const bits = [];
  const isDownloaded = !!(item?.local_path || item?.kind === "local" || isMusicAlbumDownloaded(item?.artist_key, item?.album_key) || isMusicArtistDownloaded(item?.artist_key));
  const isFavorited = !queueOnly && isMusicArtistFavorited(String(item?.artist || item?.name || ""), String(item?.artist_key || ""));
  const isQueued = isPlayerQueueItem(item);
  if (isDownloaded) bits.push(`<span class="music-status-badge is-downloaded">Downloaded</span>`);
  if (isFavorited) bits.push(`<span class="music-status-badge is-favorited">Favorited</span>`);
  if (isQueued) bits.push(`<span class="music-status-badge is-queued">In Queue</span>`);
  if (!bits.length && !queueOnly && !isDownloaded && item?.artist_key) {
    bits.push(`<span class="music-status-badge">Not Downloaded</span>`);
  }
  return bits.join("");
}

function queueTracksAtEnd(items = []) {
  const normalized = Array.isArray(items) ? items.map((entry) => normalizePlayableItem(entry)).filter(Boolean) : [];
  if (!normalized.length) return;
  clearActiveStationPlayback();
  const existing = Array.isArray(state.playerQueue) ? state.playerQueue.slice() : [];
  const merged = existing.slice();
  normalized.forEach((item) => {
    if (!merged.some((entry) => playerItemsMatch(entry, item))) {
      merged.push(item);
    }
  });
  setPlayerQueue(merged, { preserveCurrent: true });
  renderMusicPlayerQueue();
  syncBottomPlayerShell();
}

function queueTrackNext(item) {
  const normalized = normalizePlayableItem(item);
  if (!normalized) return;
  clearActiveStationPlayback();
  const existing = Array.isArray(state.playerQueue) ? state.playerQueue.slice() : [];
  const filtered = existing.filter((entry) => !playerItemsMatch(entry, normalized));
  const currentIndex = getCurrentQueueIndex();
  if (currentIndex >= 0 && currentIndex < filtered.length) {
    filtered.splice(currentIndex + 1, 0, normalized);
  } else {
    filtered.unshift(normalized);
  }
  setPlayerQueue(filtered, { preserveCurrent: true });
  renderMusicPlayerQueue();
  syncBottomPlayerShell();
}

function moveQueueItem(fromIndex, toIndex) {
  const queue = Array.isArray(state.playerQueue) ? state.playerQueue.slice() : [];
  if (fromIndex < 0 || fromIndex >= queue.length || toIndex < 0 || toIndex >= queue.length || fromIndex === toIndex) return;
  clearActiveStationPlayback();
  const [item] = queue.splice(fromIndex, 1);
  queue.splice(toIndex, 0, item);
  setPlayerQueue(queue, { preserveCurrent: true });
  renderMusicPlayerQueue();
  syncBottomPlayerShell();
}

function removeQueueItem(index) {
  const queue = Array.isArray(state.playerQueue) ? state.playerQueue.slice() : [];
  if (index < 0 || index >= queue.length) return;
  clearActiveStationPlayback();
  const [removed] = queue.splice(index, 1);
  const removedCurrent = state.playerCurrent && playerItemsMatch(state.playerCurrent, removed);
  setPlayerQueue(queue, { preserveCurrent: true });
  if (removedCurrent && !queue.length) {
    clearMusicPlayerCurrentState();
  }
  renderMusicPlayerQueue();
  syncBottomPlayerShell();
}

function clearQueue() {
  clearActiveStationPlayback();
  setPlayerQueue([], { preserveCurrent: true });
  if (!state.playerCurrent?.stream_url && !state.playerCurrent?.video_id) {
    clearMusicPlayerCurrentState();
  }
  renderMusicPlayerQueue();
  syncBottomPlayerShell();
}

function openLibraryDetailsModal(payload) {
  const modal = $("#library-details-modal");
  if (!modal || !payload) return;
  state.libraryModalPayload = payload;
  $("#library-details-title").textContent = String(payload.title || "Library Item");
  $("#library-details-subtitle").textContent = String(payload.subtitle || "").trim();
  $("#library-details-poster").src = String(payload.poster_url || "").trim() || "assets/no_artwork.png";
  $("#library-details-poster").alt = `${String(payload.title || "Artwork")} artwork`;
  $("#library-details-meta").innerHTML = Array.isArray(payload.metaChips) ? payload.metaChips.join("") : "";
  $("#library-details-status").textContent = String(payload.statusText || "").trim();
  $("#library-details-overview").textContent = String(payload.overview || "").trim();
  $("#library-details-extra").innerHTML = String(payload.extraHtml || "").trim();
  const primary = $("#library-details-primary");
  const secondary = $("#library-details-secondary");
  const tertiary = $("#library-details-tertiary");
  if (primary) {
    primary.textContent = String(payload.primaryLabel || "Open");
    primary.dataset.action = String(payload.primaryAction || "");
    primary.dataset.payload = JSON.stringify(payload.primaryPayload || {});
    primary.classList.toggle("hidden", !payload.primaryAction);
  }
  if (secondary) {
    secondary.textContent = String(payload.secondaryLabel || "More");
    secondary.dataset.action = String(payload.secondaryAction || "");
    secondary.dataset.payload = JSON.stringify(payload.secondaryPayload || {});
    secondary.classList.toggle("hidden", !payload.secondaryAction);
  }
  if (tertiary) {
    tertiary.textContent = String(payload.tertiaryLabel || "Download");
    tertiary.dataset.action = String(payload.tertiaryAction || "");
    tertiary.dataset.payload = JSON.stringify(payload.tertiaryPayload || {});
    tertiary.classList.toggle("hidden", !payload.tertiaryAction);
  }
  modal.classList.remove("hidden");
}

function closeLibraryDetailsModal() {
  const modal = $("#library-details-modal");
  if (modal) {
    modal.classList.add("hidden");
  }
  state.libraryModalPayload = null;
}

function openLibraryVideoModal(payload) {
  const modal = $("#library-video-modal");
  const player = $("#library-video-player");
  if (!modal || !player || !payload?.file_id) return;
  $("#library-video-title").textContent = String(payload.title || "Watch Video");
  $("#library-video-subtitle").textContent = [payload.source || payload.media_type, payload.file_ext].filter(Boolean).join(" • ");
  player.poster = String(payload.thumbnail_url || "").trim() || "assets/no_artwork.png";
  player.src = streamUrl(payload.file_id);
  modal.classList.remove("hidden");
}

function closeLibraryVideoModal() {
  const modal = $("#library-video-modal");
  const player = $("#library-video-player");
  if (player) {
    player.pause();
    player.removeAttribute("src");
    player.load();
  }
  if (modal) {
    modal.classList.add("hidden");
  }
}

function buildExternalPlayerTarget(item) {
  const cfg = state.config && typeof state.config === "object" ? state.config : {};
  const playback = (cfg.playback && typeof cfg.playback === "object") ? cfg.playback : {};
  const arr = (cfg.arr && typeof cfg.arr === "object") ? cfg.arr : {};
  const jellyfin = (arr.jellyfin && typeof arr.jellyfin === "object") ? arr.jellyfin : {};
  const mode = String(playback.external_player_mode || "none").trim().toLowerCase();
  const title = String(item?.title || "").trim();
  const sourceUrl = String(item?.source_url || item?.canonical_url || item?.input_url || "").trim();
  const payload = {
    title,
    file_id: String(item?.file_id || "").trim(),
    filepath: String(item?.filepath || "").trim(),
    source_url: sourceUrl,
    stream_url: item?.file_id ? `${window.location.origin}${streamUrl(item.file_id)}` : "",
    download_url: item?.file_id ? `${window.location.origin}${downloadUrl(item.file_id)}` : "",
  };
  if (mode === "jellyfin") {
    const baseUrl = String(jellyfin.base_url || "").trim().replace(/\/+$/, "");
    if (!baseUrl) return null;
    return {
      label: String(playback.external_player_label || "").trim() || "Watch in Jellyfin",
      url: `${baseUrl}/web/index.html#!/search.html?query=${encodeURIComponent(title)}`,
    };
  }
  if (mode === "plex") {
    const baseUrl = String(playback.plex_base_url || "").trim().replace(/\/+$/, "");
    if (!baseUrl) return null;
    return {
      label: String(playback.external_player_label || "").trim() || "Watch in Plex",
      url: `${baseUrl}/web/index.html#!/search?query=${encodeURIComponent(title)}`,
    };
  }
  if (mode === "custom") {
    const template = String(playback.external_player_url_template || "").trim();
    if (!template) return null;
    const expanded = {
      ...payload,
      encoded_title: encodeURIComponent(String(payload.title || "")),
      encoded_file_id: encodeURIComponent(String(payload.file_id || "")),
      encoded_filepath: encodeURIComponent(String(payload.filepath || "")),
      encoded_source_url: encodeURIComponent(String(payload.source_url || "")),
      encoded_stream_url: encodeURIComponent(String(payload.stream_url || "")),
      encoded_download_url: encodeURIComponent(String(payload.download_url || "")),
    };
    const url = template.replace(/\{(title|file_id|filepath|source_url|stream_url|download_url|encoded_title|encoded_file_id|encoded_filepath|encoded_source_url|encoded_stream_url|encoded_download_url)\}/g, (_match, key) => String(expanded[key] || ""));
    return {
      label: String(playback.external_player_label || "").trim() || "Open in External Player",
      url,
    };
  }
  return null;
}

function updatePlaybackIntegrationPreview() {
  const mode = String($("#cfg-playback-external-mode")?.value || "none").trim().toLowerCase();
  const label = String($("#cfg-playback-external-label")?.value || "").trim();
  const template = String($("#cfg-playback-external-template")?.value || "").trim();
  const plexBaseUrl = String($("#cfg-playback-plex-base-url")?.value || "").trim().replace(/\/+$/, "");
  const jellyfinBaseUrl = String($("#cfg-arr-jellyfin-base-url")?.value || "").trim().replace(/\/+$/, "");
  const previewLabelEl = $("#cfg-playback-preview-label");
  const previewUrlEl = $("#cfg-playback-preview-url");
  const sample = {
    title: "Example Movie",
    file_id: "abc123",
    filepath: "/media/movies/Example Movie (2025).mkv",
    source_url: "https://www.youtube.com/watch?v=example123",
    stream_url: `${window.location.origin}/api/files/abc123/stream`,
    download_url: `${window.location.origin}/api/files/abc123/download`,
  };
  const expanded = {
    ...sample,
    encoded_title: encodeURIComponent(sample.title),
    encoded_file_id: encodeURIComponent(sample.file_id),
    encoded_filepath: encodeURIComponent(sample.filepath),
    encoded_source_url: encodeURIComponent(sample.source_url),
    encoded_stream_url: encodeURIComponent(sample.stream_url),
    encoded_download_url: encodeURIComponent(sample.download_url),
  };
  let previewLabel = "No external player button";
  let previewUrl = "Launch URL preview appears here.";
  if (mode === "jellyfin") {
    previewLabel = label || "Watch in Jellyfin";
    previewUrl = jellyfinBaseUrl ? `${jellyfinBaseUrl}/web/index.html#!/search.html?query=${encodeURIComponent(sample.title)}` : "Enter your Jellyfin base URL in ARR Integration.";
  } else if (mode === "plex") {
    previewLabel = label || "Watch in Plex";
    previewUrl = plexBaseUrl ? `${plexBaseUrl}/web/index.html#!/search?query=${encodeURIComponent(sample.title)}` : "Enter your Plex base URL.";
  } else if (mode === "custom") {
    previewLabel = label || "Open in External Player";
    previewUrl = template
      ? template.replace(/\{(title|file_id|filepath|source_url|stream_url|download_url|encoded_title|encoded_file_id|encoded_filepath|encoded_source_url|encoded_stream_url|encoded_download_url)\}/g, (_match, key) => String(expanded[key] || ""))
      : "Enter a custom player URL template.";
  }
  if (previewLabelEl) previewLabelEl.textContent = `Button preview: ${previewLabel}`;
  if (previewUrlEl) previewUrlEl.textContent = `Launch URL: ${previewUrl}`;
}

function applyPlaybackPreset(presetId) {
  const preset = PLAYBACK_PRESETS[String(presetId || "").trim().toLowerCase()] || null;
  if (!preset) return;
  const modeEl = $("#cfg-playback-external-mode");
  const labelEl = $("#cfg-playback-external-label");
  const templateEl = $("#cfg-playback-external-template");
  if (modeEl) modeEl.value = preset.mode;
  if (labelEl) labelEl.value = preset.label;
  if (templateEl) templateEl.value = preset.template;
  updatePlaybackIntegrationPreview();
}

function parseLibraryActionPayload(raw) {
  try {
    return JSON.parse(String(raw || ""));
  } catch {
    return {};
  }
}

function buildMusicLibraryArtistModalPayload(artist) {
  const artistKey = String(artist?.artist_key || "").trim();
  const albums = getMusicLibraryFilteredAlbums().filter((entry) => String(entry.artist_key || "").trim() === artistKey).slice(0, 8);
  return {
    title: artist?.artist || "Artist",
    subtitle: "Music Library • Artist",
    poster_url: getMusicLibraryArtworkUrl(artist),
    statusText: `${artist?.album_count || 0} albums • ${artist?.track_count || 0} tracks`,
    overview: "Local library artist view. Browse albums or jump into the player for playback.",
    metaChips: [
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Albums</span><span class="arr-details-chip-value">${escapeHtml(String(artist?.album_count || 0))}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Tracks</span><span class="arr-details-chip-value">${escapeHtml(String(artist?.track_count || 0))}</span></span>`,
    ],
    extraHtml: albums.length
      ? `<div class="arr-details-cast-heading">Albums</div><div class="arr-details-cast-list">${albums.map((album) => `<span class="chip">${escapeHtml(String(album.album || "Album"))}</span>`).join("")}</div>`
      : `<div class="meta">No albums found for this artist yet.</div>`,
    primaryLabel: "Browse Albums",
    primaryAction: "music-library-open-artist",
    primaryPayload: { artist_key: artistKey },
    secondaryLabel: "Open Player",
    secondaryAction: "music-library-open-player",
    secondaryPayload: { artist_key: artistKey },
  };
}

function buildMusicLibraryAlbumModalPayload(album) {
  const artistKey = String(album?.artist_key || "").trim();
  const albumKey = String(album?.album_key || "").trim();
  const tracks = getMusicLibraryFilteredTracks()
    .filter((entry) => String(entry.artist_key || "").trim() === artistKey && String(entry.album_key || "").trim() === albumKey)
    .slice(0, 10);
  return {
    title: album?.album || "Album",
    subtitle: [album?.artist, "Music Library • Album"].filter(Boolean).join(" • "),
    poster_url: getMusicLibraryArtworkUrl(album),
    statusText: `${album?.track_count || 0} tracks`,
    overview: "Album view from your local library. Open tracks here or jump into the player for playback.",
    metaChips: [
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Artist</span><span class="arr-details-chip-value">${escapeHtml(String(album?.artist || "Unknown Artist"))}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Tracks</span><span class="arr-details-chip-value">${escapeHtml(String(album?.track_count || 0))}</span></span>`,
    ],
    extraHtml: tracks.length
      ? `<div class="arr-details-cast-heading">Track Preview</div><div class="arr-details-cast-list">${tracks.map((track) => `<span class="chip">${escapeHtml(String(track.title || "Track"))}</span>`).join("")}</div>`
      : `<div class="meta">No tracks found for this album yet.</div>`,
    primaryLabel: "View Tracks",
    primaryAction: "music-library-open-album",
    primaryPayload: { artist_key: artistKey, album_key: albumKey },
    secondaryLabel: "Open Player",
    secondaryAction: "music-library-open-player",
    secondaryPayload: { artist_key: artistKey, album_key: albumKey },
  };
}

function buildMusicLibraryTrackModalPayload(track) {
  const details = [
    ["Downloaded", formatTimestamp(track?.downloaded_at)],
    ["File", track?.local_path],
    ["Type", track?.file_ext],
    ["Media", track?.media_type],
    ["Size", formatBytes(track?.size_bytes)],
  ].filter((entry) => String(entry[1] || "").trim());
  return {
    title: track?.title || "Track",
    subtitle: [track?.artist, track?.album, "Music Library • Track"].filter(Boolean).join(" • "),
    poster_url: getMusicLibraryArtworkUrl(track),
    statusText: [formatTimestamp(track?.downloaded_at), formatBytes(track?.size_bytes), track?.file_ext].filter(Boolean).join(" • "),
    overview: String(track?.local_path || "").trim() || "Local library track ready for playback or playlist actions.",
    metaChips: [
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Artist</span><span class="arr-details-chip-value">${escapeHtml(String(track?.artist || "Unknown Artist"))}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Album</span><span class="arr-details-chip-value">${escapeHtml(String(track?.album || "Unknown Album"))}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Type</span><span class="arr-details-chip-value">${escapeHtml(String(track?.file_ext || "audio"))}</span></span>`,
    ],
    extraHtml: details.length
      ? `<div class="arr-details-cast-heading">File Details</div><div class="arr-details-cast-list">${details.map(([label, value]) => `<span class="chip">${escapeHtml(`${label}: ${String(value)}`)}</span>`).join("")}</div>`
      : "",
    primaryLabel: "Play Track",
    primaryAction: "music-library-play-track",
    primaryPayload: track,
    secondaryLabel: "Open Player",
    secondaryAction: "music-library-open-player",
    secondaryPayload: { artist_key: track?.artist_key || "", album_key: track?.album_key || "" },
  };
}

function buildVideoLibraryModalPayload(item) {
  const externalPlayer = buildExternalPlayerTarget(item);
  const detailRows = [
    ["Downloaded", formatTimestamp(item?.downloaded_at)],
    ["Size", formatBytes(item?.size_bytes)],
    ["File Type", item?.file_ext],
    ["Media Type", item?.media_type],
    ["Video ID", item?.video_id],
    ["File", item?.filepath],
    ["Source URL", item?.source_url || item?.canonical_url || item?.input_url],
  ].filter((entry) => String(entry[1] || "").trim());
  return {
    title: item?.title || "Video",
    subtitle: `${getVideoLibrarySourceBadge(item)} • Home Library`,
    poster_url: item?.thumbnail_url || "assets/no_artwork.png",
    statusText: [formatTimestamp(item?.downloaded_at), formatBytes(item?.size_bytes)].filter(Boolean).join(" • "),
    overview: String(item?.source_url || item?.canonical_url || item?.input_url || item?.filepath || "").trim(),
    metaChips: [
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Source</span><span class="arr-details-chip-value">${escapeHtml(getVideoLibrarySourceBadge(item))}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Downloaded</span><span class="arr-details-chip-value">${escapeHtml(formatTimestamp(item?.downloaded_at) || "Unknown")}</span></span>`,
      `<span class="arr-details-chip"><span class="arr-details-chip-label">Type</span><span class="arr-details-chip-value">${escapeHtml(String(item?.file_ext || "video"))}</span></span>`,
    ],
    extraHtml: detailRows.length
      ? `<div class="arr-details-cast-heading">File Details</div><div class="arr-details-cast-list">${detailRows.map(([label, value]) => `<span class="chip">${escapeHtml(`${label}: ${String(value)}`)}</span>`).join("")}</div>`
      : "",
    primaryLabel: "Watch Here",
    primaryAction: "video-library-watch-here",
    primaryPayload: item,
    secondaryLabel: externalPlayer?.label || "",
    secondaryAction: externalPlayer?.url ? "video-library-open-external" : "",
    secondaryPayload: externalPlayer?.url ? { ...item, external_url: externalPlayer.url } : {},
    tertiaryLabel: "Download File",
    tertiaryAction: "video-library-download",
    tertiaryPayload: item,
  };
}

function renderMusicLibrarySection() {
  const section = $("#music-library-section");
  const grid = $("#music-library-grid");
  const breadcrumbs = $("#music-library-breadcrumbs");
  const missingSection = $("#music-library-missing");
  const missingGrid = $("#music-library-missing-grid");
  if (!section || !grid || !breadcrumbs) return;
  const summary = state.playerLibrarySummary || { artists: [], albums: [], tracks: [] };
  const artists = Array.isArray(summary.artists) ? summary.artists : [];
  const albums = Array.isArray(summary.albums) ? summary.albums : [];
  const tracks = Array.isArray(summary.tracks) ? summary.tracks : [];
  const total = artists.length + albums.length + tracks.length;
  const shouldShowSection = total > 0 && state.currentPage === "music" && state.musicSection === "library";
  section.classList.toggle("hidden", !shouldShowSection);
  if (!total) {
    grid.innerHTML = `<div class="home-results-empty">No local music library files found yet.</div>`;
    breadcrumbs.innerHTML = "";
  }
  const missingItems = Array.isArray(state.playerMissingHistory) ? state.playerMissingHistory : [];
  if (missingSection && missingGrid) {
    missingSection.classList.toggle("hidden", !missingItems.length);
    if (missingItems.length) {
      missingGrid.innerHTML = missingItems.map((item) => `
        <article class="music-player-track-row music-player-track-row-rich">
          <div class="music-player-browser-card-art music-player-track-art">
            <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Missing track")}" loading="lazy">
          </div>
          <div class="music-player-track">
            <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
            <span class="music-player-track-meta">${escapeHtml([item.artist, item.played_at || "Previously played"].filter(Boolean).join(" • "))}</span>
            <div class="music-status-row">
              <span class="music-status-badge">Missing</span>
              <span class="music-status-badge">${escapeHtml(item.local_path ? "File missing" : "No local file")}</span>
            </div>
          </div>
          <div class="music-player-inline-actions">
            <button class="button ghost small" type="button" data-action="music-history-redownload" data-history-id="${escapeAttr(item.id || "")}" ${item.can_redownload ? "" : "disabled"}>Re-download</button>
            <button class="button ghost small" type="button" data-action="music-history-remove" data-history-id="${escapeAttr(item.id || "")}">Remove from History</button>
          </div>
        </article>
      `).join("");
    } else {
      missingGrid.innerHTML = "";
    }
  }
  if (!total) return;
  const mode = String(state.musicLibraryMode || "artists");
  const selectedArtist = getMusicLibrarySelectedArtist();
  const selectedAlbum = getMusicLibrarySelectedAlbum();
  const breadcrumbBits = [`<button class="button ghost small" type="button" data-action="music-library-reset">All Library</button>`];
  if (selectedArtist) {
    breadcrumbBits.push(`<button class="button ghost small" type="button" data-action="music-library-open-artist" data-artist-key="${escapeAttr(selectedArtist.artist_key || "")}">${escapeHtml(selectedArtist.artist || "Artist")}</button>`);
  }
  if (selectedAlbum) {
    breadcrumbBits.push(`<button class="button ghost small" type="button" data-action="music-library-open-album" data-artist-key="${escapeAttr(selectedAlbum.artist_key || "")}" data-album-key="${escapeAttr(selectedAlbum.album_key || "")}">${escapeHtml(selectedAlbum.album || "Album")}</button>`);
  }
  breadcrumbs.innerHTML = breadcrumbBits.join("");
  $$("[data-music-library-mode]").forEach((button) => {
    button.classList.toggle("active", button.dataset.musicLibraryMode === mode);
  });
  if (mode === "artists") {
    grid.innerHTML = artists.slice(0, 24).map((artist) => `
          <article class="home-result-card music-meta-card music-grid-card media-library-card" data-library-type="artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">
        <div class="music-card-thumb-shell media-library-thumb-shell">
          <img class="media-library-thumb" src="${escapeAttr(getMusicLibraryArtworkUrl(artist))}" alt="${escapeAttr(artist.artist || "Artist")}" loading="lazy">
        </div>
        <div class="music-meta-main">
          <div class="home-result-header">
            <div class="panel-title">${escapeHtml(artist.artist || "Unknown Artist")}</div>
          </div>
          <div class="meta">${escapeHtml(`${artist.album_count || 0} albums • ${artist.track_count || 0} tracks`)}</div>
        </div>
        <div class="home-candidate-action home-candidate-action-primary-stack">
          <button class="button ghost small" type="button" data-action="music-library-open-artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">Browse Albums</button>
          <button class="button ghost small" type="button" data-action="music-library-info" data-library-type="artist" data-artist-key="${escapeAttr(artist.artist_key || "")}">More Info</button>
        </div>
      </article>
    `).join("");
  } else if (mode === "albums") {
    const filteredAlbums = getMusicLibraryFilteredAlbums();
    grid.innerHTML = filteredAlbums.slice(0, 24).map((album) => `
      <article class="home-result-card music-meta-card music-grid-card media-library-card" data-library-type="album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">
        <div class="music-card-thumb-shell media-library-thumb-shell">
          <img class="media-library-thumb" src="${escapeAttr(getMusicLibraryArtworkUrl(album))}" alt="${escapeAttr(album.album || "Album")}" loading="lazy">
        </div>
        <div class="music-meta-main">
          <div class="home-result-header">
            <div class="panel-title">${escapeHtml(album.album || "Unknown Album")}</div>
          </div>
          <div class="meta">${escapeHtml([album.artist, `${album.track_count || 0} tracks`].filter(Boolean).join(" • "))}</div>
        </div>
        <div class="home-candidate-action home-candidate-action-primary-stack">
          <button class="button ghost small" type="button" data-action="music-library-open-album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">View Tracks</button>
          <button class="button ghost small" type="button" data-action="music-library-info" data-library-type="album" data-artist-key="${escapeAttr(album.artist_key || "")}" data-album-key="${escapeAttr(album.album_key || "")}">More Info</button>
        </div>
      </article>
    `).join("") || `<div class="home-results-empty">No albums available for this selection.</div>`;
  } else {
    const filteredTracks = getMusicLibraryFilteredTracks();
    grid.innerHTML = filteredTracks.slice(0, 24).map((track) => `
      <article class="home-result-card music-meta-card music-grid-card media-library-card media-library-track-card" data-library-type="track" data-track-id="${escapeAttr(track.id || "")}">
        <div class="music-card-thumb-shell media-library-thumb-shell">
          <img class="media-library-thumb" src="${escapeAttr(getMusicLibraryArtworkUrl(track))}" alt="${escapeAttr(track.title || "Track")}" loading="lazy">
        </div>
        <div class="music-meta-main">
          <div class="home-result-header">
            <div class="panel-title">${escapeHtml(track.title || "Untitled")}</div>
          </div>
          <div class="meta">${escapeHtml([track.artist, track.album].filter(Boolean).join(" • "))}</div>
        </div>
        <div class="home-candidate-action home-candidate-action-primary-stack">
          <button class="button ghost small" type="button" data-action="music-library-play-track" data-track-id="${escapeAttr(track.id || "")}">Play</button>
          <button class="button ghost small" type="button" data-action="music-library-info" data-library-type="track" data-track-id="${escapeAttr(track.id || "")}">More Info</button>
        </div>
      </article>
    `).join("") || `<div class="home-results-empty">No tracks available for this selection.</div>`;
  }
}

function renderVideoLibrarySection() {
  const section = $("#home-video-library-section");
  const grid = $("#home-video-library-grid");
  if (!section || !grid) return;
  const items = Array.isArray(state.videoLibraryItems) ? state.videoLibraryItems : [];
  section.classList.toggle("hidden", !items.length);
  if (!items.length) {
    grid.innerHTML = `<div class="home-results-empty">No local video downloads found yet.</div>`;
    return;
  }
  grid.innerHTML = items.map((item) => {
    const thumbnail = String(item.thumbnail_url || "").trim() || "assets/no_artwork.png";
    const sourceLabel = getVideoLibrarySourceBadge(item);
    const downloadHref = item.file_id ? downloadUrl(item.file_id) : "#";
    return `
      <article class="home-candidate-row media-library-card media-library-video-card" data-library-type="video" data-file-id="${escapeAttr(item.file_id || "")}">
        <div class="home-candidate-artwork media-library-video-artwork">
          <img src="${escapeAttr(thumbnail)}" alt="${escapeAttr(item.title || "Video")}" loading="lazy" onerror="this.onerror=null;this.src='assets/no_artwork.png';">
        </div>
        <div class="home-candidate-info">
          <div class="home-candidate-title-row">
            <div class="home-candidate-title">${escapeHtml(item.title || item.filename || "Video")}</div>
            <span class="home-candidate-source">${escapeHtml(sourceLabel)}</span>
          </div>
          <div class="home-candidate-meta">${escapeHtml([formatTimestamp(item.downloaded_at), formatBytes(item.size_bytes)].filter(Boolean).join(" • "))}</div>
        </div>
        <div class="home-candidate-action home-candidate-action-primary-stack">
          <a class="button ghost small home-candidate-download-primary" href="${downloadHref}">Download File</a>
          <button class="button ghost small" type="button" data-action="video-library-info" data-file-id="${escapeAttr(item.file_id || "")}">More Info</button>
        </div>
      </article>
    `;
  }).join("");
  renderVideoDiscoveryDefault();
}

function renderVideoDiscoveryDefault() {
  const grid = $("#video-discovery-preview-grid");
  const shell = $("#video-discovery-default");
  const recentBlock = $("#video-recent-searches-block");
  const recentGrid = $("#video-recent-searches");
  if (!grid || !shell) return;
  const items = Array.isArray(state.videoLibraryItems) ? state.videoLibraryItems.slice(0, 6) : [];
  const resultsVisible = !$("#home-results")?.classList.contains("hidden");
  const shouldShow = state.homeSection !== "library" && !resultsVisible && !state.homeSearchRequestId;
  shell.classList.toggle("hidden", !shouldShow);
  const recentSearches = loadRecentVideoSearches();
  if (recentBlock && recentGrid) {
    recentBlock.classList.toggle("hidden", !recentSearches.length);
    recentGrid.innerHTML = recentSearches.map((entry) => `
      <button class="button ghost video-recent-search-button" type="button" data-video-suggestion="${escapeAttr(entry)}">${escapeHtml(entry)}</button>
    `).join("");
  }
  if (!items.length) {
    grid.innerHTML = `<div class="home-results-empty">Recent video downloads will show up here once you start building your library.</div>`;
    return;
  }
  grid.innerHTML = items.map((item) => `
    <article class="home-candidate-row media-library-card media-library-video-card" data-library-type="video" data-file-id="${escapeAttr(item.file_id || "")}">
      <div class="home-candidate-artwork media-library-video-artwork">
        <img src="${escapeAttr(String(item.thumbnail_url || "").trim() || "assets/no_artwork.png")}" alt="${escapeAttr(item.title || "Video")}" loading="lazy" onerror="this.onerror=null;this.src='assets/no_artwork.png';">
      </div>
      <div class="home-candidate-info">
        <div class="home-candidate-title-row">
          <div class="home-candidate-title">${escapeHtml(item.title || item.filename || "Video")}</div>
          <span class="home-candidate-source">${escapeHtml(getVideoLibrarySourceBadge(item))}</span>
        </div>
        <div class="home-candidate-meta">${escapeHtml([formatTimestamp(item.downloaded_at), formatBytes(item.size_bytes)].filter(Boolean).join(" • "))}</div>
      </div>
      <div class="home-candidate-action home-candidate-action-primary-stack">
        <button class="button ghost small" type="button" data-action="video-library-info" data-file-id="${escapeAttr(item.file_id || "")}">More Info</button>
      </div>
    </article>
  `).join("");
}

async function hydrateMusicLibraryArtistCovers() {
  const artists = Array.isArray(state.playerLibrarySummary?.artists) ? state.playerLibrarySummary.artists.slice(0, 24) : [];
  await Promise.all(artists.map(async (artist) => {
    const key = String(artist.artist_key || "").trim();
    if (!key || getCachedArtistCoverUrl(key)) return;
    const persistent = await fetchPersistentArtistCoverEntry(key);
    if (persistent?.coverUrl) {
      setCachedArtistCoverUrl(key, persistent.coverUrl);
    }
  }));
  renderMusicLibrarySection();
}

async function loadMusicLibrarySection({ force = false } = {}) {
  const messageEl = $("#music-library-message");
  if (state.musicLibraryLoaded && !force && Array.isArray(state.playerLibrarySummary?.artists)) {
    renderMusicLibrarySection();
    return;
  }
  setMediaLibraryNotice(messageEl, "Loading music library…", false);
  try {
    const data = await fetchJson("/api/player/library/summary?limit=2000");
    state.playerLibrarySummary = (data?.summary && typeof data.summary === "object")
      ? data.summary
      : { artists: [], albums: [], tracks: [] };
    state.musicLibraryLoaded = true;
    renderMusicLibrarySection();
    setMediaLibraryNotice(messageEl, "", false);
  } catch (err) {
    setMediaLibraryNotice(messageEl, `Music library failed to load: ${toUserErrorMessage(err)}`, true);
  }
}

async function loadVideoLibrarySection({ force = false } = {}) {
  const messageEl = $("#home-video-library-message");
  if (state.videoLibraryLoaded && !force) {
    renderVideoLibrarySection();
    return;
  }
  setMediaLibraryNotice(messageEl, "Loading video library…", false);
  try {
    const data = await fetchJson("/api/library/videos?limit=24");
    state.videoLibraryItems = Array.isArray(data?.items) ? data.items : [];
    state.videoLibraryLoaded = true;
    renderVideoLibrarySection();
    setMediaLibraryNotice(messageEl, "", false);
  } catch (err) {
    setMediaLibraryNotice(messageEl, `Video library failed to load: ${toUserErrorMessage(err)}`, true);
  }
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
        setMusicPageNotice("Playlist import completed.", false);
        const summaryEl = $("#home-import-summary");
        if (summaryEl) {
          summaryEl.textContent =
            `Total: ${status.total_tracks || 0} | Resolved: ${status.resolved || 0} | ` +
            `Enqueued: ${status.enqueued || 0} | Unresolved: ${status.unresolved || 0}`;
        }
      } else {
        setMusicPageNotice(`Import failed: ${status.error || "unknown error"}`, true);
      }
    }
  } catch (err) {
    stopPlaylistImportPolling();
    state.playlistImportInProgress = false;
    setPlaylistImportControlsEnabled(true);
    setMusicPageNotice(`Import status failed: ${err.message}`, true);
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
  if (!(await ensureAdminPinSession())) {
    setConfigNotice("Admin PIN unlock is required before changing the config path.", true);
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
    setMusicPageNotice(`Status error: ${err.message}`, true);
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
    const page = state.currentPage || "home";
    modeToggle.closest(".home-mode-toggle-shell")?.classList.toggle("hidden", page === "home");
    modeToggle.querySelectorAll("button[data-mode]").forEach((button) => {
      const buttonMode = String(button.dataset.mode || "").trim();
      const isVideo = buttonMode === "video";
      const shouldHide = page === "music" ? isVideo : !isVideo;
      button.classList.toggle("hidden", shouldHide);
      const isActive = buttonMode === (state.homeMediaMode || "video");
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-checked", isActive ? "true" : "false");
    });
    const label = modeToggle.closest(".home-media-mode-group")?.querySelector(".home-media-mode-label");
    if (label) {
      label.textContent = page === "music" ? "Music Mode" : "Mode";
    }
  }
  const standardSearchContainer = $("#standard-search-container");
  if (standardSearchContainer) {
    standardSearchContainer.classList.toggle("hidden", !!state.homeMusicMode);
  }
  const musicModeConsole = $("#music-mode-console");
  if (musicModeConsole) {
    musicModeConsole.classList.toggle("hidden", !(state.homeMusicMode && state.musicAdvancedOpen));
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
  if (state.homeMusicMode && !state.homeMusicCurrentView) {
    renderMusicLanding();
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

function applyMusicCardSize(size) {
  const numeric = Number.parseInt(size, 10);
  const nextSize = Number.isFinite(numeric)
    ? Math.max(UI_CARD_SIZE_MIN, Math.min(UI_CARD_SIZE_MAX, numeric))
    : UI_DEFAULTS.music_card_size;
  state.musicCardSize = nextSize;
  const container = $("#music-results-container");
  if (container) {
    container.style.setProperty("--music-card-min", `${nextSize}px`);
  }
  const input = $("#music-card-size");
  if (input && String(input.value) !== String(nextSize)) {
    input.value = String(nextSize);
  }
  const valueLabel = $("#music-card-size-value");
  if (valueLabel) {
    valueLabel.textContent = nextSize === UI_DEFAULTS.music_card_size ? "Default" : `${nextSize}px`;
  }
}

function applyHomeVideoCardSize(size) {
  const numeric = Number.parseInt(size, 10);
  const nextSize = Number.isFinite(numeric)
    ? Math.max(UI_HOME_VIDEO_CARD_SIZE_MIN, Math.min(UI_HOME_VIDEO_CARD_SIZE_MAX, numeric))
    : UI_DEFAULTS.home_video_card_size;
  state.homeVideoCardSize = nextSize;
  const panel = $("#home-results");
  if (panel) {
    panel.style.setProperty("--home-video-card-min", `${nextSize}px`);
  }
  const list = $("#home-results-list");
  if (list) {
    list.style.setProperty("--home-video-card-min", `${nextSize}px`);
  }
  const input = $("#home-video-card-size");
  if (input && String(input.value) !== String(nextSize)) {
    input.value = String(nextSize);
  }
  const valueLabel = $("#home-video-card-size-value");
  if (valueLabel) {
    valueLabel.textContent = nextSize === UI_DEFAULTS.home_video_card_size ? "Default" : `${nextSize}px`;
  }
}

function getHomeCandidatePostedValue(candidate) {
  if (!candidate || typeof candidate !== "object") return 0;
  const direct = [
    candidate.posted_at,
    candidate.published_at,
    candidate.publish_date,
    candidate.upload_date,
  ];
  for (const value of direct) {
    const text = String(value || "").trim();
    if (!text) continue;
    const timestamp = Date.parse(text);
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
    if (/^\d{8}$/.test(text)) {
      const year = Number(text.slice(0, 4));
      const month = Number(text.slice(4, 6));
      const day = Number(text.slice(6, 8));
      const parsed = Date.UTC(year, month - 1, day);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return 0;
}

function getHomeCandidateSourcePriority(candidate) {
  const source = String(candidate?.source || "").trim().toLowerCase();
  const index = HOME_VIDEO_SOURCE_PRIORITY.indexOf(source);
  return index >= 0 ? index : HOME_VIDEO_SOURCE_PRIORITY.length;
}

function getSortedHomeCandidates(candidates = []) {
  const results = Array.isArray(candidates) ? [...candidates] : [];
  const sortMode = String(state.homeVideoSort || UI_DEFAULTS.home_video_sort);
  const byBestMatch = (a, b) => {
    const scoreDiff = Number(b?.final_score || 0) - Number(a?.final_score || 0);
    if (scoreDiff !== 0) return scoreDiff;
    const sourceDiff = getHomeCandidateSourcePriority(a) - getHomeCandidateSourcePriority(b);
    if (sourceDiff !== 0) return sourceDiff;
    const postedDiff = getHomeCandidatePostedValue(b) - getHomeCandidatePostedValue(a);
    if (postedDiff !== 0) return postedDiff;
    return String(a?.title || "").localeCompare(String(b?.title || ""));
  };
  if (sortMode === "newest") {
    return results.sort((a, b) => {
      const postedDiff = getHomeCandidatePostedValue(b) - getHomeCandidatePostedValue(a);
      if (postedDiff !== 0) return postedDiff;
      return byBestMatch(a, b);
    });
  }
  if (sortMode === "source_priority") {
    return results.sort((a, b) => {
      const sourceDiff = getHomeCandidateSourcePriority(a) - getHomeCandidateSourcePriority(b);
      if (sourceDiff !== 0) return sourceDiff;
      return byBestMatch(a, b);
    });
  }
  if (sortMode === "title_asc") {
    return results.sort((a, b) => {
      const titleDiff = String(a?.title || "").localeCompare(String(b?.title || ""));
      if (titleDiff !== 0) return titleDiff;
      return byBestMatch(a, b);
    });
  }
  return results.sort(byBestMatch);
}

function getMusicItemYearValue(item) {
  const text = String(item?.release_year || item?.latest_release_year || "").trim();
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function getSortedMusicArtists(artists = []) {
  const results = Array.isArray(artists) ? [...artists] : [];
  const sortMode = String(state.musicResultsSort || "recommended");
  const byRecommended = (a, b) => {
    const scoreA = Number.isFinite(Number(a?.recommended_score)) ? Number(a.recommended_score) : 0;
    const scoreB = Number.isFinite(Number(b?.recommended_score)) ? Number(b.recommended_score) : 0;
    if (scoreA !== scoreB) return scoreB - scoreA;
    const yearDiff = getMusicItemYearValue(b) - getMusicItemYearValue(a);
    if (yearDiff !== 0) return yearDiff;
    const releaseDiff = Number(b?.release_count || 0) - Number(a?.release_count || 0);
    if (releaseDiff !== 0) return releaseDiff;
    return String(a?.name || "").localeCompare(String(b?.name || ""));
  };
  if (sortMode === "newest_activity") {
    return results.sort((a, b) => {
      const yearDiff = getMusicItemYearValue(b) - getMusicItemYearValue(a);
      if (yearDiff !== 0) return yearDiff;
      return byRecommended(a, b);
    });
  }
  if (sortMode === "name_asc") {
    return results.sort((a, b) => {
      const diff = String(a?.name || "").localeCompare(String(b?.name || ""));
      if (diff !== 0) return diff;
      return byRecommended(a, b);
    });
  }
  if (sortMode === "release_count_desc") {
    return results.sort((a, b) => {
      const diff = Number(b?.release_count || 0) - Number(a?.release_count || 0);
      if (diff !== 0) return diff;
      return byRecommended(a, b);
    });
  }
  return results.sort(byRecommended);
}

function getSortedMusicAlbums(albums = []) {
  const results = Array.isArray(albums) ? [...albums] : [];
  const sortMode = String(state.musicResultsSort || "recommended");
  const byNewest = (a, b) => {
    const yearDiff = getMusicItemYearValue(b) - getMusicItemYearValue(a);
    if (yearDiff !== 0) return yearDiff;
    return String(a?.title || "").localeCompare(String(b?.title || ""));
  };
  if (sortMode === "oldest") {
    return results.sort((a, b) => {
      const yearDiff = getMusicItemYearValue(a) - getMusicItemYearValue(b);
      if (yearDiff !== 0) return yearDiff;
      return String(a?.title || "").localeCompare(String(b?.title || ""));
    });
  }
  if (sortMode === "title_asc" || sortMode === "name_asc") {
    return results.sort((a, b) => {
      const diff = String(a?.title || "").localeCompare(String(b?.title || ""));
      if (diff !== 0) return diff;
      return byNewest(a, b);
    });
  }
  return results.sort(byNewest);
}

function renderMusicResultsControls({ artists = [], albums = [], tracks = [], backButton = null } = {}) {
  const container = $("#music-results-container");
  const toolbarSlot = getMusicToolbarSlot();
  const navSlot = getMusicNavSlot();
  if (!container || !toolbarSlot || !navSlot) return;
  container.style.setProperty("--music-card-min", `${state.musicCardSize || UI_DEFAULTS.music_card_size}px`);
  toolbarSlot.querySelector(".music-results-toolbar")?.remove();
  navSlot.innerHTML = "";
  const hasArtists = Array.isArray(artists) && artists.length > 0;
  const hasAlbums = Array.isArray(albums) && albums.length > 0;
  const hasTracks = Array.isArray(tracks) && tracks.length > 0;
  const showSize = hasArtists || hasAlbums;
  const showSort = hasArtists || hasAlbums;
  if (!showSize && !showSort && !backButton) {
    toolbarSlot.innerHTML = "";
    return;
  }

  const toolbar = document.createElement("div");
  toolbar.className = "music-results-toolbar";
  if (backButton) {
    const nav = document.createElement("div");
    nav.className = "music-results-toolbar-nav";
    nav.appendChild(backButton);
    navSlot.appendChild(nav);
  }
  const actions = document.createElement("div");
  actions.className = "music-results-toolbar-actions";

  if (showSize) {
    const sizeLabel = document.createElement("label");
    sizeLabel.className = "music-results-size-control movies-tv-size-control";
    sizeLabel.setAttribute("for", "music-card-size");
    sizeLabel.innerHTML = `
      <span class="meta">Card Size</span>
      <input id="music-card-size" type="range" min="${UI_CARD_SIZE_MIN}" max="${UI_CARD_SIZE_MAX}" step="10" value="${state.musicCardSize || UI_DEFAULTS.music_card_size}">
      <span id="music-card-size-value" class="meta">${(state.musicCardSize || UI_DEFAULTS.music_card_size) === UI_DEFAULTS.music_card_size ? "Default" : `${state.musicCardSize}px`}</span>
    `;
    actions.appendChild(sizeLabel);
  }

  if (showSort) {
    const sortLabel = document.createElement("label");
    sortLabel.className = "music-results-sort-control movies-tv-sort-control";
    sortLabel.setAttribute("for", "music-results-sort");
    const artistOptions = `
      <option value="recommended">Recommended</option>
      <option value="newest_activity">Newest Activity</option>
      <option value="release_count_desc">Most Releases</option>
      <option value="name_asc">Name: A-Z</option>
    `;
    const albumOptions = `
      <option value="recommended">Newest Releases</option>
      <option value="oldest">Oldest Releases</option>
      <option value="title_asc">Title: A-Z</option>
    `;
    sortLabel.innerHTML = `
      <span class="meta">Sort</span>
      <select id="music-results-sort" class="input small">
        ${hasArtists ? artistOptions : albumOptions}
      </select>
    `;
    actions.appendChild(sortLabel);
  }
  toolbar.appendChild(actions);
  toolbarSlot.appendChild(toolbar);

  const sizeInput = $("#music-card-size");
  if (sizeInput) {
    sizeInput.addEventListener("input", () => applyMusicCardSize(sizeInput.value));
    sizeInput.addEventListener("change", () => persistUiPreferences({
      music_card_size: Number.parseInt(sizeInput.value, 10) || UI_DEFAULTS.music_card_size,
    }));
    applyMusicCardSize(sizeInput.value);
  }
  const sortSelect = $("#music-results-sort");
  if (sortSelect) {
    const allowed = new Set([...sortSelect.options].map((opt) => String(opt.value)));
    if (!allowed.has(String(state.musicResultsSort || "recommended"))) {
      state.musicResultsSort = hasArtists ? "recommended" : "recommended";
    }
    sortSelect.value = String(state.musicResultsSort || "recommended");
    sortSelect.addEventListener("change", () => {
      state.musicResultsSort = String(sortSelect.value || "recommended");
      persistUiPreferences({ music_sort: state.musicResultsSort });
      if (state.homeMusicCurrentView?.response) {
        renderMusicModeResults(state.homeMusicCurrentView.response, state.homeMusicCurrentView.query, { pushHistory: false });
      }
    });
  }
}

function resetLegacyMusicSearchInputs() {
  const artistInput = document.getElementById("search-artist");
  const albumInput = document.getElementById("search-album");
  const trackInput = document.getElementById("search-track");
  if (artistInput) artistInput.value = "";
  if (albumInput) albumInput.value = "";
  if (trackInput) trackInput.value = "";
}

async function enrichGenreArtistsForRecommendation(artists = []) {
  const items = Array.isArray(artists) ? artists : [];
  if (!items.length) return [];
  const concurrency = 4;
  let index = 0;
  const enriched = new Array(items.length);

  const scoreArtist = (artist, albums) => {
    const latestReleaseYear = albums.reduce((max, album) => Math.max(max, getMusicItemYearValue(album)), 0);
    const currentYear = new Date().getFullYear();
    const yearsAgo = latestReleaseYear > 0 ? Math.max(0, currentYear - latestReleaseYear) : 50;
    const recencyScore = latestReleaseYear > 0 ? Math.max(0, 40 - Math.min(40, yearsAgo * 6)) : 0;
    const releaseCount = albums.length;
    const releaseScore = Math.min(22, releaseCount * 2.2);
    const mbScore = Number.isFinite(Number(artist?.source_score)) ? Math.min(25, Number(artist.source_score) * 0.25) : 0;
    const metadataScore = [
      artist?.country,
      artist?.disambiguation,
      artist?.artist_mbid,
    ].filter(Boolean).length * 3;
    const freshnessBoost = latestReleaseYear >= currentYear - 2 ? 16 : latestReleaseYear >= currentYear - 5 ? 10 : 0;
    return {
      latest_release_year: latestReleaseYear || null,
      release_count: releaseCount,
      recommended_score: Math.round((recencyScore + releaseScore + mbScore + metadataScore + freshnessBoost) * 10) / 10,
    };
  };

  const worker = async () => {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      const artist = items[currentIndex];
      try {
        const albums = await fetchMusicAlbumsByArtist({
          name: String(artist?.name || "").trim(),
          artist_mbid: String(artist?.artist_mbid || "").trim(),
        });
        enriched[currentIndex] = {
          ...artist,
          ...scoreArtist(artist, albums),
        };
      } catch (_err) {
        enriched[currentIndex] = {
          ...artist,
          latest_release_year: null,
          release_count: 0,
          recommended_score: Number.isFinite(Number(artist?.source_score)) ? Number(artist.source_score) * 0.2 : 0,
        };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return enriched.filter(Boolean);
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
  if (nextMode !== "video") {
    state.lastMusicMode = nextMode;
  }
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
    setNotice(messageEl || getMusicPageMessageEl(), "Client delivery does not use a server destination.", true);
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
  renderVideoDiscoveryDefault();
  mountVideoToolbar();
}

function setHomeSearchActive(active) {
  const shell = state.currentPage === "music"
    ? document.querySelector("#music-panel .music-page-shell")
    : document.querySelector("#home-search-surface");
  document.body.classList.toggle("search-active", !!active);
  if (shell) {
    shell.classList.toggle("search-active", !!active);
  }
}

function clearVideoHeaderSearch() {
  const input = $("#home-search-input");
  if (input) {
    input.value = "";
  }
  setNotice($("#home-search-message"), "", false);
  resetHomeIntentConfirmation();
  renderVideoDiscoveryDefault();
}

function clearMusicHeaderSearch() {
  const queryInput = $("#music-header-query");
  if (queryInput) queryInput.value = "";
  if ($("#search-artist")) $("#search-artist").value = "";
  if ($("#search-album")) $("#search-album").value = "";
  if ($("#search-track")) $("#search-track").value = "";
  clearMusicResultsHistory();
  setMusicPageNotice("", false);
  renderMusicLanding();
  setMusicSection("browse");
}

function clearMoviesTvHeaderSearch() {
  const input = $("#movies-tv-search-input");
  if (input) {
    input.value = "";
  }
  state.arrResults = [];
  state.arrSearchQuery = "";
  state.arrSearchContext = "search";
  state.arrActiveGenre = null;
  setMoviesTvSection("search");
  renderArrResults();
  setNotice($("#movies-tv-message"), "", false);
}

function setHomeResultsState({ hasResults = false, terminal = false } = {}) {
  const section = $("#home-results");
  if (!section) return;
  section.classList.toggle("has-results", !!hasResults);
  section.classList.toggle("search-complete", !!terminal);
}

function focusHomeResults(options = {}) {
  const musicResults = $("#music-results-container");
  const homeResults = $("#home-results");
  const target =
    musicResults && musicResults.childElementCount > 0
      ? musicResults
      : homeResults;

  if (!target || target.classList.contains("hidden")) {
    return;
  }

  const behavior = options.instant ? "auto" : "smooth";
  target.scrollIntoView({ behavior, block: "start" });
}

function normalizeArrMode(mode) {
  return String(mode || "").trim().toLowerCase() === "tv" ? "tv" : "movies";
}

function getArrKind() {
  return state.arrMode === "tv" ? "tv" : "movie";
}

function getArrServiceName() {
  return state.arrMode === "tv" ? "Sonarr" : "Radarr";
}

function getArrServiceKey() {
  return state.arrMode === "tv" ? "sonarr" : "radarr";
}

function getArrGenreKind() {
  return getArrKind();
}

function normalizeArrGenreQuery(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function getCurrentArrGenres() {
  const kind = getArrGenreKind();
  return Array.isArray(state.arrGenres[kind]) ? state.arrGenres[kind] : [];
}

function findArrGenreMatch(query) {
  const normalizedQuery = normalizeArrGenreQuery(query);
  if (!normalizedQuery) return null;
  const genres = getCurrentArrGenres();
  return genres.find((item) => normalizeArrGenreQuery(item?.name) === normalizedQuery) || null;
}

function isTmdbConfigured() {
  const tmdbApiKey = String(state.config?.arr?.tmdb_api_key || "").trim();
  return !!tmdbApiKey;
}

function renderMoviesTvSetupGate() {
  const landing = $("#movies-tv-setup-landing");
  const searchRow = getMoviesTvSearchRowEl();
  const filtersPanel = getMoviesTvFiltersPanelEl();
  const sectionToggle = $("#movies-tv-panel .movies-tv-section-toggle");
  const messageRegion = $("#movies-tv-message-region");
  const setupView = $("#movies-tv-setup-view");
  const discoveryView = $("#movies-tv-discovery-view");
  const genresView = $("#movies-tv-genres-view");
  const resultsView = $("#movies-tv-results-view");
  const configured = isTmdbConfigured();
  if (landing) {
    landing.classList.toggle("hidden", configured);
  }
  if (searchRow) {
    searchRow.classList.toggle("hidden", !configured);
  }
  if (filtersPanel && !configured) {
    filtersPanel.classList.add("hidden");
  }
  if (sectionToggle) {
    sectionToggle.classList.toggle("hidden", !configured);
  }
  if (messageRegion) {
    messageRegion.classList.toggle("hidden", !configured);
  }
  if (setupView) {
    setupView.classList.toggle("hidden", configured);
  }
  if (!configured) {
    state.moviesTvSection = "search";
  }
  applyMoviesTvWorkspaceState();
  if (state.currentPage === "movies-tv") {
    mountTopbarForPage("movies-tv");
  }
  return configured;
}

async function loadArrGenres() {
  if (!renderMoviesTvSetupGate()) {
    return [];
  }
  const kind = getArrGenreKind();
  if (Array.isArray(state.arrGenres[kind]) && state.arrGenres[kind].length) {
    renderArrGenreShelf();
    loadArrEditorialShelves().catch(() => {});
    return state.arrGenres[kind];
  }
  const data = await fetchJson(`/api/arr/genres?kind=${encodeURIComponent(kind)}`);
  state.arrGenres[kind] = Array.isArray(data?.genres) ? data.genres : [];
  renderArrGenreShelf();
  loadArrEditorialShelves().catch(() => {});
  return state.arrGenres[kind];
}

function applyArrGenreTileArtwork(tile, artworkUrls) {
  if (!tile) return;
  const normalizedUrls = normalizeGenreArtworkSet(artworkUrls);
  const cells = [...tile.querySelectorAll(".movies-tv-genre-collage-cell")];
  cells.forEach((cell, index) => {
    const img = cell.querySelector("img");
    const nextUrl = normalizedUrls[index] || "";
    if (img && nextUrl) {
      img.src = nextUrl;
      img.alt = "";
      cell.classList.add("has-artwork");
    } else {
      if (img) {
        img.removeAttribute("src");
        img.alt = "";
      }
      cell.classList.remove("has-artwork");
    }
  });
  tile.classList.toggle("has-artwork", normalizedUrls.length > 0);
}

function queueArrGenreArtworkJob(genre, tile, renderToken) {
  const kind = getArrGenreKind();
  const genreId = String(genre?.id || "").trim();
  if (!genreId || !tile) return;
  const cached = getCachedArrGenreCoverUrls(kind, genreId);
  if (cached.length) {
    applyArrGenreTileArtwork(tile, cached);
    return;
  }
  window.setTimeout(async () => {
    if (state.arrGenreRenderToken !== renderToken) return;
    try {
      const persistent = await fetchPersistentArrGenreCoverEntry(kind, genreId);
      if (persistent?.coverUrls?.length) {
        setCachedArrGenreCoverUrls(kind, genreId, persistent.coverUrls);
        if (state.arrGenreRenderToken === renderToken) {
          applyArrGenreTileArtwork(tile, persistent.coverUrls);
        }
        if (!isArtworkCacheStale(persistent.updatedAt)) {
          return;
        }
      }
      const data = await fetchJson(
        `/api/arr/genre/browse?kind=${encodeURIComponent(kind)}&genre_id=${encodeURIComponent(genreId)}&limit=8`
      );
      const results = Array.isArray(data?.results) ? data.results : [];
      const coverUrls = normalizeGenreArtworkSet(results.map((item) => item?.poster_url));
      if (!coverUrls.length) return;
      setCachedArrGenreCoverUrls(kind, genreId, coverUrls);
      await persistArrGenreCoverEntry(kind, genreId, coverUrls);
      if (state.arrGenreRenderToken === renderToken) {
        applyArrGenreTileArtwork(tile, coverUrls);
      }
    } catch {
      // leave fallback theme if browse artwork load fails
    }
  }, 0);
}

function createArrGenreCard(genre, renderToken) {
  const genreId = String(genre?.id || "").trim();
  const genreName = String(genre?.name || "").trim() || "Genre";
  const card = document.createElement("article");
  card.className = "home-result-card movies-tv-genre-card";
  card.dataset.genreId = genreId;
  const tile = document.createElement("div");
  tile.className = "movies-tv-genre-tile";
  const collage = document.createElement("div");
  collage.className = "movies-tv-genre-collage";
  for (let i = 0; i < 4; i += 1) {
    const cell = document.createElement("div");
    cell.className = "movies-tv-genre-collage-cell";
    const img = document.createElement("img");
    img.loading = "lazy";
    img.alt = "";
    cell.appendChild(img);
    collage.appendChild(cell);
  }
  tile.appendChild(collage);
  const label = document.createElement("span");
  label.className = "movies-tv-genre-tile-label";
  label.textContent = genreName;
  tile.appendChild(label);
  card.appendChild(tile);
  const content = document.createElement("div");
  content.className = "movies-tv-genre-card-body";
  content.innerHTML = `
    <div class="home-candidate-title">${escapeHtml(genreName)}</div>
    <div class="home-candidate-meta">Open top recent titles in this genre</div>
  `;
  card.appendChild(content);
  const runBrowseGenre = () => browseArrGenre(genre);
  card.addEventListener("click", (event) => {
    if (event.target.closest("button, a, input, select, textarea")) return;
    runBrowseGenre();
  });
  [tile, content].forEach((el) => {
    if (!el) return;
    el.classList.add("music-card-click-target");
  });
  queueArrGenreArtworkJob(genre, tile, renderToken);
  return card;
}

function renderArrGenreShelf() {
  const section = $("#movies-tv-genres-view");
  const listEl = $("#movies-tv-genres-list");
  const statusTextEl = $("#movies-tv-genres-status-text");
  if (!section || !listEl || !statusTextEl) return;
  const genres = getCurrentArrGenres();
  if (!genres.length) {
    listEl.innerHTML = "";
    statusTextEl.textContent = "No genres loaded";
    return;
  }
  state.arrGenreRenderToken += 1;
  const renderToken = state.arrGenreRenderToken;
  listEl.innerHTML = "";
  genres.slice(0, 10).forEach((genre) => {
    listEl.appendChild(createArrGenreCard(genre, renderToken));
  });
  statusTextEl.textContent = `${Math.min(genres.length, 10)} genres ready`;
  setMoviesTvSection(state.moviesTvSection || "search");
}

async function browseArrGenre(genre, { year = "" } = {}) {
  const genreId = Number.parseInt(String(genre?.id || "").trim(), 10);
  const genreName = String(genre?.name || "").trim() || "Genre";
  if (!Number.isFinite(genreId)) return;
  const messageEl = $("#movies-tv-message");
  const yearValue = /^\d{4}$/.test(String(year || "").trim()) ? String(year).trim() : "";
  const kind = getArrGenreKind();
  const cacheKey = getArrGenreBrowseCacheKey(kind, genreId, yearValue);
  const cached = state.arrGenreBrowseCache[cacheKey];
  if (cached && Array.isArray(cached.results)) {
    state.arrActiveGenre = { id: genreId, name: genreName };
    state.arrSearchContext = "genre";
    state.arrSearchQuery = genreName;
    setMoviesTvSearchYear(yearValue);
    state.arrResults = cached.results.map((item, index) => ({ ...item, _search_rank: index }));
    state.arrConnectionStatus[getArrServiceKey()] = cached.connection || state.arrConnectionStatus[getArrServiceKey()];
    renderArrConnectionStatus();
    renderArrResults();
    setMoviesTvSection("search");
    focusMoviesTvResults();
    startArrStatusPolling();
    setNotice(messageEl, `Loaded top ${genreName} titles.`, false);
    return;
  }
  renderArrBrowseLoading(`Loading top ${genreName} titles…`, yearValue ? `Top ${kind === "tv" ? "TV shows" : "movies"} in ${genreName} for ${yearValue}` : `Top ${kind === "tv" ? "TV shows" : "movies"} in ${genreName}`);
  setNotice(messageEl, `Loading top ${genreName} titles…`, false);
  try {
    const params = new URLSearchParams();
    params.set("kind", kind);
    params.set("genre_id", String(genreId));
    params.set("limit", "24");
    if (yearValue) {
      params.set("year", yearValue);
    }
    const data = await fetchJson(`/api/arr/genre/browse?${params.toString()}`);
    state.arrGenreBrowseCache[cacheKey] = {
      results: Array.isArray(data?.results) ? data.results : [],
      connection: data?.connection || null,
    };
    state.arrActiveGenre = { id: genreId, name: genreName };
    state.arrSearchContext = "genre";
    state.arrSearchQuery = genreName;
    setMoviesTvSearchYear(yearValue);
    state.arrResults = Array.isArray(data?.results)
      ? data.results.map((item, index) => ({ ...item, _search_rank: index }))
      : [];
    state.arrConnectionStatus[getArrServiceKey()] = data?.connection || state.arrConnectionStatus[getArrServiceKey()];
    renderArrConnectionStatus();
    renderArrResults();
    setMoviesTvSection("search");
    focusMoviesTvResults();
    startArrStatusPolling();
    setNotice(messageEl, state.arrResults.length ? `Loaded top ${genreName} titles.` : `No ${genreName} titles found.`, false);
  } catch (err) {
    setNotice(messageEl, `Genre browse failed: ${toUserErrorMessage(err)}`, true);
  }
}

function focusMoviesTvResults(options = {}) {
  const target = $("#movies-tv-results-view");
  if (!target || target.classList.contains("hidden")) {
    return;
  }
  target.scrollIntoView({ behavior: options.instant ? "auto" : "smooth", block: "start" });
}

function formatArrConnectionText(label, status) {
  const info = status && typeof status === "object" ? status : {};
  if (!info.configured) {
    return `${label}: Not configured`;
  }
  if (info.reachable) {
    return `${label}: Connected`;
  }
  return `${label}: Unavailable`;
}

function applyArrConnectionState(target, label, status) {
  if (!target) return;
  target.textContent = formatArrConnectionText(label, status);
  target.classList.toggle("running", !!status?.reachable);
  target.classList.toggle("warning", !!status?.configured && !status?.reachable);
}

function renderArrConnectionStatus() {
  applyArrConnectionState($("#movies-tv-radarr-status"), "Radarr", state.arrConnectionStatus.radarr);
  applyArrConnectionState($("#movies-tv-sonarr-status"), "Sonarr", state.arrConnectionStatus.sonarr);
  const radarrStatus = $("#arr-test-radarr-status");
  if (radarrStatus) {
    radarrStatus.textContent = String(state.arrConnectionStatus.radarr?.message || "Status unknown");
  }
  const sonarrStatus = $("#arr-test-sonarr-status");
  if (sonarrStatus) {
    sonarrStatus.textContent = String(state.arrConnectionStatus.sonarr?.message || "Status unknown");
  }
}

async function refreshArrConnectionStatus({ quiet = false } = {}) {
  renderMoviesTvSetupGate();
  const [radarrResult, sonarrResult] = await Promise.allSettled([
    fetchJson("/api/arr/radarr/health"),
    fetchJson("/api/arr/sonarr/health"),
  ]);
  if (radarrResult.status === "fulfilled") {
    state.arrConnectionStatus.radarr = radarrResult.value;
  } else if (!quiet) {
    state.arrConnectionStatus.radarr = {
      configured: true,
      reachable: false,
      message: toUserErrorMessage(radarrResult.reason),
    };
  }
  if (sonarrResult.status === "fulfilled") {
    state.arrConnectionStatus.sonarr = sonarrResult.value;
  } else if (!quiet) {
    state.arrConnectionStatus.sonarr = {
      configured: true,
      reachable: false,
      message: toUserErrorMessage(sonarrResult.reason),
    };
  }
  renderArrConnectionStatus();
}

async function warmArrEditorialKind(kind) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (!normalizedKind || !isTmdbConfigured()) return;
  const existing = state.arrEditorial?.[normalizedKind];
  if (existing && Array.isArray(existing.shelves) && existing.shelves.length) {
    return;
  }
  try {
    const shelfMeta = await fetchJson(`/api/arr/editorial/shelves?kind=${encodeURIComponent(normalizedKind)}`);
    const defaultShelves = (Array.isArray(shelfMeta?.shelves) ? shelfMeta.shelves : [])
      .filter((entry) => entry?.default_visible)
      .slice(0, 4);
    const shelfPayloads = await Promise.all(
      defaultShelves.map(async (entry) => {
        const payload = await fetchJson(
          `/api/arr/editorial/shelf?kind=${encodeURIComponent(normalizedKind)}&shelf=${encodeURIComponent(String(entry.id || ""))}&limit=10`
        );
        return {
          id: String(entry.id || ""),
          title: String(entry.title || ""),
          subtitle: String(entry.subtitle || ""),
          provider: String(payload?.provider || entry.provider || ""),
          results: Array.isArray(payload?.results) ? payload.results : [],
        };
      })
    );
    state.arrEditorial = state.arrEditorial || {};
    state.arrEditorial[normalizedKind] = { shelves: shelfPayloads };
    if (normalizedKind === getArrGenreKind()) {
      renderArrEditorialShelves();
    }
  } catch {
    // best-effort warm only
  }
}

function warmArrEditorialShelvesOnStartup() {
  if (!isTmdbConfigured()) return;
  window.setTimeout(() => {
    warmArrEditorialKind("movie").catch(() => {});
    warmArrEditorialKind("tv").catch(() => {});
  }, 0);
}

function getArrStatusDescriptor(status) {
  const normalized = status && typeof status === "object" ? status : {};
  if (normalized.downloaded) {
    return { label: "Downloaded", className: "matched", detail: "ARR reports this item is downloaded and available." };
  }
  if (normalized.added) {
    return { label: "Added to ARR", className: "queued", detail: "ARR accepted this item and is managing it." };
  }
  return { label: "Not added", className: "", detail: "This item has not been added to ARR yet." };
}

function openArrDetailsModal(item) {
  const modal = $("#arr-details-modal");
  const titleEl = $("#arr-details-title");
  const posterEl = $("#arr-details-poster");
  const metaEl = $("#arr-details-meta");
  const statusEl = $("#arr-details-status");
  const overviewEl = $("#arr-details-overview");
  const castEl = $("#arr-details-cast");
  const tmdbLink = $("#arr-details-tmdb-link");
  const addButton = $("#arr-details-add");
  const trailerButton = $("#arr-details-trailer");
  if (!modal || !titleEl || !posterEl || !metaEl || !statusEl || !overviewEl || !castEl || !tmdbLink || !addButton || !trailerButton || !item) {
    return;
  }
  const status = getArrStatusDescriptor(item.arr_status);
  const serviceKey = item.kind === "tv" ? "sonarr" : "radarr";
  const serviceName = item.kind === "tv" ? "Sonarr" : "Radarr";
  const serviceStatus = state.arrConnectionStatus[serviceKey] || {};
  const chips = [];
  if (item.year) {
    chips.push(`<span class="arr-details-chip"><span class="arr-details-chip-label">Year</span><span class="arr-details-chip-value">${escapeHtml(String(item.year))}</span></span>`);
  }
  if (item.original_title && String(item.original_title).trim().toLowerCase() !== String(item.title || "").trim().toLowerCase()) {
    chips.push(`<span class="arr-details-chip"><span class="arr-details-chip-label">Original</span><span class="arr-details-chip-value">${escapeHtml(String(item.original_title).trim())}</span></span>`);
  }
  if (item.language) {
    chips.push(`<span class="arr-details-chip"><span class="arr-details-chip-label">Language</span><span class="arr-details-chip-value">${escapeHtml(String(item.language).trim().toUpperCase())}</span></span>`);
  }
  if (Number.isFinite(Number(item.rating))) {
    chips.push(`<span class="arr-details-chip"><span class="arr-details-chip-icon">★</span><span class="arr-details-chip-label">Rating</span><span class="arr-details-chip-value">${escapeHtml(Number(item.rating).toFixed(1))}/10</span></span>`);
  }
  if (Number.isFinite(Number(item.popularity))) {
    const popularityValue = Number(item.popularity);
    const popularityIcon = popularityValue >= ARR_POPULARITY_FRESH_THRESHOLD ? "🍅" : "🟢";
    chips.push(`<span class="arr-details-chip"><span class="arr-details-chip-icon">${popularityIcon}</span><span class="arr-details-chip-label">Popularity</span><span class="arr-details-chip-value">${escapeHtml(popularityValue.toFixed(1))}</span></span>`);
  }
  titleEl.textContent = String(item.title || "Details");
  posterEl.src = String(item.poster_url || "").trim() || "assets/no_artwork.png";
  posterEl.alt = `${String(item.title || "Poster")} poster`;
  metaEl.innerHTML = chips.join("");
  statusEl.textContent = status.detail;
  statusEl.classList.toggle("running", !!item?.arr_status?.downloaded);
  statusEl.classList.toggle("warning", !!item?.arr_status?.added && !item?.arr_status?.downloaded);
  overviewEl.textContent = String(item.overview || "No overview available.").trim();
  castEl.innerHTML = `<div class="meta">Loading cast...</div>`;
  tmdbLink.href = String(item.tmdb_url || "#").trim() || "#";
  addButton.textContent = serviceStatus.reachable ? `Add to ${serviceName}` : `${serviceName} Unavailable`;
  addButton.disabled = !serviceStatus.reachable;
  addButton.dataset.tmdbId = String(item.tmdb_id || "");
  trailerButton.disabled = !String(item.tmdb_id || "").trim();
  trailerButton.dataset.tmdbId = String(item.tmdb_id || "");
  state.arrDetailsItemId = String(item.tmdb_id || "");
  modal.classList.remove("hidden");
  updatePollingState();
  void loadArrCast(item);
}

function closeArrDetailsModal() {
  const modal = $("#arr-details-modal");
  if (modal) {
    modal.classList.add("hidden");
  }
  state.arrDetailsItemId = null;
  updatePollingState();
}

function renderArrCastList(tmdbId, cast = []) {
  const castEl = $("#arr-details-cast");
  if (!castEl) return;
  if (String(state.arrDetailsItemId || "") !== String(tmdbId || "")) {
    return;
  }
  const rows = Array.isArray(cast) ? cast : [];
  if (!rows.length) {
    castEl.innerHTML = `<div class="meta">No cast information available.</div>`;
    return;
  }
  castEl.innerHTML = rows.map((person) => {
    const personId = escapeHtml(String(person.person_id || ""));
    const name = escapeHtml(String(person.name || "Unknown"));
    const character = escapeHtml(String(person.character || "").trim());
    return `
      <button class="button ghost small arr-cast-button" type="button" data-action="arr-person" data-person-id="${personId}">
        <span class="arr-cast-name">${name}</span>
        ${character ? `<span class="arr-cast-character">as ${character}</span>` : ""}
      </button>
    `;
  }).join("");
}

async function loadArrCast(item) {
  const tmdbId = String(item?.tmdb_id || "").trim();
  if (!tmdbId) return;
  const castKind = String(item?.kind || getArrKind()).trim().toLowerCase() || getArrKind();
  const cacheKey = `${castKind}:${tmdbId}`;
  const cached = state.arrCastCache[cacheKey];
  if (Array.isArray(cached)) {
    renderArrCastList(tmdbId, cached);
    return;
  }
  try {
    const data = await fetchJson(
      `/api/arr/cast?kind=${encodeURIComponent(castKind)}&tmdb_id=${encodeURIComponent(tmdbId)}&limit=8`
    );
    const cast = Array.isArray(data?.cast) ? data.cast : [];
    state.arrCastCache[cacheKey] = cast;
    renderArrCastList(tmdbId, cast);
  } catch {
    state.arrCastCache[cacheKey] = [];
    renderArrCastList(tmdbId, []);
  }
}

async function openArrPersonTitles(personId) {
  const numeric = Number.parseInt(String(personId || "").trim(), 10);
  if (!Number.isFinite(numeric)) return;
  const messageEl = $("#movies-tv-message");
  const kind = getArrKind();
  const cacheKey = getArrPersonTitlesCacheKey(kind, numeric);
  const cached = state.arrPersonTitlesCache[cacheKey];
  if (cached && Array.isArray(cached.results)) {
    const personName = String(cached.person_name || "Person").trim() || "Person";
    state.arrActiveGenre = null;
    state.arrSearchContext = "person";
    state.arrSearchQuery = personName;
    setMoviesTvSearchYear("");
    state.arrResults = cached.results.map((item, index) => ({ ...item, _search_rank: index }));
    closeArrDetailsModal();
    renderArrResults();
    setMoviesTvSection("search");
    focusMoviesTvResults();
    startArrStatusPolling();
    setNotice(messageEl, state.arrResults.length ? `Loaded titles for ${personName}.` : `No titles found for ${personName}.`, false);
    return;
  }
  renderArrBrowseLoading("Loading filmography…", "Loading person filmography");
  setNotice(messageEl, "Loading filmography…", false);
  try {
    const data = await fetchJson(
      `/api/arr/person?person_id=${encodeURIComponent(String(numeric))}&kind=${encodeURIComponent(kind)}&limit=24`
    );
    state.arrPersonTitlesCache[cacheKey] = {
      person_name: data?.person_name || "",
      results: Array.isArray(data?.results) ? data.results : [],
    };
    const personName = String(data?.person_name || "Person").trim() || "Person";
    state.arrActiveGenre = null;
    state.arrSearchContext = "person";
    state.arrSearchQuery = personName;
    setMoviesTvSearchYear("");
    state.arrResults = Array.isArray(data?.results)
      ? data.results.map((item, index) => ({ ...item, _search_rank: index }))
      : [];
    closeArrDetailsModal();
    renderArrResults();
    setMoviesTvSection("search");
    focusMoviesTvResults();
    startArrStatusPolling();
    setNotice(messageEl, state.arrResults.length ? `Loaded titles for ${personName}.` : `No titles found for ${personName}.`, false);
  } catch (err) {
    setNotice(messageEl, `Filmography load failed: ${toUserErrorMessage(err)}`, true);
  }
}

function getArrItemById(tmdbId) {
  const key = String(tmdbId || "").trim();
  if (!key) return null;
  return state.arrResults.find((entry) => String(entry.tmdb_id) === key)
    || Object.values(state.arrEditorial || {}).flatMap((bucket) => Array.isArray(bucket?.shelves) ? bucket.shelves : [])
      .flatMap((shelf) => Array.isArray(shelf?.results) ? shelf.results : [])
      .find((entry) => String(entry.tmdb_id) === key)
    || null;
}

function updateArrItemById(nextItem) {
  const key = String(nextItem?.tmdb_id || "").trim();
  if (!key) return;
  state.arrResults = state.arrResults.map((item) => String(item.tmdb_id) === key ? nextItem : item);
  Object.values(state.arrEditorial || {}).forEach((bucket) => {
    if (!Array.isArray(bucket?.shelves)) return;
    bucket.shelves.forEach((shelf) => {
      if (!Array.isArray(shelf?.results)) return;
      shelf.results = shelf.results.map((item) => String(item.tmdb_id) === key ? nextItem : item);
    });
  });
}

function applyArrCardSize(size) {
  const numeric = Number.parseInt(size, 10);
  const nextSize = Number.isFinite(numeric)
    ? Math.max(UI_CARD_SIZE_MIN, Math.min(UI_CARD_SIZE_MAX, numeric))
    : UI_DEFAULTS.movies_tv_card_size;
  state.arrCardSize = nextSize;
  const panel = $("#movies-tv-panel");
  if (panel) {
    panel.style.setProperty("--movies-tv-card-min", `${nextSize}px`);
  }
  const input = $("#movies-tv-card-size");
  if (input && String(input.value) !== String(nextSize)) {
    input.value = String(nextSize);
  }
  const valueLabel = $("#movies-tv-card-size-value");
  if (valueLabel) {
    valueLabel.textContent = nextSize === UI_DEFAULTS.movies_tv_card_size ? "Default" : `${nextSize}px`;
  }
}

function getArrResultYearValue(item) {
  const text = String(item?.year || "").trim();
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function getSortedArrResults() {
  const results = Array.isArray(state.arrResults) ? [...state.arrResults] : [];
  const sortMode = String(state.arrSort || "best_match");
  const byRankThenNewest = (a, b) => {
    const rankA = Number.isFinite(Number(a?._search_rank)) ? Number(a._search_rank) : Number.MAX_SAFE_INTEGER;
    const rankB = Number.isFinite(Number(b?._search_rank)) ? Number(b._search_rank) : Number.MAX_SAFE_INTEGER;
    if (rankA !== rankB) return rankA - rankB;
    const yearDiff = getArrResultYearValue(b) - getArrResultYearValue(a);
    if (yearDiff !== 0) return yearDiff;
    return String(a?.title || "").localeCompare(String(b?.title || ""));
  };
  if (sortMode === "newest") {
    return results.sort((a, b) => {
      const yearDiff = getArrResultYearValue(b) - getArrResultYearValue(a);
      if (yearDiff !== 0) return yearDiff;
      return byRankThenNewest(a, b);
    });
  }
  if (sortMode === "oldest") {
    return results.sort((a, b) => {
      const yearDiff = getArrResultYearValue(a) - getArrResultYearValue(b);
      if (yearDiff !== 0) return yearDiff;
      return byRankThenNewest(a, b);
    });
  }
  if (sortMode === "rating_desc") {
    return results.sort((a, b) => {
      const diff = Number(b?.rating || 0) - Number(a?.rating || 0);
      if (diff !== 0) return diff;
      return byRankThenNewest(a, b);
    });
  }
  if (sortMode === "popularity_desc") {
    return results.sort((a, b) => {
      const diff = Number(b?.popularity || 0) - Number(a?.popularity || 0);
      if (diff !== 0) return diff;
      return byRankThenNewest(a, b);
    });
  }
  if (sortMode === "title_asc") {
    return results.sort((a, b) => {
      const diff = String(a?.title || "").localeCompare(String(b?.title || ""));
      if (diff !== 0) return diff;
      return byRankThenNewest(a, b);
    });
  }
  return results.sort(byRankThenNewest);
}

function renderArrResults() {
  const section = $("#movies-tv-results-view");
  const listEl = $("#movies-tv-results-list");
  const statusTextEl = $("#movies-tv-results-status-text");
  const detailEl = $("#movies-tv-results-detail");
  const sortSelect = $("#movies-tv-sort");
  if (!section || !listEl || !statusTextEl) {
    return;
  }
  if (sortSelect && sortSelect.value !== String(state.arrSort || "best_match")) {
    sortSelect.value = String(state.arrSort || "best_match");
  }
  const modeLabel = state.arrMode === "tv" ? "TV shows" : "movies";
  if (!Array.isArray(state.arrResults) || state.arrResults.length === 0) {
    listEl.innerHTML = "";
    statusTextEl.textContent = "Ready to search";
    if (detailEl) detailEl.classList.add("hidden");
    renderArrEditorialShelves();
    applyMoviesTvWorkspaceState();
    return;
  }
  statusTextEl.textContent = `Showing ${state.arrResults.length} ${modeLabel}`;
  if (detailEl) {
    const yearText = String(state.arrSearchYear || "").trim();
    const context = String(state.arrSearchContext || "search");
    if (context === "genre") {
      detailEl.textContent = yearText
        ? `Top ${modeLabel} in ${state.arrSearchQuery} for ${yearText}`
        : `Top ${modeLabel} in ${state.arrSearchQuery}`;
    } else if (context === "person") {
      detailEl.textContent = `Filmography results for ${state.arrSearchQuery}`;
    } else {
      detailEl.textContent = yearText
        ? `TMDb results for “${state.arrSearchQuery}” in ${yearText}`
        : `TMDb results for “${state.arrSearchQuery}”`;
    }
    detailEl.classList.remove("hidden");
  }
  const sortedResults = getSortedArrResults();
  listEl.innerHTML = renderArrCardGridMarkup(sortedResults);
  renderArrEditorialShelves();
  applyMoviesTvWorkspaceState();
}

function renderArrCardGridMarkup(items) {
  const serviceKey = getArrServiceKey();
  const serviceName = getArrServiceName();
  const serviceStatus = state.arrConnectionStatus[serviceKey] || {};
  return (Array.isArray(items) ? items : []).map((item) => {
    const status = getArrStatusDescriptor(item.arr_status);
    const posterUrl = String(item.poster_url || "").trim() || "assets/no_artwork.png";
    const title = escapeHtml(String(item.title || "Unknown title"));
    const originalTitle = String(item.original_title || "").trim();
    const year = escapeHtml(String(item.year || "").trim());
    const overview = escapeHtml(String(item.overview || "No overview available.").trim());
    const overviewShort = escapeHtml(String(item.overview || "No overview available.").trim());
    const tmdbUrl = escapeHtml(String(item.tmdb_url || "").trim());
    const language = escapeHtml(String(item.language || "").trim().toUpperCase());
    const popularity = Number.isFinite(Number(item.popularity)) ? Number(item.popularity).toFixed(1) : "";
    const rating = Number.isFinite(Number(item.rating)) ? Number(item.rating).toFixed(1) : "";
    const tmdbId = escapeHtml(String(item.tmdb_id || ""));
    const buttonDisabled = !serviceStatus.reachable;
    const buttonLabel = buttonDisabled ? `${serviceName} unavailable` : `Add to ${serviceName}`;
    const trailer = item.trailer && typeof item.trailer === "object" ? item.trailer : null;
    const trailerButton = trailer?.available && trailer.embed_url
      ? `
          <button
            class="button ghost small"
            type="button"
            data-action="arr-trailer"
            data-tmdb-id="${tmdbId}"
            data-embed-url="${escapeHtml(String(trailer.embed_url || ""))}"
            data-title="${escapeHtml(String(trailer.title || item.title || "Trailer"))}"
          >Trailer</button>
        `
      : "";
    return `
      <article class="home-result-card music-meta-card music-grid-card movies-tv-card" data-arr-tmdb-id="${tmdbId}" tabindex="0">
        <div class="home-candidate-artwork movies-tv-artwork" data-action="arr-artwork-preview" data-tmdb-id="${tmdbId}">
          <img src="${escapeHtml(posterUrl)}" alt="${title} poster" loading="lazy" onerror="this.onerror=null;this.src='assets/no_artwork.png';">
          <div class="movies-tv-overlay">
            <div class="home-candidate-main movies-tv-main">
              <div class="home-candidate-title-row">
                <div class="home-candidate-title">${title}</div>
                ${year ? `<span class="home-candidate-source">${year}</span>` : ""}
              </div>
              <div class="meta movies-tv-overview">${overviewShort}</div>
              <div class="meta movies-tv-status-row">
                <span class="chip ${status.className ? `status-${status.className}` : ""}">${status.label}</span>
              </div>
            </div>
            <div class="home-candidate-action home-candidate-action-primary-stack movies-tv-action">
              <button
                class="button ghost small home-candidate-download-primary movies-tv-add-btn"
                type="button"
                data-action="arr-add"
                data-tmdb-id="${tmdbId}"
                ${buttonDisabled ? "disabled" : ""}
              >${escapeHtml(buttonLabel)}</button>
              <a
                class="button ghost small home-candidate-open"
                href="${tmdbUrl}"
                target="_blank"
                rel="noopener"
              >Visit TMDb page</a>
              ${trailerButton}
              <button
                class="button ghost small"
                type="button"
                data-action="arr-toggle-info"
                data-tmdb-id="${tmdbId}"
              >More info</button>
            </div>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

function renderArrEditorialShelves() {
  const kind = getArrGenreKind();
  const shelves = Array.isArray(state.arrEditorial?.[kind]?.shelves) ? state.arrEditorial[kind].shelves : [];
  const discoveryView = $("#movies-tv-discovery-view");
  const shelvesHost = $("#movies-tv-editorial-shelves");
  const show = isTmdbConfigured() && !state.arrResults.length && (state.moviesTvSection || "search") !== "genres";
  if (discoveryView) {
    discoveryView.classList.toggle("hidden", !show);
  }
  if (shelvesHost) {
    shelvesHost.innerHTML = shelves.length
      ? shelves.map((shelf) => `
        <section class="home-results movies-tv-editorial-shelf" data-editorial-shelf="${escapeHtml(String(shelf?.id || ""))}">
          <div class="home-results-header">
            <span>${escapeHtml(String(shelf?.title || "Editorial"))}</span>
            <div class="home-results-header-actions">
              <span class="meta">${Array.isArray(shelf?.results) && shelf.results.length ? `Showing ${shelf.results.length}` : "Loading titles"}</span>
            </div>
          </div>
          ${shelf?.subtitle ? `<div class="meta home-results-detail">${escapeHtml(String(shelf.subtitle))}</div>` : ""}
          <div class="music-meta-grid movies-tv-results-grid">${Array.isArray(shelf?.results) && shelf.results.length ? renderArrCardGridMarkup(shelf.results) : `<div class="home-results-empty">Titles will appear here.</div>`}</div>
        </section>
      `).join("")
      : `<section class="home-results movies-tv-editorial-shelf"><div class="home-results-empty">Editorial shelves will appear here.</div></section>`;
  }
  setMoviesTvSection(state.moviesTvSection || "search");
}

async function loadArrEditorialShelves({ force = false } = {}) {
  if (!renderMoviesTvSetupGate()) return;
  const kind = getArrGenreKind();
  const existing = state.arrEditorial?.[kind] || {};
  if (!force && Array.isArray(existing.shelves) && existing.shelves.length) {
    renderArrEditorialShelves();
    return;
  }
  try {
    const shelfMeta = await fetchJson(`/api/arr/editorial/shelves?kind=${encodeURIComponent(kind)}`);
    const defaultShelves = (Array.isArray(shelfMeta?.shelves) ? shelfMeta.shelves : []).filter((entry) => entry?.default_visible).slice(0, 4);
    const shelfPayloads = await Promise.all(
      defaultShelves.map(async (entry) => {
        const payload = await fetchJson(
          `/api/arr/editorial/shelf?kind=${encodeURIComponent(kind)}&shelf=${encodeURIComponent(String(entry.id || ""))}&limit=10`
        );
        return {
          id: String(entry.id || ""),
          title: String(entry.title || ""),
          subtitle: String(entry.subtitle || ""),
          provider: String(payload?.provider || entry.provider || ""),
          results: Array.isArray(payload?.results) ? payload.results : [],
        };
      })
    );
    state.arrEditorial[kind] = {
      shelves: shelfPayloads,
    };
  } catch (_err) {
    state.arrEditorial[kind] = state.arrEditorial[kind] || { shelves: [] };
  }
  renderArrEditorialShelves();
}

function wireArrCardInteractions(host) {
  if (!host) return;
  host.addEventListener("click", async (event) => {
    const trailerButton = event.target.closest('button[data-action="arr-trailer"]');
    if (trailerButton) {
      let embedUrl = String(trailerButton.dataset.embedUrl || "").trim();
      const title = String(trailerButton.dataset.title || "").trim() || "Trailer";
      const tmdbId = String(trailerButton.dataset.tmdbId || "").trim();
      if (!embedUrl && tmdbId) {
        const item = await ensureArrTrailerById(tmdbId);
        embedUrl = String(item?.trailer?.embed_url || "").trim();
      }
      if (embedUrl) {
        openHomePreviewModal({
          mediaType: "video",
          embedUrl,
          source: "youtube",
          title,
        });
      }
      return;
    }
    const toggleButton = event.target.closest('button[data-action="arr-toggle-info"]');
    if (toggleButton) {
      const tmdbId = String(toggleButton.dataset.tmdbId || "").trim();
      if (!tmdbId) return;
      const item = getArrItemById(tmdbId);
      if (item) {
        openArrDetailsModal(item);
      }
      return;
    }
    const artwork = event.target.closest('[data-action="arr-artwork-preview"]');
    if (artwork) {
      const tmdbId = String(artwork.dataset.tmdbId || "").trim();
      if (!tmdbId) return;
      const item = getArrItemById(tmdbId);
      if (item) {
        openArrDetailsModal(item);
      }
      return;
    }
    const button = event.target.closest('button[data-action="arr-add"]');
    if (!button || button.disabled) return;
    const tmdbId = String(button.dataset.tmdbId || "").trim();
    if (!tmdbId) return;
    button.disabled = true;
    const originalText = button.textContent;
    button.textContent = "Adding...";
    try {
      await addArrItem(tmdbId);
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  });
  host.addEventListener("mouseover", async (event) => {
    const card = event.target.closest(".movies-tv-card");
    if (!card || !host.contains(card)) return;
    if (card.contains(event.relatedTarget)) return;
    const tmdbId = String(card.dataset.arrTmdbId || "").trim();
    if (!tmdbId) return;
    const item = await ensureArrTrailerById(tmdbId);
    if (!item) return;
    const descriptor = buildArrTrailerPreviewDescriptor(item, { hover: true });
    if (!descriptor) return;
    startHomeArtworkHoverPreview(card, descriptor);
  });
  host.addEventListener("mouseout", (event) => {
    const card = event.target.closest(".movies-tv-card");
    if (!card || !host.contains(card)) return;
    if (card.contains(event.relatedTarget)) return;
    stopHomeArtworkHoverPreview(card);
  });
  host.addEventListener("focusin", async (event) => {
    const card = event.target.closest(".movies-tv-card");
    if (!card || !host.contains(card)) return;
    const tmdbId = String(card.dataset.arrTmdbId || "").trim();
    if (!tmdbId) return;
    const item = await ensureArrTrailerById(tmdbId);
    if (!item) return;
    const descriptor = buildArrTrailerPreviewDescriptor(item, { hover: true });
    if (!descriptor) return;
    startHomeArtworkHoverPreview(card, descriptor);
  });
  host.addEventListener("focusout", (event) => {
    const card = event.target.closest(".movies-tv-card");
    if (!card || !host.contains(card)) return;
    stopHomeArtworkHoverPreview(card);
  });
}

function updateArrModeToggleUI() {
  const toggle = $("#arr-mode-toggle");
  if (!toggle) return;
  toggle.querySelectorAll("[data-mode]").forEach((button) => {
    const active = normalizeArrMode(button.dataset.mode) === state.arrMode;
    button.classList.toggle("active", active);
    button.setAttribute("aria-checked", active ? "true" : "false");
  });
}

function setArrMode(mode) {
  state.arrMode = normalizeArrMode(mode);
  state.arrResults = [];
  state.arrSearchQuery = "";
  setMoviesTvSearchYear("");
  state.arrSearchContext = "search";
  state.arrActiveGenre = null;
  updateArrModeToggleUI();
  loadArrGenres().catch(() => {});
  renderArrResults();
}

async function ensureArrTrailer(item) {
  if (!item || item.trailerFetched) {
    return item;
  }
  try {
    const data = await fetchJson(
      `/api/arr/trailer?kind=${encodeURIComponent(getArrKind())}&tmdb_id=${encodeURIComponent(String(item.tmdb_id || ""))}`
    );
    return {
      ...item,
      trailerFetched: true,
      trailer: data && typeof data === "object" ? data : { available: false },
    };
  } catch {
    return {
      ...item,
      trailerFetched: true,
      trailer: { available: false },
    };
  }
}

async function ensureArrTrailerById(tmdbId) {
  const key = String(tmdbId || "").trim();
  const item = getArrItemById(key);
  if (!item) {
    return null;
  }
  const nextItem = await ensureArrTrailer(item);
  updateArrItemById(nextItem);
  return nextItem;
}

function buildArrTrailerPreviewDescriptor(item, { hover = false } = {}) {
  const trailer = item?.trailer;
  if (!trailer?.available) {
    return null;
  }
  const embedUrl = String(
    hover ? (trailer.hover_embed_url || "") : (trailer.embed_url || "")
  ).trim();
  if (!embedUrl) {
    return null;
  }
  return {
    mediaType: "video",
    embedUrl,
    source: "youtube",
    title: String(trailer.title || item?.title || "Trailer").trim() || "Trailer",
  };
}

async function refreshArrStatuses() {
  if (state.currentPage !== "movies-tv" || !Array.isArray(state.arrResults) || !state.arrResults.length) {
    return;
  }
  try {
    const data = await fetchJson("/api/arr/status/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        kind: getArrKind(),
        tmdb_ids: state.arrResults.map((item) => item.tmdb_id),
      }),
    });
    const statuses = data?.statuses || {};
    state.arrResults = state.arrResults.map((item) => ({
      ...item,
      arr_status: statuses[String(item.tmdb_id)] || item.arr_status || { added: false, downloaded: false, status: "not_added" },
    }));
    renderArrResults();
  } catch {
    // keep stale status until next successful refresh
  }
}

function stopArrStatusPolling() {
  if (state.arrStatusPollTimer) {
    clearInterval(state.arrStatusPollTimer);
    state.arrStatusPollTimer = null;
  }
}

function startArrStatusPolling() {
  stopArrStatusPolling();
  if (state.currentPage !== "movies-tv" || !state.arrResults.length) {
    return;
  }
  state.arrStatusPollTimer = window.setInterval(() => {
    refreshArrStatuses();
  }, 15000);
}

async function performArrSearch() {
  if (!renderMoviesTvSetupGate()) {
    setNotice($("#movies-tv-setup-message"), "Add a TMDb API key to unlock Movies & TV discovery.", true);
    return;
  }
  const input = $("#movies-tv-search-input");
  const messageEl = $("#movies-tv-message");
  const query = String(input?.value || "").trim();
  const year = /^\d{4}$/.test(String(state.arrSearchYear || "").trim())
    ? String(state.arrSearchYear).trim()
    : "";
  if (!query) {
    setNotice(messageEl, "Enter a movie, show, actor, director, or genre to search TMDb.", true);
    return;
  }
  await loadArrGenres().catch(() => {});
  const matchedGenre = findArrGenreMatch(query);
  if (matchedGenre) {
    await browseArrGenre(matchedGenre, { year });
    return;
  }
  const endpoint = state.arrMode === "tv" ? "/api/arr/search/tv" : "/api/arr/search/movies";
  setNotice(messageEl, "Searching TMDb…", false);
  try {
    const params = new URLSearchParams();
    params.set("q", query);
    if (year) {
      params.set("year", year);
    }
    const data = await fetchJson(`${endpoint}?${params.toString()}`);
    state.arrActiveGenre = null;
    state.arrSearchContext = "search";
    state.arrSearchQuery = query;
    setMoviesTvSearchYear(year);
    state.arrResults = Array.isArray(data?.results)
      ? data.results.map((item, index) => ({ ...item, _search_rank: index }))
      : [];
    state.arrConnectionStatus[getArrServiceKey()] = data?.connection || state.arrConnectionStatus[getArrServiceKey()];
    renderArrConnectionStatus();
    renderArrResults();
    setMoviesTvSection("search");
    focusMoviesTvResults();
    startArrStatusPolling();
    setNotice(
      messageEl,
      state.arrResults.length
        ? `Loaded ${state.arrResults.length} results.${year ? ` Filtered to ${year}.` : ""}`
        : "No results found.",
      false
    );
  } catch (err) {
    state.arrResults = [];
    renderArrResults();
    setNotice(messageEl, `Movies & TV search failed: ${toUserErrorMessage(err)}`, true);
  }
}

async function addArrItem(tmdbId) {
  const serviceName = getArrServiceName();
  const messageEl = $("#movies-tv-message");
  const endpoint = state.arrMode === "tv" ? "/api/arr/sonarr/add" : "/api/arr/radarr/add";
  try {
    const data = await fetchJson(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tmdb_id: tmdbId }),
    });
    const nextStatus = data?.status || { added: true, downloaded: false, status: "added" };
    state.arrResults = state.arrResults.map((item) => (
      String(item.tmdb_id) === String(tmdbId) ? { ...item, arr_status: nextStatus } : item
    ));
    renderArrResults();
    startArrStatusPolling();
    setNotice(messageEl, `Sent to ${serviceName}.`, false);
  } catch (err) {
    setNotice(messageEl, `Failed to add item to ${serviceName}: ${toUserErrorMessage(err)}`, true);
  }
}

function updateAdvancedPageVisibility() {
  // Movies & TV is now always visible in primary navigation.
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
      ? "Use Search and then click Download on a result when you want to save it."
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
    if (state.currentPage === "video" && String(text || "").trim() === "Searching sources…") {
      statusEl.textContent = "Results";
      return;
    }
    statusEl.textContent = text;
  }
}

function setHomeResultsDetail(text, isError = false) {
  const detailEl = $("#home-results-detail");
  if (!detailEl) {
    return;
  }
  if (state.currentPage === "video" && !isError) {
    detailEl.textContent = "";
    detailEl.classList.remove("home-results-error");
    detailEl.classList.add("hidden");
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
      landing_view: !!response?.landing_view,
    },
    query: String(query || ""),
  };
}

function getMusicArtistAlbumsCacheKey(artist) {
  const name = typeof artist === "object" && artist !== null
    ? String(artist.name || "").trim()
    : String(artist || "").trim();
  const artistMbid = typeof artist === "object" && artist !== null
    ? String(artist.artist_mbid || "").trim()
    : "";
  return `${artistMbid || name}`.trim().toLowerCase();
}

function getMusicGenreBrowseCacheKey(genre) {
  return normalizeMusicGenreIntent(genre).trim().toLowerCase();
}

function getArrGenreBrowseCacheKey(kind, genreId, year = "") {
  return `${String(kind || "").trim().toLowerCase()}::${String(genreId || "").trim()}::${String(year || "").trim()}`;
}

function getArrPersonTitlesCacheKey(kind, personId) {
  return `${String(kind || "").trim().toLowerCase()}::${String(personId || "").trim()}`;
}

function renderMusicBrowseLoading(message) {
  const container = document.getElementById("music-results-container");
  if (!container) return;
  state.homeMusicResultMap = {};
  const renderToken = ++state.homeMusicRenderToken;
  container.innerHTML = "";
  const loading = document.createElement("div");
  loading.className = "home-results-empty";
  loading.textContent = message;
  container.appendChild(loading);
  setHomeResultsStatus("Loading music results…");
  setHomeResultsDetail(message, false);
  state.homeMusicCurrentView = null;
  return renderToken;
}

function renderArrBrowseLoading(message, detail = "") {
  const section = $("#movies-tv-results");
  const listEl = $("#movies-tv-results-list");
  const statusTextEl = $("#movies-tv-results-status-text");
  const detailEl = $("#movies-tv-results-detail");
  if (!section || !listEl || !statusTextEl) return;
  section.classList.remove("hidden");
  statusTextEl.textContent = "Loading…";
  listEl.innerHTML = `<div class="home-results-empty">${escapeHtml(message)}</div>`;
  if (detailEl) {
    if (detail) {
      detailEl.textContent = detail;
      detailEl.classList.remove("hidden");
    } else {
      detailEl.textContent = "";
      detailEl.classList.add("hidden");
    }
  }
}

function normalizeMusicPreferences(payload = {}) {
  const raw = payload && typeof payload === "object" ? payload : {};
  const favoriteGenres = [];
  const genreSeen = new Set();
  const rawGenres = Array.isArray(raw.favorite_genres) ? raw.favorite_genres : [];
  rawGenres.forEach((value) => {
    const genre = normalizeMusicGenreIntent(value);
    const key = genre.toLowerCase();
    if (!genre || genreSeen.has(key)) return;
    genreSeen.add(key);
    favoriteGenres.push(genre);
  });
  const favoriteArtists = [];
  const artistSeen = new Set();
  const rawArtists = Array.isArray(raw.favorite_artists) ? raw.favorite_artists : [];
  rawArtists.forEach((item) => {
    if (!item || typeof item !== "object") return;
    const name = String(item.name || "").trim();
    const artistMbid = String(item.artist_mbid || "").trim() || null;
    const key = (artistMbid || name).toLowerCase();
    if (!name || artistSeen.has(key)) return;
    artistSeen.add(key);
    favoriteArtists.push({ name, artist_mbid: artistMbid });
  });
  return {
    favorite_genres: favoriteGenres,
    favorite_artists: favoriteArtists,
  };
}

function syncMusicPreferencesFromConfig(cfg = {}) {
  const next = normalizeMusicPreferences(cfg?.music_preferences || {});
  state.musicPreferences = next;
  if (state.config && typeof state.config === "object") {
    state.config.music_preferences = next;
  }
  scheduleMusicGenrePrewarm();
}

function isFavoriteGenre(genre) {
  const key = normalizeMusicGenreIntent(genre).toLowerCase();
  return !!key && state.musicPreferences.favorite_genres.some((value) => String(value || "").trim().toLowerCase() === key);
}

function isFavoriteArtist(artist = {}) {
  const artistMbid = String(artist?.artist_mbid || "").trim().toLowerCase();
  const artistName = String(artist?.name || "").trim().toLowerCase();
  return state.musicPreferences.favorite_artists.some((entry) => {
    const entryMbid = String(entry?.artist_mbid || "").trim().toLowerCase();
    const entryName = String(entry?.name || "").trim().toLowerCase();
    return artistMbid ? entryMbid === artistMbid : (!!artistName && entryName === artistName);
  });
}

async function saveMusicPreferences(nextPrefs) {
  const normalized = normalizeMusicPreferences(nextPrefs);
  const saved = await fetchJson("/api/music/preferences", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(normalized),
  });
  syncMusicPreferencesFromConfig({ music_preferences: saved });
  return state.musicPreferences;
}

function applyMusicPreferencesLocal(nextPrefs) {
  const normalized = normalizeMusicPreferences(nextPrefs);
  syncMusicPreferencesFromConfig({ music_preferences: normalized });
  return state.musicPreferences;
}

async function toggleFavoriteGenre(genre) {
  const normalizedGenre = normalizeMusicGenreIntent(genre);
  if (!normalizedGenre) return;
  const previous = normalizeMusicPreferences(state.musicPreferences);
  const next = normalizeMusicPreferences(state.musicPreferences);
  const key = normalizedGenre.toLowerCase();
  if (next.favorite_genres.some((value) => String(value || "").trim().toLowerCase() === key)) {
    next.favorite_genres = next.favorite_genres.filter((value) => String(value || "").trim().toLowerCase() !== key);
  } else {
    next.favorite_genres.push(normalizedGenre);
    next.favorite_genres.sort((a, b) => a.localeCompare(b));
  }
  applyMusicPreferencesLocal(next);
  try {
    await saveMusicPreferences(next);
  } catch (err) {
    applyMusicPreferencesLocal(previous);
    throw err;
  }
}

async function toggleFavoriteArtist(artist) {
  const normalizedArtist = {
    name: String(artist?.name || "").trim(),
    artist_mbid: String(artist?.artist_mbid || "").trim() || null,
  };
  if (!normalizedArtist.name) return;
  const next = normalizeMusicPreferences(state.musicPreferences);
  const dedupeKey = (normalizedArtist.artist_mbid || normalizedArtist.name).toLowerCase();
  if (next.favorite_artists.some((entry) => ((entry.artist_mbid || entry.name) || "").toLowerCase() === dedupeKey)) {
    next.favorite_artists = next.favorite_artists.filter(
      (entry) => (((entry.artist_mbid || entry.name) || "").toLowerCase() !== dedupeKey)
    );
  } else {
    next.favorite_artists.push(normalizedArtist);
    next.favorite_artists.sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));
  }
  await saveMusicPreferences(next);
}

function createMusicFavoriteButton({ active = false, label = "Favorite" } = {}) {
  const button = document.createElement("button");
  button.type = "button";
  setMusicFavoriteButtonState(button, active, label);
  return button;
}

function setMusicFavoriteButtonState(button, active, label = "Favorite") {
  if (!button) return;
  button.className = `music-favorite-button${active ? " active" : ""}`;
  button.setAttribute("aria-pressed", active ? "true" : "false");
  button.setAttribute("aria-label", active ? `Remove ${label} from favorites` : `Add ${label} to favorites`);
  button.textContent = active ? "♥" : "♡";
}

function updateMatchingMusicFavoriteButtons(kind, key, active, label) {
  const normalizedKind = String(kind || "").trim();
  const normalizedKey = String(key || "").trim().toLowerCase();
  if (!normalizedKind || !normalizedKey) return;
  document.querySelectorAll(`.music-favorite-button[data-favorite-kind="${normalizedKind}"]`).forEach((button) => {
    const buttonKey = String(button.dataset.favoriteKey || "").trim().toLowerCase();
    if (buttonKey !== normalizedKey) return;
    setMusicFavoriteButtonState(button, active, label);
  });
}

const MUSIC_GENRE_THEMES = {
  rock: { accent: "#ff6b57", glow: "rgba(255,107,87,0.32)", base: "#21131a" },
  pop: { accent: "#ff7fd1", glow: "rgba(255,127,209,0.3)", base: "#201427" },
  "hip hop": { accent: "#f59e0b", glow: "rgba(245,158,11,0.28)", base: "#22180d" },
  jazz: { accent: "#38bdf8", glow: "rgba(56,189,248,0.28)", base: "#0f1d28" },
  electronic: { accent: "#7c3aed", glow: "rgba(124,58,237,0.3)", base: "#151228" },
  metal: { accent: "#94a3b8", glow: "rgba(148,163,184,0.22)", base: "#14171d" },
  country: { accent: "#f59e0b", glow: "rgba(245,158,11,0.24)", base: "#24190f" },
  folk: { accent: "#84cc16", glow: "rgba(132,204,22,0.24)", base: "#17210f" },
  "r&b": { accent: "#ec4899", glow: "rgba(236,72,153,0.26)", base: "#24121d" },
  classical: { accent: "#eab308", glow: "rgba(234,179,8,0.24)", base: "#221d11" },
  "contemporary christian": { accent: "#a78bfa", glow: "rgba(167,139,250,0.24)", base: "#171327" },
  indie: { accent: "#22c55e", glow: "rgba(34,197,94,0.22)", base: "#102118" },
};

function getMusicGenreTheme(genre) {
  const key = normalizeMusicGenreIntent(genre).toLowerCase();
  return MUSIC_GENRE_THEMES[key] || {
    accent: "#60a5fa",
    glow: "rgba(96,165,250,0.24)",
    base: "#121a27",
  };
}

function applyGenreTileTheme(tile, genre) {
  if (!tile) return;
  const theme = getMusicGenreTheme(genre);
  tile.style.setProperty("--genre-accent", theme.accent);
  tile.style.setProperty("--genre-glow", theme.glow);
  tile.style.setProperty("--genre-base", theme.base);
}

function applyGenreTileArtwork(tile, artworkUrls) {
  if (!tile) return;
  const normalizedUrls = normalizeGenreArtworkSet(artworkUrls);
  const collage = tile.querySelector(".music-genre-collage");
  const cells = collage ? [...collage.querySelectorAll(".music-genre-collage-cell")] : [];
  cells.forEach((cell, index) => {
    const img = cell.querySelector("img");
    const nextUrl = normalizedUrls[index] || "";
    if (img && nextUrl) {
      img.src = nextUrl;
      img.alt = "";
      cell.classList.add("has-artwork");
    } else {
      if (img) {
        img.removeAttribute("src");
        img.alt = "";
      }
      cell.classList.remove("has-artwork");
    }
  });
  tile.classList.toggle("has-artwork", normalizedUrls.length > 0);
}

function queueGenreArtworkJob(genre, tile, thumbnailJobs, renderToken) {
  const genreKey = normalizeMusicGenreIntent(genre);
  if (!genreKey || !tile) return;
  const cachedCovers = getCachedGenreCoverUrls(genreKey);
  if (cachedCovers.length) {
    applyGenreTileArtwork(tile, cachedCovers);
    return;
  }
  if (Object.prototype.hasOwnProperty.call(state.homeGenreCoverCache, genreKey.toLowerCase()) && state.homeGenreCoverCache[genreKey.toLowerCase()] === null) {
    return;
  }
  thumbnailJobs.push(async (activeToken) => {
    if (state.homeMusicRenderToken !== activeToken) return;
    try {
      const persistent = await fetchPersistentGenreCoverEntry(genreKey);
      if (persistent?.coverUrls?.length) {
        setCachedGenreCoverUrls(genreKey, persistent.coverUrls);
        if (state.homeMusicRenderToken === activeToken) {
          applyGenreTileArtwork(tile, persistent.coverUrls);
        }
        if (!isArtworkCacheStale(persistent.updatedAt)) {
          return;
        }
      }
      const artists = await fetchArtistsForGenreIntent(genreKey, { limit: 10 });
      const candidateReleaseGroups = [];
      await Promise.all(
        artists.slice(0, 8).map(async (artist, artistIndex) => {
          const artistMbid = String(artist?.artist_mbid || "").trim();
          const artistName = String(artist?.name || "").trim();
          if (!artistMbid && !artistName) {
            return;
          }
          const albums = await fetchMusicAlbumsByArtist({
            name: artistName,
            artist_mbid: artistMbid,
          });
          (Array.isArray(albums) ? albums : []).slice(0, 4).forEach((album, albumIndex) => {
            const releaseGroupMbid = String(album?.release_group_mbid || "").trim();
            if (!releaseGroupMbid) return;
            const releaseYear = Number.parseInt(String(album?.release_year || ""), 10);
            candidateReleaseGroups.push({
              releaseGroupMbid,
              artistMbid,
              artistName,
              artistIndex,
              albumIndex,
              releaseYear: Number.isFinite(releaseYear) ? releaseYear : 0,
            });
          });
        })
      );
      candidateReleaseGroups.sort((a, b) => {
        if (b.releaseYear !== a.releaseYear) return b.releaseYear - a.releaseYear;
        if (a.artistIndex !== b.artistIndex) return a.artistIndex - b.artistIndex;
        return a.albumIndex - b.albumIndex;
      });
      const selectedCovers = [];
      const seenCovers = new Set();
      const seenReleaseGroups = new Set();
      const uniqueCandidates = [];
      for (const candidate of candidateReleaseGroups) {
        const releaseGroupKey = String(candidate.releaseGroupMbid || "").trim();
        if (!releaseGroupKey || seenReleaseGroups.has(releaseGroupKey)) {
          continue;
        }
        seenReleaseGroups.add(releaseGroupKey);
        uniqueCandidates.push(candidate);
        if (uniqueCandidates.length >= 12) break;
      }
      const coverResults = await Promise.all(
        uniqueCandidates.map(async (candidate) => {
          const releaseGroupKey = String(candidate.releaseGroupMbid || "").trim();
          const cachedCover = getCachedAlbumCoverUrl(releaseGroupKey);
          const coverUrl = cachedCover || await fetchHomeAlbumCoverUrl(releaseGroupKey);
          if (coverUrl && candidate.artistMbid) {
            setCachedArtistCoverUrl(candidate.artistMbid, coverUrl);
          }
          return {
            candidate,
            coverUrl: String(coverUrl || "").trim(),
          };
        })
      );
      coverResults.forEach(({ coverUrl }) => {
        if (!coverUrl || seenCovers.has(coverUrl) || selectedCovers.length >= 4) {
          return;
        }
        seenCovers.add(coverUrl);
        selectedCovers.push(coverUrl);
      });
      if (selectedCovers.length) {
        setCachedGenreCoverUrls(genreKey, selectedCovers);
        await persistGenreCoverEntry(genreKey, selectedCovers);
        if (state.homeMusicRenderToken === activeToken) {
          applyGenreTileArtwork(tile, selectedCovers);
        }
        return;
      }
      state.homeGenreCoverCache[genreKey.toLowerCase()] = null;
    } catch (_err) {
      state.homeGenreCoverCache[genreKey.toLowerCase()] = null;
    }
  });
}

function createMusicGenreCard(genre, thumbnailJobs, renderToken, { dismissible = false } = {}) {
  const displayGenre = normalizeMusicGenreIntent(genre);
  const card = document.createElement("article");
  card.className = "home-result-card music-meta-card music-grid-card music-genre-grid-card";
  if (dismissible) {
    const dismissButton = document.createElement("button");
    dismissButton.type = "button";
    dismissButton.className = "music-genre-dismiss-button";
    dismissButton.title = `Hide ${displayGenre}`;
    dismissButton.setAttribute("aria-label", `Hide ${displayGenre}`);
    dismissButton.textContent = "⊘";
    dismissButton.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const previousHidden = normalizeHiddenMusicGenres(state.musicHiddenGenres);
      const nextHidden = normalizeHiddenMusicGenres([...state.musicHiddenGenres, displayGenre]);
      state.musicHiddenGenres = nextHidden;
      renderMusicLanding();
      try {
        await persistUiPreferences({ music_hidden_genres: nextHidden });
      } catch (err) {
        state.musicHiddenGenres = previousHidden;
        renderMusicLanding();
        setMusicPageNotice(`Hide genre failed: ${toUserErrorMessage(err)}`, true);
      }
    });
    card.appendChild(dismissButton);
  }
  const favoriteButton = createMusicFavoriteButton({
    active: isFavoriteGenre(displayGenre),
    label: `${displayGenre} genre`,
  });
  favoriteButton.dataset.favoriteKind = "genre";
  favoriteButton.dataset.favoriteKey = displayGenre.toLowerCase();
  favoriteButton.addEventListener("click", async (event) => {
    event.preventDefault();
    event.stopPropagation();
    favoriteButton.disabled = true;
    const previousActive = isFavoriteGenre(displayGenre);
    try {
      const persistPromise = toggleFavoriteGenre(displayGenre);
      const nextActive = !previousActive;
      updateMatchingMusicFavoriteButtons(
        "genre",
        displayGenre,
        nextActive,
        `${displayGenre} genre`
      );
      await persistPromise;
      if (state.playerView === "browse") {
        renderMusicLanding();
      }
    } catch (err) {
      updateMatchingMusicFavoriteButtons(
        "genre",
        displayGenre,
        previousActive,
        `${displayGenre} genre`
      );
      setMusicPageNotice(`Favorite update failed: ${toUserErrorMessage(err)}`, true);
    } finally {
      favoriteButton.disabled = false;
    }
  });
  card.appendChild(favoriteButton);
  const tile = document.createElement("div");
  tile.className = "music-genre-tile";
  applyGenreTileTheme(tile, displayGenre);
  const collage = document.createElement("div");
  collage.className = "music-genre-collage";
  for (let i = 0; i < 4; i += 1) {
    const cell = document.createElement("div");
    cell.className = "music-genre-collage-cell";
    const img = document.createElement("img");
    img.loading = "lazy";
    img.alt = "";
    cell.appendChild(img);
    collage.appendChild(cell);
  }
  tile.appendChild(collage);
  const tileLabel = document.createElement("span");
  tileLabel.className = "music-genre-tile-label";
  tileLabel.textContent = displayGenre;
  tile.appendChild(tileLabel);
  card.appendChild(tile);
  const content = document.createElement("div");
  content.className = "music-meta-main";
  const title = document.createElement("div");
  title.className = "home-candidate-title";
  title.textContent = displayGenre;
  content.appendChild(title);
  const meta = document.createElement("div");
  meta.className = "home-candidate-meta";
  meta.textContent = "Open artists in this genre";
  content.appendChild(meta);
  card.appendChild(content);
  const action = document.createElement("div");
  action.className = "home-candidate-action";
  const playButton = document.createElement("button");
  playButton.className = "button ghost small";
  playButton.textContent = "Play";
  playButton.addEventListener("click", async (event) => {
    event.preventDefault();
    event.stopPropagation();
    if (playButton.disabled) return;
    playButton.disabled = true;
    playButton.textContent = "Loading...";
    try {
      await playMusicGenreFromBrowse(displayGenre);
    } catch (err) {
      setMusicPageNotice(`Play failed: ${toUserErrorMessage(err)}`, true);
    } finally {
      playButton.disabled = false;
      playButton.textContent = "Play";
    }
  });
  action.appendChild(playButton);
  card.appendChild(action);
  const runBrowseGenre = () => browseMusicGenre(displayGenre, { pushHistory: true });
  card.addEventListener("click", (event) => {
    if (event.target.closest(".music-favorite-button, .home-candidate-action, button, a, input, select, textarea")) {
      return;
    }
    runBrowseGenre();
  });
  [tile, title, content].forEach((el) => {
    if (!el) return;
    el.classList.add("music-card-click-target");
  });
  queueGenreArtworkJob(displayGenre, tile, thumbnailJobs, renderToken);
  return card;
}

async function browseMusicGenre(genre, { pushHistory = true } = {}) {
  const genreValue = normalizeMusicGenreIntent(genre);
  if (!genreValue) return;
  const messageEl = getMusicPageMessageEl();
  const cacheKey = getMusicGenreBrowseCacheKey(genreValue);
  const cached = state.musicGenreBrowseCache[cacheKey];
  if (cached && Array.isArray(cached.artists)) {
    state.musicResultsSort = "recommended";
    renderMusicModeResults(
      {
        artists: cached.artists,
        albums: [],
        tracks: [],
        mode_used: "artist",
      },
      genreValue,
      { pushHistory, browseContext: true }
    );
    setNotice(messageEl, `Loaded artists for ${genreValue}.`, false);
    return;
  }
  renderMusicBrowseLoading(`Loading artists for ${genreValue}...`);
  setNotice(messageEl, `Loading artists for ${genreValue}...`, false);
  try {
    const mergedArtists = await fetchArtistsForGenreIntent(genreValue, { limit: 24 });
    state.musicGenreBrowseCache[cacheKey] = { artists: mergedArtists };
    state.musicResultsSort = "recommended";
    renderMusicModeResults(
      {
        artists: mergedArtists,
        albums: [],
        tracks: [],
        mode_used: "artist",
      },
      genreValue,
      { pushHistory, browseContext: true }
    );
    setNotice(messageEl, `Loaded artists for ${genreValue}.`, false);
  } catch (err) {
    setNotice(messageEl, `Genre browse failed: ${toUserErrorMessage(err)}`, true);
  }
}

function createMusicArtistCard(artistItem, thumbnailJobs, renderToken, { dismissible = false } = {}) {
  const card = document.createElement("article");
  card.className = "home-result-card music-meta-card music-grid-card music-artist-grid-card";
  const artistThumb = createMusicCardThumb(
    artistItem?.name ? `${artistItem.name} artwork` : "Artist artwork"
  );
  card.appendChild(artistThumb.shell);
  if (dismissible) {
    const dismissButton = document.createElement("button");
    dismissButton.type = "button";
    dismissButton.className = "music-genre-dismiss-button";
    dismissButton.title = `Hide ${String(artistItem?.name || "artist").trim()}`;
    dismissButton.setAttribute("aria-label", `Hide ${String(artistItem?.name || "artist").trim()}`);
    dismissButton.textContent = "⊘";
    dismissButton.addEventListener("click", async (event) => {
      event.preventDefault();
      event.stopPropagation();
      const previousHidden = normalizeHiddenMusicArtists(state.musicHiddenArtists);
      const nextHidden = normalizeHiddenMusicArtists([...state.musicHiddenArtists, {
        name: String(artistItem?.name || "").trim(),
        artist_mbid: String(artistItem?.artist_mbid || "").trim() || null,
      }]);
      state.musicHiddenArtists = nextHidden;
      renderMusicLanding();
      try {
        await persistUiPreferences({ music_hidden_artists: nextHidden });
      } catch (err) {
        state.musicHiddenArtists = previousHidden;
        renderMusicLanding();
        setMusicPageNotice(`Hide artist failed: ${toUserErrorMessage(err)}`, true);
      }
    });
    card.appendChild(dismissButton);
  }
  const favoriteButton = createMusicFavoriteButton({
    active: isFavoriteArtist(artistItem),
    label: String(artistItem?.name || "artist"),
  });
  favoriteButton.dataset.favoriteKind = "artist";
  favoriteButton.dataset.favoriteKey = String(artistItem?.artist_mbid || artistItem?.name || "").trim().toLowerCase();
  favoriteButton.addEventListener("click", async (event) => {
    event.preventDefault();
    event.stopPropagation();
    favoriteButton.disabled = true;
    try {
      await toggleFavoriteArtist(artistItem);
      updateMatchingMusicFavoriteButtons(
        "artist",
        String(artistItem?.artist_mbid || artistItem?.name || "").trim().toLowerCase(),
        isFavoriteArtist(artistItem),
        String(artistItem?.name || "artist")
      );
    } catch (err) {
      setMusicPageNotice(`Favorite update failed: ${toUserErrorMessage(err)}`, true);
    } finally {
      favoriteButton.disabled = false;
    }
  });
  card.appendChild(favoriteButton);
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
  if (artistItem?.genre) metaParts.push(String(artistItem.genre));
  if (artistItem?.disambiguation) metaParts.push(String(artistItem.disambiguation));
  meta.textContent = metaParts.join(" • ");
  content.appendChild(meta);
  const statusRow = document.createElement("div");
  statusRow.className = "music-status-row";
  statusRow.innerHTML = buildMusicStatusBadges(artistItem);
  content.appendChild(statusRow);
  const artistRef = document.createElement("div");
  artistRef.className = "home-mb-entity-ref";
  const artistMbid = String(artistItem?.artist_mbid || "").trim();
  artistRef.textContent = artistMbid ? `MB: artist ${artistMbid}` : "MB: artist (unknown)";
  content.appendChild(artistRef);
  card.appendChild(content);
  const action = document.createElement("div");
  action.className = "home-candidate-action";
  const playButton = document.createElement("button");
  playButton.className = "button primary small";
  playButton.textContent = "Play";
  playButton.addEventListener("click", async (event) => {
    event.preventDefault();
    event.stopPropagation();
    if (playButton.disabled) return;
    playButton.disabled = true;
    playButton.textContent = "Loading...";
    try {
      await playMusicArtistFromBrowse(artistItem);
    } catch (err) {
      setMusicPageNotice(`Play failed: ${toUserErrorMessage(err)}`, true);
    } finally {
      playButton.disabled = false;
      playButton.textContent = "Play";
    }
  });
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
    setMusicPageNotice(`Loading albums for ${nextQuery}...`, false);
    try {
      const albums = await fetchMusicAlbumsByArtist({ name: nextQuery, artist_mbid: nextArtistMbid });
      renderMusicModeResults(
        { artists: [], albums, tracks: [], mode_used: "album" },
        nextQuery,
        { pushHistory: true }
      );
      setMusicPageNotice(`Loaded ${albums.length} album candidates for ${nextQuery}.`, false);
    } catch (err) {
      button.disabled = false;
      button.textContent = previousLabel;
      setMusicPageNotice(`View Albums failed: ${toUserErrorMessage(err)}`, true);
    }
  };
  button.addEventListener("click", runViewAlbums);
  card.addEventListener("click", (event) => {
    if (event.target.closest(".music-favorite-button, .home-candidate-action, button, a, input, select, textarea")) {
      return;
    }
    if (button.disabled) return;
    runViewAlbums();
  });
  [artistThumb.shell, title, content].forEach((el) => {
    if (!el) return;
    el.classList.add("music-card-click-target");
  });
  action.appendChild(playButton);
  action.appendChild(button);
  card.appendChild(action);

  const artistMbidValue = String(artistItem?.artist_mbid || "").trim();
  if (artistMbidValue) {
    const cachedCover = getCachedArtistCoverUrl(artistMbidValue);
    if (cachedCover) {
      artistThumb.setImage(cachedCover);
    } else if (Object.prototype.hasOwnProperty.call(state.homeArtistCoverCache, artistMbidValue) && state.homeArtistCoverCache[artistMbidValue] === null) {
      artistThumb.setNoArt();
    } else {
      thumbnailJobs.push(async (activeToken) => {
        if (state.homeMusicRenderToken !== activeToken) {
          return;
        }
        try {
          const persistent = await fetchPersistentArtistCoverEntry(artistMbidValue);
          if (persistent?.coverUrl) {
            setCachedArtistCoverUrl(artistMbidValue, persistent.coverUrl);
            if (state.homeMusicRenderToken === activeToken) {
              artistThumb.setImage(persistent.coverUrl);
            }
            if (!isArtworkCacheStale(persistent.updatedAt)) {
              return;
            }
          }
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
          setCachedArtistCoverUrl(artistMbidValue, coverUrl);
          await persistArtistCoverEntry(artistMbidValue, coverUrl);
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
  return card;
}

function createMusicAlbumCard(albumItem, thumbnailJobs, renderToken, { onQueued } = {}) {
  const releaseGroupMbid = String(albumItem?.release_group_mbid || "").trim();
  const card = document.createElement("article");
  card.className = "home-result-card album-card music-meta-card music-grid-card music-album-grid-card";
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
  const statusRow = document.createElement("div");
  statusRow.className = "music-status-row";
  statusRow.innerHTML = buildMusicStatusBadges(albumItem);
  content.appendChild(statusRow);
  const albumRef = document.createElement("div");
  albumRef.className = "home-mb-entity-ref";
  albumRef.textContent = releaseGroupMbid ? `MB: release-group ${releaseGroupMbid}` : "MB: release-group (unknown)";
  content.appendChild(albumRef);
  card.appendChild(content);
  const action = document.createElement("div");
  action.className = "home-candidate-action home-candidate-action-primary-stack";
  action.dataset.actionKind = "album";
  action.dataset.cardKind = "music";
  const viewTracksButton = document.createElement("button");
  viewTracksButton.className = "button ghost small album-view-tracks-btn";
  viewTracksButton.dataset.releaseGroupMbid = releaseGroupMbid;
  viewTracksButton.dataset.releaseGroupId = releaseGroupMbid;
  viewTracksButton.dataset.albumTitle = String(albumItem?.title || "");
  viewTracksButton.dataset.artistCredit = String(albumItem?.artist || "");
  viewTracksButton.textContent = "View Tracks";
  const button = document.createElement("button");
  button.className = "button primary small album-download-btn home-candidate-download-primary";
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
    setMusicPageNotice(`Loading tracks for ${albumTitle || "album"}...`, false);
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
      setMusicPageNotice(`Loaded ${tracks.length} tracks for ${albumTitle || "album"}.`, false);
    } catch (err) {
      viewTracksButton.disabled = false;
      button.disabled = false;
      viewTracksButton.textContent = previousViewLabel;
      button.textContent = previousDownloadLabel;
      setMusicPageNotice(`View Tracks failed: ${toUserErrorMessage(err)}`, true);
    }
  };
  viewTracksButton.addEventListener("click", runViewTracks);
  card.addEventListener("click", (event) => {
    if (event.target.closest(".home-candidate-action, button, a, input, select, textarea")) {
      return;
    }
    if (viewTracksButton.disabled) return;
    runViewTracks();
  });
  [albumThumb.shell, title, content].forEach((el) => {
    if (!el) return;
    el.classList.add("music-card-click-target");
  });
  button.addEventListener("click", async () => {
    const releaseGroupMbidValue = String(button.dataset.releaseGroupMbid || "").trim();
    if (!releaseGroupMbidValue) return;
    button.disabled = true;
    button.textContent = "Queueing...";
    setMusicPageNotice(`Queueing ${albumItem?.title || "album"}...`, false);
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
      if (typeof onQueued === "function") {
        onQueued(result, albumItem);
      }
    } catch (err) {
      button.disabled = false;
      button.textContent = "Download";
      setMusicPageNotice(`Album queue failed: ${err.message}`, true);
    }
  });
  const playAlbumButton = document.createElement("button");
  playAlbumButton.className = "button primary small music-search-album-play-btn home-candidate-download-primary";
  playAlbumButton.dataset.releaseGroupMbid = releaseGroupMbid;
  playAlbumButton.dataset.albumTitle = String(albumItem?.title || "");
  playAlbumButton.dataset.artistCredit = String(albumItem?.artist || "");
  playAlbumButton.textContent = "Play Album";
  playAlbumButton.addEventListener("click", async (e) => {
    e.stopPropagation();
    if (playAlbumButton.disabled) return;
    playAlbumButton.disabled = true;
    playAlbumButton.textContent = "Loading...";
    try {
      await playMusicAlbumFromSearch(albumItem);
    } catch (err) {
      setMusicPageNotice(`Play Album failed: ${toUserErrorMessage(err)}`, true);
    } finally {
      playAlbumButton.disabled = false;
      playAlbumButton.textContent = "Play Album";
    }
  });
  button.className = "button ghost small album-download-btn";
  action.appendChild(playAlbumButton);
  action.appendChild(button);
  action.appendChild(viewTracksButton);
  card.appendChild(action);

  if (releaseGroupMbid) {
    const cachedCover = getCachedAlbumCoverUrl(releaseGroupMbid);
    if (cachedCover) {
      albumThumb.setImage(cachedCover);
    } else if (Object.prototype.hasOwnProperty.call(state.homeAlbumCoverCache, releaseGroupMbid) && state.homeAlbumCoverCache[releaseGroupMbid] === null) {
      albumThumb.setNoArt();
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

  return card;
}

function getMusicLandingRecentArtists(limit = 6) {
  const historyItems = Array.isArray(state.playerHistory)
    ? state.playerHistory.filter((item) => !item?.is_missing_local)
    : [];
  const libraryArtists = Array.isArray(state.playerLibrarySummary?.artists)
    ? state.playerLibrarySummary.artists
    : [];
  const favorites = Array.isArray(state.musicPreferences?.favorite_artists)
    ? state.musicPreferences.favorite_artists
    : [];
  const seen = new Set();
  const results = [];
  historyItems.forEach((item) => {
    const artistName = String(item?.artist || "").trim();
    const artistKey = String(item?.artist_key || artistName).trim().toLowerCase();
    if (!artistName || !artistKey || seen.has(artistKey) || isBrowseSuppressedArtist({ name: artistName })) {
      return;
    }
    seen.add(artistKey);
    const libraryMatch = libraryArtists.find((entry) => String(entry.artist_key || "").trim().toLowerCase() === artistKey);
    const favoriteMatch = favorites.find((entry) => String(entry?.artist_mbid || entry?.name || "").trim().toLowerCase() === artistKey)
      || favorites.find((entry) => String(entry?.name || "").trim().toLowerCase() === artistName.toLowerCase());
    if (libraryMatch) {
      results.push({
        ...libraryMatch,
        name: libraryMatch.artist || artistName,
      });
      return;
    }
    results.push({
      name: artistName,
      artist_mbid: String(item?.artist_mbid || favoriteMatch?.artist_mbid || "").trim() || null,
      genre: favoriteMatch?.genre || item?.genre || "",
      disambiguation: "Recently played",
    });
  });
  return results.slice(0, limit);
}

function createMusicLandingTrackRow(item) {
  return `
    <article class="music-player-track-row music-player-track-row-rich">
      <div class="music-player-browser-card-art music-player-track-art">
        <img src="${escapeAttr(getMusicLibraryArtworkUrl(item))}" alt="${escapeAttr(item.title || "Track")}" loading="lazy">
      </div>
      <button class="music-player-track" type="button" data-action="player-play" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">
        <span class="music-player-track-title">${escapeHtml(item.title || "Untitled")}</span>
        <span class="music-player-track-meta">${escapeHtml([item.artist, item.album || item.played_at].filter(Boolean).join(" • "))}</span>
      </button>
      <button class="button ghost small" type="button" data-action="player-play-next" data-stream-url="${escapeAttr(item.stream_url || "")}" data-title="${escapeAttr(item.title || "")}" data-artist="${escapeAttr(item.artist || "")}" data-album="${escapeAttr(item.album || "")}" data-local-path="${escapeAttr(item.local_path || "")}" data-source-kind="${escapeAttr(item.source_kind || "local")}">Play Next</button>
    </article>
  `;
}

function renderMusicLanding() {
  const toolbarSlot = getMusicToolbarSlot();
  if (toolbarSlot) toolbarSlot.innerHTML = "";
  const navSlot = getMusicNavSlot();
  if (navSlot) navSlot.innerHTML = "";
  const container = document.getElementById("music-results-container");
  if (!container) {
    return;
  }
  if (state.currentPage !== "music" && !state.homeMusicMode) {
    return;
  }
  const artistInput = String(document.getElementById("search-artist")?.value || "").trim();
  const albumInput = String(document.getElementById("search-album")?.value || "").trim();
  const trackInput = String(document.getElementById("search-track")?.value || "").trim();
  if (artistInput || albumInput || trackInput) {
    return;
  }
  state.homeMusicCurrentView = snapshotMusicResultsView({ landing_view: true }, "");
  state.homeMusicResultMap = {};
  const renderToken = ++state.homeMusicRenderToken;
  const thumbnailJobs = [];
  container.innerHTML = "";
  container.classList.add("music-discovery-landing", "music-home-landing");
  const summary = state.playerLibrarySummary || { artists: [], albums: [], tracks: [] };
  const libraryAlbums = Array.isArray(summary.albums) ? summary.albums : [];
  const recentTracks = Array.isArray(state.playerHistory)
    ? state.playerHistory.filter((item) => !item?.is_missing_local).slice(0, 4)
    : [];
  const recentArtistItems = getMusicLandingRecentArtists(6);
  const recentSearches = loadRecentMusicSearches();
  const favoriteGenres = Array.isArray(state.musicPreferences?.favorite_genres)
    ? state.musicPreferences.favorite_genres
    : [];
  const favoriteArtists = Array.isArray(state.musicPreferences?.favorite_artists)
    ? state.musicPreferences.favorite_artists
    : [];
  const hiddenGenreKeys = new Set(
    normalizeHiddenMusicGenres(state.musicHiddenGenres).map((genre) => genre.toLowerCase())
  );
  const hiddenArtistKeys = new Set(
    normalizeHiddenMusicArtists(state.musicHiddenArtists).map((artist) => getHiddenMusicArtistKey(artist))
  );
  const visiblePopularGenres = [...DEFAULT_MUSIC_GENRES].filter(
    (genre) => !hiddenGenreKeys.has(normalizeMusicGenreIntent(genre).toLowerCase())
  );
  const visibleFavoriteArtists = favoriteArtists.filter(
    (artist) => !hiddenArtistKeys.has(getHiddenMusicArtistKey(artist)) && !isBrowseSuppressedArtist(artist)
  );
  const recentlyAdded = libraryAlbums.slice(0, 6);
  const newFromFavorites = recentlyAdded
    .filter((album) => isMusicArtistFavorited(String(album.artist || ""), String(album.artist_key || "")))
    .slice(0, 6);
  const topGenreSeeds = (favoriteGenres.length ? favoriteGenres : visiblePopularGenres).slice(0, favoriteGenres.length ? 4 : 8);

  warmMusicGenreArtistCaches(topGenreSeeds, { limit: 24 });
  if (visibleFavoriteArtists.length) {
    prefetchMusicAlbumsForArtists(visibleFavoriteArtists, { limit: 6 });
  }
  if (recentArtistItems.length) {
    prefetchMusicAlbumsForArtists(recentArtistItems, { limit: 4 });
  }

  const intro = document.createElement("section");
  intro.className = "music-home-intro";
  intro.innerHTML = `
    <div class="music-home-intro-copy">
      <div class="music-discovery-eyebrow">Music</div>
      <h1 class="music-home-title">Your music, your next move.</h1>
      <p class="music-home-subtitle">Pick up where you left off, re-enter favorites and library picks, or browse outward when you want something new.</p>
    </div>
    <div class="music-home-summary">
      <div class="music-home-summary-pill">
        <span class="music-home-summary-value">${libraryAlbums.length}</span>
        <span class="music-home-summary-label">Albums in library</span>
      </div>
      <div class="music-home-summary-pill">
        <span class="music-home-summary-value">${visibleFavoriteArtists.length}</span>
        <span class="music-home-summary-label">Favorite artists</span>
      </div>
      <div class="music-home-summary-pill">
        <span class="music-home-summary-value">${recentTracks.length}</span>
        <span class="music-home-summary-label">Recent plays</span>
      </div>
    </div>
  `;
  container.appendChild(intro);

  const priorityGrid = document.createElement("section");
  priorityGrid.className = "music-home-priority-grid";

  const continueModule = document.createElement("section");
  continueModule.className = "music-home-module";
  continueModule.innerHTML = `
    <div class="music-home-module-header">
      <div>
        <div class="group-title">${recentTracks.length ? "Continue listening" : "Start from your library"}</div>
        <div class="music-discovery-section-subtitle">${recentTracks.length ? "The quickest way back into what you were already playing." : (recentlyAdded.length ? "Your newest downloaded albums are ready to revisit." : "Start with a few strong browse paths instead of a blank screen.")}</div>
      </div>
      ${recentlyAdded.length ? '<button class="button ghost small" type="button" data-action="music-go-library">Open Library</button>' : ""}
    </div>
  `;
  const continueBody = document.createElement("div");
  if (recentTracks.length) {
    continueBody.className = "music-player-track-list";
    continueBody.innerHTML = recentTracks.map((item) => createMusicLandingTrackRow(item)).join("");
  } else if (recentlyAdded.length) {
    continueBody.className = "music-player-browser-grid music-home-compact-grid";
    recentlyAdded.slice(0, 4).forEach((album) => {
      continueBody.appendChild(createMusicAlbumCard(album, thumbnailJobs, renderToken));
    });
  } else {
    continueBody.className = "music-meta-grid music-home-compact-grid";
    visiblePopularGenres.slice(0, 4).forEach((genre) => {
      continueBody.appendChild(createMusicGenreCard(genre, thumbnailJobs, renderToken, { dismissible: true }));
    });
  }
  continueModule.appendChild(continueBody);
  priorityGrid.appendChild(continueModule);

  const jumpBackModule = document.createElement("section");
  jumpBackModule.className = "music-home-module";
  jumpBackModule.innerHTML = `
    <div class="music-home-module-header">
      <div>
        <div class="group-title">Jump back in</div>
        <div class="music-discovery-section-subtitle">Your recent searches, recent artists, and favorites stay within reach.</div>
      </div>
    </div>
  `;
  const jumpBackBody = document.createElement("div");
  jumpBackBody.className = "music-home-reentry-stack";
  if (recentSearches.length) {
    const searchCluster = document.createElement("div");
    searchCluster.className = "music-home-submodule";
    searchCluster.innerHTML = '<div class="music-home-submodule-title">Recently searched</div>';
    const pills = document.createElement("div");
    pills.className = "music-home-search-pills";
    recentSearches.forEach((entry) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "button ghost music-home-search-pill";
      button.textContent = entry;
      button.addEventListener("click", async () => {
        const queryInput = document.getElementById("music-header-query");
        if (queryInput) {
          queryInput.value = entry;
        }
        await runMusicHeaderSearch();
      });
      pills.appendChild(button);
    });
    searchCluster.appendChild(pills);
    jumpBackBody.appendChild(searchCluster);
  }
  const reentryArtists = recentArtistItems.length ? recentArtistItems : visibleFavoriteArtists.slice(0, 3);
  if (reentryArtists.length) {
    const artistCluster = document.createElement("div");
    artistCluster.className = "music-home-submodule";
    artistCluster.innerHTML = `<div class="music-home-submodule-title">${recentArtistItems.length ? "Artists you revisited" : "Favorite artists"}</div>`;
    const artistGrid = document.createElement("div");
    artistGrid.className = "music-meta-grid music-home-compact-grid music-home-reentry-grid";
    reentryArtists.slice(0, 3).forEach((artist) => {
      artistGrid.appendChild(createMusicArtistCard(artist, thumbnailJobs, renderToken, { dismissible: false }));
    });
    artistCluster.appendChild(artistGrid);
    jumpBackBody.appendChild(artistCluster);
  }
  if (!jumpBackBody.children.length) {
    jumpBackBody.innerHTML = '<div class="home-results-empty">Search for an artist, album, or track once and your re-entry points will appear here.</div>';
  }
  jumpBackModule.appendChild(jumpBackBody);
  priorityGrid.appendChild(jumpBackModule);
  container.appendChild(priorityGrid);

  const appendSection = (titleText, subtitleText, sectionKey, layout = "grid", actionMarkup = "") => {
    const section = document.createElement("section");
    section.className = `music-mode-section music-mode-section-${layout}`;
    section.dataset.discoverySection = sectionKey;
    const sectionHeader = document.createElement("div");
    sectionHeader.className = "music-home-section-header";
    const copy = document.createElement("div");
    copy.className = "music-discovery-section-header";
    const title = document.createElement("div");
    title.className = "group-title";
    title.textContent = titleText;
    copy.appendChild(title);
    if (subtitleText) {
      const subtitle = document.createElement("div");
      subtitle.className = "music-discovery-section-subtitle";
      subtitle.textContent = subtitleText;
      copy.appendChild(subtitle);
    }
    sectionHeader.appendChild(copy);
    if (actionMarkup) {
      const actionSlot = document.createElement("div");
      actionSlot.className = "music-home-section-action";
      actionSlot.innerHTML = actionMarkup;
      sectionHeader.appendChild(actionSlot);
    }
    section.appendChild(sectionHeader);
    const body = document.createElement("div");
    body.className = layout === "grid" ? "music-meta-grid" : "music-meta-stack";
    section.appendChild(body);
    container.appendChild(section);
    return body;
  };

  const updateSectionEmptyState = (body, message) => {
    if (!body) return;
    body.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "home-results-empty";
    empty.textContent = message;
    body.appendChild(empty);
  };

  if (recentlyAdded.length) {
    const libraryGrid = appendSection(
      "From your library",
      "Recently added albums ready to open, play, or queue.",
      "from-library",
      "grid",
      '<button class="button ghost small" type="button" data-action="music-go-library">Open Library</button>'
    );
    recentlyAdded.forEach((album) => {
      libraryGrid.appendChild(createMusicAlbumCard(album, thumbnailJobs, renderToken));
    });
  }

  if (visibleFavoriteArtists.length) {
    const favoriteArtistGrid = appendSection(
      "Jump back into favorite artists",
      "Artists you keep close for quick re-entry.",
      "favorite-artists",
      "grid",
      '<button class="button ghost small" type="button" data-action="music-go-favorites">Open Favorites</button>'
    );
    visibleFavoriteArtists.slice(0, 6).forEach((artist) => {
      favoriteArtistGrid.appendChild(createMusicArtistCard(artist, thumbnailJobs, renderToken, { dismissible: true }));
    });
  }

  const genreActions = `
    ${hiddenGenreKeys.size ? '<button class="button ghost small" type="button" data-action="music-show-hidden-genres">Show Hidden Genres</button>' : ""}
    ${hiddenArtistKeys.size ? '<button class="button ghost small" type="button" data-action="music-show-hidden-artists">Show Hidden Artists</button>' : ""}
  `;
  const genreGrid = appendSection(
    favoriteGenres.length ? "Favorite genres" : "Browse genres",
    favoriteGenres.length
      ? "Pinned styles stay near the top so discovery always starts with your taste."
      : "A clean browse-first path when you do not feel like typing a search yet.",
    "browse-genres",
    "grid",
    genreActions
  );
  const genresToRender = favoriteGenres.length ? favoriteGenres : visiblePopularGenres.slice(0, 8);
  if (genresToRender.length) {
    genresToRender.forEach((genre) => {
      genreGrid.appendChild(createMusicGenreCard(genre, thumbnailJobs, renderToken, { dismissible: !favoriteGenres.length }));
    });
  } else {
    updateSectionEmptyState(genreGrid, "No genres are visible right now.");
  }

  if (newFromFavorites.length) {
    const newFavoritesGrid = appendSection(
      "New from favorite artists",
      "Downloaded albums from artists already in your taste profile.",
      "new-from-favorites",
      "grid"
    );
    newFromFavorites.forEach((album) => {
      newFavoritesGrid.appendChild(createMusicAlbumCard(album, thumbnailJobs, renderToken));
    });
  } else if (topGenreSeeds.length) {
    const recommendationsGrid = appendSection(
      favoriteGenres.length ? "Popular in your top genres" : "Artists to start with",
      favoriteGenres.length
        ? `A broader pass through ${topGenreSeeds.slice(0, 2).join(" and ")}.`
        : "A few strong artist starting points from the genres people reach for most often.",
      "genre-recommendations",
      "grid"
    );
    updateSectionEmptyState(recommendationsGrid, "Loading artist picks...");
    (async () => {
      try {
        const responses = await Promise.all(
          topGenreSeeds.slice(0, favoriteGenres.length ? 2 : 1).map(async (genre) => ({
            genre,
            artists: await fetchArtistsForGenreIntent(genre, { limit: 12 }),
          }))
        );
        if (state.homeMusicRenderToken !== renderToken) return;
        const merged = [];
        const seen = new Set();
        responses.forEach(({ artists }) => {
          (Array.isArray(artists) ? artists : []).forEach((artist) => {
            const key = String(artist?.artist_mbid || artist?.name || "").trim().toLowerCase();
            if (!key || seen.has(key) || isBrowseSuppressedArtist(artist) || hiddenArtistKeys.has(key)) {
              return;
            }
            seen.add(key);
            merged.push(artist);
          });
        });
        recommendationsGrid.innerHTML = "";
        if (!merged.length) {
          updateSectionEmptyState(recommendationsGrid, "No artist picks are available right now.");
          return;
        }
        merged.slice(0, 8).forEach((artist) => {
          recommendationsGrid.appendChild(createMusicArtistCard(artist, thumbnailJobs, renderToken, { dismissible: true }));
        });
        prefetchMusicAlbumsForArtists(merged, { limit: 4 });
        runPrioritizedThumbnailJobs(thumbnailJobs, renderToken, {
          visibleCount: 12,
          immediateConcurrency: 10,
          deferredConcurrency: 4,
          deferredDelayMs: 60,
        });
      } catch (err) {
        if (state.homeMusicRenderToken !== renderToken) return;
        updateSectionEmptyState(recommendationsGrid, `Artist picks unavailable: ${toUserErrorMessage(err)}`);
      }
    })();
  }

  setMusicPageNotice("", false);
  runPrioritizedThumbnailJobs(thumbnailJobs, renderToken, {
    visibleCount: 14,
    immediateConcurrency: 10,
    deferredConcurrency: 4,
    deferredDelayMs: 60,
  });
}

function clearMusicResultsHistory() {
  state.homeMusicCurrentView = null;
  state.homeMusicViewStack = [];
}

async function enqueueAlbum(releaseGroupMbid) {
  if (!assertMusicModeDeliveryAllowed(getMusicPageMessageEl())) {
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
  const messageEl = getMusicPageMessageEl();
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

  let detailsEl = document.getElementById("music-album-failed-details");
  if (!detailsEl) {
    detailsEl = document.createElement("details");
    detailsEl.id = "music-album-failed-details";
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
  if (!assertMusicModeDeliveryAllowed(getMusicPageMessageEl())) {
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

async function fetchMusicTrackPreview(payload = {}) {
  return fetchJson("/api/music/preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      recording_mbid: String(payload.recording_mbid || "").trim(),
      release_mbid: String(payload.release_mbid || payload.mb_release_id || "").trim() || null,
      release_group_mbid: String(payload.release_group_mbid || payload.mb_release_group_id || "").trim() || null,
      artist: String(payload.artist || "").trim() || null,
      track: String(payload.track || "").trim() || null,
      album: String(payload.album || "").trim() || null,
      media_mode: state.homeMediaMode === "music_video" ? "music_video" : "music",
    }),
  });
}

function buildPreviewStreamUrl(sourceUrl) {
  const raw = String(sourceUrl || "").trim();
  if (!raw || !isValidHttpUrl(raw)) {
    return null;
  }
  return `/api/music/preview/stream?url=${encodeURIComponent(raw)}`;
}

function buildLocalPlayerStreamUrl(localPath) {
  const raw = String(localPath || "").trim();
  if (!raw) return null;
  return `/api/player/stream/local?path=${encodeURIComponent(raw)}`;
}

function isYouTubeFamilySource(source, url = "") {
  const sourceKey = String(source || "").trim().toLowerCase();
  if (sourceKey === "youtube" || sourceKey === "youtube_music") return true;
  return !!extractYouTubeVideoId(url);
}

async function recordRuntimeResolution(payload = {}) {
  const recordingMbid = String(payload.recording_mbid || "").trim();
  const sourceUrl = String(payload.source_url || "").trim();
  const source = String(payload.source || "").trim().toLowerCase();
  if (!recordingMbid || !sourceUrl || !isYouTubeFamilySource(source, sourceUrl)) return;
  try {
    await fetchJson("/api/music/runtime-resolution", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        recording_mbid: recordingMbid,
        source_url: sourceUrl,
        source,
        source_id: String(payload.source_id || payload.video_id || extractYouTubeVideoId(sourceUrl) || "").trim() || null,
        artist: String(payload.artist || "").trim() || null,
        track: String(payload.track || "").trim() || null,
        album: String(payload.album || "").trim() || null,
        resolved_via: String(payload.resolved_via || "").trim() || null,
      }),
    });
  } catch (_err) {}
}

// Resolve a recording MBID to a stream URL using the local resolution index, then
// the preview/YouTube endpoint as a fallback. Returns a stream_url string or null.
// Extract YouTube video ID from a watch URL or youtu.be shortlink.
function extractYouTubeVideoId(url) {
  const s = String(url || "").trim();
  if (!s) return null;
  try {
    const u = new URL(s);
    if (u.hostname === "youtu.be") return u.pathname.slice(1) || null;
    if (u.hostname.includes("youtube.com")) return u.searchParams.get("v") || null;
  } catch (_e) {}
  return null;
}

// Resolve a recording MBID to stream metadata.
// Returns { stream_url, video_id, source, resolved_via } or null.
// video_id is populated for YouTube sources — playMusicPlayerItem uses it to trigger
// the YT IFrame player path instead of the <audio> element.
async function resolveRecordingStreamUrl(recordingMbid, trackMeta = {}) {
  // 1. Resolution index (local DB — fast, no external calls required)
  if (recordingMbid) {
    try {
      const resolution = await fetchJson(`/resolve/recording/${encodeURIComponent(recordingMbid)}`);
      const selectedUrl = String(resolution?.selection?.selected_url || "").trim();
      const source = String(resolution?.best_source?.source || "").toLowerCase();
      if (selectedUrl && isYouTubeFamilySource(source, selectedUrl)) {
        const videoId = extractYouTubeVideoId(selectedUrl);
        if (videoId) {
          // YouTube URL — no proxy needed, use IFrame player directly.
          return { stream_url: selectedUrl, video_id: videoId, source: source || "youtube", resolved_via: "resolution_index" };
        }
      }
    } catch (_err) {
      // index miss — fall through
    }
  }
  // 2. Preview endpoint (community cache + YouTube fallback, may take a few seconds)
  try {
    const response = await fetchMusicTrackPreview({ recording_mbid: recordingMbid, ...trackMeta });
    const sourceUrl = String(response?.source_url || "").trim();
    const source = String(response?.source || "").trim().toLowerCase();
    const videoId = String(response?.video_id || extractYouTubeVideoId(sourceUrl) || "").trim() || null;
    if (sourceUrl && videoId && isYouTubeFamilySource(source, sourceUrl)) {
      await recordRuntimeResolution({
        recording_mbid: recordingMbid,
        source_url: sourceUrl,
        source: source || "youtube",
        video_id: videoId,
        artist: trackMeta.artist,
        track: trackMeta.track,
        album: trackMeta.album,
        resolved_via: "preview_api",
      });
      return { stream_url: sourceUrl, video_id: videoId, source: source || "youtube", resolved_via: "preview_api" };
    }
  } catch (_err) {
    // preview unavailable
  }
  return null;
}

// Resolve a search-result track to a stream and play it in the main player.
// Checks the local library first; falls back to the resolution index and preview API.
async function playMusicSearchResult(result) {
  const recordingMbid = String(result?.recording_mbid || "").trim();
  // 1. Local library match by MBID
  const localMatch = recordingMbid
    ? (state.playerLibrary || []).find((item) => String(item?.recording_mbid || "") === recordingMbid)
    : null;
  if (localMatch && (localMatch.stream_url || localMatch.local_path)) {
    const item = normalizePlayableItem({
      ...localMatch,
      kind: "local",
      title: localMatch.title || result.track || "Track",
      artist: localMatch.artist || result.artist || "",
      album: localMatch.album || result.album || "",
    });
    clearActiveStationPlayback();
    setPlayerQueue([item]);
    setMusicPlayerView("queue");
    setMusicSection("player");
    await playMusicPlayerItem(item);
    return;
  }
  // 2. Resolution index → preview API
  const resolved = await resolveRecordingStreamUrl(recordingMbid, {
    artist: result.artist,
    track: result.track,
    album: result.album,
    release_mbid: result.mb_release_id,
    release_group_mbid: result.mb_release_group_id,
  });
  if (!resolved?.stream_url && !resolved?.video_id) throw new Error("No playable stream found. Try downloading the track first.");
  const item = normalizePlayableItem({
    id: `search:${recordingMbid || result.track || "track"}`,
    title: result.track || "Track",
    artist: result.artist || "",
    album: result.album || "",
    stream_url: resolved.stream_url || null,
    video_id: resolved.video_id || null,
    kind: resolved.video_id ? "youtube" : "cached",
    source: resolved.source,
    resolved_via: resolved.resolved_via,
    recording_mbid: recordingMbid || null,
    artwork_url: result.artwork_url || null,
    mb_release_group_id: String(result?.mb_release_group_id || "").trim() || null,
  });
  clearActiveStationPlayback();
  setPlayerQueue([item]);
  setMusicPlayerView("queue");
  setMusicSection("player");
  await playMusicPlayerItem(item);
}

async function playMusicArtistFromBrowse(artistItem) {
  const nextQuery = String(artistItem?.name || artistItem?.artist || "").trim();
  const nextArtistMbid = String(artistItem?.artist_mbid || artistItem?.id || "").trim();
  if (!nextQuery) throw new Error("Artist identity unknown.");
  const albums = await fetchMusicAlbumsByArtist({ name: nextQuery, artist_mbid: nextArtistMbid });
  if (!Array.isArray(albums) || !albums.length) throw new Error("No playable albums found for this artist.");
  await playMusicAlbumFromSearch(albums[0]);
}

async function playMusicGenreFromBrowse(genreValue) {
  const artists = await fetchArtistsForGenreIntent(genreValue, { limit: 12 });
  if (!Array.isArray(artists) || !artists.length) throw new Error("No playable artists found for this genre.");
  await playMusicArtistFromBrowse(artists[0]);
}

// Fetch album tracks and queue them for playback.
// Local files play immediately; unresolved tracks are queued and resolved lazily by the station engine.
async function playMusicAlbumFromSearch(albumItem) {
  const releaseGroupMbid = String(albumItem?.release_group_mbid || "").trim();
  const artistQuery = String(albumItem?.artist || "").trim();
  const albumTitle = String(albumItem?.title || "").trim();
  if (!releaseGroupMbid && !artistQuery && !albumTitle) throw new Error("Album identity unknown.");
  const tracks = await fetchMusicTracksByAlbum({
    artist: artistQuery,
    album: albumTitle,
    releaseGroupMbid,
    limit: 100,
  });
  if (!tracks.length) throw new Error("No tracks found for this album.");
  const queueItems = tracks.map((t) => {
    const mbid = String(t?.recording_mbid || "").trim();
    const local = mbid
      ? (state.playerLibrary || []).find((item) => String(item?.recording_mbid || "") === mbid)
      : null;
    if (local && (local.stream_url || local.local_path)) {
      return normalizePlayableItem({ ...local, kind: "local", title: local.title || t.track, artist: local.artist || artistQuery, album: albumTitle });
    }
    return normalizePlayableItem({
      id: `search:${mbid || t.track || "track"}`,
      title: t.track || "Track",
      artist: t.artist || artistQuery,
      album: albumTitle,
      recording_mbid: mbid || null,
      mb_release_group_id: releaseGroupMbid || null,
      kind: "unresolved",
      stream_url: null,
    });
  });
  clearActiveStationPlayback();
  setPlayerQueue(queueItems);
  setMusicPlayerView("queue");
  setMusicSection("player");
  // Play first local item immediately if available; otherwise resolve via index → preview API.
  const firstLocalIndex = queueItems.findIndex((i) => i.kind === "local" && (i.stream_url || i.local_path));
  if (firstLocalIndex >= 0) {
    await playMusicPlayerItem(queueItems[firstLocalIndex]);
    _prefetchNextUnresolved(firstLocalIndex + 1);
    return;
  }
  // No local files — resolve first unresolved track and start playback.
  const firstUnresolved = queueItems[0];
  if (!firstUnresolved?.recording_mbid) return;
  const resolved = await resolveRecordingStreamUrl(firstUnresolved.recording_mbid, {
    artist: firstUnresolved.artist, track: firstUnresolved.title, album: albumTitle, release_group_mbid: releaseGroupMbid,
  });
  if (!resolved?.stream_url && !resolved?.video_id) return;
  const firstItem = normalizePlayableItem({ ...firstUnresolved, ...resolved, kind: resolved.video_id ? "youtube" : "cached" });
  queueItems[0] = firstItem;
  setPlayerQueue(queueItems);
  await playMusicPlayerItem(firstItem);
  _prefetchNextUnresolved(1);
}

function createMusicTrackResultCard(result, thumbnailJobs, renderToken, { badgeText = "MusicBrainz" } = {}) {
  const key = `${result.recording_mbid}::${result.mb_release_id}`;
  state.homeMusicResultMap[key] = result;
  const card = document.createElement("article");
  card.className = "home-result-card music-meta-card";
  card.dataset.recordingMbid = String(result.recording_mbid || "").trim();
  card.dataset.releaseMbid = String(result.mb_release_id || "").trim();
  card.dataset.releaseGroupMbid = String(result.mb_release_group_id || "").trim();
  const trackThumb = createMusicCardThumb(result?.album ? `${result.album} cover` : "Album cover");
  card.appendChild(trackThumb.shell);

  const content = document.createElement("div");
  content.className = "music-meta-main";
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
  badge.textContent = badgeText;
  header.appendChild(badge);
  content.appendChild(header);

  const meta = document.createElement("div");
  meta.className = "home-candidate-meta";
  const durationText = Number.isFinite(result.duration_ms) ? formatDuration(result.duration_ms / 1000) : "-";
  meta.textContent = `${result.artist} • ${durationText}`;
  content.appendChild(meta);
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
  content.appendChild(albumLine);
  const statusRow = document.createElement("div");
  statusRow.className = "music-status-row";
  statusRow.innerHTML = buildMusicStatusBadges(result);
  content.appendChild(statusRow);

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
  content.appendChild(entityRef);
  card.appendChild(content);

  const action = document.createElement("div");
  action.className = "home-candidate-action";
  action.dataset.actionKind = "track";
  action.dataset.cardKind = "music";
  const previewButton = document.createElement("button");
  previewButton.className = "button ghost small music-preview-btn";
  previewButton.dataset.musicResultKey = key;
  previewButton.textContent = "Preview";
  const playButton = document.createElement("button");
  playButton.className = "button primary small music-search-play-btn home-candidate-download-primary";
  playButton.dataset.musicResultKey = key;
  playButton.textContent = "Play";
  const button = document.createElement("button");
  button.className = "button ghost small music-download-btn";
  button.dataset.musicResultKey = key;
  button.dataset.recordingMbid = String(result.recording_mbid || "").trim();
  button.dataset.releaseMbid = String(result.mb_release_id || "").trim();
  button.dataset.releaseGroupMbid = String(result.mb_release_group_id || "").trim();
  button.textContent = "Download";
  action.appendChild(playButton);
  action.appendChild(button);
  action.appendChild(previewButton);
  card.appendChild(action);

  const releaseGroupMbid = String(result?.mb_release_group_id || "").trim();
  if (releaseGroupMbid) {
    const cachedCover = getCachedAlbumCoverUrl(releaseGroupMbid);
    if (cachedCover) {
      trackThumb.setImage(cachedCover);
    } else if (Object.prototype.hasOwnProperty.call(state.homeAlbumCoverCache, releaseGroupMbid) && state.homeAlbumCoverCache[releaseGroupMbid] === null) {
      trackThumb.setNoArt();
    } else {
      thumbnailJobs.push(async (activeToken) => {
        const coverUrl = await fetchHomeAlbumCoverUrl(releaseGroupMbid);
        if (!coverUrl || state.homeMusicRenderToken !== activeToken) {
          if (!coverUrl && state.homeMusicRenderToken === activeToken) {
            trackThumb.setNoArt();
          }
          return;
        }
        trackThumb.setImage(coverUrl);
      });
    }
  } else if (result.artwork_url) {
    trackThumb.setImage(result.artwork_url);
  } else {
    trackThumb.setNoArt();
  }

  return card;
}

function renderMusicModeResults(response, query = "", { pushHistory = false, browseContext = false } = {}) {
  if (response?.landing_view) {
    resetLegacyMusicSearchInputs();
    renderMusicLanding();
    return;
  }
  if (pushHistory && state.homeMusicCurrentView) {
    state.homeMusicViewStack.push(state.homeMusicCurrentView);
  }
  state.homeMusicCurrentView = snapshotMusicResultsView(response, query);
  const artists = Array.isArray(response?.artists) ? response.artists : [];
  const albums = Array.isArray(response?.albums) ? response.albums : [];
  const tracks = normalizeMusicSearchResults(response?.tracks);
  const hiddenArtistKeys = new Set(
    normalizeHiddenMusicArtists(state.musicHiddenArtists).map((artist) => getHiddenMusicArtistKey(artist))
  );
  const visibleArtists = browseContext
    ? artists.filter((artist) => !hiddenArtistKeys.has(getHiddenMusicArtistKey(artist)))
    : artists;
  const container = document.getElementById("music-results-container");
  const toolbarSlot = getMusicToolbarSlot();
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

  if (!visibleArtists.length && !albums.length && !tracks.length) {
    if (toolbarSlot) toolbarSlot.innerHTML = "";
    const empty = document.createElement("div");
    empty.className = "home-results-empty";
    empty.textContent = "No music metadata matches found.";
    container.appendChild(empty);
    setMusicPageNotice("No music metadata matches found. Try a different query or mode.", true);
    return;
  }

  setMusicPageNotice("", false);

  // Back button — rendered directly above the results grid so it's always visible.
  const navSlot = getMusicNavSlot();
  if (navSlot) navSlot.innerHTML = "";
  if (state.homeMusicViewStack.length) {
    const backButton = document.createElement("button");
    backButton.className = "button ghost small";
    const currentIsAlbumView = !artists.length && albums.length > 0 && !tracks.length;
    let targetIndex = state.homeMusicViewStack.length - 1;
    if (currentIsAlbumView) {
      const landingIndex = state.homeMusicViewStack.findIndex((entry) => !!entry?.response?.landing_view);
      if (landingIndex >= 0) {
        targetIndex = landingIndex;
      }
    }
    const previousView = state.homeMusicViewStack[targetIndex];
    const previousIsLanding = !!previousView?.response?.landing_view;
    const previousHasAlbums = Array.isArray(previousView?.response?.albums) && previousView.response.albums.length > 0;
    backButton.textContent = previousIsLanding ? "← Home" : (previousHasAlbums ? "← Albums" : "← Back");
    backButton.addEventListener("click", () => {
      const previous = state.homeMusicViewStack[targetIndex];
      if (!previous) return;
      state.homeMusicViewStack = state.homeMusicViewStack.slice(0, targetIndex);
      renderMusicModeResults(previous.response, previous.query, { pushHistory: false });
    });
    if (navSlot) navSlot.appendChild(backButton);
    renderMusicResultsControls({ artists: visibleArtists, albums, tracks });
  } else {
    renderMusicResultsControls({ artists: visibleArtists, albums, tracks });
  }

  function appendSection(titleText, layout = "stack") {
    const section = document.createElement("section");
    section.className = `music-mode-section music-mode-section-${layout}`;
    const sectionHeader = document.createElement("div");
    sectionHeader.className = "group-title";
    sectionHeader.textContent = titleText;
    section.appendChild(sectionHeader);
    const body = document.createElement("div");
    body.className = layout === "grid" ? "music-meta-grid" : "music-meta-stack";
    section.appendChild(body);
    container.appendChild(section);
    return body;
  }

  const rawTopMatch = getMusicTopMatchCandidate({ artists: visibleArtists, albums, tracks }, query);
  const topMatch = rawTopMatch?.kind === "track" ? rawTopMatch : null;
  const filteredArtists = topMatch?.kind === "artist"
    ? visibleArtists.filter((artist) => String(artist?.artist_mbid || artist?.name || "") !== String(topMatch.key || ""))
    : visibleArtists;
  const filteredAlbums = topMatch?.kind === "album"
    ? albums.filter((album) => String(album?.release_group_mbid || album?.title || "") !== String(topMatch.key || ""))
    : albums;
  const filteredTracks = topMatch?.kind === "track"
    ? tracks.filter((track, index) => `${track?.recording_mbid || track?.track || "track"}::${track?.mb_release_id || index}` !== String(topMatch.key || ""))
    : tracks;

  if (topMatch) {
    const topMatchBody = appendSection("Top Match", topMatch.kind === "track" ? "stack" : "grid");
    if (topMatch.kind === "artist") {
      topMatchBody.appendChild(createMusicArtistCard(topMatch.item, thumbnailJobs, renderToken, { dismissible: browseContext }));
    } else if (topMatch.kind === "album") {
      topMatchBody.appendChild(createMusicAlbumCard(topMatch.item, thumbnailJobs, renderToken));
    } else {
      topMatchBody.appendChild(createMusicTrackResultCard(topMatch.item, thumbnailJobs, renderToken, { badgeText: "Top Match" }));
    }
  }

  if (filteredArtists.length) {
    const artistGrid = appendSection("Artists", "grid");
    const sortedArtists = getSortedMusicArtists(filteredArtists);
    sortedArtists.forEach((artistItem) => {
      artistGrid.appendChild(createMusicArtistCard(artistItem, thumbnailJobs, renderToken, { dismissible: browseContext }));
    });
    prefetchMusicAlbumsForArtists(sortedArtists, { limit: 6 });
  }

  if (filteredAlbums.length) {
    const albumGrid = appendSection("Albums", "grid");
    getSortedMusicAlbums(filteredAlbums).forEach((albumItem) => {
      albumGrid.appendChild(createMusicAlbumCard(albumItem, thumbnailJobs, renderToken));
    });
  }
  runPrioritizedThumbnailJobs(thumbnailJobs, renderToken, {
    visibleCount: filteredTracks.length ? 10 : 12,
    immediateConcurrency: filteredTracks.length ? 8 : 10,
    deferredConcurrency: 4,
    deferredDelayMs: 80,
  });

  if (filteredTracks.length) {
    const trackStack = appendSection("Tracks", "stack");
    const orderedTracks = [...filteredTracks].sort((a, b) => {
      const discA = Number.isFinite(Number(a?.disc_number)) ? Number(a.disc_number) : Number.MAX_SAFE_INTEGER;
      const discB = Number.isFinite(Number(b?.disc_number)) ? Number(b.disc_number) : Number.MAX_SAFE_INTEGER;
      if (discA !== discB) return discA - discB;
      const trackA = Number.isFinite(Number(a?.track_number)) ? Number(a.track_number) : Number.MAX_SAFE_INTEGER;
      const trackB = Number.isFinite(Number(b?.track_number)) ? Number(b.track_number) : Number.MAX_SAFE_INTEGER;
      if (trackA !== trackB) return trackA - trackB;
      return String(a?.track || "").localeCompare(String(b?.track || ""));
    });
    orderedTracks.forEach((result) => {
      trackStack.appendChild(createMusicTrackResultCard(result, thumbnailJobs, renderToken));
    });
  }
  focusMusicResults();
}

async function fetchMusicAlbumsByArtist(artist) {
  const query = typeof artist === "object" && artist !== null
    ? String(artist.name || "").trim()
    : String(artist || "").trim();
  const artistMbid = typeof artist === "object" && artist !== null
    ? String(artist.artist_mbid || "").trim()
    : "";
  const cacheKey = getMusicArtistAlbumsCacheKey(artist);
  if (cacheKey && Array.isArray(state.musicArtistAlbumsCache[cacheKey])) {
    return state.musicArtistAlbumsCache[cacheKey].map((item) => ({ ...item }));
  }
  if (cacheKey && state.musicArtistAlbumsInFlight[cacheKey]) {
    const pending = await state.musicArtistAlbumsInFlight[cacheKey];
    return Array.isArray(pending) ? pending.map((item) => ({ ...item })) : [];
  }
  if (!query) {
    return [];
  }
  const params = new URLSearchParams();
  params.set("q", query);
  params.set("limit", "50");
  if (artistMbid) {
    params.set("artist_mbid", artistMbid);
  }
  const requestPromise = (async () => {
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
    if (cacheKey) {
      state.musicArtistAlbumsCache[cacheKey] = out.map((item) => ({ ...item }));
    }
    return out;
  })();
  if (cacheKey) {
    state.musicArtistAlbumsInFlight[cacheKey] = requestPromise;
  }
  try {
    const out = await requestPromise;
    return out.map((item) => ({ ...item }));
  } finally {
    if (cacheKey) {
      delete state.musicArtistAlbumsInFlight[cacheKey];
    }
  }
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
  const musicModeEnabledNow = !!state.homeMusicMode || state.currentPage === "music";
  if (!musicModeEnabledNow) {
    return;
  }
  if (!artist && !album && !track) {
    if (requestSeq !== state.homeMusicSearchSeq) {
      return;
    }
    if (!state.homeMusicMode && state.currentPage !== "music") {
      return;
    }
    clearMusicResultsHistory();
    renderMusicModeResults({ artists: [], albums: [], tracks: [], mode_used: "auto" });
    return;
  }
  const modeSelect = document.getElementById("music-mode-select");
  const mode = modeSelect ? modeSelect.value : "auto";
  const response = await fetch(
    `/api/music/search?artist=${encodeURIComponent(artist)}&album=${encodeURIComponent(album)}&track=${encodeURIComponent(track)}&mode=${encodeURIComponent(mode)}&offset=0&limit=48`
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
  const musicModeStillEnabled = !!state.homeMusicMode || state.currentPage === "music";
  if (!musicModeStillEnabled) {
    return;
  }
  const displayQuery = [artist, album, track].filter(Boolean).join(" ");
  clearMusicResultsHistory();
  renderMusicModeResults(payload, displayQuery);
  focusHomeResults();
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
  state.homeCandidatesByItem = {};
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
  setNotice(messageEl || getMusicPageMessageEl(), "Music Mode: loading metadata results...", false);
  clearLegacyHomeSearchState();
  await performMusicModeSearch();
  setHomeSearchControlsEnabled(true);
  setHomeSearchActive(false);
}

async function handleHomeStandardSearch(autoEnqueue, inputValue, messageEl) {
  const payload = buildHomeSearchPayload(autoEnqueue, inputValue);
  const modeLabel = "Search";
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
  state.homeCandidatesByItem = {};
  state.homeSearchRequestId = data.request_id;
  state.homeSearchMode = autoEnqueue ? "download" : "searchOnly";
  updateHomeViewAdvancedLink();
  setNotice(messageEl, "", false);
  showHomeResults(true);
  focusHomeResults();
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
    actions.className = "home-candidate-action home-candidate-action-primary-stack";
    actions.dataset.actionKind = "album";
    actions.dataset.cardKind = "video";
    const viewTracksButton = document.createElement("button");
    viewTracksButton.className = "button ghost small album-view-tracks-btn";
    viewTracksButton.dataset.releaseGroupId = candidate.release_group_id || "";
    viewTracksButton.dataset.albumTitle = candidate.title || "";
    viewTracksButton.dataset.artistCredit = candidate.artist_credit || "";
    viewTracksButton.textContent = "View Tracks";
    const button = document.createElement("button");
    button.className = "button primary small album-download-btn home-candidate-download-primary";
    button.dataset.releaseGroupId = candidate.release_group_id || "";
    button.dataset.albumTitle = candidate.title || "";
    const alreadyQueued = state.homeQueuedAlbumReleaseGroups.has(candidate.release_group_id || "");
    button.textContent = alreadyQueued ? "Queued..." : "Download Album";
    button.disabled = alreadyQueued;
    actions.appendChild(button);
    actions.appendChild(viewTracksButton);
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
      setMusicPageNotice(`Loading tracks for ${albumTitle || "album"}...`, false);
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
        setMusicPageNotice(`Loaded ${tracks.length} tracks for ${albumTitle || "album"}.`, false);
      } catch (err) {
        viewTracksButton.disabled = false;
        viewTracksButton.textContent = previousViewLabel;
        if (relatedDownloadButton) relatedDownloadButton.disabled = previousDownloadDisabled;
        setMusicPageNotice(`View Tracks failed: ${toUserErrorMessage(err)}`, true);
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
    setMusicPageNotice(`Queueing ${button.dataset.albumTitle || "album"}...`, false);
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
      setMusicPageNotice(`Album queue failed: ${err.message}`, true);
    }
  });
  header.insertAdjacentElement("afterend", container);
  const cards = Array.from(container.querySelectorAll(".album-card"));
  scheduleAlbumCardArtworkLoads(cards, normalized, { visibleCount: 10 });
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
  focusHomeResults();
}

async function fetchHomeAlbumCoverUrl(albumId) {
  const key = String(albumId || "").trim();
  if (!key) {
    return null;
  }
  const directCoverUrl = `https://coverartarchive.org/release-group/${encodeURIComponent(key)}/front-250`;
  const cached = getCachedAlbumCoverUrl(key);
  if (cached) {
    return cached;
  }
  if (state.homeAlbumCoverInFlight[key]) {
    return state.homeAlbumCoverInFlight[key];
  }
  const requestPromise = (async () => {
  try {
    const data = await fetchJson(`/api/music/album/art/${encodeURIComponent(key)}`);
    const url = normalizeArtworkUrl(data?.cover_url);
    // Cache positive hits only; avoid pinning transient misses/rate limits as permanent null.
    if (url) {
      setCachedAlbumCoverUrl(key, url);
      return url;
    }
    // Backend returned no result; allow browser to attempt direct cover-art endpoint.
    setCachedAlbumCoverUrl(key, directCoverUrl);
    return directCoverUrl;
  } catch (_err) {
    // Backend lookup failed; still attempt direct cover-art endpoint from browser.
    setCachedAlbumCoverUrl(key, directCoverUrl);
    return directCoverUrl;
  }
  })();
  state.homeAlbumCoverInFlight[key] = requestPromise;
  try {
    return await requestPromise;
  } finally {
    delete state.homeAlbumCoverInFlight[key];
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
    setHomeResultsStatus("Results");
    setHomeResultsDetail("Search results appear here. Choose a result when you are ready to download.", false);
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
  candidateList.className = "home-candidate-list home-candidate-grid";
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

function renderHomeCandidatesIntoContainer(item, container, candidates = []) {
  if (!container) return;
  container.textContent = "";
  const sortedCandidates = getSortedHomeCandidates(candidates);
  if (!sortedCandidates.length) {
    const placeholder = document.createElement("div");
    placeholder.className = "home-results-empty";
    placeholder.textContent = "Searching…";
    container.appendChild(placeholder);
    return;
  }
  const requestId = item?.request_id || state.homeSearchRequestId;
  const snapshot = state.homeJobSnapshot && state.homeJobSnapshot.requestId === requestId
    ? state.homeJobSnapshot
    : null;
  const rendered = new Set();
  let bestScore = Number.NEGATIVE_INFINITY;
  sortedCandidates.forEach((candidate) => {
    if (candidate?.id) {
      rendered.add(candidate.id);
      state.homeCandidateData[candidate.id] = { candidate, item };
    }
    if (candidate?.final_score !== undefined) {
      const score = Number(candidate.final_score);
      if (Number.isFinite(score) && score > bestScore) {
        bestScore = score;
      }
    }
    const row = renderHomeCandidateRow(candidate, item);
    const job = snapshot && candidate?.url ? snapshot.jobsByUrl.get(candidate.url) : null;
    updateHomeCandidateRowState(row, candidate, item, job || null);
    container.appendChild(row);
  });
  if (item?.id) {
    state.homeCandidateCache[item.id] = rendered;
  }
  if (Number.isFinite(bestScore) && requestId) {
    recordHomeCandidateScore(requestId, bestScore);
  }
}

function rerenderAllHomeCandidateLists() {
  Object.entries(state.homeCandidatesByItem || {}).forEach(([itemId, entry]) => {
    if (!entry?.item || !Array.isArray(entry?.candidates)) return;
    const selector = `.home-result-card[data-item-id="${CSS.escape(itemId)}"] .home-candidate-list`;
    const container = document.querySelector(selector);
    if (!container) return;
    renderHomeCandidatesIntoContainer(entry.item, container, entry.candidates);
  });
}

function getHomeItemBestCandidate(item) {
  if (!item?.id) return null;
  const entry = state.homeCandidatesByItem?.[item.id];
  const candidates = Array.isArray(entry?.candidates) ? entry.candidates : Array.isArray(item.candidates) ? item.candidates : [];
  if (!candidates.length) return null;
  return getSortedHomeCandidates(candidates)[0] || null;
}

function getSortedHomeResultItems(items = []) {
  const results = Array.isArray(items) ? [...items] : [];
  const sortMode = String(state.homeVideoSort || UI_DEFAULTS.home_video_sort);
  const byBestMatch = (a, b) => {
    const candidateA = getHomeItemBestCandidate(a);
    const candidateB = getHomeItemBestCandidate(b);
    const scoreDiff = Number(candidateB?.final_score || 0) - Number(candidateA?.final_score || 0);
    if (scoreDiff !== 0) return scoreDiff;
    const sourceDiff = getHomeCandidateSourcePriority(candidateA) - getHomeCandidateSourcePriority(candidateB);
    if (sourceDiff !== 0) return sourceDiff;
    const postedDiff = getHomeCandidatePostedValue(candidateB) - getHomeCandidatePostedValue(candidateA);
    if (postedDiff !== 0) return postedDiff;
    return String(a?.title || "").localeCompare(String(b?.title || ""));
  };
  if (sortMode === "newest") {
    return results.sort((a, b) => {
      const candidateA = getHomeItemBestCandidate(a);
      const candidateB = getHomeItemBestCandidate(b);
      const postedDiff = getHomeCandidatePostedValue(candidateB) - getHomeCandidatePostedValue(candidateA);
      if (postedDiff !== 0) return postedDiff;
      return byBestMatch(a, b);
    });
  }
  if (sortMode === "source_priority") {
    return results.sort((a, b) => {
      const candidateA = getHomeItemBestCandidate(a);
      const candidateB = getHomeItemBestCandidate(b);
      const sourceDiff = getHomeCandidateSourcePriority(candidateA) - getHomeCandidateSourcePriority(candidateB);
      if (sourceDiff !== 0) return sourceDiff;
      return byBestMatch(a, b);
    });
  }
  if (sortMode === "title_asc") {
    return results.sort((a, b) => {
      const titleDiff = String(a?.title || "").localeCompare(String(b?.title || ""));
      if (titleDiff !== 0) return titleDiff;
      return byBestMatch(a, b);
    });
  }
  return results.sort(byBestMatch);
}

function getVisibleHomeResultItems(items = []) {
  const sortedItems = getSortedHomeResultItems(items);
  const configuredLimit = Number.parseInt(String(state.homeVideoResultsLimit || 0), 10);
  if (!Number.isFinite(configuredLimit) || configuredLimit <= 0) {
    return sortedItems;
  }
  return sortedItems.slice(0, configuredLimit);
}

function rerenderHomeResultCards() {
  const container = $("#home-results-list");
  if (!container) return;
  const items = Array.isArray(state.homeResultItems) ? state.homeResultItems : [];
  if (!items.length) return;
  const sortedItems = getVisibleHomeResultItems(items);
  const existingCards = new Map();
  container.querySelectorAll(".home-result-card[data-item-id]").forEach((card) => {
    existingCards.set(card.dataset.itemId, card);
  });
  const fragment = document.createDocumentFragment();
  sortedItems.forEach((item) => {
    if (!item?.id) return;
    let card = existingCards.get(String(item.id));
    if (!card) {
      card = renderHomeResultItem(item);
    } else {
      updateHomeResultItemCard(card, item);
    }
    fragment.appendChild(card);
  });
  container.replaceChildren(fragment);
  rerenderAllHomeCandidateLists();
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
    state.homeCandidatesByItem[item.id] = { item, candidates: [...candidates] };
    setHomeResultsState({ hasResults: true, terminal: false });
    renderHomeCandidatesIntoContainer(item, container, candidates);
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
        const activeEntry = state.homeCandidatesByItem[item.id];
        renderHomeCandidatesIntoContainer(item, container, activeEntry?.candidates || candidates);
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
  artwork.className = `home-candidate-artwork ${artworkUrl ? "loading" : "no-art"}`;
  if (artworkUrl) {
    const img = document.createElement("img");
    img.src = artworkUrl;
    img.alt = candidate.source || "";
    img.loading = "lazy";
    img.addEventListener("load", () => {
      artwork.classList.remove("loading", "no-art");
      artwork.classList.add("loaded");
    });
    img.addEventListener("error", () => {
      artwork.classList.remove("loading", "loaded");
      artwork.classList.add("no-art");
      img.removeAttribute("src");
    });
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
  const postedDate = formatCandidatePostedDate(candidate);
  const durationSeconds = Number(candidate?.duration_sec);
  const durationText = Number.isFinite(durationSeconds) && durationSeconds > 0
    ? formatDuration(durationSeconds)
    : "";
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
  if (item.media_type !== "music") {
    action.classList.add("home-candidate-action-video");
  }
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

    if (item.media_type !== "music") {
      button.classList.add("home-candidate-download-primary");
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
    if (item.media_type !== "music") {
      button.classList.add("home-candidate-download-primary");
    }
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
      previewButton.dataset.previewDescription = String(
        candidate.description || candidate.summary || candidate.snippet || ""
      ).trim();
      previewButton.dataset.previewPostedText = postedDate;
      previewButton.dataset.previewDurationText = durationText;
      previewButton.dataset.previewItemId = String(item.id || "").trim();
      previewButton.dataset.previewCandidateId = String(candidate.id || "").trim();
      previewButton.dataset.previewDirectUrl = String(candidate.url || "").trim();
      action.appendChild(previewButton);
      titleEl.dataset.previewEnabled = "true";
      artwork.dataset.previewEnabled = "true";
    }

    const hoverPreviewDescriptor = item.media_type !== "music"
      ? buildHomeHoverPreviewDescriptor(candidate)
      : null;
    if (hoverPreviewDescriptor) {
      row.addEventListener("mouseenter", () => {
        startHomeArtworkHoverPreview(row, hoverPreviewDescriptor);
      });
      row.addEventListener("mouseleave", () => {
        stopHomeArtworkHoverPreview(row);
      });
      row.addEventListener("focusin", () => {
        startHomeArtworkHoverPreview(row, hoverPreviewDescriptor);
      });
      row.addEventListener("focusout", () => {
        stopHomeArtworkHoverPreview(row);
      });
    }

    const openLink = document.createElement("a");
    openLink.className = "button ghost small home-candidate-open";
    openLink.textContent = formatOpenSourceLabel(sourceKey);
    openLink.href = candidate.url;
    openLink.target = "_blank";
    openLink.rel = "noopener noreferrer";
    action.appendChild(openLink);
  }
  row.appendChild(action);

  const footerMeta = [
    postedDate ? `Posted: ${postedDate}` : "",
    durationText ? `Duration: ${durationText}` : "",
  ].filter(Boolean).join(" • ");
  if (footerMeta) {
    const postedEl = document.createElement("div");
    postedEl.className = "home-candidate-posted";
    postedEl.textContent = footerMeta;
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
    "Search results appear here. Choose a result when you are ready to download.",
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
  state.homeCandidatesByItem = {};
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
  state.homeCandidatesByItem = {};
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
    state.homeResultItems = Array.isArray(items) ? [...items] : [];
    const currentIds = new Set();
    const sortedItems = getVisibleHomeResultItems(items);
    sortedItems.forEach((item) => {
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
  persistRecentVideoSearch(inputValue);
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
  state.homeCandidatesByItem = {};
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
    setNotice(messageEl, "Client delivery does not save to a server destination. Use Search and choose a result to continue.", true);
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
  state.homeCandidatesByItem = {};
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
  focusHomeResults();
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
    focusHomeResults();
  } catch (_err) {
    // Best-effort only.
  }
}

async function importHomePlaylistFile() {
  const inputEl = $("#home-import-file");
  const summaryEl = $("#home-import-summary");
  const messageEl = (state.currentPage === "music" || state.homeMusicMode)
    ? getMusicPageMessageEl()
    : $("#home-search-message");
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
  focusHomeResults();
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
  syncMusicPreferencesFromConfig(cfg);
  const uiPrefs = (cfg && typeof cfg.ui_preferences === "object") ? cfg.ui_preferences : {};
  state.homeVideoCardSize = Number.isFinite(Number(uiPrefs.home_video_card_size))
    ? Math.max(UI_HOME_VIDEO_CARD_SIZE_MIN, Math.min(UI_HOME_VIDEO_CARD_SIZE_MAX, Number(uiPrefs.home_video_card_size)))
    : UI_DEFAULTS.home_video_card_size;
  state.homeVideoSort = String(uiPrefs.home_video_sort || UI_DEFAULTS.home_video_sort);
  state.homeVideoResultsLimit = Number.isFinite(Number(uiPrefs.home_video_results_limit))
    ? Math.max(0, Number.parseInt(uiPrefs.home_video_results_limit, 10))
    : UI_DEFAULTS.home_video_results_limit;
  state.musicCardSize = Number.isFinite(Number(uiPrefs.music_card_size))
    ? Math.max(UI_CARD_SIZE_MIN, Math.min(UI_CARD_SIZE_MAX, Number(uiPrefs.music_card_size)))
    : UI_DEFAULTS.music_card_size;
  state.musicResultsSort = String(uiPrefs.music_sort || UI_DEFAULTS.music_sort);
  state.musicHideSuggestedGenres = !!uiPrefs.music_hide_suggested_genres;
  state.musicHiddenGenres = normalizeHiddenMusicGenres(uiPrefs.music_hidden_genres || UI_DEFAULTS.music_hidden_genres);
  state.musicHiddenArtists = normalizeHiddenMusicArtists(uiPrefs.music_hidden_artists || UI_DEFAULTS.music_hidden_artists);
  state.arrCardSize = Number.isFinite(Number(uiPrefs.movies_tv_card_size))
    ? Math.max(UI_CARD_SIZE_MIN, Math.min(UI_CARD_SIZE_MAX, Number(uiPrefs.movies_tv_card_size)))
    : UI_DEFAULTS.movies_tv_card_size;
  state.arrSort = String(uiPrefs.movies_tv_sort || UI_DEFAULTS.movies_tv_sort);
  applyHomeVideoCardSize(state.homeVideoCardSize);
  applyArrCardSize(state.arrCardSize);
  const homeVideoSort = $("#home-video-sort");
  if (homeVideoSort) {
    homeVideoSort.value = state.homeVideoSort;
  }
  const homeVideoResultsLimit = $("#home-video-results-limit");
  if (homeVideoResultsLimit) {
    homeVideoResultsLimit.value = String(state.homeVideoResultsLimit || 0);
  }
  rerenderAllHomeCandidateLists();
  applySettingsAdvancedMode(!!cfg.settings_advanced_mode, { persist: false });
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
  $("#cfg-long-term-retry-enabled").checked = cfg.long_term_retry_enabled !== false;
  $("#cfg-long-term-retry-interval-hours").value = Number.isFinite(cfg.long_term_retry_interval_hours)
    ? Number(cfg.long_term_retry_interval_hours)
    : 24;
  $("#cfg-long-term-retry-max-attempts").value = Number.isFinite(cfg.long_term_retry_max_attempts)
    ? Number(cfg.long_term_retry_max_attempts)
    : 0;
  const arrCfg = (cfg && typeof cfg.arr === "object") ? cfg.arr : {};
  const arrRadarr = (arrCfg && typeof arrCfg.radarr === "object") ? arrCfg.radarr : {};
  const arrSonarr = (arrCfg && typeof arrCfg.sonarr === "object") ? arrCfg.sonarr : {};
  const arrReadarr = (arrCfg && typeof arrCfg.readarr === "object") ? arrCfg.readarr : {};
  const arrProwlarr = (arrCfg && typeof arrCfg.prowlarr === "object") ? arrCfg.prowlarr : {};
  const arrBazarr = (arrCfg && typeof arrCfg.bazarr === "object") ? arrCfg.bazarr : {};
  const arrQbit = (arrCfg && typeof arrCfg.qbittorrent === "object") ? arrCfg.qbittorrent : {};
  const arrJellyfin = (arrCfg && typeof arrCfg.jellyfin === "object") ? arrCfg.jellyfin : {};
  const arrVpn = (arrCfg && typeof arrCfg.vpn === "object") ? arrCfg.vpn : {};
  const playbackCfg = (cfg && typeof cfg.playback === "object") ? cfg.playback : {};
  const setupCfg = (cfg && typeof cfg.setup === "object") ? cfg.setup : {};
  const setupStack = (setupCfg && typeof setupCfg.stack === "object") ? setupCfg.stack : {};
  const securityCfg = (cfg && typeof cfg.security === "object") ? cfg.security : {};
  $("#cfg-arr-tmdb-api-key").value = arrCfg.tmdb_api_key ?? "";
  $("#cfg-arr-movies-root").value = setupStack.movies_root ?? "./media/movies";
  $("#cfg-arr-tv-root").value = setupStack.tv_root ?? "./media/tv";
  $("#cfg-arr-books-root").value = setupStack.books_root ?? "./media/books";
  $("#cfg-arr-radarr-base-url").value = arrRadarr.base_url ?? "";
  $("#cfg-arr-radarr-api-key").value = arrRadarr.api_key ?? "";
  $("#cfg-arr-sonarr-base-url").value = arrSonarr.base_url ?? "";
  $("#cfg-arr-sonarr-api-key").value = arrSonarr.api_key ?? "";
  $("#cfg-arr-readarr-base-url").value = arrReadarr.base_url ?? "";
  $("#cfg-arr-readarr-api-key").value = arrReadarr.api_key ?? "";
  $("#cfg-arr-prowlarr-base-url").value = arrProwlarr.base_url ?? "";
  $("#cfg-arr-prowlarr-api-key").value = arrProwlarr.api_key ?? "";
  $("#cfg-arr-bazarr-base-url").value = arrBazarr.base_url ?? "";
  $("#cfg-arr-bazarr-api-key").value = arrBazarr.api_key ?? "";
  $("#cfg-arr-qbittorrent-base-url").value = arrQbit.base_url ?? "";
  $("#cfg-arr-qbittorrent-username").value = arrQbit.username ?? "";
  $("#cfg-arr-qbittorrent-password").value = arrQbit.password ?? "";
  $("#cfg-arr-qbittorrent-download-dir").value = arrQbit.download_dir ?? "";
  $("#cfg-arr-jellyfin-base-url").value = arrJellyfin.base_url ?? "";
  $("#cfg-arr-jellyfin-api-key").value = arrJellyfin.api_key ?? "";
  $("#cfg-arr-vpn-enabled").checked = !!arrVpn.enabled;
  $("#cfg-arr-vpn-provider").value = arrVpn.provider ?? "gluetun";
  $("#cfg-arr-vpn-control-url").value = arrVpn.control_url ?? "";
  $("#cfg-arr-vpn-route-qbittorrent").checked = arrVpn.route_qbittorrent !== false;
  $("#cfg-arr-vpn-route-prowlarr").checked = !!arrVpn.route_prowlarr;
  $("#cfg-arr-vpn-route-retreivr").checked = !!arrVpn.route_retreivr;
  $("#cfg-playback-external-mode").value = playbackCfg.external_player_mode ?? "none";
  $("#cfg-playback-external-label").value = playbackCfg.external_player_label ?? "";
  $("#cfg-playback-external-template").value = playbackCfg.external_player_url_template ?? "";
  $("#cfg-playback-plex-base-url").value = playbackCfg.plex_base_url ?? "";
  updatePlaybackIntegrationPreview();
  $("#cfg-security-admin-pin-enabled").checked = !!securityCfg.admin_pin_enabled;
  $("#cfg-security-admin-pin-session-minutes").value = Number.isFinite(securityCfg.admin_pin_session_minutes)
    ? Number(securityCfg.admin_pin_session_minutes)
    : 30;
  if ($("#cfg-security-admin-pin-current")) $("#cfg-security-admin-pin-current").value = "";
  if ($("#cfg-security-admin-pin-new")) $("#cfg-security-admin-pin-new").value = "";
  if ($("#cfg-security-admin-pin-confirm")) $("#cfg-security-admin-pin-confirm").value = "";
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
    syncMusicPreferencesFromConfig(cfg);
    renderConfig(cfg);
    refreshCommunityPublishStatus().catch(() => {});
    updateSearchDestinationDisplay();
    applyHomeDefaultDestination({ force: false });
    applyHomeDefaultActiveFormat({ force: true });
    state.configDirty = false;
    updatePollingState();
    await refreshAdminSecurityStatus();
    refreshArrConnectionStatus({ quiet: true }).catch(() => {});
    setConfigNotice("Config loaded", false);
  } catch (err) {
    setConfigNotice(`Config error: ${err.message}`, true);
  }
}

async function persistUiPreferences(partial = {}) {
  const payload = {};
  if ("home_video_card_size" in partial) {
    payload.home_video_card_size = Math.max(
      UI_HOME_VIDEO_CARD_SIZE_MIN,
      Math.min(UI_HOME_VIDEO_CARD_SIZE_MAX, Number.parseInt(partial.home_video_card_size, 10) || UI_DEFAULTS.home_video_card_size)
    );
  }
  if ("home_video_sort" in partial) {
    payload.home_video_sort = String(partial.home_video_sort || UI_DEFAULTS.home_video_sort);
  }
  if ("home_video_results_limit" in partial) {
    payload.home_video_results_limit = Math.max(
      0,
      Number.parseInt(partial.home_video_results_limit, 10) || UI_DEFAULTS.home_video_results_limit
    );
  }
  if ("music_card_size" in partial) {
    payload.music_card_size = Math.max(
      UI_CARD_SIZE_MIN,
      Math.min(UI_CARD_SIZE_MAX, Number.parseInt(partial.music_card_size, 10) || UI_DEFAULTS.music_card_size)
    );
  }
  if ("music_sort" in partial) {
    payload.music_sort = String(partial.music_sort || UI_DEFAULTS.music_sort);
  }
  if ("music_hide_suggested_genres" in partial) {
    payload.music_hide_suggested_genres = !!partial.music_hide_suggested_genres;
  }
  if ("music_hidden_genres" in partial) {
    payload.music_hidden_genres = normalizeHiddenMusicGenres(partial.music_hidden_genres);
  }
  if ("music_hidden_artists" in partial) {
    payload.music_hidden_artists = normalizeHiddenMusicArtists(partial.music_hidden_artists);
  }
  if ("movies_tv_card_size" in partial) {
    payload.movies_tv_card_size = Math.max(
      UI_CARD_SIZE_MIN,
      Math.min(UI_CARD_SIZE_MAX, Number.parseInt(partial.movies_tv_card_size, 10) || UI_DEFAULTS.movies_tv_card_size)
    );
  }
  if ("movies_tv_sort" in partial) {
    payload.movies_tv_sort = String(partial.movies_tv_sort || UI_DEFAULTS.movies_tv_sort);
  }
  if (!Object.keys(payload).length) {
    return;
  }
  try {
    const prefs = await fetchJson("/api/config/ui-preferences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.config = state.config && typeof state.config === "object" ? state.config : {};
    state.config.ui_preferences = prefs;
  } catch (_err) {
    // Keep UI responsive even if preference persistence fails.
  }
}

function buildConfigFromForm() {
  const base = state.config ? JSON.parse(JSON.stringify(state.config)) : {};
  const errors = [];

  base.settings_advanced_mode = !!$("#settings-advanced-mode")?.checked;

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

  base.long_term_retry_enabled = !!$("#cfg-long-term-retry-enabled").checked;
  const longTermRetryIntervalRaw = $("#cfg-long-term-retry-interval-hours").value.trim();
  if (longTermRetryIntervalRaw) {
    const parsed = Number.parseInt(longTermRetryIntervalRaw, 10);
    if (!Number.isInteger(parsed) || parsed < 1) {
      errors.push("Long-term retry interval must be an integer >= 1");
    } else {
      base.long_term_retry_interval_hours = parsed;
    }
  } else {
    base.long_term_retry_interval_hours = 24;
  }
  const longTermRetryMaxAttemptsRaw = $("#cfg-long-term-retry-max-attempts").value.trim();
  if (longTermRetryMaxAttemptsRaw) {
    const parsed = Number.parseInt(longTermRetryMaxAttemptsRaw, 10);
    if (!Number.isInteger(parsed) || parsed < 0) {
      errors.push("Long-term retry max attempts must be an integer >= 0");
    } else {
      base.long_term_retry_max_attempts = parsed;
    }
  } else {
    base.long_term_retry_max_attempts = 0;
  }

  const arr = (base.arr && typeof base.arr === "object") ? { ...base.arr } : {};
  arr.tmdb_api_key = $("#cfg-arr-tmdb-api-key").value.trim();
  arr.radarr = (arr.radarr && typeof arr.radarr === "object") ? { ...arr.radarr } : {};
  arr.sonarr = (arr.sonarr && typeof arr.sonarr === "object") ? { ...arr.sonarr } : {};
  arr.readarr = (arr.readarr && typeof arr.readarr === "object") ? { ...arr.readarr } : {};
  arr.prowlarr = (arr.prowlarr && typeof arr.prowlarr === "object") ? { ...arr.prowlarr } : {};
  arr.bazarr = (arr.bazarr && typeof arr.bazarr === "object") ? { ...arr.bazarr } : {};
  arr.qbittorrent = (arr.qbittorrent && typeof arr.qbittorrent === "object") ? { ...arr.qbittorrent } : {};
  arr.jellyfin = (arr.jellyfin && typeof arr.jellyfin === "object") ? { ...arr.jellyfin } : {};
  arr.vpn = (arr.vpn && typeof arr.vpn === "object") ? { ...arr.vpn } : {};
  arr.radarr.base_url = $("#cfg-arr-radarr-base-url").value.trim();
  arr.radarr.api_key = $("#cfg-arr-radarr-api-key").value.trim();
  arr.sonarr.base_url = $("#cfg-arr-sonarr-base-url").value.trim();
  arr.sonarr.api_key = $("#cfg-arr-sonarr-api-key").value.trim();
  arr.readarr.base_url = $("#cfg-arr-readarr-base-url").value.trim();
  arr.readarr.api_key = $("#cfg-arr-readarr-api-key").value.trim();
  arr.prowlarr.base_url = $("#cfg-arr-prowlarr-base-url").value.trim();
  arr.prowlarr.api_key = $("#cfg-arr-prowlarr-api-key").value.trim();
  arr.bazarr.base_url = $("#cfg-arr-bazarr-base-url").value.trim();
  arr.bazarr.api_key = $("#cfg-arr-bazarr-api-key").value.trim();
  arr.qbittorrent.base_url = $("#cfg-arr-qbittorrent-base-url").value.trim();
  arr.qbittorrent.username = $("#cfg-arr-qbittorrent-username").value.trim();
  arr.qbittorrent.password = $("#cfg-arr-qbittorrent-password").value.trim();
  arr.qbittorrent.download_dir = $("#cfg-arr-qbittorrent-download-dir").value.trim();
  arr.jellyfin.base_url = $("#cfg-arr-jellyfin-base-url").value.trim();
  arr.jellyfin.api_key = $("#cfg-arr-jellyfin-api-key").value.trim();
  arr.vpn.enabled = !!$("#cfg-arr-vpn-enabled").checked;
  arr.vpn.provider = $("#cfg-arr-vpn-provider").value.trim() || "gluetun";
  arr.vpn.control_url = $("#cfg-arr-vpn-control-url").value.trim();
  arr.vpn.route_qbittorrent = !!$("#cfg-arr-vpn-route-qbittorrent").checked;
  arr.vpn.route_prowlarr = !!$("#cfg-arr-vpn-route-prowlarr").checked;
  arr.vpn.route_retreivr = !!$("#cfg-arr-vpn-route-retreivr").checked;
  base.arr = arr;

  const playback = (base.playback && typeof base.playback === "object") ? { ...base.playback } : {};
  playback.external_player_mode = $("#cfg-playback-external-mode").value.trim() || "none";
  playback.external_player_label = $("#cfg-playback-external-label").value.trim();
  playback.external_player_url_template = $("#cfg-playback-external-template").value.trim();
  playback.plex_base_url = $("#cfg-playback-plex-base-url").value.trim();
  base.playback = playback;

  const setup = (base.setup && typeof base.setup === "object") ? { ...base.setup } : {};
  const setupStack = (setup.stack && typeof setup.stack === "object") ? { ...setup.stack } : {};
  setupStack.movies_root = $("#cfg-arr-movies-root").value.trim() || "./media/movies";
  setupStack.tv_root = $("#cfg-arr-tv-root").value.trim() || "./media/tv";
  setupStack.books_root = $("#cfg-arr-books-root").value.trim() || "./media/books";
  setup.stack = setupStack;
  base.setup = setup;

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
  if (!(await ensureAdminPinSession())) {
    setConfigNotice("Admin PIN unlock is required before saving settings.", true);
    return;
  }

  try {
    await fetchJson("/api/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.config),
    });
    setConfigNotice("Settings saved", false, true);
    state.config = result.config;
    renderConfig(state.config);
    applyHomeDefaultDestination({ force: false });
    applyHomeDefaultActiveFormat({ force: true });
    updateSearchDestinationDisplay();
    renderMoviesTvSetupGate();
    await refreshSchedule();
    state.configDirty = false;
    updatePollingState();
    refreshArrConnectionStatus({ quiet: true }).catch(() => {});
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
  const reviewNavButton = document.querySelector('.nav-button[data-page="review"]');
  if (reviewNavButton) {
    reviewNavButton.classList.toggle("hidden", pendingCount <= 0);
  }
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
  const musicAlert = $("#music-page-review-alert");
  const musicAlertText = $("#music-page-review-alert-text");
  if (musicAlert && musicAlertText) {
    if (pendingCount > 0) {
      musicAlertText.textContent = `${pendingCount} item${pendingCount === 1 ? "" : "s"} waiting for review.`;
      musicAlert.classList.remove("hidden");
    } else {
      musicAlert.classList.add("hidden");
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
    } else if (state.currentPage === "video") {
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
  const brandHome = $("#brand-home");
  if (brandHome) {
    brandHome.addEventListener("click", () => {
      setPage("home");
      window.location.hash = "home";
    });
  }
  const appSidebarToggle = $("#app-sidebar-toggle");
  if (appSidebarToggle) {
    appSidebarToggle.addEventListener("click", () => {
      applyAppSidebarCollapsed(!state.appSidebarCollapsed, { persist: true });
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
  const homeVideoLibraryRefresh = $("#home-video-library-refresh");
  if (homeVideoLibraryRefresh) {
    homeVideoLibraryRefresh.addEventListener("click", () => {
      loadVideoLibrarySection({ force: true }).catch(() => {});
    });
  }
  const musicLibraryRefresh = $("#music-library-refresh");
  if (musicLibraryRefresh) {
    musicLibraryRefresh.addEventListener("click", () => {
      loadMusicLibrarySection({ force: true }).catch(() => {});
    });
  }
  $$("[data-music-library-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      state.musicLibraryMode = button.dataset.musicLibraryMode || "artists";
      if (state.musicLibraryMode === "artists") {
        state.playerSelectedArtistKey = "";
        state.playerSelectedAlbumKey = "";
      } else if (state.musicLibraryMode === "albums") {
        state.playerSelectedAlbumKey = "";
      }
      renderMusicLibrarySection();
    });
  });
  const musicLibraryGrid = $("#music-library-grid");
  if (musicLibraryGrid) {
    musicLibraryGrid.addEventListener("click", async (event) => {
      const action = event.target.closest("[data-action]");
      if (!action) {
        const card = event.target.closest(".media-library-card");
        if (!card) return;
        const type = String(card.dataset.libraryType || "");
        if (type === "artist") {
          const artist = (state.playerLibrarySummary?.artists || []).find((entry) => String(entry.artist_key || "") === String(card.dataset.artistKey || ""));
          if (artist) {
            openLibraryDetailsModal(buildMusicLibraryArtistModalPayload(artist));
          }
        } else if (type === "album") {
          const album = (state.playerLibrarySummary?.albums || []).find((entry) =>
            String(entry.artist_key || "") === String(card.dataset.artistKey || "") &&
            String(entry.album_key || "") === String(card.dataset.albumKey || "")
          );
          if (album) {
            openLibraryDetailsModal(buildMusicLibraryAlbumModalPayload(album));
          }
        } else if (type === "track") {
          const track = getMusicLibraryFilteredTracks().find((entry) => String(entry.id || "") === String(card.dataset.trackId || ""));
          if (track) {
            openLibraryDetailsModal(buildMusicLibraryTrackModalPayload(track));
          }
        }
        return;
      }
      const actionName = String(action.dataset.action || "");
      if (actionName === "music-library-reset") {
        state.musicLibraryMode = "albums";
        state.playerSelectedArtistKey = "";
        state.playerSelectedAlbumKey = "";
        renderMusicLibrarySection();
        return;
      }
      if (actionName === "music-library-open-artist") {
        state.playerSelectedArtistKey = String(action.dataset.artistKey || "").trim();
        state.playerSelectedAlbumKey = "";
        state.musicLibraryMode = "albums";
        renderMusicLibrarySection();
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "music-library-open-album") {
        state.playerSelectedArtistKey = String(action.dataset.artistKey || "").trim();
        state.playerSelectedAlbumKey = String(action.dataset.albumKey || "").trim();
        state.musicLibraryMode = "tracks";
        renderMusicLibrarySection();
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "music-library-play-track") {
        const trackId = String(action.dataset.trackId || "").trim();
        const track = getMusicLibraryFilteredTracks().find((entry) => String(entry.id || "") === trackId);
        if (track) {
          await playMusicPlayerItem(track);
        }
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "music-library-info") {
        const type = String(action.dataset.libraryType || "");
        if (type === "artist") {
          const artist = (state.playerLibrarySummary?.artists || []).find((entry) => String(entry.artist_key || "") === String(action.dataset.artistKey || ""));
          if (artist) {
            openLibraryDetailsModal(buildMusicLibraryArtistModalPayload(artist));
          }
        } else if (type === "album") {
          const album = (state.playerLibrarySummary?.albums || []).find((entry) =>
            String(entry.artist_key || "") === String(action.dataset.artistKey || "") &&
            String(entry.album_key || "") === String(action.dataset.albumKey || "")
          );
          if (album) {
            openLibraryDetailsModal(buildMusicLibraryAlbumModalPayload(album));
          }
        } else if (type === "track") {
          const track = getMusicLibraryFilteredTracks().find((entry) => String(entry.id || "") === String(action.dataset.trackId || ""));
          if (track) {
            openLibraryDetailsModal(buildMusicLibraryTrackModalPayload(track));
          }
        }
      }
    });
  }
  const musicBreadcrumbs = $("#music-library-breadcrumbs");
  if (musicBreadcrumbs) {
    musicBreadcrumbs.addEventListener("click", (event) => {
      const action = event.target.closest("[data-action]");
      if (!action) return;
      const actionName = String(action.dataset.action || "");
      if (actionName === "music-library-reset") {
        state.musicLibraryMode = "albums";
        state.playerSelectedArtistKey = "";
        state.playerSelectedAlbumKey = "";
      } else if (actionName === "music-library-open-artist") {
        state.playerSelectedArtistKey = String(action.dataset.artistKey || "").trim();
        state.playerSelectedAlbumKey = "";
        state.musicLibraryMode = "albums";
      } else if (actionName === "music-library-open-album") {
        state.playerSelectedArtistKey = String(action.dataset.artistKey || "").trim();
        state.playerSelectedAlbumKey = String(action.dataset.albumKey || "").trim();
        state.musicLibraryMode = "tracks";
      }
      renderMusicLibrarySection();
    });
  }
  const musicMissingGrid = $("#music-library-missing-grid");
  if (musicMissingGrid) {
    musicMissingGrid.addEventListener("click", async (event) => {
      const action = event.target.closest("[data-action]");
      if (!action) return;
      const historyId = Number(action.dataset.historyId || 0) || null;
      if (!historyId) return;
      const actionName = String(action.dataset.action || "");
      const messageEl = $("#music-library-message");
      try {
        if (actionName === "music-history-redownload") {
          setMediaLibraryNotice(messageEl, "Queueing re-download…", false);
          await fetchJson(`/api/player/history/${encodeURIComponent(historyId)}/redownload`, { method: "POST" });
          setMediaLibraryNotice(messageEl, "Re-download queued. Review Queue and Search will show progress as it resolves.", false);
          return;
        }
        if (actionName === "music-history-remove") {
          await fetchJson(`/api/player/history/${encodeURIComponent(historyId)}`, { method: "DELETE" });
          state.playerMissingHistory = (state.playerMissingHistory || []).filter((item) => Number(item?.id || 0) !== historyId);
          renderMusicLibrarySection();
          setMediaLibraryNotice(messageEl, "Removed from history.", false);
        }
      } catch (err) {
        setMediaLibraryNotice(messageEl, toUserErrorMessage(err), true);
      }
    });
  }
  const videoLibraryGrid = $("#home-video-library-grid");
  if (videoLibraryGrid) {
    videoLibraryGrid.addEventListener("click", (event) => {
      const action = event.target.closest("[data-action]");
      if (!action) {
        const card = event.target.closest(".media-library-video-card");
        if (!card) return;
        const fileId = String(card.dataset.fileId || "").trim();
        const item = state.videoLibraryItems.find((entry) => String(entry.file_id || "") === fileId);
        if (item) {
          openLibraryDetailsModal(buildVideoLibraryModalPayload(item));
        }
        return;
      }
      const actionName = String(action.dataset.action || "");
      if (actionName === "video-library-info") {
        const fileId = String(action.dataset.fileId || "").trim();
        const item = state.videoLibraryItems.find((entry) => String(entry.file_id || "") === fileId);
        if (item) {
          openLibraryDetailsModal(buildVideoLibraryModalPayload(item));
        }
      }
    });
  }
  const libraryModal = $("#library-details-modal");
  if (libraryModal) {
    $("#library-details-close")?.addEventListener("click", closeLibraryDetailsModal);
    libraryModal.addEventListener("click", async (event) => {
      if (event.target === libraryModal) {
        closeLibraryDetailsModal();
        return;
      }
      const button = event.target.closest("#library-details-primary, #library-details-secondary");
      const tertiaryButton = event.target.closest("#library-details-tertiary");
      const targetButton = button || tertiaryButton;
      if (!targetButton) return;
      const actionName = String(targetButton.dataset.action || "");
      const payload = parseLibraryActionPayload(targetButton.dataset.payload || "");
      if (!actionName) return;
      if (actionName === "music-library-open-player") {
        setPage("music");
        setMusicSection("library");
        window.location.hash = "music";
        if (payload.artist_key) {
          state.playerSelectedArtistKey = String(payload.artist_key || "");
        }
        if (payload.album_key) {
          state.playerSelectedAlbumKey = String(payload.album_key || "");
        }
        closeLibraryDetailsModal();
        loadMusicPlayerView().catch(() => {});
        return;
      }
      if (actionName === "music-library-open-artist") {
        state.playerSelectedArtistKey = String(payload.artist_key || "");
        state.playerSelectedAlbumKey = "";
        state.musicLibraryMode = "albums";
        renderMusicLibrarySection();
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "music-library-open-album") {
        state.playerSelectedArtistKey = String(payload.artist_key || "");
        state.playerSelectedAlbumKey = String(payload.album_key || "");
        state.musicLibraryMode = "tracks";
        renderMusicLibrarySection();
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "music-library-play-track") {
        await playMusicPlayerItem(payload);
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "video-library-download") {
        const fileId = String(payload.file_id || "").trim();
        if (fileId) {
          window.location.href = downloadUrl(fileId);
        }
        closeLibraryDetailsModal();
        return;
      }
      if (actionName === "video-library-watch-here") {
        closeLibraryDetailsModal();
        openLibraryVideoModal(payload);
        return;
      }
      if (actionName === "video-library-open-external") {
        const targetUrl = String(payload.external_url || "").trim();
        if (targetUrl) {
          window.open(targetUrl, "_blank", "noopener");
        }
        return;
      }
      if (actionName === "video-library-open-source") {
        const sourceUrl = String(payload.source_url || payload.canonical_url || payload.input_url || "").trim();
        if (sourceUrl) {
          window.open(sourceUrl, "_blank", "noopener");
        }
      }
    });
  }
  const libraryVideoModal = $("#library-video-modal");
  if (libraryVideoModal) {
    $("#library-video-close")?.addEventListener("click", closeLibraryVideoModal);
    libraryVideoModal.addEventListener("click", (event) => {
      if (event.target === libraryVideoModal) {
        closeLibraryVideoModal();
      }
    });
  }
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
        clearMusicResultsHistory();
        renderMusicLanding();
        return;
      }
      try {
        setMusicPageNotice("Music Mode: searching metadata...", false);
        await performMusicModeSearch();
      } catch (err) {
        setMusicPageNotice(`Music search failed: ${err.message}`, true);
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
  const homeSearchOnly = $("#home-search-only");
  if (homeSearchOnly) {
    homeSearchOnly.addEventListener("click", () => submitHomeSearch(false));
  }
  const homeSearchClear = $("#home-search-clear");
  if (homeSearchClear) {
    homeSearchClear.addEventListener("click", clearVideoHeaderSearch);
  }
  const homeImportButton = $("#home-import-button");
  if (homeImportButton) {
    homeImportButton.addEventListener("click", importHomePlaylistFile);
  }
  const moviesTvSearchButton = $("#movies-tv-search-button");
  if (moviesTvSearchButton) {
    moviesTvSearchButton.addEventListener("click", performArrSearch);
  }
  const moviesTvSearchInput = $("#movies-tv-search-input");
  if (moviesTvSearchInput) {
    moviesTvSearchInput.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        event.preventDefault();
        performArrSearch();
      }
    });
  }
  const moviesTvSearchClear = $("#movies-tv-search-clear");
  if (moviesTvSearchClear) {
    moviesTvSearchClear.addEventListener("click", clearMoviesTvHeaderSearch);
  }
  const moviesTvFiltersToggle = $("#movies-tv-filters-toggle");
  if (moviesTvFiltersToggle) {
    moviesTvFiltersToggle.addEventListener("click", () => {
      setMoviesTvFiltersOpen(!state.arrFiltersOpen);
    });
  }
  const moviesTvYearChips = $("#movies-tv-year-chips");
  if (moviesTvYearChips) {
    moviesTvYearChips.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-arr-year]");
      if (!button) return;
      const nextYear = String(button.dataset.arrYear || "").trim();
      const previousYear = String(state.arrSearchYear || "").trim();
      setMoviesTvSearchYear(nextYear);
      if (!state.arrResults.length) {
        return;
      }
      const activeQuery = String(state.arrSearchQuery || "").trim();
      const context = String(state.arrSearchContext || "search");
      if (context === "genre" && state.arrActiveGenre?.id) {
        await browseArrGenre(state.arrActiveGenre, { year: nextYear });
        return;
      }
      if (context === "search" && activeQuery && nextYear !== previousYear) {
        await performArrSearch();
      }
    });
  }
  const arrModeToggle = $("#arr-mode-toggle");
  if (arrModeToggle) {
    arrModeToggle.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-mode]");
      if (!button) return;
      setArrMode(button.dataset.mode || "movies");
    });
  }
  const moviesTvRefreshConnections = $("#movies-tv-refresh-connections");
  if (moviesTvRefreshConnections) {
    moviesTvRefreshConnections.addEventListener("click", async () => {
      await refreshArrConnectionStatus();
      await refreshArrStatuses();
    });
  }
  const moviesTvCardSize = $("#movies-tv-card-size");
  if (moviesTvCardSize) {
    moviesTvCardSize.addEventListener("input", () => {
      applyArrCardSize(moviesTvCardSize.value);
    });
    moviesTvCardSize.addEventListener("change", () => {
      persistUiPreferences({ movies_tv_card_size: moviesTvCardSize.value });
    });
  }
  const moviesTvSort = $("#movies-tv-sort");
  if (moviesTvSort) {
    moviesTvSort.addEventListener("change", () => {
      state.arrSort = String(moviesTvSort.value || "best_match");
      persistUiPreferences({ movies_tv_sort: state.arrSort });
      renderArrResults();
    });
  }
  const homeVideoCardSize = $("#home-video-card-size");
  if (homeVideoCardSize) {
    homeVideoCardSize.addEventListener("input", () => {
      applyHomeVideoCardSize(homeVideoCardSize.value);
    });
    homeVideoCardSize.addEventListener("change", () => {
      persistUiPreferences({ home_video_card_size: homeVideoCardSize.value });
    });
  }
  const homeVideoSort = $("#home-video-sort");
  if (homeVideoSort) {
    homeVideoSort.addEventListener("change", () => {
      state.homeVideoSort = String(homeVideoSort.value || UI_DEFAULTS.home_video_sort);
      persistUiPreferences({ home_video_sort: state.homeVideoSort });
      rerenderHomeResultCards();
    });
  }
  const homeVideoResultsLimit = $("#home-video-results-limit");
  if (homeVideoResultsLimit) {
    homeVideoResultsLimit.addEventListener("change", () => {
      state.homeVideoResultsLimit = Math.max(0, Number.parseInt(homeVideoResultsLimit.value, 10) || UI_DEFAULTS.home_video_results_limit);
      persistUiPreferences({ home_video_results_limit: state.homeVideoResultsLimit });
      rerenderHomeResultCards();
    });
  }
  const moviesTvResults = $("#movies-tv-results-list");
  if (moviesTvResults) {
    wireArrCardInteractions(moviesTvResults);
  }
  const moviesTvEditorial = $("#movies-tv-editorial-shelves");
  if (moviesTvEditorial) {
    wireArrCardInteractions(moviesTvEditorial);
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
      syncTopbarSubbarVisibility();
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
        persistVideoSourcePreferences(
          Array.from(homeSourcePanel.querySelectorAll('input[type="checkbox"][data-source]:checked')).map((el) => el.dataset.source)
        );
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
      syncTopbarSubbarVisibility();
    });
  }
  const launcherGrid = $("#home-launcher-grid");
  if (launcherGrid) {
    launcherGrid.addEventListener("click", (event) => {
      const tile = event.target.closest("[data-home-launch]");
      if (!tile) return;
      const page = String(tile.dataset.homeLaunch || "home");
      const hash = String(tile.dataset.homeHash || page);
      if (tile.id === "home-launcher-setup") {
        setHomeSetupOverlayOpen(true);
        return;
      }
      setPage(page);
      window.location.hash = hash;
    });
  }
  const homeSetupOverlay = $("#home-setup-overlay");
  if (homeSetupOverlay) {
    homeSetupOverlay.addEventListener("click", (event) => {
      if (event.target === homeSetupOverlay) {
        setHomeSetupOverlayOpen(false);
      }
    });
  }
  const homeSetupOverlayClose = $("#home-setup-overlay-close");
  if (homeSetupOverlayClose) {
    homeSetupOverlayClose.addEventListener("click", () => {
      setHomeSetupOverlayOpen(false);
    });
  }
  const homeSetupOverlayOpenSettings = $("#home-setup-overlay-open-settings");
  if (homeSetupOverlayOpenSettings) {
    homeSetupOverlayOpenSettings.addEventListener("click", () => {
      setHomeSetupOverlayOpen(false);
      setPage("config");
      showSettingsPage("settings-guided-setup", { jump: false, smooth: false });
      window.location.hash = "settings-guided-setup";
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !$("#home-setup-overlay")?.classList.contains("hidden")) {
      setHomeSetupOverlayOpen(false);
    }
  });
  const musicHeaderSubmit = $("#music-header-search-submit");
  if (musicHeaderSubmit) {
    musicHeaderSubmit.addEventListener("click", runMusicHeaderSearch);
  }
  const musicHeaderAdvancedToggle = $("#music-header-advanced-toggle");
  const musicAdvancedPanel = $("#music-mode-console");
  if (musicHeaderAdvancedToggle && musicAdvancedPanel) {
    musicHeaderAdvancedToggle.setAttribute("aria-expanded", state.musicAdvancedOpen ? "true" : "false");
    musicHeaderAdvancedToggle.addEventListener("click", () => {
      state.musicAdvancedOpen = !state.musicAdvancedOpen;
      musicAdvancedPanel.classList.toggle("hidden", !state.musicAdvancedOpen);
      musicHeaderAdvancedToggle.setAttribute("aria-expanded", state.musicAdvancedOpen ? "true" : "false");
      syncTopbarSubbarVisibility();
    });
  }
  const musicHeaderClear = $("#music-header-search-clear");
  if (musicHeaderClear) {
    musicHeaderClear.addEventListener("click", clearMusicHeaderSearch);
  }
  const musicHeaderQuery = $("#music-header-query");
  if (musicHeaderQuery) {
    musicHeaderQuery.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.ctrlKey &&
        !event.altKey &&
        !event.metaKey
      ) {
        event.preventDefault();
        runMusicHeaderSearch();
      }
    });
  }
  const videoDiscoveryDefault = $("#video-discovery-default");
  if (videoDiscoveryDefault) {
    videoDiscoveryDefault.addEventListener("click", (event) => {
      const chip = event.target.closest("[data-video-suggestion]");
      if (!chip) return;
      const input = $("#home-search-input");
      if (!input) return;
      const nextValue = String(chip.dataset.videoSuggestion || chip.textContent || "").trim();
      if (!nextValue) return;
      input.value = nextValue;
      input.focus();
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
        const row = previewButton.closest(".home-candidate-row");
        const descriptor = buildHomePreviewDescriptorFromRow(row);
        if (!descriptor) return;
        openHomePreviewModal(descriptor);
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
    const playBtn = event.target.closest(".music-search-play-btn");
    if (playBtn) {
      if (playBtn.disabled) return;
      const resultKey = String(playBtn.dataset.musicResultKey || "").trim();
      const selectedResult = resultKey ? state.homeMusicResultMap[resultKey] : null;
      if (!selectedResult) {
        setMusicPageNotice("Track metadata is unavailable.", true);
        return;
      }
      const originalText = playBtn.textContent;
      playBtn.disabled = true;
      playBtn.textContent = "Resolving...";
      try {
        await playMusicSearchResult(selectedResult);
      } catch (err) {
        setMusicPageNotice(`Play failed: ${toUserErrorMessage(err)}`, true);
      } finally {
        playBtn.disabled = false;
        playBtn.textContent = originalText;
      }
      return;
    }
  });
  document.addEventListener("click", async (event) => {
    const previewBtn = event.target.closest(".music-preview-btn");
    if (previewBtn) {
      if (previewBtn.disabled) return;
      const resultKey = String(previewBtn.dataset.musicResultKey || "").trim();
      const selectedResult = resultKey ? state.homeMusicResultMap[resultKey] : null;
      if (!selectedResult) {
        setMusicPageNotice("Preview metadata is unavailable for this track.", true);
        return;
      }
      const originalText = previewBtn.textContent;
      previewBtn.disabled = true;
      previewBtn.textContent = "Resolving...";
      try {
        const response = await fetchMusicTrackPreview(selectedResult);
        const previewType = String(response?.preview_type || "").trim().toLowerCase();
        const source = String(response?.source || "").trim();
        const title = String(response?.title || selectedResult.track || "Preview").trim() || "Preview";
        const sourceUrl = String(response?.source_url || "").trim();
        if (previewType === "audio") {
          const streamUrl = String(response?.stream_url || buildPreviewStreamUrl(sourceUrl) || "").trim();
          if (!streamUrl) {
            throw new Error("Audio preview unavailable");
          }
          openHomePreviewModal({
            mediaType: "audio",
            streamUrl,
            source,
            title,
          });
        } else {
          const descriptor = buildHomePreviewDescriptor({
            source,
            url: sourceUrl,
            title,
          });
          if (!descriptor) {
            throw new Error("Video preview unavailable");
          }
          openHomePreviewModal({
            mediaType: "video",
            embedUrl: descriptor.embedUrl,
            source: descriptor.source,
            title: descriptor.title,
          });
        }
      } catch (err) {
        setMusicPageNotice(`Preview failed: ${toUserErrorMessage(err)}`, true);
      } finally {
        previewBtn.disabled = false;
        previewBtn.textContent = originalText;
      }
      return;
    }
  });
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
        setMusicPageNotice("Track queued.", false);
      } else {
        btn.disabled = false;
        btn.textContent = originalText;
        const reason = String(response?.dedupe_reason || "").trim();
        const message = reason
          ? `Track not queued (${reason.replaceAll("_", " ")}).`
          : "Track already queued/downloaded; not queued again.";
        setMusicPageNotice(message, false);
      }
    } catch (err) {
      btn.disabled = false;
      btn.textContent = originalText;
      setMusicPageNotice(`Music enqueue failed: ${err.message}`, true);
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
  ["#cfg-playback-external-mode", "#cfg-playback-external-label", "#cfg-playback-external-template", "#cfg-playback-plex-base-url", "#cfg-arr-jellyfin-base-url"].forEach((selector) => {
    const input = $(selector);
    if (input) {
      input.addEventListener("input", updatePlaybackIntegrationPreview);
      input.addEventListener("change", updatePlaybackIntegrationPreview);
    }
  });
  $$("[data-playback-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      applyPlaybackPreset(button.dataset.playbackPreset || "");
    });
  });
  const securityAdminPinSave = $("#security-admin-pin-save");
  if (securityAdminPinSave) {
    securityAdminPinSave.addEventListener("click", () => {
      saveAdminPinSettings().catch((err) => {
        const statusEl = $("#security-admin-pin-status");
        if (statusEl) statusEl.textContent = toUserErrorMessage(err);
      });
    });
  }
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
  const setupPathBrowsers = [
    ["browse-setup-env-path", "setup-env-path", "config", "file", ""],
    ["browse-setup-media-root", "setup-media-root", "downloads", "dir", ""],
    ["browse-setup-movies-root", "setup-movies-root", "downloads", "dir", ""],
    ["browse-setup-tv-root", "setup-tv-root", "downloads", "dir", ""],
    ["browse-setup-downloads-root", "setup-downloads-root", "downloads", "dir", ""],
    ["browse-setup-books-root", "setup-books-root", "downloads", "dir", ""],
    ["browse-cfg-arr-movies-root", "cfg-arr-movies-root", "downloads", "dir", ""],
    ["browse-cfg-arr-tv-root", "cfg-arr-tv-root", "downloads", "dir", ""],
    ["browse-cfg-arr-books-root", "cfg-arr-books-root", "downloads", "dir", ""],
    ["browse-cfg-arr-qbittorrent-download-dir", "cfg-arr-qbittorrent-download-dir", "downloads", "dir", ""],
  ];
  setupPathBrowsers.forEach(([buttonId, inputId, root, mode, ext]) => {
    const button = $(`#${buttonId}`);
    if (!button) return;
    button.addEventListener("click", () => {
      const input = $(`#${inputId}`);
      if (!input) return;
      openBrowser(input, root, mode, ext, resolveBrowseStart(root, input.value));
    });
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
  const arrTestRadarr = $("#arr-test-radarr");
  if (arrTestRadarr) {
    arrTestRadarr.addEventListener("click", async () => {
      await refreshArrConnectionStatus();
    });
  }
  const arrTestSonarr = $("#arr-test-sonarr");
  if (arrTestSonarr) {
    arrTestSonarr.addEventListener("click", async () => {
      await refreshArrConnectionStatus();
    });
  }
  const setupRefresh = $("#setup-refresh");
  if (setupRefresh) {
    setupRefresh.addEventListener("click", () => {
      refreshSetupStatus().catch(() => {});
    });
  }
  const setupSaveStack = $("#setup-save-stack");
  if (setupSaveStack) {
    setupSaveStack.addEventListener("click", () => {
      saveSetupStack().catch((err) => setNotice($("#setup-command-helper"), toUserErrorMessage(err), true));
    });
  }
  const setupApplyStack = $("#setup-apply-stack");
  if (setupApplyStack) {
    setupApplyStack.addEventListener("click", () => {
      applySetupStack().catch((err) => setNotice($("#setup-command-helper"), toUserErrorMessage(err), true));
    });
  }
  const setupWizard = $("#setup-wizard");
  if (setupWizard) {
    setupWizard.addEventListener("click", async (event) => {
      const navButton = event.target.closest("[data-setup-nav]");
      if (navButton) {
        const messageEl = $("#setup-wizard-message");
        if (navButton.dataset.setupNav === "back") {
          state.setupWizard.feedback = null;
          advanceSetupWizardStep(-1);
        } else {
          const currentStep = getSetupWizardSteps()[Math.max(0, Number(state.setupWizard?.stepIndex || 0))];
          const validationError = validateSetupWizardStep(currentStep);
          if (validationError) {
            state.setupWizard.feedback = {
              stepId: currentStep?.id || "",
              tone: "error",
              text: validationError,
            };
            renderSetupWizard();
            return;
          }
          try {
            setNotice(messageEl, "Saving your progress...", false);
            await saveSetupWizardProgress();
            state.setupWizard.feedback = null;
            advanceSetupWizardStep(1);
            state.setupWizard.feedback = {
              stepId: getSetupWizardSteps()[Math.max(0, Number(state.setupWizard?.stepIndex || 0))]?.id || "",
              tone: "success",
              text: "Saved. Continue when you are ready.",
            };
          } catch (err) {
            setNotice(messageEl, toUserErrorMessage(err), true);
            return;
          }
        }
        renderSetupWizard();
        return;
      }
      const choiceButton = event.target.closest("[data-setup-choice]");
      if (choiceButton) {
        const key = String(choiceButton.dataset.setupChoice || "");
        const rawValue = String(choiceButton.dataset.value || "");
        const nextValue = rawValue === "true" ? true : rawValue === "false" ? false : rawValue;
        const revealOnYes = new Set(["wants_tmdb", "enable_vpn", "wants_youtube", "wants_telegram", "enable_jellyfin", "managed_jellyfin"]);
        updateSetupWizardDraftField(key, nextValue);
        syncSetupWizardToLegacyFields();
        if (revealOnYes.has(key) && nextValue === true) {
          if (key === "enable_jellyfin") {
            state.setupWizard.feedback = {
              stepId: getSetupWizardSteps()[Math.max(0, Number(state.setupWizard?.stepIndex || 0))]?.id || "",
              tone: "success",
              text: "Great. Retreivr is looking for likely Jellyfin addresses now.",
            };
            renderSetupWizard();
            await discoverJellyfinForSetup();
            return;
          }
          state.setupWizard.feedback = {
            stepId: getSetupWizardSteps()[Math.max(0, Number(state.setupWizard?.stepIndex || 0))]?.id || "",
            tone: "success",
            text: "Great. Finish the details below to keep going.",
          };
          renderSetupWizard();
          return;
        }
        if ((key === "arr_setup_mode" && nextValue !== "none") || (key === "direct_manage")) {
          state.setupWizard.feedback = null;
          advanceSetupWizardStep(1);
          renderSetupWizard();
          return;
        }
        state.setupWizard.feedback = null;
        advanceSetupWizardStep(1);
        renderSetupWizard();
        return;
      }
      const toggleChoiceButton = event.target.closest("[data-setup-toggle-choice]");
      if (toggleChoiceButton) {
        const key = String(toggleChoiceButton.dataset.setupToggleChoice || "");
        if (!key) return;
        const current = !!state.setupWizard?.draft?.[key];
        updateSetupWizardDraftField(key, !current);
        syncSetupWizardToLegacyFields();
        renderSetupWizard();
        return;
      }
      const fillButton = event.target.closest("[data-setup-fill]");
      if (fillButton) {
        const key = String(fillButton.dataset.setupFill || "");
        const value = String(fillButton.dataset.value || "");
        if (key) {
          updateSetupWizardDraftField(key, value);
          syncSetupWizardToLegacyFields();
          state.setupWizard.feedback = {
            stepId: getSetupWizardSteps()[Math.max(0, Number(state.setupWizard?.stepIndex || 0))]?.id || "",
            tone: "success",
            text: "Filled in. You can keep going or edit it first.",
          };
          renderSetupWizard();
        }
        return;
      }
      const actionButton = event.target.closest("[data-setup-action]");
      if (actionButton) {
        const messageEl = $("#setup-wizard-message");
        try {
          if (actionButton.dataset.setupAction === "start-over") {
            resetSetupWizardDraft();
            renderSetupWizard();
            setNotice($("#setup-wizard-message"), "Setup guide reset to your saved defaults.", false);
          } else if (actionButton.dataset.setupAction === "discover-jellyfin") {
            await discoverJellyfinForSetup();
          } else if (actionButton.dataset.setupAction === "save-progress") {
            setNotice(messageEl, "Saving your setup choices...", false);
            await saveSetupWizardProgress();
            await refreshSetupStatus();
            setNotice(messageEl, "Your setup choices were saved.", false);
          } else if (actionButton.dataset.setupAction === "apply-env") {
            setNotice(messageEl, "Preparing your setup...", false);
            await applySetupWizardEnv();
            await refreshSetupStatus();
            setNotice(messageEl, "Setup prepared. Follow the restart step shown below.", false);
          }
        } catch (err) {
          setNotice(messageEl, toUserErrorMessage(err), true);
        }
        return;
      }
      const browseButton = event.target.closest("[data-setup-browse]");
      if (browseButton) {
        const key = String(browseButton.dataset.setupBrowse || "");
        if (key === "yt_dlp_cookies") {
          const targetInput = setupWizard.querySelector('[data-setup-input="yt_dlp_cookies"]');
          const start = resolveBrowseStart("tokens", state.setupWizard?.draft?.yt_dlp_cookies || "");
          openBrowser(targetInput, "tokens", "file", ".txt", start);
        }
      }
    });
    setupWizard.addEventListener("change", (event) => {
      const toggle = event.target.closest("[data-setup-toggle]");
      if (toggle) {
        updateSetupWizardDraftField(String(toggle.dataset.setupToggle || ""), !!toggle.checked);
        renderSetupWizard();
        return;
      }
      const input = event.target.closest("[data-setup-input]");
      if (input) {
        updateSetupWizardDraftField(String(input.dataset.setupInput || ""), String(input.value || ""));
      }
    });
    setupWizard.addEventListener("input", (event) => {
      const input = event.target.closest("[data-setup-input]");
      if (input) {
        updateSetupWizardDraftField(String(input.dataset.setupInput || ""), String(input.value || ""));
      }
    });
  }
  const connectionsRefresh = $("#connections-refresh");
  if (connectionsRefresh) {
    connectionsRefresh.addEventListener("click", () => {
      refreshConnectionsStatus().catch(() => {});
    });
  }
  const connectionsAutoconfigure = $("#connections-autoconfigure");
  if (connectionsAutoconfigure) {
    connectionsAutoconfigure.addEventListener("click", () => {
      autoConfigureConnections().catch(() => {});
    });
  }
  const adminPinClose = $("#admin-pin-close");
  if (adminPinClose) {
    adminPinClose.addEventListener("click", () => setAdminPinModalOpen(false));
  }
  const adminPinSubmit = $("#admin-pin-submit");
  if (adminPinSubmit) {
    adminPinSubmit.addEventListener("click", () => {
      submitAdminPinUnlock().catch(() => {});
    });
  }
  const adminPinInput = $("#admin-pin-input");
  if (adminPinInput) {
    adminPinInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        submitAdminPinUnlock().catch(() => {});
      }
    });
  }
  const startupSetupClose = $("#startup-setup-close");
  if (startupSetupClose) {
    startupSetupClose.addEventListener("click", () => {
      state.startupSetupPromptDismissedForSession = true;
      setStartupSetupModalOpen(false);
    });
  }
  const startupSetupRemind = $("#startup-setup-remind");
  if (startupSetupRemind) {
    startupSetupRemind.addEventListener("click", () => {
      state.startupSetupPromptDismissedForSession = true;
      setStartupSetupModalOpen(false);
    });
  }
  const startupSetupRun = $("#startup-setup-run");
  if (startupSetupRun) {
    startupSetupRun.addEventListener("click", () => {
      state.startupSetupPromptPending = false;
      setStartupSetupModalOpen(false);
      setHomeSetupOverlayOpen(true);
    });
  }
  const startupSetupDismiss = $("#startup-setup-dismiss");
  if (startupSetupDismiss) {
    startupSetupDismiss.addEventListener("click", async () => {
      const messageEl = $("#startup-setup-message");
      try {
        if (messageEl) messageEl.textContent = "Saving preference…";
        await persistSetupStartupPreference(false);
        state.startupSetupPromptPending = false;
        state.startupSetupPromptDismissedForSession = true;
        setStartupSetupModalOpen(false);
      } catch (err) {
        if (messageEl) {
          messageEl.textContent = `Could not save preference: ${toUserErrorMessage(err)}`;
        }
      }
    });
  }
  const startupSetupModal = $("#startup-setup-modal");
  if (startupSetupModal) {
    startupSetupModal.addEventListener("click", (event) => {
      if (event.target === startupSetupModal) {
        state.startupSetupPromptDismissedForSession = true;
        setStartupSetupModalOpen(false);
      }
    });
  }
  $$(".music-app-nav").forEach((button) => {
    button.addEventListener("click", () => {
      const section = button.dataset.musicSection || "browse";
      // Clicking Browse always returns to the top-level landing regardless of current state.
      if (section === "browse") {
        clearMusicHeaderSearch();
        return;
      }
      setMusicSection(section);
    });
  });
  $$("[data-home-section]").forEach((button) => {
    button.addEventListener("click", () => {
      setHomeSection(button.dataset.homeSection || "search");
    });
  });
  $$("[data-movies-tv-section]").forEach((button) => {
    button.addEventListener("click", () => {
      setMoviesTvSection(button.dataset.moviesTvSection || "search");
    });
  });
  const musicBottomToggle = $("#music-bottom-player-toggle");
  if (musicBottomToggle) {
    musicBottomToggle.addEventListener("click", () => {
      if (activePlayerIsPaused()) activePlayerPlay(); else activePlayerPause();
      syncBottomPlayerShell();
      updateMusicPlayerTransportUI();
    });
  }
  const musicBottomPrev = $("#music-bottom-player-prev");
  if (musicBottomPrev) {
    musicBottomPrev.addEventListener("click", () => {
      playPreviousPlayerItem().catch(() => {});
    });
  }
  const musicBottomNext = $("#music-bottom-player-next");
  if (musicBottomNext) {
    musicBottomNext.addEventListener("click", () => {
      playNextPlayerItem().catch(() => {});
    });
  }
  const musicBottomExpand = $("#music-bottom-player-expand");
  if (musicBottomExpand) {
    musicBottomExpand.addEventListener("click", () => {
      openMusicPlayerModal();
    });
  }
  const musicBottomQueue = $("#music-bottom-player-queue");
  if (musicBottomQueue) {
    musicBottomQueue.addEventListener("click", () => {
      setMusicSection("queue");
    });
  }
  const musicPlayerModalClose = $("#music-player-modal-close");
  if (musicPlayerModalClose) {
    musicPlayerModalClose.addEventListener("click", () => {
      closeMusicPlayerModal();
    });
  }
  $("#music-player-station-seed-type")?.addEventListener("change", () => {
    syncMusicStationCreateForm();
  });
  syncMusicStationCreateForm();
  const handlePlayerHostClick = async (event) => {
      const createStationButton = event.target.closest("#music-player-create-station");
      if (createStationButton) {
        const name = String($("#music-player-station-name")?.value || "").trim();
        const seedType = String($("#music-player-station-seed-type")?.value || "artist").trim();
        const stationMode = String($("#music-player-station-mode")?.value || "mix").trim();
        const seedValue = String($("#music-player-station-seed-value")?.value || "").trim();
        if (!seedValue && seedType !== "favorites") {
          setNotice($("#music-player-message"), "Seed value is required for this station.", true);
          return;
        }
        const seedIdentity = seedType === "album"
          ? { album_name: seedValue }
          : seedType === "artist"
            ? { artist_name: seedValue }
            : seedType === "genre"
              ? { genre_key: seedValue }
              : {};
        await fetchJson("/api/player/stations", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name, seed_type: seedType, seed_value: seedValue || "favorites", station_mode: stationMode, seed_identity: seedIdentity }),
        });
        await loadMusicPlayerView();
        setMusicPlayerView("radio");
        setMusicSection("radio");
        return;
      }
      const createPlaylistButton = event.target.closest("#music-player-create-playlist");
      if (createPlaylistButton) {
        const name = String($("#music-player-playlist-name")?.value || "").trim();
        if (!name) {
          setNotice($("#music-player-message"), "Playlist name is required.", true);
          return;
        }
        const payload = await fetchJson("/api/player/playlists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name }),
        });
        state.playerSelectedPlaylistId = Number(payload?.playlist?.id || 0) || null;
        await loadMusicPlayerView();
        setMusicPlayerView("library");
        setMusicSection("playlists");
        setNotice($("#music-player-message"), "Playlist created.", false);
        return;
      }
      const loadStationButton = event.target.closest('[data-action="player-load-station"]');
      if (loadStationButton) {
        const stationId = String(loadStationButton.dataset.stationId || "").trim();
        const payload = await fetchJson(`/api/player/stations/${encodeURIComponent(stationId)}/start`, { method: "POST" });
        setPlayerQueue(Array.isArray(payload?.queue) ? payload.queue : []);
        if (!state.playerQueue.length) {
          clearActiveStationPlayback();
          setMusicPlayerView("radio");
          setMusicSection("radio");
          setNotice($("#music-player-message"), "This station does not have any playable items yet. Try a different seed or browse Community Cache below.", true);
          return;
        }
        state.playerActiveStationId = Number(stationId || 0) || null;
        state.playerActiveStationRuntime = payload?.runtime || null;
        state.playerShuffle = false;
        state.playerRepeatMode = "off";
        setMusicPlayerView("queue");
        setMusicSection("player");
        // Prefer the explicit current_item from the server; fall back to queue[0] if absent
        // so playback always starts if any item is available.
        const firstCandidate = normalizePlayableItem(payload?.current_item || null);
        const firstItem = firstCandidate && (firstCandidate.stream_url || firstCandidate.video_id || firstCandidate.recording_mbid || firstCandidate.local_path)
          ? firstCandidate
          : (state.playerQueue[0] || null);
        if (firstItem) {
          await playMusicPlayerItem(firstItem, { preserveStation: true });
          scheduleStationPrime(stationId, 600);
          const readyCount = Number(payload?.runtime?.ready_count || state.playerQueue.length || 0);
          setNotice($("#music-player-message"), `Station ready with ${readyCount} prepared item${readyCount === 1 ? "" : "s"}.`, false);
        }
        return;
      }
      const primeStationButton = event.target.closest('[data-action="player-prime-station"]');
      if (primeStationButton) {
        const stationId = String(primeStationButton.dataset.stationId || "").trim();
        const payload = await fetchJson(`/api/player/stations/${encodeURIComponent(stationId)}/prime`, { method: "POST" });
        const nextStation = payload?.station || null;
        if (nextStation) {
          state.playerStations = (Array.isArray(state.playerStations) ? state.playerStations : []).map((entry) =>
            Number(entry?.id || 0) === Number(stationId || 0) ? nextStation : entry
          );
          renderMusicPlayerStations();
        }
        if (Number(state.playerActiveStationId || 0) === Number(stationId || 0) && Array.isArray(payload?.queue)) {
          setPlayerQueue(payload.queue, { preserveCurrent: true });
          state.playerActiveStationRuntime = payload?.runtime || state.playerActiveStationRuntime;
        }
        setNotice($("#music-player-message"), "Station primed.", false);
        return;
      }
      const deleteStationButton = event.target.closest('[data-action="player-delete-station"]');
      if (deleteStationButton) {
        const stationId = String(deleteStationButton.dataset.stationId || "").trim();
        await fetchJson(`/api/player/stations/${encodeURIComponent(stationId)}`, { method: "DELETE" });
        if (Number(state.playerActiveStationId || 0) === Number(stationId || 0)) {
          clearActiveStationPlayback();
        }
        await loadMusicPlayerView();
        setMusicPlayerView("radio");
        setMusicSection("radio");
        return;
      }
      const libraryModeButton = event.target.closest('[data-action="player-library-mode"]');
      if (libraryModeButton) {
        state.playerLibraryMode = String(libraryModeButton.dataset.libraryMode || "artists");
        renderMusicPlayerLibrary();
        setMusicSection("library");
        return;
      }
      const musicGoLibrary = event.target.closest('[data-action="music-go-library"]');
      if (musicGoLibrary) {
        state.playerLibraryMode = "albums";
        state.playerSelectedArtistKey = "";
        state.playerSelectedAlbumKey = "";
        renderMusicPlayerLibrary();
        setMusicSection("library");
        return;
      }
      const musicGoSearch = event.target.closest('[data-action="music-go-search"]');
      if (musicGoSearch) {
        setMusicSection("browse");
        return;
      }
      const musicGoFavorites = event.target.closest('[data-action="music-go-favorites"]');
      if (musicGoFavorites) {
        setMusicSection("favorites");
        return;
      }
      const showHiddenGenresButton = event.target.closest('[data-action="music-show-hidden-genres"]');
      if (showHiddenGenresButton) {
        const previousHidden = normalizeHiddenMusicGenres(state.musicHiddenGenres);
        state.musicHiddenGenres = [];
        try {
          await persistUiPreferences({ music_hidden_genres: [] });
        } catch (err) {
          state.musicHiddenGenres = previousHidden;
          setMusicPageNotice(`Restore genres failed: ${toUserErrorMessage(err)}`, true);
        }
        renderMusicLanding();
        return;
      }
      const showHiddenArtistsButton = event.target.closest('[data-action="music-show-hidden-artists"]');
      if (showHiddenArtistsButton) {
        const previousHidden = normalizeHiddenMusicArtists(state.musicHiddenArtists);
        state.musicHiddenArtists = [];
        try {
          await persistUiPreferences({ music_hidden_artists: [] });
        } catch (err) {
          state.musicHiddenArtists = previousHidden;
          setMusicPageNotice(`Restore artists failed: ${toUserErrorMessage(err)}`, true);
        }
        renderMusicLanding();
        return;
      }
      const resetLibraryButton = event.target.closest('[data-action="player-reset-library"]');
      if (resetLibraryButton) {
        state.playerSelectedArtistKey = "";
        state.playerSelectedAlbumKey = "";
        state.playerLibraryMode = "albums";
        renderMusicPlayerLibrary();
        setMusicSection("library");
        return;
      }
      const openArtistButton = event.target.closest('[data-action="player-open-artist"]');
      if (openArtistButton) {
        state.playerSelectedArtistKey = String(openArtistButton.dataset.artistKey || "");
        state.playerSelectedAlbumKey = "";
        state.playerLibraryMode = "albums";
        renderMusicPlayerLibrary();
        setMusicSection("library");
        return;
      }
      const openAlbumButton = event.target.closest('[data-action="player-open-album"]');
      if (openAlbumButton) {
        state.playerSelectedArtistKey = String(openAlbumButton.dataset.artistKey || state.playerSelectedArtistKey || "");
        state.playerSelectedAlbumKey = String(openAlbumButton.dataset.albumKey || "");
        state.playerLibraryMode = "tracks";
        renderMusicPlayerLibrary();
        setMusicSection("library");
        return;
      }
      const playArtistButton = event.target.closest('[data-action="player-play-artist"]');
      if (playArtistButton) {
        const artistKey = String(playArtistButton.dataset.artistKey || state.playerSelectedArtistKey || "");
        const tracks = getPlayerTracksForArtist(artistKey);
        if (!tracks.length) return;
        setPlayerQueue(tracks);
        await playMusicPlayerItem(normalizePlayableItem(tracks[0]));
        setMusicSection("library");
        return;
      }
      const shuffleArtistButton = event.target.closest('[data-action="player-shuffle-artist"]');
      if (shuffleArtistButton) {
        const artistKey = String(shuffleArtistButton.dataset.artistKey || state.playerSelectedArtistKey || "");
        const tracks = getPlayerTracksForArtist(artistKey);
        if (!tracks.length) return;
        const shuffled = tracks.slice().sort(() => Math.random() - 0.5);
        setPlayerQueue(shuffled);
        await playMusicPlayerItem(normalizePlayableItem(shuffled[0]));
        setMusicSection("library");
        return;
      }
      const playAlbumButton = event.target.closest('[data-action="player-play-album"]');
      if (playAlbumButton) {
        const artistKey = String(playAlbumButton.dataset.artistKey || state.playerSelectedArtistKey || "");
        const albumKey = String(playAlbumButton.dataset.albumKey || state.playerSelectedAlbumKey || "");
        const tracks = getPlayerTracksForAlbum(artistKey, albumKey);
        if (!tracks.length) return;
        setPlayerQueue(tracks);
        await playMusicPlayerItem(normalizePlayableItem(tracks[0]));
        setMusicSection("library");
        return;
      }
      const queueAlbumButton = event.target.closest('[data-action="player-queue-album"]');
      if (queueAlbumButton) {
        const artistKey = String(queueAlbumButton.dataset.artistKey || state.playerSelectedArtistKey || "");
        const albumKey = String(queueAlbumButton.dataset.albumKey || state.playerSelectedAlbumKey || "");
        const tracks = getPlayerTracksForAlbum(artistKey, albumKey);
        if (!tracks.length) return;
        queueTracksAtEnd(tracks);
        setNotice($("#music-player-message"), `${tracks.length} track${tracks.length === 1 ? "" : "s"} added to queue.`, false);
        renderMusicPlayerLibrary();
        return;
      }
      const openPlaylistButton = event.target.closest('[data-action="player-open-playlist"]');
      if (openPlaylistButton) {
        const playlistId = Number(openPlaylistButton.dataset.playlistId || 0) || null;
        if (!playlistId) return;
        state.playerSelectedPlaylistId = playlistId;
        const detailPayload = await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}`);
        state.playerSelectedPlaylistItems = Array.isArray(detailPayload?.items) ? detailPayload.items : [];
        renderMusicPlayerLibrary();
        setMusicSection("playlists");
        return;
      }
      const playPlaylistButton = event.target.closest('[data-action="player-play-playlist"]');
      if (playPlaylistButton) {
        const playlistItems = Array.isArray(state.playerSelectedPlaylistItems) ? state.playerSelectedPlaylistItems : [];
        if (!playlistItems.length) return;
        setPlayerQueue(playlistItems);
        await playMusicPlayerItem(normalizePlayableItem(playlistItems[0]));
        setMusicSection("playlists");
        return;
      }
      const deletePlaylistButton = event.target.closest('[data-action="player-delete-playlist"]');
      if (deletePlaylistButton) {
        const playlistId = Number(deletePlaylistButton.dataset.playlistId || 0) || null;
        if (!playlistId) return;
        await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}`, { method: "DELETE" });
        if (Number(state.playerSelectedPlaylistId || 0) === playlistId) {
          state.playerSelectedPlaylistId = null;
          state.playerSelectedPlaylistItems = [];
        }
        await loadMusicPlayerView();
        setMusicPlayerView("library");
        setMusicSection("playlists");
        return;
      }
      const removePlaylistItemButton = event.target.closest('[data-action="player-remove-playlist-item"]');
      if (removePlaylistItemButton) {
        const playlistId = Number(removePlaylistItemButton.dataset.playlistId || 0) || null;
        const itemId = Number(removePlaylistItemButton.dataset.itemId || 0) || null;
        if (!playlistId || !itemId) return;
        await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}/items/${encodeURIComponent(itemId)}`, { method: "DELETE" });
        if (Number(state.playerSelectedPlaylistId || 0) === playlistId) {
          const detailPayload = await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}`);
          state.playerSelectedPlaylistItems = Array.isArray(detailPayload?.items) ? detailPayload.items : [];
        }
        const playlistsPayload = await fetchJson("/api/player/playlists");
        state.playerPlaylists = Array.isArray(playlistsPayload?.playlists) ? playlistsPayload.playlists : [];
        renderMusicPlayerLibrary();
        return;
      }
      const addToPlaylistButton = event.target.closest('[data-action="player-add-to-playlist"]');
      if (addToPlaylistButton) {
        const playlistId = Number(addToPlaylistButton.dataset.playlistId || state.playerSelectedPlaylistId || 0) || null;
        if (!playlistId) {
          setNotice($("#music-player-message"), "Select or create a playlist first.", true);
          return;
        }
        await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}/items`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            id: String(addToPlaylistButton.dataset.localPath || addToPlaylistButton.dataset.streamUrl || ""),
            title: String(addToPlaylistButton.dataset.title || ""),
            artist: String(addToPlaylistButton.dataset.artist || ""),
            album: String(addToPlaylistButton.dataset.album || ""),
            local_path: String(addToPlaylistButton.dataset.localPath || ""),
            stream_url: String(addToPlaylistButton.dataset.streamUrl || ""),
            kind: String(addToPlaylistButton.dataset.sourceKind || "local"),
          }),
        });
        state.playerSelectedPlaylistId = playlistId;
        const [detailPayload, playlistsPayload] = await Promise.all([
          fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}`),
          fetchJson("/api/player/playlists"),
        ]);
        state.playerSelectedPlaylistItems = Array.isArray(detailPayload?.items) ? detailPayload.items : [];
        state.playerPlaylists = Array.isArray(playlistsPayload?.playlists) ? playlistsPayload.playlists : [];
        renderMusicPlayerLibrary();
        setMusicSection("playlists");
        setNotice($("#music-player-message"), "Track added to playlist.", false);
        return;
      }
      const queueTrackButton = event.target.closest('[data-action="player-queue-track"]');
      if (queueTrackButton) {
        const payload = buildPlayableItemFromButton(queueTrackButton);
        queueTracksAtEnd([payload]);
        renderMusicPlayerLibrary();
        setNotice($("#music-player-message"), "Added to queue.", false);
        return;
      }
      const playNextButton = event.target.closest('[data-action="player-play-next"]');
      if (playNextButton) {
        const payload = buildPlayableItemFromButton(playNextButton);
        queueTrackNext(payload);
        renderMusicPlayerLibrary();
        setNotice($("#music-player-message"), "Will play next.", false);
        return;
      }
      const moveQueueUpButton = event.target.closest('[data-action="player-queue-move-up"]');
      if (moveQueueUpButton) {
        moveQueueItem(Number(moveQueueUpButton.dataset.queueIndex || 0), Number(moveQueueUpButton.dataset.queueIndex || 0) - 1);
        return;
      }
      const moveQueueDownButton = event.target.closest('[data-action="player-queue-move-down"]');
      if (moveQueueDownButton) {
        moveQueueItem(Number(moveQueueDownButton.dataset.queueIndex || 0), Number(moveQueueDownButton.dataset.queueIndex || 0) + 1);
        return;
      }
      const removeQueueItemButton = event.target.closest('[data-action="player-remove-queue-item"]');
      if (removeQueueItemButton) {
        removeQueueItem(Number(removeQueueItemButton.dataset.queueIndex || 0));
        return;
      }
      const clearQueueButton = event.target.closest('[data-action="player-clear-queue"]');
      if (clearQueueButton) {
        clearQueue();
        setNotice($("#music-player-message"), "Queue cleared.", false);
        return;
      }
      const saveQueuePlaylistButton = event.target.closest('[data-action="player-save-queue-playlist"]');
      if (saveQueuePlaylistButton) {
        const queue = Array.isArray(state.playerQueue) ? state.playerQueue : [];
        if (!queue.length) return;
        const name = window.prompt("Playlist name", `Queue ${new Date().toLocaleDateString()}`);
        if (!name || !String(name).trim()) return;
        const payload = await fetchJson("/api/player/playlists", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: String(name).trim() }),
        });
        const playlistId = Number(payload?.playlist?.id || 0) || null;
        if (!playlistId) return;
        for (const item of queue) {
          // preserve order by inserting sequentially
          await fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}/items`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(item),
          });
        }
        state.playerSelectedPlaylistId = playlistId;
        const [detailPayload, playlistsPayload] = await Promise.all([
          fetchJson(`/api/player/playlists/${encodeURIComponent(playlistId)}`),
          fetchJson("/api/player/playlists"),
        ]);
        state.playerSelectedPlaylistItems = Array.isArray(detailPayload?.items) ? detailPayload.items : [];
        state.playerPlaylists = Array.isArray(playlistsPayload?.playlists) ? playlistsPayload.playlists : [];
        renderMusicPlayerLibrary();
        setMusicSection("playlists");
        setNotice($("#music-player-message"), "Queue saved as playlist.", false);
        return;
      }
      const playButton = event.target.closest('[data-action="player-play"]');
      if (playButton) {
        const payload = buildPlayableItemFromButton(playButton);
        const preserveStation = !!playButton.closest("#music-player-queue") && Number(state.playerActiveStationId || 0) > 0;
        if (!preserveStation) {
          clearActiveStationPlayback();
        }
        if (playButton.closest("#music-player-queue")) {
          // preserve current queue order
        } else if (playButton.closest(".music-player-playlist-items")) {
          const playlistItems = Array.isArray(state.playerSelectedPlaylistItems) ? state.playerSelectedPlaylistItems : [];
          setPlayerQueue(buildQueueFromTracks(playlistItems, payload));
        } else if (playButton.closest("#music-player-recent")) {
          const historyItems = Array.isArray(state.playerHistory) ? state.playerHistory.filter((entry) => entry?.stream_url) : [];
          setPlayerQueue(buildQueueFromTracks(historyItems, payload));
        } else if (playButton.closest("#music-player-library")) {
          const libraryTracks = getMusicPlayerFilteredTracks();
          setPlayerQueue(buildQueueFromTracks(libraryTracks, payload));
        }
        await playMusicPlayerItem(payload, { preserveStation });
      }
    };
  [$("#music-panel"), $("#music-player-modal")].filter(Boolean).forEach((host) => {
    host.addEventListener("click", (event) => {
      handlePlayerHostClick(event).catch(() => {});
    });
  });
  // Main transport controls — route through active-player abstraction so the same
  // buttons work for both local <audio> and YouTube IFrame player.
  $("#music-player-playpause")?.addEventListener("click", () => {
    if (activePlayerIsPaused()) activePlayerPlay(); else activePlayerPause();
    updateMusicPlayerTransportUI();
    syncBottomPlayerShell();
  });
  $("#music-player-prev")?.addEventListener("click", () => { playPreviousPlayerItem().catch(() => {}); });
  $("#music-player-next")?.addEventListener("click", () => { playNextPlayerItem().catch(() => {}); });
  $("#music-player-shuffle")?.addEventListener("click", () => { togglePlayerShuffle(); });
  $("#music-player-repeat")?.addEventListener("click", () => { cyclePlayerRepeatMode(); });
  $("#music-player-video-toggle")?.addEventListener("click", () => {
    if (!state.playerCurrentHasVideo) return;
    state.playerVideoVisible = !state.playerVideoVisible;
    syncMusicPlayerVideoShell();
  });
  $("#music-player-video-hide")?.addEventListener("click", () => {
    state.playerVideoVisible = false;
    syncMusicPlayerVideoShell();
  });
  const progress = $("#music-player-progress");
  if (progress) {
    progress.addEventListener("input", () => {
      state.playerProgressDragging = true;
      const duration = activePlayerDuration();
      const value = Number(progress.value || 0);
      const targetTime = duration > 0 ? (value / 1000) * duration : 0;
      const el = $("#music-player-current-time");
      if (el) el.textContent = formatPlayerTime(targetTime);
    });
    progress.addEventListener("change", () => {
      const duration = activePlayerDuration();
      const value = Number(progress.value || 0);
      if (duration > 0) activePlayerSeek((value / 1000) * duration);
      state.playerProgressDragging = false;
      updateMusicPlayerTransportUI();
    });
  }
  // <audio> element events — still needed for local-file playback.
  const playerAudio = $("#music-player-audio");
  if (playerAudio) {
    playerAudio.addEventListener("timeupdate", () => { if (!activePlayerIsYT()) updateMusicPlayerTransportUI(); });
    playerAudio.addEventListener("loadedmetadata", () => { if (!activePlayerIsYT()) updateMusicPlayerTransportUI(); });
    playerAudio.addEventListener("play", () => { if (!activePlayerIsYT()) syncBottomPlayerShell(); });
    playerAudio.addEventListener("pause", () => { if (!activePlayerIsYT()) syncBottomPlayerShell(); });
    playerAudio.addEventListener("ended", async () => {
      if (!activePlayerIsYT()) await playNextPlayerItem({ autoAdvance: true });
    });
  }
  const moviesTvInlineTmdbSave = $("#movies-tv-inline-tmdb-save");
  if (moviesTvInlineTmdbSave) {
    moviesTvInlineTmdbSave.addEventListener("click", async () => {
      const key = String($("#movies-tv-inline-tmdb-key")?.value || "").trim();
      if (!state.config) {
        await loadConfig();
      }
      if (!(await ensureAdminPinSession())) {
        setNotice($("#movies-tv-setup-message"), "Admin PIN unlock is required before saving TMDb settings.", true);
        return;
      }
      const payload = state.config ? JSON.parse(JSON.stringify(state.config)) : {};
      payload.arr = (payload.arr && typeof payload.arr === "object") ? payload.arr : {};
      payload.arr.tmdb_api_key = key;
      try {
        await fetchJson("/api/config", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        state.config = payload;
        setNotice($("#movies-tv-setup-message"), "TMDb API key saved. Movies & TV is ready.", false);
        renderMoviesTvSetupGate();
        loadArrGenres().catch(() => {});
      } catch (err) {
        setNotice($("#movies-tv-setup-message"), `TMDb save failed: ${toUserErrorMessage(err)}`, true);
      }
    });
  }
  const moviesTvOpenSettings = $("#movies-tv-open-settings");
  if (moviesTvOpenSettings) {
    moviesTvOpenSettings.addEventListener("click", () => {
      setPage("config");
      window.location.hash = "settings-arr";
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
  const homePreviewDownload = $("#home-preview-download");
  if (homePreviewDownload) {
    homePreviewDownload.addEventListener("click", async () => {
      if (homePreviewDownload.disabled) return;
      const directUrl = String(previewState.directUrl || "").trim();
      if (!directUrl) return;
      homePreviewDownload.disabled = true;
      const originalText = homePreviewDownload.textContent;
      homePreviewDownload.textContent = "Queued";
      try {
        if (previewState.itemId && previewState.candidateId) {
          await enqueueSearchCandidate(previewState.itemId, previewState.candidateId, { messageEl: $("#home-search-message") });
          if (state.homeSearchRequestId) {
            await refreshHomeResults(state.homeSearchRequestId);
          }
        } else {
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
        }
      } catch (err) {
        setNotice($("#home-search-message"), `Download failed: ${toUserErrorMessage(err)}`, true);
      } finally {
        homePreviewDownload.disabled = false;
        homePreviewDownload.textContent = originalText;
      }
    });
  }
  const arrDetailsClose = $("#arr-details-close");
  if (arrDetailsClose) {
    arrDetailsClose.addEventListener("click", closeArrDetailsModal);
  }
  const arrDetailsAdd = $("#arr-details-add");
  if (arrDetailsAdd) {
    arrDetailsAdd.addEventListener("click", async () => {
      const tmdbId = String(arrDetailsAdd.dataset.tmdbId || "").trim();
      if (!tmdbId || arrDetailsAdd.disabled) return;
      const item = state.arrResults.find((entry) => String(entry.tmdb_id) === tmdbId);
      if (!item) return;
      arrDetailsAdd.disabled = true;
      try {
        await addArrItem(tmdbId);
        openArrDetailsModal(item);
      } finally {
        const refreshedItem = state.arrResults.find((entry) => String(entry.tmdb_id) === tmdbId);
        if (refreshedItem) {
          openArrDetailsModal(refreshedItem);
        }
      }
    });
  }
  const arrDetailsTrailer = $("#arr-details-trailer");
  if (arrDetailsTrailer) {
    arrDetailsTrailer.addEventListener("click", async () => {
      const tmdbId = String(arrDetailsTrailer.dataset.tmdbId || "").trim();
      if (!tmdbId) return;
      const item = await ensureArrTrailerById(tmdbId);
      if (!item) return;
      const descriptor = buildArrTrailerPreviewDescriptor(item, { hover: false });
      if (descriptor) {
        openHomePreviewModal(descriptor);
      }
    });
  }
  const arrDetailsPoster = $("#arr-details-poster");
  if (arrDetailsPoster) {
    arrDetailsPoster.addEventListener("click", async () => {
      const tmdbId = String(state.arrDetailsItemId || "").trim();
      if (!tmdbId) return;
      const item = await ensureArrTrailerById(tmdbId);
      if (!item) return;
      const descriptor = buildArrTrailerPreviewDescriptor(item, { hover: false });
      if (descriptor) {
        openHomePreviewModal(descriptor);
      }
    });
  }
  const arrDetailsModal = $("#arr-details-modal");
  if (arrDetailsModal) {
    arrDetailsModal.addEventListener("click", async (event) => {
      const actorButton = event.target.closest('button[data-action="arr-person"]');
      if (actorButton) {
        const personId = String(actorButton.dataset.personId || "").trim();
        if (!personId) return;
        await openArrPersonTitles(personId);
        return;
      }
      if (event.target === arrDetailsModal) {
        closeArrDetailsModal();
      }
    });
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
  const musicReviewAlertOpen = $("#music-page-review-alert-open");
  if (musicReviewAlertOpen) {
    musicReviewAlertOpen.addEventListener("click", () => {
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
      applySettingsAdvancedMode(settingsAdvancedToggle.checked, { persist: true });
    });
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
  updateArrModeToggleUI();
  renderMoviesTvYearChips();
  setMoviesTvFiltersOpen(false);
  renderArrConnectionStatus();
  applyHomeDefaultVideoFormat({ force: true });
  updateMusicModeFormatControl();
  syncSettingsMainWidthLock();
  setActiveSettingsSection(state.settingsActiveSectionId || getDefaultSettingsSectionId(), { jump: false, smooth: false });

}

function loadAppSidebarCollapsedPreference() {
  try {
    return localStorage.getItem(APP_SIDEBAR_COLLAPSED_KEY) === "true";
  } catch (_err) {
    return false;
  }
}

function applyAppSidebarCollapsed(collapsed, { persist = true } = {}) {
  const normalized = !!collapsed;
  state.appSidebarCollapsed = normalized;
  document.body.classList.toggle("app-sidebar-collapsed", normalized);
  const toggle = $("#app-sidebar-toggle");
  if (toggle) {
    toggle.textContent = normalized ? "⇥" : "⇤";
    toggle.setAttribute("aria-label", normalized ? "Expand sidebar" : "Collapse sidebar");
    toggle.title = normalized ? "Expand sidebar" : "Collapse sidebar";
  }
  if (persist) {
    try {
      localStorage.setItem(APP_SIDEBAR_COLLAPSED_KEY, normalized ? "true" : "false");
    } catch (_err) {
      // ignore localStorage failures
    }
  }
}

async function init() {
  state.adminPinToken = localStorage.getItem(ADMIN_PIN_TOKEN_KEY) || "";
  state.appSidebarCollapsed = loadAppSidebarCollapsedPreference();
  state.musicHeaderMode = loadMusicHeaderModePreference();
  mountSettingsSubpages();
  mountHomePageNodes();
  window.addEventListener("spotify-oauth-complete", () => {
    setNotice(
      $("#home-results-detail"),
      "Spotify connected successfully. Initial sync has started.",
      false
    );
  });
  applyTheme(resolveTheme());
  bindEvents();
  applyAppSidebarCollapsed(state.appSidebarCollapsed, { persist: false });
  setMusicHeaderMode(state.musicHeaderMode, { persist: false });
  setupHeaderScrollVisibility();
  applyHomeVideoCardSize(state.homeVideoCardSize);
  applyArrCardSize(state.arrCardSize);
  syncBottomPlayerShell();
  updateReviewPendingIndicators();
  try {
    const cfg = await fetchJson("/api/config");
    state.config = cfg;
    syncMusicPreferencesFromConfig(cfg);
    renderConfig(cfg);
    updateSearchDestinationDisplay();
    renderMoviesTvSetupGate();
    warmArrEditorialShelvesOnStartup();
  } catch (_err) {
    // keep boot non-fatal; settings page load path will surface config errors later
  }
  // Home should always boot into the video-first surface. Preserve the saved
  // music mode only for when the user navigates to the dedicated Music page.
  const storedHomeMediaMode = loadHomeMediaModePreference();
  if (storedHomeMediaMode !== "video") {
    state.lastMusicMode = storedHomeMediaMode;
  }
  setHomeMediaMode("video", { persist: false, clearResultsOnDisable: false });
  applyHomeDefaultDestination({ force: true });
  applyHomeDefaultActiveFormat({ force: true });
  setHomeDeliveryMode(getHomeDeliveryMode());
  setupNavActions();
  await loadPaths();
  let initialPage = (window.location.hash || "#home").replace("#", "");
  state.startupSetupPromptPending = shouldShowStartupSetupPrompt();
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
// Pre-load the YouTube IFrame API in the background so it's ready when the user first plays a track.
window.addEventListener("DOMContentLoaded", () => { ensureYouTubeAPILoaded(); });

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
  const videoSection = document.querySelector('section[data-page="video"]');
  const musicSection = document.querySelector('section[data-page="music"]');
  if (!videoSection && !musicSection) return;

  const syncPageClass = () => {
    const videoVisible = videoSection ? !videoSection.classList.contains("page-hidden") : false;
    const musicVisible = musicSection ? !musicSection.classList.contains("page-hidden") : false;
    const videoLike = videoVisible || musicVisible;
    document.body.classList.toggle("home-page", videoLike);
    document.body.classList.toggle("video-page", videoVisible);
    document.body.classList.toggle("launcher-page", !videoVisible && !musicVisible && state.currentPage === "home");
    document.body.classList.toggle("music-page", musicVisible);
    if (videoVisible) {
      document.body.dataset.page = "video";
    } else if (musicVisible) {
      document.body.dataset.page = "music";
    } else if (state.currentPage) {
      document.body.dataset.page = state.currentPage;
    }
  };

  const observer = new MutationObserver(() => {
    syncPageClass();
  });
  if (videoSection) observer.observe(videoSection, { attributes: true, attributeFilter: ["class"] });
  if (musicSection) observer.observe(musicSection, { attributes: true, attributeFilter: ["class"] });
  syncPageClass();
})();
