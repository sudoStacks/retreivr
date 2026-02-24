const { invoke } = window.__TAURI__.core;
const openWeb = window.__TAURI__?.opener?.open;

document.addEventListener("DOMContentLoaded", () => {
  const summaryEl = document.getElementById("summary");
  const statusEl = document.getElementById("status");

  const refreshBtn = document.getElementById("refreshBtn");
  const recheckDockerBtn = document.getElementById("recheckDocker");
  const saveBtn = document.getElementById("saveBtn");
  const resetBtn = document.getElementById("resetBtn");
  const installBtn = document.getElementById("installBtn");
  const stopBtn = document.getElementById("stopBtn");
  const openBtn = document.getElementById("openUI");
  const openDataFolderBtn = document.getElementById("openDataFolderBtn");
  const openComposeFolderBtn = document.getElementById("openComposeFolderBtn");
  const viewLogsBtn = document.getElementById("viewLogsBtn");
  const copyDiagBtn = document.getElementById("copyDiagBtn");
  const checkLauncherUpdateBtn = document.getElementById("checkLauncherUpdateBtn");
  const downloadLauncherUpdateBtn = document.getElementById("downloadLauncherUpdateBtn");
  const checkImageUpdateBtn = document.getElementById("checkImageUpdateBtn");
  const updateRestartBtn = document.getElementById("updateRestartBtn");
  const openDockerInstallBtn = document.getElementById("openDockerInstall");
  const applyPresetBtn = document.getElementById("applyPresetBtn");
  const browseDownloadsDirBtn = document.getElementById("browseDownloadsDirBtn");
  const browseConfigDirBtn = document.getElementById("browseConfigDirBtn");
  const browseTokensDirBtn = document.getElementById("browseTokensDirBtn");
  const browseLogsDirBtn = document.getElementById("browseLogsDirBtn");
  const browseDataDirBtn = document.getElementById("browseDataDirBtn");

  const hostPortInput = document.getElementById("hostPort");
  const imageInput = document.getElementById("image");
  const containerNameInput = document.getElementById("containerName");
  const downloadsDirInput = document.getElementById("downloadsDir");
  const configDirInput = document.getElementById("configDir");
  const tokensDirInput = document.getElementById("tokensDir");
  const logsDirInput = document.getElementById("logsDir");
  const dataDirInput = document.getElementById("dataDir");
  const presetSelect = document.getElementById("presetSelect");
  const settingsForm = document.getElementById("settingsForm");

  const diagDockerInstalled = document.getElementById("diagDockerInstalled");
  const diagDockerRunning = document.getElementById("diagDockerRunning");
  const diagComposeAvailable = document.getElementById("diagComposeAvailable");
  const diagComposeExists = document.getElementById("diagComposeExists");
  const diagContainerRunning = document.getElementById("diagContainerRunning");
  const diagWebReachable = document.getElementById("diagWebReachable");
  const diagErrorEl = document.getElementById("diagError");
  const composePathEl = document.getElementById("composePath");
  const runtimeDirEl = document.getElementById("runtimeDir");
  const dockerGuideCard = document.getElementById("dockerGuideCard");
  const guideTitleEl = document.getElementById("guideTitle");
  const guideSummaryEl = document.getElementById("guideSummary");
  const guideStepsEl = document.getElementById("guideSteps");
  const preflightSummaryEl = document.getElementById("preflightSummary");
  const preflightChecksEl = document.getElementById("preflightChecks");
  const errorPanelEl = document.getElementById("errorPanel");
  const errorTitleEl = document.getElementById("errorTitle");
  const errorMessageEl = document.getElementById("errorMessage");
  const errorFixesEl = document.getElementById("errorFixes");
  const logsPanelEl = document.getElementById("logsPanel");
  const logsOutputEl = document.getElementById("logsOutput");
  const onboardingSummaryEl = document.getElementById("onboardingSummary");
  const onboardingListEl = document.getElementById("onboardingList");
  const launcherVersionEl = document.getElementById("launcherVersion");
  const launcherLatestEl = document.getElementById("launcherLatest");
  const retreivrImageEl = document.getElementById("retreivrImage");
  const retreivrImageUpdateEl = document.getElementById("retreivrImageUpdate");

  let currentWebUrl = "http://localhost:8090";
  let dockerInstallUrl = "https://www.docker.com/products/docker-desktop/";
  let launcherReleaseUrl = "https://github.com/sudostacks/retreivr/releases";
  let installGuide = null;
  let latestDiagnostics = null;
  let latestPreflight = null;
  let latestSettings = null;
  let latestLauncherVersion = null;
  let latestImageUpdate = null;
  let busy = false;

  function reconcileUpdateButtons() {
    downloadLauncherUpdateBtn.disabled =
      busy || !(latestLauncherVersion && latestLauncherVersion.update_available);
    updateRestartBtn.disabled = busy || !(latestImageUpdate && latestImageUpdate.update_available);
  }

  function setBusy(nextBusy) {
    busy = nextBusy;
    refreshBtn.disabled = nextBusy;
    recheckDockerBtn.disabled = nextBusy;
    saveBtn.disabled = nextBusy;
    resetBtn.disabled = nextBusy;
    installBtn.disabled = nextBusy;
    stopBtn.disabled = nextBusy;
    openBtn.disabled = nextBusy;
    openDataFolderBtn.disabled = nextBusy;
    openComposeFolderBtn.disabled = nextBusy;
    viewLogsBtn.disabled = nextBusy;
    copyDiagBtn.disabled = nextBusy;
    checkLauncherUpdateBtn.disabled = nextBusy;
    checkImageUpdateBtn.disabled = nextBusy;
    downloadLauncherUpdateBtn.disabled = nextBusy;
    updateRestartBtn.disabled = nextBusy;
    openDockerInstallBtn.disabled = nextBusy;
    applyPresetBtn.disabled = nextBusy;
    browseDownloadsDirBtn.disabled = nextBusy;
    browseConfigDirBtn.disabled = nextBusy;
    browseTokensDirBtn.disabled = nextBusy;
    browseLogsDirBtn.disabled = nextBusy;
    browseDataDirBtn.disabled = nextBusy;
    if (!nextBusy) {
      reconcileUpdateButtons();
    }
  }

  function renderBool(el, value) {
    el.textContent = value ? "Yes" : "No";
    el.className = value ? "ok" : "warn";
  }

  function readSettingsFromForm() {
    return {
      host_port: Number.parseInt(hostPortInput.value, 10),
      image: imageInput.value.trim(),
      container_name: containerNameInput.value.trim(),
      downloads_dir: downloadsDirInput.value.trim(),
      config_dir: configDirInput.value.trim(),
      tokens_dir: tokensDirInput.value.trim(),
      logs_dir: logsDirInput.value.trim(),
      data_dir: dataDirInput.value.trim(),
    };
  }

  function applySettingsToForm(settings) {
    hostPortInput.value = String(settings.host_port);
    imageInput.value = settings.image;
    containerNameInput.value = settings.container_name;
    downloadsDirInput.value = settings.downloads_dir;
    configDirInput.value = settings.config_dir;
    tokensDirInput.value = settings.tokens_dir;
    logsDirInput.value = settings.logs_dir;
    dataDirInput.value = settings.data_dir;
  }

  function applyPreset(preset) {
    if (preset === "alt_port") {
      hostPortInput.value = "9000";
      imageInput.value = "ghcr.io/sudostacks/retreivr:latest";
      containerNameInput.value = "retreivr";
      return;
    }

    if (preset === "edge") {
      hostPortInput.value = "8090";
      imageInput.value = "ghcr.io/sudostacks/retreivr:edge";
      containerNameInput.value = "retreivr-edge";
      return;
    }

    hostPortInput.value = "8090";
    imageInput.value = "ghcr.io/sudostacks/retreivr:latest";
    containerNameInput.value = "retreivr";
  }

  function renderInstallGuidance(diagnostics) {
    const needsInstall = !diagnostics.docker_installed;
    const needsStart = diagnostics.docker_installed && !diagnostics.docker_running;
    const showGuidance = needsInstall || needsStart;

    dockerGuideCard.classList.toggle("hidden", !showGuidance);
    if (!showGuidance || !installGuide) {
      return;
    }

    guideTitleEl.textContent = needsInstall
      ? "Step 1.5: Install Docker"
      : "Step 1.5: Start Docker Desktop";

    guideSummaryEl.textContent = needsInstall
      ? `Docker is not installed (${installGuide.os}). Install it, then recheck.`
      : "Docker is installed but not running. Start Docker Desktop, then recheck.";

    guideStepsEl.innerHTML = "";
    installGuide.steps.forEach((step) => {
      const li = document.createElement("li");
      li.textContent = step;
      guideStepsEl.appendChild(li);
    });
  }

  function setErrorPanel(title, message, fixes = []) {
    errorPanelEl.classList.remove("hidden");
    errorTitleEl.textContent = title;
    errorMessageEl.textContent = message;
    errorFixesEl.innerHTML = "";
    fixes.forEach((fix) => {
      const li = document.createElement("li");
      li.textContent = fix;
      errorFixesEl.appendChild(li);
    });
  }

  function clearErrorPanel() {
    errorPanelEl.classList.add("hidden");
    errorFixesEl.innerHTML = "";
  }

  function toErrorText(error) {
    if (typeof error === "string") return error;
    if (error && typeof error.message === "string") return error.message;
    return String(error);
  }

  function actionableFixes(errorText) {
    const text = errorText.toLowerCase();
    if (text.includes("permission denied")) {
      return {
        title: "Docker Permission Issue",
        message: errorText,
        fixes: [
          "Start Docker Desktop and ensure your user can access Docker.",
          "If using Linux, verify user Docker group permissions.",
          "Run Refresh Status after fixing permissions.",
        ],
      };
    }
    if (text.includes("cannot connect to the docker daemon") || text.includes("docker daemon")) {
      return {
        title: "Docker Engine Not Running",
        message: errorText,
        fixes: [
          "Launch Docker Desktop and wait until it reports running.",
          "Use Recheck Docker to refresh diagnostics.",
        ],
      };
    }
    if (text.includes("port is in use") || text.includes("address already in use")) {
      return {
        title: "Host Port Conflict",
        message: errorText,
        fixes: [
          "Set a different Host Port in Step 2 and save configuration.",
          "Retry Start Retreivr after saving.",
        ],
      };
    }
    if (text.includes("compose")) {
      return {
        title: "Docker Compose Error",
        message: errorText,
        fixes: [
          "Check image/container/port values in Step 2.",
          "Run Start Retreivr again after saving configuration.",
        ],
      };
    }
    return {
      title: "Runtime Error",
      message: errorText,
      fixes: [
        "Review diagnostics and preflight results in this window.",
        "Copy diagnostics and logs for troubleshooting.",
      ],
    };
  }

  function renderPreflight(report) {
    latestPreflight = report;
    preflightChecksEl.innerHTML = "";
    if (!report || !Array.isArray(report.checks)) {
      preflightSummaryEl.textContent = "";
      return;
    }

    preflightSummaryEl.textContent = report.ok
      ? "Preflight passed. Safe to start."
      : "Preflight failed. Resolve issues before starting.";

    report.checks.forEach((check) => {
      const li = document.createElement("li");
      const state = check.ok ? "PASS" : "FAIL";
      li.className = check.ok ? "ok" : "warn";
      li.textContent = `${state} - ${check.label}: ${check.details}`;
      preflightChecksEl.appendChild(li);
    });
  }

  async function copyDiagnosticsReport() {
    const report = {
      timestamp: new Date().toISOString(),
      diagnostics: latestDiagnostics,
      preflight: latestPreflight,
      settings: latestSettings,
      launcher_version: latestLauncherVersion,
      image_update: latestImageUpdate,
      status: statusEl.textContent,
      summary: summaryEl.textContent,
    };
    const payload = JSON.stringify(report, null, 2);
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(payload);
      return;
    }
    throw new Error("Clipboard API unavailable in this context.");
  }

  function renderLauncherVersionInfo(info) {
    latestLauncherVersion = info;
    launcherVersionEl.textContent = info.current_version || "Unknown";
    launcherLatestEl.textContent = info.latest_version || "Unavailable";
    launcherLatestEl.className = info.update_available ? "warn" : "ok";
    launcherReleaseUrl = info.release_url || launcherReleaseUrl;
    reconcileUpdateButtons();
  }

  function renderImageUpdateInfo(info) {
    latestImageUpdate = info;
    retreivrImageEl.textContent = info.image || "Unknown";
    if (info.check_error) {
      retreivrImageUpdateEl.textContent = "Check failed";
      retreivrImageUpdateEl.className = "warn";
      updateRestartBtn.disabled = true;
      return;
    }

    retreivrImageUpdateEl.textContent = info.update_available ? "Available" : "Current";
    retreivrImageUpdateEl.className = info.update_available ? "warn" : "ok";
    reconcileUpdateButtons();
  }

  async function refreshOnboardingChecklist() {
    const checklist = await invoke("onboarding_checklist");
    onboardingSummaryEl.textContent = `${checklist.completed}/${checklist.total} onboarding checks complete`;
    onboardingListEl.innerHTML = "";
    checklist.items.forEach((item) => {
      const li = document.createElement("li");
      li.className = item.done ? "ok" : "warn";
      const state = item.done ? "DONE" : "TODO";
      li.textContent = `${state} - ${item.label}: ${item.details}`;
      onboardingListEl.appendChild(li);
    });
  }

  async function refreshDiagnostics() {
    const diagnostics = await invoke("docker_diagnostics");
    latestDiagnostics = diagnostics;
    currentWebUrl = diagnostics.web_url || currentWebUrl;

    renderBool(diagDockerInstalled, diagnostics.docker_installed);
    renderBool(diagDockerRunning, diagnostics.docker_running);
    renderBool(diagComposeAvailable, diagnostics.compose_available);
    renderBool(diagComposeExists, diagnostics.compose_exists);
    renderBool(diagContainerRunning, diagnostics.container_running);
    renderBool(diagWebReachable, diagnostics.service_reachable);

    diagErrorEl.textContent = diagnostics.last_error || "All checks passed.";
    diagErrorEl.className = diagnostics.last_error ? "diag-error warn" : "diag-error ok";

    summaryEl.textContent =
      `Web URL: ${currentWebUrl} | Compose file: ${diagnostics.compose_exists ? "Present" : "Missing"}`;
    composePathEl.textContent = `Compose path: ${diagnostics.compose_path}`;
    runtimeDirEl.textContent = `Runtime dir: ${diagnostics.runtime_dir}`;
    renderInstallGuidance(diagnostics);

    installBtn.disabled = busy || !diagnostics.docker_running || !diagnostics.compose_available;
    stopBtn.disabled = busy || !diagnostics.container_running;
    openBtn.disabled = busy || !diagnostics.service_reachable;
  }

  async function browseInto(inputEl) {
    setBusy(true);
    statusEl.textContent = "Opening folder picker...";
    try {
      const selected = await invoke("browse_for_directory");
      if (selected) {
        inputEl.value = selected;
        statusEl.textContent = "Folder selected. Save configuration to persist.";
      } else {
        statusEl.textContent = "Folder selection cancelled";
      }
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Folder browse failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
    }
  }

  async function refreshAll() {
    setBusy(true);
    statusEl.textContent = "Refreshing launcher status...";

    try {
      installGuide = await invoke("install_guidance");
      dockerInstallUrl = installGuide.install_url || dockerInstallUrl;
      const settings = await invoke("get_launcher_settings");
      latestSettings = settings;
      applySettingsToForm(settings);
      retreivrImageEl.textContent = settings.image;
      retreivrImageUpdateEl.textContent = "Unknown";
      retreivrImageUpdateEl.className = "";
      latestImageUpdate = null;
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
      statusEl.textContent = "Ready";
    } catch (error) {
      summaryEl.textContent = "Launcher refresh failed";
      const errorText = toErrorText(error);
      statusEl.textContent = `Refresh failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
      console.error(errorText);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  }

  settingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setBusy(true);
    statusEl.textContent = "Saving configuration...";

    try {
      const settings = readSettingsFromForm();
      await invoke("save_launcher_settings", { settings });
      latestSettings = settings;
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
      statusEl.textContent = "Configuration saved";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Save failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
      console.error(errorText);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  resetBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Restoring default configuration...";

    try {
      const defaults = await invoke("reset_launcher_settings");
      latestSettings = defaults;
      applySettingsToForm(defaults);
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
      statusEl.textContent = "Defaults restored";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Reset failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
      console.error(errorText);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  applyPresetBtn.addEventListener("click", async () => {
    if (busy) return;
    applyPreset(presetSelect.value);
    statusEl.textContent = "Preset applied. Save configuration to persist.";
  });

  browseDownloadsDirBtn.addEventListener("click", async () => {
    await browseInto(downloadsDirInput);
  });

  browseConfigDirBtn.addEventListener("click", async () => {
    await browseInto(configDirInput);
  });

  browseTokensDirBtn.addEventListener("click", async () => {
    await browseInto(tokensDirInput);
  });

  browseLogsDirBtn.addEventListener("click", async () => {
    await browseInto(logsDirInput);
  });

  browseDataDirBtn.addEventListener("click", async () => {
    await browseInto(dataDirInput);
  });

  checkLauncherUpdateBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Checking launcher updates...";
    try {
      const info = await invoke("launcher_version_info");
      renderLauncherVersionInfo(info);
      statusEl.textContent = info.update_available
        ? "Launcher update available"
        : "Launcher is up to date";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Launcher update check failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
    }
  });

  downloadLauncherUpdateBtn.addEventListener("click", async () => {
    if (typeof openWeb === "function") {
      await openWeb(launcherReleaseUrl);
      statusEl.textContent = "Opened launcher release page";
    }
  });

  checkImageUpdateBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Checking Retreivr image update...";
    try {
      const info = await invoke("check_retreivr_image_update");
      renderImageUpdateInfo(info);
      if (info.check_error) {
        const issue = actionableFixes(info.check_error);
        setErrorPanel(issue.title, issue.message, issue.fixes);
        statusEl.textContent = "Image update check failed";
      } else {
        statusEl.textContent = info.update_available
          ? "New Retreivr image available"
          : "Retreivr image is current";
        clearErrorPanel();
      }
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Image update check failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  updateRestartBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Updating Retreivr image and restarting...";
    try {
      const message = await invoke("update_retreivr_and_restart");
      statusEl.textContent = message;
      clearErrorPanel();
      const info = await invoke("check_retreivr_image_update");
      renderImageUpdateInfo(info);
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Update failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  refreshBtn.addEventListener("click", async () => {
    if (busy) return;
    await refreshAll();
  });

  recheckDockerBtn.addEventListener("click", async () => {
    if (busy) return;
    await refreshAll();
  });

  openDockerInstallBtn.addEventListener("click", async () => {
    if (typeof openWeb === "function") {
      await openWeb(dockerInstallUrl);
    }
  });

  installBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Running preflight checks...";

    try {
      const settings = readSettingsFromForm();
      await invoke("save_launcher_settings", { settings });
      latestSettings = settings;

      const preflight = await invoke("preflight_start_checks");
      renderPreflight(preflight);
      if (!preflight.ok) {
        const failing = preflight.checks
          .filter((check) => !check.ok)
          .map((check) => `${check.label}: ${check.fix}`);
        setErrorPanel(
          "Preflight Blocked Start",
          "Start was skipped because one or more checks failed.",
          failing
        );
        statusEl.textContent = "Preflight failed";
        return;
      }

      statusEl.textContent = "Starting Retreivr...";
      await invoke("install_retreivr");
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
      statusEl.textContent = "Retreivr started";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Start failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
      console.error(errorText);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  stopBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Stopping Retreivr...";

    try {
      await invoke("stop_retreivr");
      await refreshDiagnostics();
      await refreshOnboardingChecklist();
      statusEl.textContent = "Retreivr stopped";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Stop failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
      console.error(errorText);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
      await refreshOnboardingChecklist().catch(() => {});
    }
  });

  openBtn.addEventListener("click", async () => {
    if (typeof openWeb === "function") {
      await openWeb(currentWebUrl);
    }
  });

  openDataFolderBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Opening data folder...";
    try {
      await invoke("open_data_folder");
      statusEl.textContent = "Data folder opened";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Open folder failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
    }
  });

  openComposeFolderBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Opening compose folder...";
    try {
      await invoke("open_compose_folder");
      statusEl.textContent = "Compose folder opened";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Open folder failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
    }
  });

  viewLogsBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Loading logs...";
    try {
      const logs = await invoke("view_retreivr_logs", { lines: 250 });
      logsPanelEl.classList.remove("hidden");
      logsOutputEl.textContent = logs || "(No logs returned)";
      statusEl.textContent = "Logs loaded";
      clearErrorPanel();
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Logs failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    } finally {
      setBusy(false);
    }
  });

  copyDiagBtn.addEventListener("click", async () => {
    try {
      await copyDiagnosticsReport();
      statusEl.textContent = "Diagnostics copied to clipboard";
    } catch (error) {
      const errorText = toErrorText(error);
      statusEl.textContent = `Copy failed: ${errorText}`;
      const issue = actionableFixes(errorText);
      setErrorPanel(issue.title, issue.message, issue.fixes);
    }
  });

  refreshAll();
});
