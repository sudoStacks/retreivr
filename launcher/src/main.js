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
  const openDockerInstallBtn = document.getElementById("openDockerInstall");

  const hostPortInput = document.getElementById("hostPort");
  const imageInput = document.getElementById("image");
  const containerNameInput = document.getElementById("containerName");
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

  let currentWebUrl = "http://localhost:8090";
  let dockerInstallUrl = "https://www.docker.com/products/docker-desktop/";
  let installGuide = null;
  let busy = false;

  function setBusy(nextBusy) {
    busy = nextBusy;
    refreshBtn.disabled = nextBusy;
    recheckDockerBtn.disabled = nextBusy;
    saveBtn.disabled = nextBusy;
    resetBtn.disabled = nextBusy;
    installBtn.disabled = nextBusy;
    stopBtn.disabled = nextBusy;
    openBtn.disabled = nextBusy;
    openDockerInstallBtn.disabled = nextBusy;
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
    };
  }

  function applySettingsToForm(settings) {
    hostPortInput.value = String(settings.host_port);
    imageInput.value = settings.image;
    containerNameInput.value = settings.container_name;
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

  async function refreshDiagnostics() {
    const diagnostics = await invoke("docker_diagnostics");
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

  async function refreshAll() {
    setBusy(true);
    statusEl.textContent = "Refreshing launcher status...";

    try {
      installGuide = await invoke("install_guidance");
      dockerInstallUrl = installGuide.install_url || dockerInstallUrl;
      const settings = await invoke("get_launcher_settings");
      applySettingsToForm(settings);
      await refreshDiagnostics();
      statusEl.textContent = "Ready";
    } catch (error) {
      summaryEl.textContent = "Launcher refresh failed";
      statusEl.textContent = `Refresh failed: ${String(error)}`;
      console.error(error);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
    }
  }

  settingsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setBusy(true);
    statusEl.textContent = "Saving configuration...";

    try {
      const settings = readSettingsFromForm();
      await invoke("save_launcher_settings", { settings });
      await refreshDiagnostics();
      statusEl.textContent = "Configuration saved";
    } catch (error) {
      statusEl.textContent = `Save failed: ${String(error)}`;
      console.error(error);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
    }
  });

  resetBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Restoring default configuration...";

    try {
      const defaults = await invoke("reset_launcher_settings");
      applySettingsToForm(defaults);
      await refreshDiagnostics();
      statusEl.textContent = "Defaults restored";
    } catch (error) {
      statusEl.textContent = `Reset failed: ${String(error)}`;
      console.error(error);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
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
    statusEl.textContent = "Starting Retreivr...";

    try {
      const settings = readSettingsFromForm();
      await invoke("save_launcher_settings", { settings });
      await invoke("install_retreivr");
      await refreshDiagnostics();
      statusEl.textContent = "Retreivr started";
    } catch (error) {
      statusEl.textContent = `Start failed: ${String(error)}`;
      console.error(error);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
    }
  });

  stopBtn.addEventListener("click", async () => {
    setBusy(true);
    statusEl.textContent = "Stopping Retreivr...";

    try {
      await invoke("stop_retreivr");
      await refreshDiagnostics();
      statusEl.textContent = "Retreivr stopped";
    } catch (error) {
      statusEl.textContent = `Stop failed: ${String(error)}`;
      console.error(error);
    } finally {
      setBusy(false);
      await refreshDiagnostics().catch(() => {});
    }
  });

  openBtn.addEventListener("click", async () => {
    if (typeof openWeb === "function") {
      await openWeb(currentWebUrl);
    }
  });

  refreshAll();
});
