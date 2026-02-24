const { invoke } = window.__TAURI__.core;
const openWeb = window.__TAURI__?.opener?.open;
const RETREIVR_UI_URL = "http://localhost:8090";

document.addEventListener("DOMContentLoaded", () => {
  const summaryEl = document.getElementById("summary");
  const statusEl = document.getElementById("status");

  const installBtn = document.getElementById("installBtn");
  const stopBtn = document.getElementById("stopBtn");
  const openBtn = document.getElementById("openUI");

  async function refreshState() {
    statusEl.textContent = "Refreshing state...";

    try {
      const docker = await invoke("docker_available");
      if (!docker) {
        summaryEl.textContent = "Docker: Not available";
        installBtn.disabled = true;
        stopBtn.disabled = true;
        openBtn.disabled = true;
        statusEl.textContent = "Start Docker Desktop and relaunch this app.";
        return;
      }

      const hasCompose = await invoke("compose_exists");
      const running = await invoke("container_running");

      summaryEl.textContent =
        `Docker: OK | Compose: ${hasCompose ? "Present" : "Missing"} | Container: ${running ? "Running" : "Stopped"}`;

      installBtn.disabled = running;
      stopBtn.disabled = !running;
      openBtn.disabled = !running;

      statusEl.textContent = "Ready";
    } catch (error) {
      summaryEl.textContent = "State check failed";
      statusEl.textContent = "Unable to refresh launcher state.";
      installBtn.disabled = true;
      stopBtn.disabled = true;
      openBtn.disabled = true;
      console.error(error);
    }
  }

  installBtn.addEventListener("click", async () => {
    statusEl.textContent = "Installing / starting Retreivr...";
    try {
      await invoke("install_retreivr");
      await refreshState();
    } catch (e) {
      statusEl.textContent = "Install failed";
      console.error(e);
    }
  });

  stopBtn.addEventListener("click", async () => {
    statusEl.textContent = "Stopping Retreivr...";
    try {
      await invoke("stop_retreivr");
      await refreshState();
    } catch (e) {
      statusEl.textContent = "Stop failed";
      console.error(e);
    }
  });

  openBtn.addEventListener("click", async () => {
    if (typeof openWeb === "function") {
      await openWeb(RETREIVR_UI_URL);
    }
  });

  refreshState();
});
