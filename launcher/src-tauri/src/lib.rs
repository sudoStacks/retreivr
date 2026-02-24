use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};

fn app_support_dir(app: &AppHandle) -> PathBuf {
    app.path()
        .app_data_dir()
        .expect("failed to resolve app data dir")
        .join("Retreivr")
}

fn compose_path(app: &AppHandle) -> PathBuf {
    app_support_dir(app).join("compose.yaml")
}

fn settings_path(app: &AppHandle) -> PathBuf {
    app_support_dir(app).join("launcher_settings.json")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LauncherSettings {
    host_port: u16,
    image: String,
    container_name: String,
}

impl Default for LauncherSettings {
    fn default() -> Self {
        Self {
            host_port: 8090,
            image: "ghcr.io/sudoStacks/retreivr:latest".to_string(),
            container_name: "retreivr".to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct DockerDiagnostics {
    docker_installed: bool,
    docker_running: bool,
    compose_available: bool,
    compose_exists: bool,
    container_running: bool,
    service_reachable: bool,
    web_url: String,
}

fn web_url(settings: &LauncherSettings) -> String {
    format!("http://localhost:{}", settings.host_port)
}

fn load_settings(app: &AppHandle) -> LauncherSettings {
    let path = settings_path(app);
    let content = match fs::read_to_string(path) {
        Ok(value) => value,
        Err(_) => return LauncherSettings::default(),
    };

    serde_json::from_str(&content).unwrap_or_else(|_| LauncherSettings::default())
}

fn save_settings_to_disk(app: &AppHandle, settings: &LauncherSettings) -> Result<(), String> {
    let dir = app_support_dir(app);
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let payload = serde_json::to_string_pretty(settings).map_err(|e| e.to_string())?;
    fs::write(settings_path(app), payload).map_err(|e| e.to_string())
}

fn validate_settings(settings: &LauncherSettings) -> Result<(), String> {
    if settings.host_port == 0 {
        return Err("host_port must be between 1 and 65535".to_string());
    }

    if settings.image.trim().is_empty() {
        return Err("image cannot be empty".to_string());
    }

    if settings.container_name.trim().is_empty() {
        return Err("container_name cannot be empty".to_string());
    }

    if !settings
        .container_name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err("container_name may only contain letters, numbers, '-', '_' and '.'".to_string());
    }

    Ok(())
}

fn render_compose(settings: &LauncherSettings) -> String {
    format!(
        r#"services:
  retreivr:
    image: {image}
    container_name: {container_name}
    restart: unless-stopped
    ports:
      - "{host_port}:8000"
    volumes:
      - "./config:/config"
      - "./data:/data"
      - "./downloads:/downloads"
      - "./logs:/logs"
      - "./tokens:/tokens"
"#,
        image = settings.image,
        container_name = settings.container_name,
        host_port = settings.host_port
    )
}

fn ensure_runtime_dirs(app: &AppHandle) -> Result<(), String> {
    let dir = app_support_dir(app);
    for child in ["config", "data", "downloads", "logs", "tokens"] {
        fs::create_dir_all(dir.join(child)).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn service_reachable(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&addr, Duration::from_millis(700)).is_ok()
}

fn command_success(mut cmd: Command) -> bool {
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}

#[tauri::command]
fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tauri::command]
fn compose_exists(app: AppHandle) -> bool {
    compose_path(&app).exists()
}

#[tauri::command]
fn get_launcher_settings(app: AppHandle) -> LauncherSettings {
    load_settings(&app)
}

#[tauri::command]
fn save_launcher_settings(
    app: AppHandle,
    settings: LauncherSettings,
) -> Result<LauncherSettings, String> {
    validate_settings(&settings)?;
    save_settings_to_disk(&app, &settings)?;
    Ok(settings)
}

#[tauri::command]
fn container_running(app: AppHandle) -> bool {
    if !compose_path(&app).exists() {
        return false;
    }

    Command::new("docker")
        .args(["compose", "ps", "-q"])
        .current_dir(app_support_dir(&app))
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false)
}

#[tauri::command]
fn docker_diagnostics(app: AppHandle) -> DockerDiagnostics {
    let settings = load_settings(&app);
    let docker_installed = command_success({
        let mut cmd = Command::new("docker");
        cmd.arg("--version");
        cmd
    });
    let docker_running = command_success({
        let mut cmd = Command::new("docker");
        cmd.arg("info");
        cmd
    });
    let compose_available = command_success({
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "version"]);
        cmd
    });
    let compose_exists = compose_path(&app).exists();
    let container_running = docker_running
        && compose_exists
        && Command::new("docker")
            .args(["compose", "ps", "-q"])
            .current_dir(app_support_dir(&app))
            .output()
            .map(|o| !o.stdout.is_empty())
            .unwrap_or(false);

    let service_reachable = container_running && service_reachable(settings.host_port);

    DockerDiagnostics {
        docker_installed,
        docker_running,
        compose_available,
        compose_exists,
        container_running,
        service_reachable,
        web_url: web_url(&settings),
    }
}

#[tauri::command]
fn install_retreivr(app: AppHandle) -> Result<(), String> {
    let dir = app_support_dir(&app);
    let compose = compose_path(&app);
    let settings = load_settings(&app);

    validate_settings(&settings)?;
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    ensure_runtime_dirs(&app)?;
    fs::write(&compose, render_compose(&settings)).map_err(|e| e.to_string())?;

    let output = Command::new("docker")
        .args(["compose", "up", "-d"])
        .current_dir(&dir)
        .output()
        .map_err(|e| e.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            "docker compose up failed".to_string()
        } else {
            stderr
        });
    }

    Ok(())
}

#[tauri::command]
fn stop_retreivr(app: AppHandle) -> Result<(), String> {
    let output = Command::new("docker")
        .args(["compose", "down"])
        .current_dir(app_support_dir(&app))
        .output()
        .map_err(|e| e.to_string())?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(if stderr.is_empty() {
            "docker compose down failed".to_string()
        } else {
            stderr
        });
    }

    Ok(())
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            docker_available,
            compose_exists,
            get_launcher_settings,
            save_launcher_settings,
            container_running,
            docker_diagnostics,
            install_retreivr,
            stop_retreivr
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
