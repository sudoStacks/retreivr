use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};

const LAUNCHER_RELEASE_API: &str = "https://api.github.com/repos/sudoStacks/retreivr/releases/latest";
const LAUNCHER_RELEASES_URL: &str = "https://github.com/sudoStacks/retreivr/releases";

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
    compose_path: String,
    runtime_dir: String,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct InstallGuidance {
    os: String,
    install_url: String,
    install_cta: String,
    steps: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PreflightCheck {
    key: String,
    label: String,
    ok: bool,
    details: String,
    fix: String,
}

#[derive(Debug, Serialize)]
struct PreflightReport {
    ok: bool,
    checks: Vec<PreflightCheck>,
}

#[derive(Debug, Serialize)]
struct ChecklistItem {
    key: String,
    label: String,
    done: bool,
    details: String,
}

#[derive(Debug, Serialize)]
struct OnboardingChecklist {
    completed: usize,
    total: usize,
    items: Vec<ChecklistItem>,
}

#[derive(Debug, Serialize)]
struct LauncherVersionInfo {
    current_version: String,
    latest_version: Option<String>,
    update_available: bool,
    release_url: Option<String>,
    check_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ImageUpdateStatus {
    image: String,
    local_image_id: Option<String>,
    remote_image_id: Option<String>,
    update_available: bool,
    check_error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubReleaseResponse {
    tag_name: String,
    html_url: String,
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

fn host_port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn command_success(mut cmd: Command) -> bool {
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}

fn command_output(mut cmd: Command) -> Result<String, String> {
    let output = cmd.output().map_err(|e| e.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Err(if stderr.is_empty() {
            "command failed".to_string()
        } else {
            stderr
        })
    }
}

fn diagnostics_failure_message(
    docker_installed: bool,
    docker_running: bool,
    compose_available: bool,
    compose_exists: bool,
    container_running: bool,
    service_reachable: bool,
) -> Option<String> {
    if !docker_installed {
        return Some("Docker CLI not found on PATH.".to_string());
    }
    if !docker_running {
        return Some("Docker daemon not running. Start Docker Desktop.".to_string());
    }
    if !compose_available {
        return Some("Docker Compose plugin unavailable.".to_string());
    }
    if !compose_exists {
        return Some("Compose file has not been generated yet.".to_string());
    }
    if !container_running {
        return Some("Retreivr container is not running.".to_string());
    }
    if !service_reachable {
        return Some("Retreivr service is not reachable on configured host port.".to_string());
    }
    None
}

fn run_compose_with_output(app: &AppHandle, args: &[&str]) -> Result<String, String> {
    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(args).current_dir(app_support_dir(app));
        cmd
    })
}

fn open_in_file_manager(path: &Path) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = Command::new("open");
        c.arg(path);
        c
    };

    #[cfg(target_os = "windows")]
    let mut cmd = {
        let mut c = Command::new("explorer");
        c.arg(path);
        c
    };

    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    let mut cmd = {
        let mut c = Command::new("xdg-open");
        c.arg(path);
        c
    };

    cmd.status().map_err(|e| e.to_string()).and_then(|s| {
        if s.success() {
            Ok(())
        } else {
            Err("Failed to open folder in file manager.".to_string())
        }
    })
}

fn normalize_release_tag(tag: &str) -> String {
    tag.trim()
        .trim_start_matches("launcher-v")
        .trim_start_matches('v')
        .to_string()
}

fn parse_version_triplet(value: &str) -> Option<(u64, u64, u64)> {
    let clean = normalize_release_tag(value);
    let mut parts = clean.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.parse::<u64>().ok()?;
    Some((major, minor, patch))
}

fn image_id_for(image: &str) -> Option<String> {
    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["image", "inspect", image, "--format", "{{.Id}}"]);
        cmd
    })
    .ok()
    .filter(|value| !value.is_empty())
}

#[tauri::command]
fn install_guidance() -> InstallGuidance {
    let os = std::env::consts::OS.to_string();

    match os.as_str() {
        "macos" => InstallGuidance {
            os,
            install_url: "https://www.docker.com/products/docker-desktop/".to_string(),
            install_cta: "Download Docker Desktop for Mac".to_string(),
            steps: vec![
                "Install Docker Desktop and launch it.".to_string(),
                "Wait until Docker Desktop shows it is running.".to_string(),
                "Return to this launcher and click Recheck Docker.".to_string(),
            ],
        },
        "windows" => InstallGuidance {
            os,
            install_url: "https://www.docker.com/products/docker-desktop/".to_string(),
            install_cta: "Download Docker Desktop for Windows".to_string(),
            steps: vec![
                "Install Docker Desktop and enable required virtualization features.".to_string(),
                "Launch Docker Desktop and wait for engine startup.".to_string(),
                "Return to this launcher and click Recheck Docker.".to_string(),
            ],
        },
        _ => InstallGuidance {
            os,
            install_url: "https://docs.docker.com/engine/install/".to_string(),
            install_cta: "Open Docker Engine Install Docs".to_string(),
            steps: vec![
                "Install Docker Engine using your distribution guide.".to_string(),
                "Start the Docker daemon and verify `docker info` works.".to_string(),
                "Return to this launcher and click Recheck Docker.".to_string(),
            ],
        },
    }
}

#[tauri::command]
fn launcher_version_info(app: AppHandle) -> LauncherVersionInfo {
    let current_version = app.package_info().version.to_string();
    let release = command_output({
        let mut cmd = Command::new("curl");
        cmd.args([
            "-fsSL",
            "-H",
            "User-Agent: retreivr-launcher",
            LAUNCHER_RELEASE_API,
        ]);
        cmd
    })
    .and_then(|json| serde_json::from_str::<GithubReleaseResponse>(&json).map_err(|e| e.to_string()));

    match release {
        Ok(latest) => {
            let latest_clean = normalize_release_tag(&latest.tag_name);
            let update_available = match (
                parse_version_triplet(&current_version),
                parse_version_triplet(&latest_clean),
            ) {
                (Some(current), Some(remote)) => remote > current,
                _ => latest_clean != normalize_release_tag(&current_version),
            };

            LauncherVersionInfo {
                current_version,
                latest_version: Some(latest_clean),
                update_available,
                release_url: Some(latest.html_url),
                check_error: None,
            }
        }
        Err(err) => LauncherVersionInfo {
            current_version,
            latest_version: None,
            update_available: false,
            release_url: Some(LAUNCHER_RELEASES_URL.to_string()),
            check_error: Some(err),
        },
    }
}

#[tauri::command]
fn check_retreivr_image_update(app: AppHandle) -> ImageUpdateStatus {
    let settings = load_settings(&app);
    let image = settings.image;
    let local_image_id = image_id_for(&image);

    let pull_result = command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["pull", &image]);
        cmd
    });

    if let Err(error) = pull_result {
        return ImageUpdateStatus {
            image,
            local_image_id,
            remote_image_id: None,
            update_available: false,
            check_error: Some(error),
        };
    }

    let remote_image_id = image_id_for(&image);
    let update_available = match (&local_image_id, &remote_image_id) {
        (Some(local), Some(remote)) => local != remote,
        (None, Some(_)) => false,
        _ => false,
    };

    ImageUpdateStatus {
        image,
        local_image_id,
        remote_image_id,
        update_available,
        check_error: None,
    }
}

#[tauri::command]
fn update_retreivr_and_restart(app: AppHandle) -> Result<String, String> {
    let settings = load_settings(&app);
    validate_settings(&settings)?;
    fs::create_dir_all(app_support_dir(&app)).map_err(|e| e.to_string())?;
    ensure_runtime_dirs(&app)?;
    fs::write(compose_path(&app), render_compose(&settings)).map_err(|e| e.to_string())?;

    let image = settings.image;
    let before = image_id_for(&image);
    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["pull", &image]);
        cmd
    })?;
    let after = image_id_for(&image);
    let updated = match (&before, &after) {
        (Some(lhs), Some(rhs)) => lhs != rhs,
        _ => false,
    };

    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "up", "-d", "retreivr"])
            .current_dir(app_support_dir(&app));
        cmd
    })?;

    Ok(if updated {
        "Retreivr image updated and container restarted.".to_string()
    } else {
        "Retreivr image already current; container restart applied.".to_string()
    })
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
fn reset_launcher_settings(app: AppHandle) -> Result<LauncherSettings, String> {
    let defaults = LauncherSettings::default();
    fs::create_dir_all(app_support_dir(&app)).map_err(|e| e.to_string())?;
    save_settings_to_disk(&app, &defaults)?;
    fs::write(compose_path(&app), render_compose(&defaults)).map_err(|e| e.to_string())?;
    Ok(defaults)
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
    let runtime_dir = app_support_dir(&app);
    let compose_file = compose_path(&app);
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
    let compose_exists = compose_file.exists();
    let container_running = docker_running
        && compose_exists
        && command_output({
            let mut cmd = Command::new("docker");
            cmd.args(["compose", "ps", "-q"]).current_dir(&runtime_dir);
            cmd
        })
        .map(|stdout| !stdout.is_empty())
        .unwrap_or(false);

    let service_reachable = container_running && service_reachable(settings.host_port);
    let last_error = diagnostics_failure_message(
        docker_installed,
        docker_running,
        compose_available,
        compose_exists,
        container_running,
        service_reachable,
    );

    DockerDiagnostics {
        docker_installed,
        docker_running,
        compose_available,
        compose_exists,
        container_running,
        service_reachable,
        web_url: web_url(&settings),
        compose_path: compose_file.to_string_lossy().to_string(),
        runtime_dir: runtime_dir.to_string_lossy().to_string(),
        last_error,
    }
}

#[tauri::command]
fn preflight_start_checks(app: AppHandle) -> PreflightReport {
    let mut checks: Vec<PreflightCheck> = Vec::new();
    let settings = load_settings(&app);

    let settings_ok = validate_settings(&settings).is_ok();
    checks.push(PreflightCheck {
        key: "settings_valid".to_string(),
        label: "Configuration validity".to_string(),
        ok: settings_ok,
        details: if settings_ok {
            "Configuration values are valid.".to_string()
        } else {
            "One or more configuration values are invalid.".to_string()
        },
        fix: "Update host port, image, and container name, then save configuration.".to_string(),
    });

    if settings_ok {
        if let Err(e) = fs::create_dir_all(app_support_dir(&app)) {
            checks.push(PreflightCheck {
                key: "runtime_dir_writable".to_string(),
                label: "Runtime directory".to_string(),
                ok: false,
                details: e.to_string(),
                fix: "Ensure your user can write to the launcher app-data directory.".to_string(),
            });
        } else if let Err(e) = fs::write(compose_path(&app), render_compose(&settings)) {
            checks.push(PreflightCheck {
                key: "compose_render".to_string(),
                label: "Compose generation".to_string(),
                ok: false,
                details: e.to_string(),
                fix: "Verify app-data permissions and retry.".to_string(),
            });
        } else {
            checks.push(PreflightCheck {
                key: "compose_render".to_string(),
                label: "Compose generation".to_string(),
                ok: true,
                details: "Compose file generated from current settings.".to_string(),
                fix: "No action needed.".to_string(),
            });
        }
    }

    let docker_installed = command_success({
        let mut cmd = Command::new("docker");
        cmd.arg("--version");
        cmd
    });
    checks.push(PreflightCheck {
        key: "docker_installed".to_string(),
        label: "Docker CLI available".to_string(),
        ok: docker_installed,
        details: if docker_installed {
            "Docker CLI detected.".to_string()
        } else {
            "Docker CLI not found on PATH.".to_string()
        },
        fix: "Install Docker Desktop and relaunch the launcher.".to_string(),
    });

    let docker_running = command_success({
        let mut cmd = Command::new("docker");
        cmd.arg("info");
        cmd
    });
    checks.push(PreflightCheck {
        key: "docker_running".to_string(),
        label: "Docker engine running".to_string(),
        ok: docker_running,
        details: if docker_running {
            "Docker daemon is available.".to_string()
        } else {
            "Docker daemon unavailable.".to_string()
        },
        fix: "Start Docker Desktop and wait for engine startup.".to_string(),
    });

    let docker_permissions = run_compose_with_output(&app, &["compose", "version"]).is_ok();
    checks.push(PreflightCheck {
        key: "docker_permissions".to_string(),
        label: "Docker compose access".to_string(),
        ok: docker_permissions,
        details: if docker_permissions {
            "Docker compose command is executable.".to_string()
        } else {
            "Cannot execute docker compose.".to_string()
        },
        fix: "Check Docker permissions and ensure compose plugin is installed.".to_string(),
    });

    let port_free = host_port_available(settings.host_port);
    checks.push(PreflightCheck {
        key: "host_port_available".to_string(),
        label: format!("Host port {} availability", settings.host_port),
        ok: port_free,
        details: if port_free {
            "Configured host port is available.".to_string()
        } else {
            "Configured host port is in use.".to_string()
        },
        fix: "Choose another host port in configuration and save.".to_string(),
    });

    let compose_valid = run_compose_with_output(&app, &["compose", "config"]).is_ok();
    checks.push(PreflightCheck {
        key: "compose_valid".to_string(),
        label: "Compose file validation".to_string(),
        ok: compose_valid,
        details: if compose_valid {
            "Compose file validation passed.".to_string()
        } else {
            "Compose validation failed.".to_string()
        },
        fix: "Review configuration values and retry.".to_string(),
    });

    let ok = checks.iter().all(|check| check.ok);
    PreflightReport { ok, checks }
}

#[tauri::command]
fn onboarding_checklist(app: AppHandle) -> OnboardingChecklist {
    let diagnostics = docker_diagnostics(app.clone());
    let settings_saved = settings_path(&app).exists();

    let items = vec![
        ChecklistItem {
            key: "docker_ready".to_string(),
            label: "Docker ready".to_string(),
            done: diagnostics.docker_running && diagnostics.compose_available,
            details: if diagnostics.docker_running && diagnostics.compose_available {
                "Docker engine and compose are available.".to_string()
            } else {
                "Start Docker Desktop and verify compose availability.".to_string()
            },
        },
        ChecklistItem {
            key: "config_saved".to_string(),
            label: "Configuration saved".to_string(),
            done: settings_saved,
            details: if settings_saved {
                "Launcher configuration file found.".to_string()
            } else {
                "Save configuration in Step 2.".to_string()
            },
        },
        ChecklistItem {
            key: "container_healthy".to_string(),
            label: "Container healthy".to_string(),
            done: diagnostics.container_running,
            details: if diagnostics.container_running {
                "Retreivr container is running.".to_string()
            } else {
                "Start Retreivr in Step 3.".to_string()
            },
        },
        ChecklistItem {
            key: "ui_reachable".to_string(),
            label: "Web UI reachable".to_string(),
            done: diagnostics.service_reachable,
            details: if diagnostics.service_reachable {
                format!("UI responds at {}.", diagnostics.web_url)
            } else {
                format!("Web UI not reachable yet at {}.", diagnostics.web_url)
            },
        },
    ];

    let completed = items.iter().filter(|item| item.done).count();
    OnboardingChecklist {
        completed,
        total: items.len(),
        items,
    }
}

#[tauri::command]
fn open_compose_folder(app: AppHandle) -> Result<(), String> {
    let runtime = app_support_dir(&app);
    fs::create_dir_all(&runtime).map_err(|e| e.to_string())?;
    open_in_file_manager(&runtime)
}

#[tauri::command]
fn open_data_folder(app: AppHandle) -> Result<(), String> {
    let data_dir = app_support_dir(&app).join("data");
    fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
    open_in_file_manager(&data_dir)
}

#[tauri::command]
fn view_retreivr_logs(app: AppHandle, lines: Option<u32>) -> Result<String, String> {
    let tail = lines.unwrap_or(200).clamp(20, 2000).to_string();
    run_compose_with_output(&app, &["compose", "logs", "--tail", &tail, "retreivr"])
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
            install_guidance,
            launcher_version_info,
            check_retreivr_image_update,
            update_retreivr_and_restart,
            get_launcher_settings,
            save_launcher_settings,
            reset_launcher_settings,
            onboarding_checklist,
            open_compose_folder,
            open_data_folder,
            preflight_start_checks,
            view_retreivr_logs,
            container_running,
            docker_diagnostics,
            install_retreivr,
            stop_retreivr
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
