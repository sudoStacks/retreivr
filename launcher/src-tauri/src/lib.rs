use std::env;
use std::ffi::OsStr;
use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Manager};

const LAUNCHER_TAGS_API: &str = "https://api.github.com/repos/sudostacks/retreivr/tags?per_page=100";
const LAUNCHER_RELEASES_URL: &str = "https://github.com/sudostacks/retreivr/releases";
const DEFAULT_CONFIG_JSON: &str = include_str!("../../../config/config_sample.json");

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
    #[serde(default = "default_host_port")]
    host_port: u16,
    #[serde(default = "default_image")]
    image: String,
    #[serde(default = "default_container_name")]
    container_name: String,
    #[serde(default = "default_config_dir")]
    config_dir: String,
    #[serde(default = "default_data_dir")]
    data_dir: String,
    #[serde(default = "default_downloads_dir")]
    downloads_dir: String,
    #[serde(default = "default_logs_dir")]
    logs_dir: String,
    #[serde(default = "default_tokens_dir")]
    tokens_dir: String,
}

fn default_host_port() -> u16 {
    8090
}

fn default_image() -> String {
    "ghcr.io/sudostacks/retreivr:latest".to_string()
}

fn default_container_name() -> String {
    "retreivr".to_string()
}

fn default_config_dir() -> String {
    "./config".to_string()
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_downloads_dir() -> String {
    "./downloads".to_string()
}

fn default_logs_dir() -> String {
    "./logs".to_string()
}

fn default_tokens_dir() -> String {
    "./tokens".to_string()
}

impl Default for LauncherSettings {
    fn default() -> Self {
        Self {
            host_port: default_host_port(),
            image: default_image(),
            container_name: default_container_name(),
            config_dir: default_config_dir(),
            data_dir: default_data_dir(),
            downloads_dir: default_downloads_dir(),
            logs_dir: default_logs_dir(),
            tokens_dir: default_tokens_dir(),
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
struct GithubTagResponse {
    name: String,
}

#[derive(Debug)]
struct LauncherTagInfo {
    tag_name: String,
    html_url: String,
}

fn web_url(settings: &LauncherSettings) -> String {
    format!("http://localhost:{}", settings.host_port)
}

fn canonicalize_image_ref(image: &str) -> String {
    image.trim().to_ascii_lowercase()
}

fn canonicalize_mount_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    if Path::new(trimmed).is_absolute() {
        return trimmed.to_string();
    }

    if let Some(rest) = trimmed.strip_prefix("./") {
        return format!("./{}", rest.trim_start_matches('/'));
    }

    format!("./{}", trimmed.trim_start_matches('/'))
}

fn normalize_settings(settings: &LauncherSettings) -> LauncherSettings {
    let mut out = settings.clone();
    out.image = canonicalize_image_ref(&out.image);
    out.config_dir = canonicalize_mount_path(&out.config_dir);
    out.data_dir = canonicalize_mount_path(&out.data_dir);
    out.downloads_dir = canonicalize_mount_path(&out.downloads_dir);
    out.logs_dir = canonicalize_mount_path(&out.logs_dir);
    out.tokens_dir = canonicalize_mount_path(&out.tokens_dir);
    out
}

fn load_settings(app: &AppHandle) -> LauncherSettings {
    let path = settings_path(app);
    let content = match fs::read_to_string(path) {
        Ok(value) => value,
        Err(_) => return LauncherSettings::default(),
    };

    let parsed = serde_json::from_str(&content).unwrap_or_else(|_| LauncherSettings::default());
    normalize_settings(&parsed)
}

fn save_settings_to_disk(app: &AppHandle, settings: &LauncherSettings) -> Result<(), String> {
    let dir = app_support_dir(app);
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let normalized = normalize_settings(settings);
    let payload = serde_json::to_string_pretty(&normalized).map_err(|e| e.to_string())?;
    fs::write(settings_path(app), payload).map_err(|e| e.to_string())
}

fn validate_settings(settings: &LauncherSettings) -> Result<(), String> {
    if settings.host_port == 0 {
        return Err("host_port must be between 1 and 65535".to_string());
    }

    if settings.image.trim().is_empty() {
        return Err("image cannot be empty".to_string());
    }

    if settings.image.chars().any(|c| c.is_ascii_uppercase()) {
        return Err("image must be lowercase (Docker image refs are case-sensitive)".to_string());
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

    for (name, value) in [
        ("config_dir", settings.config_dir.as_str()),
        ("data_dir", settings.data_dir.as_str()),
        ("downloads_dir", settings.downloads_dir.as_str()),
        ("logs_dir", settings.logs_dir.as_str()),
        ("tokens_dir", settings.tokens_dir.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{name} cannot be empty"));
        }
    }

    Ok(())
}

fn resolve_mount_source(app: &AppHandle, configured: &str) -> PathBuf {
    let path = Path::new(configured);
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let cleaned = configured.trim_start_matches("./");
    app_support_dir(app).join(cleaned)
}

fn yaml_quote_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

fn render_compose(app: &AppHandle, settings: &LauncherSettings) -> String {
    let config_source = resolve_mount_source(app, &settings.config_dir);
    let data_source = resolve_mount_source(app, &settings.data_dir);
    let downloads_source = resolve_mount_source(app, &settings.downloads_dir);
    let logs_source = resolve_mount_source(app, &settings.logs_dir);
    let tokens_source = resolve_mount_source(app, &settings.tokens_dir);

    format!(
        r#"services:
  retreivr:
    image: {image}
    container_name: {container_name}
    restart: unless-stopped
    ports:
      - "{host_port}:8000"
    volumes:
      - type: bind
        source: "{config_source}"
        target: "/config"
      - type: bind
        source: "{data_source}"
        target: "/data"
      - type: bind
        source: "{downloads_source}"
        target: "/downloads"
      - type: bind
        source: "{logs_source}"
        target: "/logs"
      - type: bind
        source: "{tokens_source}"
        target: "/tokens"
"#,
        image = settings.image,
        container_name = settings.container_name,
        host_port = settings.host_port,
        config_source = yaml_quote_path(&config_source),
        data_source = yaml_quote_path(&data_source),
        downloads_source = yaml_quote_path(&downloads_source),
        logs_source = yaml_quote_path(&logs_source),
        tokens_source = yaml_quote_path(&tokens_source)
    )
}

fn ensure_runtime_dirs(app: &AppHandle, settings: &LauncherSettings) -> Result<(), String> {
    let config_dir = resolve_mount_source(app, &settings.config_dir);
    let data_dir = resolve_mount_source(app, &settings.data_dir);
    let downloads_dir = resolve_mount_source(app, &settings.downloads_dir);
    let logs_dir = resolve_mount_source(app, &settings.logs_dir);
    let tokens_dir = resolve_mount_source(app, &settings.tokens_dir);

    for dir in [&config_dir, &data_dir, &downloads_dir, &logs_dir, &tokens_dir] {
        fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }

    let config_json_path = config_dir.join("config.json");
    if !config_json_path.exists() {
        fs::write(&config_json_path, DEFAULT_CONFIG_JSON).map_err(|e| e.to_string())?;
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

fn with_runtime_path(cmd: &mut Command) {
    let program = cmd.get_program();
    let is_docker = program == OsStr::new("docker")
        || Path::new(program)
            .file_name()
            .is_some_and(|name| name == OsStr::new("docker"));

    if !is_docker {
        return;
    }

    let mut path_entries: Vec<String> = env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .collect();

    for candidate in [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/Applications/Docker.app/Contents/Resources/bin",
    ] {
        if Path::new(candidate).exists() && !path_entries.iter().any(|p| p == candidate) {
            path_entries.push(candidate.to_string());
        }
    }

    cmd.env("PATH", path_entries.join(":"));
}

fn command_success(mut cmd: Command) -> bool {
    with_runtime_path(&mut cmd);
    cmd.output().map(|o| o.status.success()).unwrap_or(false)
}

fn command_output(mut cmd: Command) -> Result<String, String> {
    with_runtime_path(&mut cmd);
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

fn pick_folder_via_system() -> Result<Option<String>, String> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("osascript")
            .args([
                "-e",
                "try",
                "-e",
                "POSIX path of (choose folder with prompt \"Select folder\")",
                "-e",
                "on error number -128",
                "-e",
                "return \"\"",
                "-e",
                "end try",
            ])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
        }
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Ok(if value.is_empty() { None } else { Some(value) });
    }

    #[cfg(target_os = "windows")]
    {
        let script = "[void][Reflection.Assembly]::LoadWithPartialName('System.Windows.Forms'); $dialog = New-Object System.Windows.Forms.FolderBrowserDialog; if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) { $dialog.SelectedPath }";
        let output = Command::new("powershell")
            .args(["-NoProfile", "-Command", script])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
        }
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Ok(if value.is_empty() { None } else { Some(value) });
    }

    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        let output = Command::new("zenity")
            .args(["--file-selection", "--directory"])
            .output()
            .map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Ok(None);
        }
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Ok(if value.is_empty() { None } else { Some(value) });
    }
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

fn parse_tags_from_json(payload: &str) -> Result<Vec<GithubTagResponse>, String> {
    serde_json::from_str::<Vec<GithubTagResponse>>(payload).map_err(|e| e.to_string())
}

fn pick_latest_launcher_tag(tags: &[GithubTagResponse]) -> Option<String> {
    let mut parsed: Vec<((u64, u64, u64), String)> = tags
        .iter()
        .filter(|tag| tag.name.starts_with("launcher-v"))
        .filter_map(|tag| parse_version_triplet(&tag.name).map(|sem| (sem, tag.name.clone())))
        .collect();

    if parsed.is_empty() {
        return None;
    }

    parsed.sort_by(|a, b| a.0.cmp(&b.0));
    parsed.last().map(|value| value.1.clone())
}

fn fetch_latest_launcher_release() -> Result<Option<LauncherTagInfo>, String> {
    let curl_result = command_output({
        let mut cmd = Command::new("curl");
        cmd.args([
            "-fsSL",
            "-H",
            "User-Agent: retreivr-launcher",
            LAUNCHER_TAGS_API,
        ]);
        cmd
    })
    .and_then(|json| parse_tags_from_json(&json));

    if curl_result.is_ok() {
        let tags = curl_result?;
        let latest = pick_latest_launcher_tag(&tags).map(|tag_name| LauncherTagInfo {
            html_url: format!("https://github.com/sudostacks/retreivr/releases/tag/{tag_name}"),
            tag_name,
        });
        return Ok(latest);
    }

    #[cfg(target_os = "windows")]
    {
        let pwsh_result = command_output({
            let mut cmd = Command::new("powershell");
            cmd.args([
                "-NoProfile",
                "-Command",
                "$ProgressPreference='SilentlyContinue'; (Invoke-RestMethod -Uri 'https://api.github.com/repos/sudostacks/retreivr/tags?per_page=100' -Headers @{ 'User-Agent'='retreivr-launcher' } | ConvertTo-Json -Compress)",
            ]);
            cmd
        })
        .and_then(|json| parse_tags_from_json(&json));

        if pwsh_result.is_ok() {
            let tags = pwsh_result?;
            let latest = pick_latest_launcher_tag(&tags).map(|tag_name| LauncherTagInfo {
                html_url: format!("https://github.com/sudostacks/retreivr/releases/tag/{tag_name}"),
                tag_name,
            });
            return Ok(latest);
        }
    }

    let wget_result = command_output({
        let mut cmd = Command::new("wget");
        cmd.args([
            "-qO-",
            "--header=User-Agent: retreivr-launcher",
            LAUNCHER_TAGS_API,
        ]);
        cmd
    })
    .and_then(|json| parse_tags_from_json(&json));

    if wget_result.is_ok() {
        let tags = wget_result?;
        let latest = pick_latest_launcher_tag(&tags).map(|tag_name| LauncherTagInfo {
            html_url: format!("https://github.com/sudostacks/retreivr/releases/tag/{tag_name}"),
            tag_name,
        });
        return Ok(latest);
    }

    Err(
        "Unable to check launcher tag metadata (curl/powershell/wget not available or request failed)."
            .to_string(),
    )
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
    let release = fetch_latest_launcher_release();

    match release {
        Ok(Some(latest)) => {
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
        Ok(None) => LauncherVersionInfo {
            current_version,
            latest_version: None,
            update_available: false,
            release_url: Some(LAUNCHER_RELEASES_URL.to_string()),
            check_error: None,
        },
        Err(err) => LauncherVersionInfo {
            current_version,
            latest_version: None,
            update_available: false,
            release_url: Some(LAUNCHER_RELEASES_URL.to_string()),
            check_error: Some(err),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_release_tag, parse_version_triplet};

    #[test]
    fn normalize_release_tag_handles_prefixes() {
        assert_eq!(normalize_release_tag("launcher-v1.2.3"), "1.2.3");
        assert_eq!(normalize_release_tag("v1.2.3"), "1.2.3");
        assert_eq!(normalize_release_tag("1.2.3"), "1.2.3");
    }

    #[test]
    fn parse_version_triplet_parses_semver_core() {
        assert_eq!(parse_version_triplet("launcher-v2.10.4"), Some((2, 10, 4)));
        assert_eq!(parse_version_triplet("v0.9.6"), Some((0, 9, 6)));
        assert_eq!(parse_version_triplet("bad"), None);
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
    ensure_runtime_dirs(&app, &settings)?;
    fs::write(compose_path(&app), render_compose(&app, &settings)).map_err(|e| e.to_string())?;

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
    command_success({
        let mut cmd = Command::new("docker");
        cmd.arg("info");
        cmd
    })
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
    let normalized = normalize_settings(&settings);
    validate_settings(&normalized)?;
    save_settings_to_disk(&app, &normalized)?;
    ensure_runtime_dirs(&app, &normalized)?;
    fs::write(compose_path(&app), render_compose(&app, &normalized)).map_err(|e| e.to_string())?;
    Ok(normalized)
}

#[tauri::command]
fn reset_launcher_settings(app: AppHandle) -> Result<LauncherSettings, String> {
    let defaults = LauncherSettings::default();
    fs::create_dir_all(app_support_dir(&app)).map_err(|e| e.to_string())?;
    save_settings_to_disk(&app, &defaults)?;
    fs::write(compose_path(&app), render_compose(&app, &defaults)).map_err(|e| e.to_string())?;
    ensure_runtime_dirs(&app, &defaults)?;
    Ok(defaults)
}

#[tauri::command]
fn container_running(app: AppHandle) -> bool {
    if !compose_path(&app).exists() {
        return false;
    }

    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "ps", "-q"])
            .current_dir(app_support_dir(&app));
        cmd
    })
    .map(|stdout| !stdout.is_empty())
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
        } else if let Err(e) = fs::write(compose_path(&app), render_compose(&app, &settings)) {
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
    let settings = load_settings(&app);
    let data_dir = resolve_mount_source(&app, &settings.data_dir);
    fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
    open_in_file_manager(&data_dir)
}

#[tauri::command]
fn browse_for_directory() -> Result<Option<String>, String> {
    pick_folder_via_system()
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
    ensure_runtime_dirs(&app, &settings)?;
    fs::write(&compose, render_compose(&app, &settings)).map_err(|e| e.to_string())?;

    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "up", "-d"]).current_dir(&dir);
        cmd
    })?;

    Ok(())
}

#[tauri::command]
fn stop_retreivr(app: AppHandle) -> Result<(), String> {
    command_output({
        let mut cmd = Command::new("docker");
        cmd.args(["compose", "down"])
            .current_dir(app_support_dir(&app));
        cmd
    })?;

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
            browse_for_directory,
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
