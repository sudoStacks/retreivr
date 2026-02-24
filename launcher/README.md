# Retreivr Launcher

Tauri desktop launcher for local Retreivr Docker setup and runtime control.

## Update Awareness

- Launcher version awareness checks latest GitHub release metadata.
- Launcher updates are currently download-driven (open release page).
- Retreivr image updates support detect + one-click update/restart in the launcher UI.

## Local Development

Requirements:
- Node.js 20+
- Rust stable toolchain
- Tauri prerequisites for your OS

Run:

```bash
cd launcher
npm ci
npm run tauri dev
```

Build:

```bash
cd launcher
npm run tauri build
```

## Release and Signing

GitHub Actions workflow:
- `.github/workflows/launcher-release.yml`

Trigger:
- Push a tag matching `launcher-v*` (example: `launcher-v0.9.6`)
- Or run manually via `workflow_dispatch`

### Required GitHub Secrets

macOS signing + notarization:
- `APPLE_CERTIFICATE` (base64-encoded `.p12`)
- `APPLE_CERTIFICATE_PASSWORD`
- `APPLE_SIGNING_IDENTITY`
- `APPLE_ID`
- `APPLE_PASSWORD` (app-specific password)
- `APPLE_TEAM_ID`

Windows code signing:
- `WINDOWS_CERTIFICATE` (base64-encoded `.pfx`)
- `WINDOWS_CERTIFICATE_PASSWORD`

### CI Outputs

Per platform artifact bundles are uploaded from:
- `launcher/src-tauri/target/release/bundle`

Checksums:
- `SHA256SUMS-macos.txt`
- `SHA256SUMS-windows.txt`
- Combined release manifest: `SHA256SUMS.txt`
