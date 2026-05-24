use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

const APP_NAME: &str = "RustWx Model Maps";

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(err) => {
            eprintln!("{APP_NAME} could not start:");
            eprintln!("{err}");
            let _ = pause_when_interactive();
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<u8, String> {
    let exe = env::current_exe().map_err(|err| format!("Unable to locate launcher: {err}"))?;
    let app_root = exe
        .parent()
        .ok_or_else(|| "Unable to locate launcher directory.".to_string())?;
    let python = app_root.join("app").join("python").join("python.exe");
    if !python.is_file() {
        return Err(format!(
            "Embedded Python was not found at {}",
            python.display()
        ));
    }

    let bin_dir = app_root.join("app").join("bin");
    let assets_dir = app_root.join("app").join("assets");
    let data_home = data_home(app_root);
    let out_root = data_home.join("outputs");
    let cache_dir = data_home.join("cache");
    let log_dir = data_home.join("logs");
    fs::create_dir_all(&out_root)
        .map_err(|err| format!("Unable to create {}: {err}", out_root.display()))?;
    fs::create_dir_all(&cache_dir)
        .map_err(|err| format!("Unable to create {}: {err}", cache_dir.display()))?;
    fs::create_dir_all(&log_dir)
        .map_err(|err| format!("Unable to create {}: {err}", log_dir.display()))?;
    let stdout_log = log_dir.join("launcher.out.log");
    let stderr_log = log_dir.join("launcher.err.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_log)
        .map_err(|err| format!("Unable to open {}: {err}", stdout_log.display()))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_log)
        .map_err(|err| format!("Unable to open {}: {err}", stderr_log.display()))?;

    let mut command = Command::new(&python);
    command
        .current_dir(app_root)
        .env("RUSTWX_MODEL_MAPS_HOME", &data_home)
        .env("RUSTWX_ASSETS_DIR", &assets_dir)
        .env("RUSTWX_PACKAGED", "1")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .arg("-m")
        .arg("rustwx.model_maps")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg("0")
        .arg("--out-root")
        .arg(&out_root)
        .arg("--cache-dir")
        .arg(&cache_dir)
        .arg("--bin-dir")
        .arg(&bin_dir);

    for arg in env::args_os().skip(1) {
        command.arg(arg);
    }

    let status = command
        .status()
        .map_err(|err| format!("Unable to launch {}: {err}", python.display()))?;
    if !status.success() {
        eprintln!(
            "{APP_NAME} exited with status {}. Logs:\n{}\n{}",
            status,
            stdout_log.display(),
            stderr_log.display()
        );
        let _ = pause_when_interactive();
    }
    Ok(status.code().unwrap_or(1).clamp(0, 255) as u8)
}

fn data_home(app_root: &Path) -> PathBuf {
    env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| app_root.to_path_buf())
        .join("RustWx")
        .join("ModelMaps")
}

fn pause_when_interactive() -> io::Result<()> {
    if env::var_os("RUSTWX_NO_LAUNCHER_PAUSE").is_some() {
        return Ok(());
    }
    eprint!("Press Enter to close.");
    io::stderr().flush()?;
    let mut line = String::new();
    let _ = io::stdin().read_line(&mut line)?;
    Ok(())
}
