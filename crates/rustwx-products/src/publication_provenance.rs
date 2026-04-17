//! Build and dependency provenance for run manifests.
//!
//! A run manifest should be auditable: "which rustwx tree built this,
//! which sibling dependency trees contributed, and under which toolchain
//! were we running?" This module captures that data at manifest publish
//! time. Everything here is best-effort — if the git metadata or
//! `Cargo.lock` is unavailable, the affected field is `None` rather than
//! a hard error, so operational runs don't fail just because the local
//! tree isn't a git checkout.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::publication::sha256_hex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildProvenance {
    pub rustwx: GitRepoProvenance,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub siblings: Vec<GitRepoProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cargo_lock_sha256: Option<String>,
    pub toolchain: ToolchainProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitRepoProvenance {
    pub name: String,
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolchainProvenance {
    /// `"debug"` or `"release"`, derived from `cfg!(debug_assertions)` at
    /// the call site. Baked into the binary at compile time rather than
    /// pulled from an env var so it is accurate regardless of how the
    /// runner is invoked.
    pub profile: String,
    /// Rough target triple from `std::env::consts::ARCH`/`OS`. Not a
    /// full rustc triple — we avoid shelling to `rustc` here to keep
    /// provenance capture free of shell-out dependencies. Precise
    /// toolchain identity is still captured via the Cargo.lock hash.
    pub target_triple: String,
    /// `rustc --version` output when the binary is invoked inside a
    /// working rustup/cargo install. Left `None` when `rustc` isn't on
    /// PATH so runs don't fail in stripped-down environments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rustc_version: Option<String>,
}

/// Names + paths (relative to the workspace root's parent) of sibling
/// rust repos that rustwx consumes as path dependencies. Baking this
/// curated list keeps provenance deterministic and avoids having to
/// parse `Cargo.lock` at runtime. When the user adds new sibling path
/// deps, adding a line here records their identity too.
const SIBLING_REPO_DIRS: &[(&str, &str)] = &[
    ("cfrust", "cfrust"),
    ("metrust-py", "metrust-py"),
    ("wrf-rust-plots", "wrf-rust-plots"),
    ("sharprs", "sharprs"),
];

/// Capture build provenance rooted at `workspace_root`. The workspace
/// root should be the directory that contains `Cargo.toml` with the
/// `[workspace]` section — typically resolved from
/// `env!("CARGO_MANIFEST_DIR")` by the calling binary via
/// [`workspace_root_from_manifest_dir`].
pub fn capture_build_provenance(workspace_root: &Path) -> BuildProvenance {
    let rustwx = capture_git_repo_provenance("rustwx", workspace_root);
    let siblings_parent = workspace_root.parent().unwrap_or(workspace_root);
    let siblings = SIBLING_REPO_DIRS
        .iter()
        .map(|(name, dir)| {
            let repo_root = siblings_parent.join(dir);
            capture_git_repo_provenance(name, &repo_root)
        })
        .filter(|repo| repo.path.exists())
        .collect();
    let cargo_lock_sha256 = fs::read(workspace_root.join("Cargo.lock"))
        .ok()
        .map(|bytes| sha256_hex(&bytes));
    BuildProvenance {
        rustwx,
        siblings,
        cargo_lock_sha256,
        toolchain: capture_toolchain_provenance(),
    }
}

/// Walk up from a crate-level `CARGO_MANIFEST_DIR` (baked at compile
/// time via `env!`) until a parent that contains a `Cargo.lock` is
/// found; that's the workspace root. Falls back to the given path if
/// nothing is found — in that case provenance will still populate for
/// the rustwx tree, just without a cargo lock hash.
pub fn workspace_root_from_manifest_dir(manifest_dir: &str) -> PathBuf {
    let start = PathBuf::from(manifest_dir);
    for ancestor in start.ancestors() {
        if ancestor.join("Cargo.lock").exists() {
            return ancestor.to_path_buf();
        }
    }
    start
}

/// Capture provenance anchored at this crate's own `CARGO_MANIFEST_DIR`.
///
/// Runners that live in the same workspace can call this directly
/// without threading a path through their call chain — the compile-time
/// `env!` expands inside rustwx-products and walks up to the workspace
/// root, so the captured SHAs are always those of the tree that built
/// this binary.
pub fn capture_default_build_provenance() -> BuildProvenance {
    let workspace_root = workspace_root_from_manifest_dir(env!("CARGO_MANIFEST_DIR"));
    capture_build_provenance(&workspace_root)
}

pub fn capture_git_repo_provenance(name: &str, repo_root: &Path) -> GitRepoProvenance {
    let (git_sha, git_ref) = resolve_git_head(repo_root);
    let dirty = if repo_root.exists() {
        check_dirty(repo_root)
    } else {
        None
    };
    GitRepoProvenance {
        name: name.to_string(),
        path: repo_root.to_path_buf(),
        git_sha,
        git_ref,
        dirty,
    }
}

/// Resolve `repo_root/.git/HEAD` to a `(sha, ref)` pair. Pure file I/O
/// — no shell-out — so this works in constrained environments as long
/// as the `.git` layout is readable.
fn resolve_git_head(repo_root: &Path) -> (Option<String>, Option<String>) {
    let git_dir = repo_root.join(".git");
    if !git_dir.exists() {
        return (None, None);
    }
    // Support `.git` files that point to a gitdir (worktrees, submodules).
    let git_dir = if git_dir.is_file() {
        match fs::read_to_string(&git_dir)
            .ok()
            .and_then(|contents| contents.trim().strip_prefix("gitdir:").map(str::trim).map(PathBuf::from))
        {
            Some(redirect) => {
                if redirect.is_absolute() {
                    redirect
                } else {
                    repo_root.join(redirect)
                }
            }
            None => return (None, None),
        }
    } else {
        git_dir
    };
    let head = match fs::read_to_string(git_dir.join("HEAD")) {
        Ok(text) => text.trim().to_string(),
        Err(_) => return (None, None),
    };
    if let Some(ref_name) = head.strip_prefix("ref: ") {
        let ref_name = ref_name.trim().to_string();
        if let Ok(sha) = fs::read_to_string(git_dir.join(&ref_name)) {
            return (Some(sha.trim().to_string()), Some(ref_name));
        }
        if let Ok(packed) = fs::read_to_string(git_dir.join("packed-refs")) {
            for line in packed.lines() {
                if line.starts_with('#') || line.starts_with('^') || line.trim().is_empty() {
                    continue;
                }
                if let Some((sha, r)) = line.split_once(' ') {
                    if r.trim() == ref_name {
                        return (Some(sha.trim().to_string()), Some(ref_name));
                    }
                }
            }
        }
        (None, Some(ref_name))
    } else if !head.is_empty() {
        (Some(head), None)
    } else {
        (None, None)
    }
}

/// Shell to `git status --porcelain` when the binary is on PATH.
/// Returns `None` if git is not available so provenance never blocks
/// on missing tooling; returns `Some(false)` for a clean tree and
/// `Some(true)` when uncommitted changes exist.
fn check_dirty(repo_root: &Path) -> Option<bool> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(!output.stdout.is_empty())
}

fn capture_toolchain_provenance() -> ToolchainProvenance {
    let profile = if cfg!(debug_assertions) {
        "debug".to_string()
    } else {
        "release".to_string()
    };
    let target_triple = format!(
        "{}-{}",
        std::env::consts::ARCH,
        std::env::consts::OS
    );
    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if text.is_empty() { None } else { Some(text) }
            } else {
                None
            }
        });
    ToolchainProvenance {
        profile,
        target_triple,
        rustc_version,
    }
}

/// Allocate a 16-hex-digit attempt id that mixes the current process id,
/// a monotonic counter, and the current wall-clock ns. Collisions across
/// concurrent invocations of different binaries would require them to
/// pick the same pid, the same counter value, and the same time —
/// extremely unlikely in practice.
pub fn new_attempt_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() as u64;
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_nanos() as u64)
        .unwrap_or(0);
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    // FNV-1a 64-bit mix of (pid, now_ns, counter) → 16 hex digits.
    let mut hash: u64 = 0xcbf29ce484222325;
    for value in [pid, now_ns, counter] {
        for byte in value.to_le_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_git_head_from_loose_ref() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_loose_{}",
            std::process::id()
        ));
        let git = root.join(".git");
        fs::create_dir_all(git.join("refs/heads")).unwrap();
        fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(
            git.join("refs/heads/main"),
            "abcdef0123456789abcdef0123456789abcdef01\n",
        )
        .unwrap();

        let (sha, r) = resolve_git_head(&root);
        assert_eq!(sha.as_deref(), Some("abcdef0123456789abcdef0123456789abcdef01"));
        assert_eq!(r.as_deref(), Some("refs/heads/main"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolves_git_head_from_packed_ref() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_packed_{}",
            std::process::id()
        ));
        let git = root.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(
            git.join("packed-refs"),
            "# pack-refs with: peeled fully-peeled sorted\nfeedfacefeedfacefeedfacefeedfacefeedface refs/heads/main\n",
        )
        .unwrap();

        let (sha, r) = resolve_git_head(&root);
        assert_eq!(sha.as_deref(), Some("feedfacefeedfacefeedfacefeedfacefeedface"));
        assert_eq!(r.as_deref(), Some("refs/heads/main"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resolves_detached_head_sha() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_detached_{}",
            std::process::id()
        ));
        let git = root.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(git.join("HEAD"), "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n").unwrap();

        let (sha, r) = resolve_git_head(&root);
        assert_eq!(
            sha.as_deref(),
            Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
        );
        assert!(r.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_git_dir_yields_none() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_nogit_{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();

        let (sha, r) = resolve_git_head(&root);
        assert!(sha.is_none());
        assert!(r.is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn capture_build_provenance_includes_profile_and_target() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_capture_{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("Cargo.lock"), b"[[package]]\nname = \"demo\"\n").unwrap();

        let provenance = capture_build_provenance(&root);
        // Profile assertion reflects the current test binary's build profile.
        let expected_profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        assert_eq!(provenance.toolchain.profile, expected_profile);
        assert!(!provenance.toolchain.target_triple.is_empty());
        assert!(provenance.cargo_lock_sha256.is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn attempt_ids_are_unique_within_process() {
        let a = new_attempt_id();
        let b = new_attempt_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 16);
        assert_eq!(b.len(), 16);
    }

    #[test]
    fn workspace_root_walks_up_to_cargo_lock() {
        let root = std::env::temp_dir().join(format!(
            "rustwx_prov_ws_{}",
            std::process::id()
        ));
        let deep = root.join("crates/rustwx-cli");
        fs::create_dir_all(&deep).unwrap();
        fs::write(root.join("Cargo.lock"), b"# empty\n").unwrap();

        let discovered = workspace_root_from_manifest_dir(deep.to_str().unwrap());
        assert_eq!(fs::canonicalize(discovered).unwrap(), fs::canonicalize(&root).unwrap());

        let _ = fs::remove_dir_all(root);
    }
}
