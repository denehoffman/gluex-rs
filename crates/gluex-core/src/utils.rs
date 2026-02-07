use std::{
    io,
    path::{Path, PathBuf},
};

/// Resolve a filesystem path by expanding `~` and canonicalizing the result.
///
/// Canonicalization resolves `.` and `..` path segments and returns an absolute
/// path.
///
/// # Errors
/// Returns an error if the expanded path cannot be canonicalized.
pub fn resolve_path(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let raw = path.as_ref().to_string_lossy();
    let expanded = shellexpand::tilde(&raw);
    Path::new(expanded.as_ref()).canonicalize()
}
