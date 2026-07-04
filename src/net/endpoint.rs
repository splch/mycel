use crate::Result;
use std::path::Path;

/// Load the node identity from `identity.key`, creating it on first run.
/// File format: 64 lowercase hex chars of the secret key + '\n', mode 0600.
pub fn load_or_create_identity(path: &Path) -> Result<iroh::SecretKey> {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let bytes: [u8; 32] = hex::decode(s.trim())
                .map_err(|e| format!("{}: not valid hex: {e}", path.display()))?
                .try_into()
                .map_err(|_| format!("{}: expected 32 bytes of key material", path.display()))?;
            warn_if_permissive(path);
            Ok(iroh::SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let sk = iroh::SecretKey::generate();
            write_new_0600(path, &format!("{}\n", hex::encode(sk.to_bytes())))?;
            Ok(sk)
        }
        Err(e) => Err(format!("failed to read {}: {e}", path.display()).into()),
    }
}

/// The public endpoint id string operators exchange and paste into peer lists.
pub fn endpoint_id(sk: &iroh::SecretKey) -> String {
    sk.public().to_string()
}

fn write_new_0600(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(contents.as_bytes())?;
    f.sync_all()?;
    Ok(())
}

fn warn_if_permissive(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(path) {
            let mode = md.permissions().mode();
            if mode & 0o077 != 0 {
                tracing::warn!(
                    "permissions {:o} on {} are too open (want 0600)",
                    mode & 0o777,
                    path.display()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_roundtrip_is_stable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.key");
        let a = load_or_create_identity(&path).unwrap();
        let b = load_or_create_identity(&path).unwrap();
        assert_eq!(endpoint_id(&a), endpoint_id(&b));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn corrupt_identity_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.key");
        std::fs::write(&path, "not hex at all\n").unwrap();
        assert!(load_or_create_identity(&path).is_err());
    }
}
