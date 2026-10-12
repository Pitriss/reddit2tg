use std::io::{Read, Write};

use anyhow::{anyhow, bail, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use ring::{
    aead, digest,
    rand::{SecureRandom, SystemRandom},
};
use serde::{de::DeserializeOwned, Serialize};

const PAYLOAD_MAGIC: &[u8; 8] = b"R2TGREP1";
const NONCE_LEN: usize = 12;

pub fn derive_key(secret: &str, cluster_id: &str) -> [u8; 32] {
    let mut material = Vec::with_capacity(secret.len() + cluster_id.len() + 64);
    material.extend_from_slice(b"reddit2tg-ha-replication-key-v1\0");
    material.extend_from_slice(cluster_id.as_bytes());
    material.push(0);
    material.extend_from_slice(secret.as_bytes());

    let hash = digest::digest(&digest::SHA256, &material);
    let mut key = [0u8; 32];
    key.copy_from_slice(hash.as_ref());
    key
}

pub fn random_epoch() -> Result<String> {
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow!("failed to generate HA replication epoch"))?;
    Ok(hex(&bytes))
}

pub fn seal_json<T: Serialize>(
    value: &T,
    key_bytes: &[u8; 32],
    aad: &str,
) -> Result<(Vec<u8>, String)> {
    let plaintext = serde_json::to_vec(value).context("serialize HA replication payload")?;
    let plaintext_sha256 = sha256_hex(&plaintext);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&plaintext)
        .context("compress HA replication payload")?;
    let mut ciphertext = encoder
        .finish()
        .context("finish HA replication compression")?;

    let mut nonce_bytes = [0u8; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow!("failed to generate HA replication nonce"))?;

    let unbound = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, key_bytes)
        .map_err(|_| anyhow!("invalid HA replication encryption key"))?;
    let key = aead::LessSafeKey::new(unbound);
    key.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce_bytes),
        aead::Aad::from(aad.as_bytes()),
        &mut ciphertext,
    )
    .map_err(|_| anyhow!("encrypt HA replication payload"))?;

    let mut output = Vec::with_capacity(PAYLOAD_MAGIC.len() + NONCE_LEN + ciphertext.len());
    output.extend_from_slice(PAYLOAD_MAGIC);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&ciphertext);
    Ok((output, plaintext_sha256))
}

pub fn open_json<T: DeserializeOwned>(
    input: &[u8],
    key_bytes: &[u8; 32],
    aad: &str,
) -> Result<(T, String)> {
    if input.len() < PAYLOAD_MAGIC.len() + NONCE_LEN + aead::CHACHA20_POLY1305.tag_len() {
        bail!("HA replication payload is too short");
    }
    if &input[..PAYLOAD_MAGIC.len()] != PAYLOAD_MAGIC {
        bail!("HA replication payload has an unknown format");
    }

    let nonce_start = PAYLOAD_MAGIC.len();
    let nonce_end = nonce_start + NONCE_LEN;
    let nonce_bytes: [u8; NONCE_LEN] = input[nonce_start..nonce_end]
        .try_into()
        .context("invalid HA replication nonce")?;
    let mut ciphertext = input[nonce_end..].to_vec();

    let unbound = aead::UnboundKey::new(&aead::CHACHA20_POLY1305, key_bytes)
        .map_err(|_| anyhow!("invalid HA replication decryption key"))?;
    let key = aead::LessSafeKey::new(unbound);
    let compressed = key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce_bytes),
            aead::Aad::from(aad.as_bytes()),
            &mut ciphertext,
        )
        .map_err(|_| anyhow!("HA replication payload authentication failed"))?;

    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut plaintext = Vec::new();
    decoder
        .read_to_end(&mut plaintext)
        .context("decompress HA replication payload")?;
    let plaintext_sha256 = sha256_hex(&plaintext);
    let value = serde_json::from_slice(&plaintext).context("parse HA replication payload")?;
    Ok((value, plaintext_sha256))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, bytes).as_ref())
}

pub fn split_chunks(bytes: &[u8], chunk_size: usize) -> Result<Vec<Vec<u8>>> {
    if chunk_size == 0 {
        bail!("HA replication chunk size must be non-zero");
    }
    if bytes.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    Ok(bytes
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Sample {
        value: String,
        number: u64,
    }

    #[test]
    fn encrypted_payload_round_trips() {
        let key = derive_key("123:test-secret", "prod");
        let input = Sample {
            value: "hello HA".to_owned(),
            number: 42,
        };
        let (sealed, input_sha) = seal_json(&input, &key, "snapshot:prod").unwrap();
        assert!(!String::from_utf8_lossy(&sealed).contains("hello HA"));

        let (decoded, decoded_sha): (Sample, String) =
            open_json(&sealed, &key, "snapshot:prod").unwrap();
        assert_eq!(decoded, input);
        assert_eq!(decoded_sha, input_sha);
    }

    #[test]
    fn encrypted_payload_rejects_wrong_cluster_aad() {
        let key = derive_key("123:test-secret", "prod");
        let (sealed, _) = seal_json(&vec!["secret"], &key, "delta:prod").unwrap();
        let error = open_json::<Vec<String>>(&sealed, &key, "delta:other").unwrap_err();
        assert!(format!("{error:#}").contains("authentication failed"));
    }

    #[test]
    fn chunking_is_lossless() {
        let input = (0u8..=250).cycle().take(4097).collect::<Vec<_>>();
        let parts = split_chunks(&input, 1024).unwrap();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts.concat(), input);
    }
}
