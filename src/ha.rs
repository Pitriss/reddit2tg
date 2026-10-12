use std::collections::{BTreeMap, HashSet};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::{HaConfig, HaInstanceConfig};

pub const HA_PROTOCOL_VERSION: u32 = 1;
pub const HA_CONTROL_PREFIX: &str = "reddit2tg-ha-v1\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InstanceMode {
    #[default]
    Auto,
    Disconnected,
    Maintenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ControlPhase {
    #[default]
    Paused,
    Lease,
    Claim,
    Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Connect,
    Disconnect,
    Switch,
    MaintenanceOn,
    MaintenanceOff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaRequest {
    pub request_id: String,
    pub kind: RequestKind,
    pub target: String,
    #[serde(default)]
    pub force: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaDocument {
    pub protocol: u32,
    pub cluster_id: String,
    pub members_fingerprint: String,
    pub generation: u64,
    #[serde(default)]
    pub phase: ControlPhase,
    pub owner: Option<String>,
    pub owner_priority: Option<u32>,
    #[serde(default)]
    pub lease_expires_at_ms: i64,
    #[serde(default)]
    pub failover_allowed: bool,
    pub handover_to: Option<String>,
    #[serde(default)]
    pub modes: BTreeMap<String, InstanceMode>,
    pub request: Option<HaRequest>,
}

impl HaDocument {
    pub fn new(cfg: &HaConfig) -> Self {
        let modes = cfg
            .instances
            .iter()
            .map(|member| (member.id.clone(), InstanceMode::Auto))
            .collect();
        Self {
            protocol: HA_PROTOCOL_VERSION,
            cluster_id: cfg.cluster_id.clone(),
            members_fingerprint: members_fingerprint(cfg),
            generation: 0,
            phase: ControlPhase::Paused,
            owner: None,
            owner_priority: None,
            lease_expires_at_ms: 0,
            failover_allowed: false,
            handover_to: None,
            modes,
            request: None,
        }
    }

    pub fn mode(&self, instance_id: &str) -> InstanceMode {
        self.modes
            .get(instance_id)
            .copied()
            .unwrap_or(InstanceMode::Auto)
    }

    pub fn lease_is_live(&self, now_ms: i64) -> bool {
        self.owner.is_some() && self.lease_expires_at_ms > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaCommand {
    ListInstances,
    RedditStatus,
    Connect { instance_id: String, force: bool },
    Disconnect { instance_id: String },
    Switch { instance_id: String },
    MaintenanceShow { instance_id: String },
    MaintenanceSet { instance_id: String, enabled: bool },
}

pub fn validate_config(cfg: &HaConfig) -> Result<()> {
    if !cfg.enabled {
        return Ok(());
    }

    validate_identifier("ha.cluster_id", &cfg.cluster_id)?;
    validate_identifier("ha.instance_id", &cfg.instance_id)?;

    if cfg.control_bot_token.trim().is_empty() {
        bail!("ha.control_bot_token must not be empty when HA is enabled");
    }
    if cfg.control_chat_id == 0 {
        bail!("ha.control_chat_id must not be 0 when HA is enabled");
    }
    if !(15..=300).contains(&cfg.lease_ttl_secs) {
        bail!("ha.lease_ttl_secs must be in 15..300");
    }
    if !(2..cfg.lease_ttl_secs).contains(&cfg.renew_interval_secs) {
        bail!("ha.renew_interval_secs must be at least 2 and lower than lease_ttl_secs");
    }
    if !(1..cfg.lease_ttl_secs).contains(&cfg.claim_settle_secs) {
        bail!("ha.claim_settle_secs must be at least 1 and lower than lease_ttl_secs");
    }
    if !(1..cfg.lease_ttl_secs).contains(&cfg.fail_closed_margin_secs) {
        bail!("ha.fail_closed_margin_secs must be at least 1 and lower than lease_ttl_secs");
    }
    if cfg
        .renew_interval_secs
        .saturating_add(cfg.fail_closed_margin_secs)
        >= cfg.lease_ttl_secs
    {
        bail!("ha.renew_interval_secs + ha.fail_closed_margin_secs must be lower than ha.lease_ttl_secs");
    }
    if cfg.instances.is_empty() {
        bail!("ha.instances must contain at least one instance when HA is enabled");
    }

    let mut ids = HashSet::new();
    for member in &cfg.instances {
        validate_identifier("ha.instances[].id", &member.id)?;
        if !ids.insert(member.id.as_str()) {
            bail!("duplicate HA instance id: {}", member.id);
        }
    }

    if !cfg
        .instances
        .iter()
        .any(|member| member.id == cfg.instance_id)
    {
        bail!(
            "ha.instance_id {} is not present in ha.instances",
            cfg.instance_id
        );
    }

    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{name} must not be empty when HA is enabled");
    }
    if value.len() > 64 {
        bail!("{name} must not exceed 64 bytes");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{name} may contain only ASCII letters, digits, '.', '_' and '-'");
    }
    Ok(())
}

pub fn member<'a>(cfg: &'a HaConfig, instance_id: &str) -> Option<&'a HaInstanceConfig> {
    cfg.instances.iter().find(|member| member.id == instance_id)
}

pub fn members_by_priority(cfg: &HaConfig) -> Vec<&HaInstanceConfig> {
    let mut members = cfg.instances.iter().collect::<Vec<_>>();
    members.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.id.cmp(&right.id))
    });
    members
}

pub fn members_fingerprint(cfg: &HaConfig) -> String {
    let mut bytes = Vec::new();
    for member in members_by_priority(cfg) {
        bytes.extend_from_slice(member.id.as_bytes());
        bytes.push(b':');
        bytes.extend_from_slice(member.priority.to_string().as_bytes());
        bytes.push(b':');
        bytes.push(u8::from(member.auto_failover));
        bytes.push(b':');
        bytes.push(u8::from(member.allow_fallback));
        bytes.push(b':');
        bytes.push(u8::from(member.auto_failback));
        bytes.push(b';');
    }

    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub fn document_matches_config(cfg: &HaConfig, document: &HaDocument) -> bool {
    document.protocol == HA_PROTOCOL_VERSION
        && document.cluster_id == cfg.cluster_id
        && document.members_fingerprint == members_fingerprint(cfg)
}

pub fn encode_document(document: &HaDocument) -> Result<String> {
    let json = serde_json::to_string(document).context("serialize HA control document")?;
    let encoded = format!("{HA_CONTROL_PREFIX}{json}");
    if encoded.chars().count() > 4000 {
        bail!("HA control document exceeds Telegram message safety limit");
    }
    Ok(encoded)
}

pub fn decode_document(text: &str) -> Option<Result<HaDocument>> {
    let json = text.strip_prefix(HA_CONTROL_PREFIX)?;
    Some(serde_json::from_str(json).context("parse HA control document"))
}

fn eligible_candidates<'a>(cfg: &'a HaConfig, document: &HaDocument) -> Vec<&'a HaInstanceConfig> {
    if let Some(target) = document.handover_to.as_deref() {
        return member(cfg, target)
            .filter(|target| document.mode(&target.id) == InstanceMode::Auto)
            .into_iter()
            .collect();
    }

    let previous_owner = document
        .owner
        .as_deref()
        .and_then(|owner| member(cfg, owner));

    members_by_priority(cfg)
        .into_iter()
        .filter(|candidate| {
            if !candidate.auto_failover || document.mode(&candidate.id) != InstanceMode::Auto {
                return false;
            }

            !matches!(
                previous_owner,
                Some(previous)
                    if candidate.priority > previous.priority && !previous.allow_fallback
            )
        })
        .collect()
}

pub fn automatic_candidate<'a>(
    cfg: &'a HaConfig,
    document: &HaDocument,
    now_ms: i64,
) -> Option<&'a HaInstanceConfig> {
    if !document_matches_config(cfg, document) || document.lease_is_live(now_ms) {
        return None;
    }

    eligible_candidates(cfg, document).into_iter().next()
}

pub fn claim_rank(
    cfg: &HaConfig,
    document: &HaDocument,
    now_ms: i64,
    instance_id: &str,
) -> Option<usize> {
    if !document_matches_config(cfg, document) || document.lease_is_live(now_ms) {
        return None;
    }

    eligible_candidates(cfg, document)
        .into_iter()
        .position(|candidate| candidate.id == instance_id)
}

pub fn should_auto_failback(
    cfg: &HaConfig,
    document: &HaDocument,
    instance_id: &str,
    now_ms: i64,
) -> bool {
    if !document_matches_config(cfg, document) || !document.lease_is_live(now_ms) {
        return false;
    }
    let Some(owner_id) = document.owner.as_deref() else {
        return false;
    };
    if owner_id == instance_id || document.mode(instance_id) != InstanceMode::Auto {
        return false;
    }

    let Some(candidate) = member(cfg, instance_id) else {
        return false;
    };
    let Some(owner) = member(cfg, owner_id) else {
        return false;
    };

    candidate.auto_failback && candidate.priority < owner.priority
}

pub fn parse_command(input: &str) -> Option<std::result::Result<HaCommand, &'static str>> {
    let mut parts = input.split_whitespace();
    let first = parts.next()?;
    let command = first.split('@').next().unwrap_or(first);

    match command {
        "/list-instances" | "/list_instances" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::ListInstances))
            } else {
                Some(Err("Použití: /list-instances"))
            }
        }
        "/reddit-status" | "/reddit_status" => {
            if parts.next().is_none() {
                Some(Ok(HaCommand::RedditStatus))
            } else {
                Some(Err("Použití: /reddit-status"))
            }
        }
        "/reddit-connect" | "/reddit_connect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
            };
            let force = match parts.next() {
                None => false,
                Some("force") => true,
                Some(_) => {
                    return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
                }
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-connect <instance-id> [force]"));
            }
            Some(Ok(HaCommand::Connect {
                instance_id: instance_id.to_owned(),
                force,
            }))
        }
        "/reddit-disconnect" | "/reddit_disconnect" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-disconnect <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-disconnect <instance-id>"));
            }
            Some(Ok(HaCommand::Disconnect {
                instance_id: instance_id.to_owned(),
            }))
        }
        "/reddit-switch" | "/reddit_switch" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /reddit-switch <instance-id>"));
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /reddit-switch <instance-id>"));
            }
            Some(Ok(HaCommand::Switch {
                instance_id: instance_id.to_owned(),
            }))
        }
        "/maintenance" => {
            let Some(instance_id) = parts.next() else {
                return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
            };
            let result = match parts.next() {
                None => HaCommand::MaintenanceShow {
                    instance_id: instance_id.to_owned(),
                },
                Some("on") => HaCommand::MaintenanceSet {
                    instance_id: instance_id.to_owned(),
                    enabled: true,
                },
                Some("off") => HaCommand::MaintenanceSet {
                    instance_id: instance_id.to_owned(),
                    enabled: false,
                },
                Some(_) => {
                    return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
                }
            };
            if parts.next().is_some() {
                return Some(Err("Použití: /maintenance <instance-id> [on|off]"));
            }
            Some(Ok(result))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HaConfig {
        HaConfig {
            enabled: true,
            cluster_id: "prod".to_owned(),
            instance_id: "ostrava".to_owned(),
            control_bot_token: "123:test".to_owned(),
            control_chat_id: -100123,
            lease_ttl_secs: 30,
            renew_interval_secs: 10,
            claim_settle_secs: 5,
            fail_closed_margin_secs: 5,
            instances: vec![
                HaInstanceConfig {
                    id: "ostrava".to_owned(),
                    priority: 10,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
                HaInstanceConfig {
                    id: "brno".to_owned(),
                    priority: 20,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
                HaInstanceConfig {
                    id: "praha".to_owned(),
                    priority: 30,
                    auto_failover: true,
                    allow_fallback: true,
                    auto_failback: false,
                },
            ],
        }
    }

    #[test]
    fn config_accepts_valid_cluster() {
        validate_config(&cfg()).unwrap();
    }

    #[test]
    fn config_rejects_duplicate_instance_ids() {
        let mut cfg = cfg();
        cfg.instances.push(cfg.instances[0].clone());
        assert!(validate_config(&cfg).is_err());
    }

    #[test]
    fn member_fingerprint_is_order_independent() {
        let first = cfg();
        let mut second = cfg();
        second.instances.reverse();
        assert_eq!(members_fingerprint(&first), members_fingerprint(&second));
    }

    #[test]
    fn control_document_round_trips() {
        let cfg = cfg();
        let document = HaDocument::new(&cfg);
        let encoded = encode_document(&document).unwrap();
        let decoded = decode_document(&encoded).unwrap().unwrap();
        assert_eq!(decoded, document);
        assert!(document_matches_config(&cfg, &decoded));
    }

    #[test]
    fn automatic_election_chooses_best_priority() {
        let cfg = cfg();
        let document = HaDocument::new(&cfg);
        assert_eq!(
            automatic_candidate(&cfg, &document, 1).map(|member| member.id.as_str()),
            Some("ostrava")
        );
        assert_eq!(claim_rank(&cfg, &document, 1, "ostrava"), Some(0));
        assert_eq!(claim_rank(&cfg, &document, 1, "brno"), Some(1));
        assert_eq!(claim_rank(&cfg, &document, 1, "praha"), Some(2));
    }

    #[test]
    fn maintenance_excludes_instance_from_election() {
        let cfg = cfg();
        let mut document = HaDocument::new(&cfg);
        document
            .modes
            .insert("ostrava".to_owned(), InstanceMode::Maintenance);
        assert_eq!(
            automatic_candidate(&cfg, &document, 1).map(|member| member.id.as_str()),
            Some("brno")
        );
    }

    #[test]
    fn previous_owner_can_forbid_lower_priority_fallback() {
        let mut cfg = cfg();
        cfg.instances[0].allow_fallback = false;
        let mut document = HaDocument::new(&cfg);
        document.owner = Some("ostrava".to_owned());
        document.owner_priority = Some(10);
        document.lease_expires_at_ms = 10;
        assert_eq!(
            automatic_candidate(&cfg, &document, 20).map(|member| member.id.as_str()),
            Some("ostrava")
        );
        document
            .modes
            .insert("ostrava".to_owned(), InstanceMode::Disconnected);
        assert_eq!(
            automatic_candidate(&cfg, &document, 20).map(|member| member.id.as_str()),
            None
        );
    }

    #[test]
    fn explicit_handover_targets_only_requested_instance() {
        let cfg = cfg();
        let mut document = HaDocument::new(&cfg);
        document.handover_to = Some("praha".to_owned());
        assert_eq!(
            automatic_candidate(&cfg, &document, 1).map(|member| member.id.as_str()),
            Some("praha")
        );
        assert_eq!(claim_rank(&cfg, &document, 1, "ostrava"), None);
        assert_eq!(claim_rank(&cfg, &document, 1, "praha"), Some(0));
    }

    #[test]
    fn auto_failback_requires_explicit_member_policy() {
        let mut cfg = cfg();
        cfg.instances[0].auto_failback = true;
        let mut document = HaDocument::new(&cfg);
        document.phase = ControlPhase::Lease;
        document.owner = Some("brno".to_owned());
        document.owner_priority = Some(20);
        document.lease_expires_at_ms = 10_000;
        assert!(should_auto_failback(&cfg, &document, "ostrava", 1_000));
        assert!(!should_auto_failback(&cfg, &document, "praha", 1_000));
    }

    #[test]
    fn parses_requested_general_commands() {
        assert_eq!(
            parse_command("/reddit-connect brno force"),
            Some(Ok(HaCommand::Connect {
                instance_id: "brno".to_owned(),
                force: true,
            }))
        );
        assert_eq!(
            parse_command("/reddit-disconnect ostrava"),
            Some(Ok(HaCommand::Disconnect {
                instance_id: "ostrava".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/reddit-switch praha"),
            Some(Ok(HaCommand::Switch {
                instance_id: "praha".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/maintenance brno on"),
            Some(Ok(HaCommand::MaintenanceSet {
                instance_id: "brno".to_owned(),
                enabled: true,
            }))
        );
        assert_eq!(
            parse_command("/maintenance brno"),
            Some(Ok(HaCommand::MaintenanceShow {
                instance_id: "brno".to_owned(),
            }))
        );
        assert_eq!(
            parse_command("/list-instances@reddit2tg_bot"),
            Some(Ok(HaCommand::ListInstances))
        );
        assert_eq!(
            parse_command("/reddit-status"),
            Some(Ok(HaCommand::RedditStatus))
        );
    }

    #[test]
    fn rejects_bad_command_shapes() {
        assert!(parse_command("/reddit-connect").unwrap().is_err());
        assert!(parse_command("/reddit-connect brno now").unwrap().is_err());
        assert!(parse_command("/maintenance brno maybe").unwrap().is_err());
        assert!(parse_command("/list-instances extra").unwrap().is_err());
    }
}
