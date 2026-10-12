# reddit2tg

Self-hosted Reddit Chat <-> Telegram bridge written in Rust.

The target deployment is Linux/Devuan without Rust, Cargo, a system OpenSSL installation, or a system SQLite library. Release builds are fully static MUSL binaries for `x86_64`, `aarch64`, and ARMv7 hard-float. Matrix and Telegram HTTPS use rustls; the Reddit `/chat/` token-mint request uses vendored native TLS/OpenSSL because live testing showed Reddit returning HTTP 403 for the equivalent rustls request. SQLite is compiled into the binary through `rusqlite`'s `bundled` feature.

## Supported functionality

### Messaging and conversation mapping

- bidirectional Reddit Chat text/notice relay: Reddit -> Telegram and Telegram -> Reddit
- live image bridging in both directions: Reddit `m.image` media is downloaded from Reddit Matrix/CDN and uploaded to Telegram; Telegram photos and image documents are downloaded through Bot API, validated by file signature, uploaded to Reddit Matrix media, and sent as `m.image`
- Reddit upload supports JPEG, PNG, GIF and WebP; Telegram -> Reddit rejects unsupported video/audio/PDF/other attachments with a readable topic message instead of retrying forever
- Telegram media captions are forwarded to Reddit as a following text message because Reddit's native image event shape uses `body: "Image"` rather than a caption field
- one Reddit Matrix room maps to one topic in a private Telegram forum supergroup
- Reddit message requests become `REQUEST · <name>` topics and can be handled with `/accept` or `/decline`
- Telegram messages are accepted only from the configured `telegram.operator_user_id`
- human-readable Reddit names are resolved for both the remote participant and the local Reddit account
- generic or stale topic names can be corrected with `/reconcile-names` or `/reconcile_names`; tracked Reddit profile intro posts are updated at the same time
- successful Reddit -> Telegram delivery advances Reddit-side `m.fully_read`, `m.read`, and `m.read.private` markers
- replies are bridged in both directions: Reddit represents a reply as a Matrix `m.thread` relation with an `m.in_reply_to` fallback; reddit2tg converts that to a Telegram reply, and Telegram replies are sent back using Reddit's thread relation format
- Reddit/Matrix deletions are mirrored for bridge-created Telegram copies: `/allow-delete yes` deletes the Telegram copy, while `/allow-delete no` preserves it and hides captured text/images behind Telegram spoiler UI; the setting is stored per Reddit room and defaults to `yes`
- SQLite keeps durable Matrix event <-> Telegram message mappings and the Telegram message ID of newly created Reddit profile intro posts; one Matrix event may map to multiple Telegram message IDs when long text is split into chunks
- pre-upgrade messages do not have this mapping automatically; `/reload-history` or `/reset-chat` can seed mappings for replayed history

### History recovery and chat repair

- `/reload-history [N] [dry-run]` reloads recent Reddit/Matrix history into the currently mapped Telegram topic without changing the global Matrix `/sync` checkpoint
- `/reset-chat [N] [dry-run]` creates a replacement Telegram topic, replays history, atomically switches the SQLite mapping only after a successful replay, and archives/closes the old topic
- history is fetched with Matrix pagination instead of relying on a single `/messages` page
- every historical Reddit/Matrix event is sent as an individual Telegram message rather than as one combined transcript
- replayed outgoing messages use a compact `>> <name>:` header and incoming messages use `<< <name>:`
- history replay is Telegram rate-limit aware and honors Bot API `retry_after` responses
- long-running administrative commands acknowledge the Telegram update before replay so a transient failure cannot execute the same `/reset-chat` repeatedly
- replayed Matrix event IDs are persisted for deduplication so normal `/sync` processing does not forward the same history again

The default history limit is 50 replayable text/notice/image events and the accepted maximum is 2000. `dry-run` reports what would be processed without changing Telegram topics or SQLite mappings.

### Multi-instance HA

> **Status: experimental / alpha.**
>
> The HA lease/control plane is implemented, but bridge SQLite state is not yet
> replicated between nodes. Read the state-replication limitation below before
> enabling unattended production failover.

HA mode allows multiple `reddit2tg` processes to run at different locations
while only one process at a time is allowed to authenticate to Reddit/Matrix
and poll the normal Telegram data bot.

The user-facing Telegram layout stays simple: **one existing forum supergroup,
two bots, one reserved control topic, any number of instances**.

```text
Telegram forum supergroup
├── General
├── Reddit user topics...
└── ha-control-room
      ├── HA commands
      └── pinned reddit2tg HA state
```

The two bots are:

- **data bot** — the normal `telegram.bot_token`; the same token is configured
  on every HA node, but only the active Reddit lease owner calls `getUpdates`
- **HA control bot** — `ha.control_bot_token`; one cluster-elected node polls it
  and processes commands only inside `ha-control-room`

The control bot token must be different from the data bot token.

For three instances the topology is therefore still only two Telegram bots:

```text
                       Reddit
                         |
                  current ACTIVE
                         |
                 shared data bot
                         |
             existing Telegram forum
                         |
                 ha-control-room
                         |
                 shared HA bot
                         |
              +----------+----------+
              |          |          |
           ostrava      brno       praha
           prio 10     prio 20    prio 30
```

Lower numeric priority is preferred.

#### Create the HA control bot

When converting an existing non-HA installation to HA, **keep the current
reddit2tg bot unchanged**. It remains the shared data bot configured as:

```toml
[telegram]
bot_token = "EXISTING_REDDIT2TG_BOT_TOKEN"
chat_id = -1001234567890
operator_user_id = 123456789
```

Create exactly one additional Telegram bot for HA control.

Open a private chat with the official `@BotFather` account and send:

```text
/newbot
```

Choose a display name, for example:

```text
reddit2tg HA
```

and then a unique bot username, for example:

```text
my_reddit2tg_ha_bot
```

BotFather returns a Bot API token. Store that token as:

```toml
[ha]
control_bot_token = "NEW_HA_CONTROL_BOT_TOKEN"
```

The HA bot must be different from `telegram.bot_token`. `reddit2tg check`
rejects a configuration that uses the same token for both roles.

Treat both bot tokens as passwords. Do not commit them, paste them into issues,
or leave them in shell history.

Load the HA token temporarily without echoing it:

```sh
read -rsp 'HA control bot token: ' HA_BOT_TOKEN; echo; export HA_BOT_TOKEN
```

Verify that it belongs to the expected bot:

```sh
curl -sS "https://api.telegram.org/bot${HA_BOT_TOKEN}/getMe" | jq .
```

The response should contain `"ok": true` and the expected HA bot username.

Verify that no webhook is configured:

```sh
curl -sS "https://api.telegram.org/bot${HA_BOT_TOKEN}/getWebhookInfo" | jq .result
```

The `.url` field must be empty. If this bot was used elsewhere and still has a
webhook, remove it:

```sh
curl -sS "https://api.telegram.org/bot${HA_BOT_TOKEN}/deleteWebhook" | jq .
```

Do not use `drop_pending_updates=true` unless pending HA bot updates should
intentionally be discarded.

Add the new HA bot to the **same existing Telegram forum supergroup** used by
`reddit2tg`.

Promote it to administrator and grant only the permission required by the
current HA implementation:

```text
Pin Messages / Připínat zprávy: YES
Manage Topics / Spravovat témata: NO
Delete Messages: NO
Ban/Restrict Users: NO
Invite Users: NO
Promote Admins: NO
Manage Video Chats: NO
```

The HA control topic is created manually by the operator, so the HA bot does not
need `Manage Topics`.

The existing data bot keeps its current administrator permissions, including
`Manage Topics`.

Telegram Group Privacy Mode does not need to be disabled for the HA bot because
it is an administrator of the supergroup.

#### Create `ha-control-room`

In the existing Telegram forum supergroup used by `reddit2tg`:

1. create a normal forum topic named exactly `ha-control-room`
2. add the HA control bot to the same supergroup
3. promote it to administrator and allow it to pin messages
4. keep the existing data bot in the group with its normal **Manage Topics**
   permission
5. obtain the numeric `message_thread_id` of `ha-control-room`

Before starting the HA daemon, the thread ID can be obtained by sending a
temporary message in `ha-control-room` and querying the HA bot:

```sh
read -rsp 'HA bot token: ' HA_BOT_TOKEN; echo; export HA_BOT_TOKEN
```

```sh
curl -sS "https://api.telegram.org/bot${HA_BOT_TOKEN}/getUpdates" | jq -r '.result[] | select(.message.message_thread_id != null) | "\(.message.message_thread_id)  \(.message.text // "")"'
```

Use the thread ID belonging to the temporary message as:

```toml
ha.control_thread_id = 12345
```

Then remove the temporary shell variable:

```sh
unset HA_BOT_TOKEN
```

The topic is reserved by `reddit2tg`. Ordinary data-bot traffic from that
thread is ignored. If a Reddit counterpart is literally named
`ha-control-room`, its Telegram topic is created with a `Reddit · ` prefix
instead.

#### HA configuration

Example three-node membership:

```toml
[ha]
enabled = true
cluster_id = "prod"
instance_id = "ostrava"

control_bot_token = "123456789:SHARED_HA_CONTROL_BOT_TOKEN"
control_thread_id = 12345

lease_ttl_secs = 30
renew_interval_secs = 10
claim_settle_secs = 5
fail_closed_margin_secs = 5

[[ha.instances]]
id = "ostrava"
priority = 10
auto_failover = true
allow_fallback = true
auto_failback = false

[[ha.instances]]
id = "brno"
priority = 20
auto_failover = true
allow_fallback = true
auto_failback = false

[[ha.instances]]
id = "praha"
priority = 30
auto_failover = true
allow_fallback = true
auto_failback = false
```

All nodes in one cluster must share:

- `telegram.bot_token`
- `telegram.chat_id`
- `telegram.operator_user_id`
- `ha.cluster_id`
- `ha.control_bot_token`
- `ha.control_thread_id`
- all lease timing values
- the complete ordered-equivalent `[[ha.instances]]` membership/policies

Only `ha.instance_id` identifies the local node and therefore differs between
nodes.

The HA control document includes a fingerprint of the control-thread ID, lease
timings and complete member policy. A mismatching node fails closed instead of
participating with different election rules.

Every node that may become active must also contain a valid `reddit.session`.
Standby nodes keep the credential configured but do not authenticate Reddit
until they own a settled Reddit lease.

#### What the two leases do

The pinned HA document coordinates two independent roles:

1. **Reddit owner** — may authenticate Matrix, poll Reddit and poll the shared
   Telegram data bot
2. **HA command poller** — may poll the shared HA bot and answer commands in
   `ha-control-room`

The second role exists so HA administration still works when the Reddit role is
paused, disconnected or between owners.

Both roles include the process-incarnation nonce, so a restarted process cannot
inherit a live lease merely by reusing the same `instance_id`.

The Reddit owner performs:

```text
standby -> claim -> settle -> Reddit authentication -> active
```

Reddit authentication starts only after the claim survives
`claim_settle_secs`.

Active Reddit and Telegram-data operations are bounded by:

```text
lease expiry - fail_closed_margin_secs
```

If the HA state cannot be safely renewed, new Reddit work stops before another
node may take over.

The shared data-bot update offset is stored in the HA document rather than only
in a node-local SQLite database. A new active node therefore continues the same
Telegram `getUpdates` stream instead of starting from that machine's old local
offset.

The HA-bot update offset is stored in the same document, allowing another node
to take over HA command polling after the command-poller lease expires.

#### HA commands

Send HA commands **only inside `ha-control-room`** from the configured
`telegram.operator_user_id`.

Recommended short commands:

```text
/help
/status
/instances
/switch <instance-id>
/connect <instance-id> [force]
/disconnect <instance-id>
/maintenance <instance-id> [on|off]
```

The older names remain accepted as aliases:

```text
/reddit-status
/list-instances
/reddit-switch <instance-id>
/reddit-connect <instance-id> [force]
/reddit-disconnect <instance-id>
```

Examples:

```text
/status
```

```text
/switch brno
```

```text
/maintenance ostrava on
```

```text
/disconnect praha
```

```text
/connect praha
```

`/disconnect` is an explicit administrative stop and does not itself trigger
automatic fallback.

`/maintenance <id> on` removes the selected instance from Reddit candidacy but
does not stop the process from participating in the HA command-poller election.

With `auto_failback = false` (recommended), a recovered higher-priority node
does not automatically steal the Reddit role from a healthy lower-priority
owner.

#### Convert an existing non-HA installation

Use the existing reddit2tg bot as the data bot. The new bot created above is
used only for HA control.

Do **not** swap their roles.

Recommended migration order:

```text
1. keep the existing data bot unchanged
2. create the new HA control bot
3. add the HA bot to the existing Telegram forum
4. grant the HA bot Pin Messages permission
5. create ha-control-room
6. determine ha.control_thread_id
7. prepare identical HA membership on all planned nodes
8. keep the current production non-HA process running until configuration is ready
9. stop the old non-HA reddit2tg process
10. start only the preferred HA node
11. verify that it becomes ACTIVE
12. start the second node and verify STANDBY
13. start any remaining nodes
```

Never run the old non-HA process and the new HA runtime simultaneously with the
same `telegram.bot_token`. The non-HA process does not understand the HA lease
and would compete for the shared data bot's `getUpdates` stream.

For a three-node cluster all nodes therefore contain the same two bot tokens:

```text
telegram.bot_token     = existing shared reddit2tg data bot
ha.control_bot_token   = new shared HA control bot
```

Only `ha.instance_id` differs by node.

#### Adding another instance

To add a new node such as `brno`:

1. install the same `reddit2tg` version on the new host
2. copy the shared data-bot token and the same Telegram forum/operator IDs
3. copy the shared HA control-bot token and `ha-control_thread_id`
4. configure the same Reddit browser session
5. add `brno` to `[[ha.instances]]` on **every** node
6. set `ha.instance_id = "brno"` only on the new node
7. update/restart the existing nodes so all members have the same membership
   fingerprint
8. run `reddit2tg check` on the new node
9. start the new service

In HA mode:

```sh
sudo -u reddit2tg /usr/bin/reddit2tg --config /etc/reddit2tg/config.toml check
```

checks the data bot and HA control plane but intentionally does **not**
authenticate Reddit.

Start the first HA node and let it create/pin the control document before
starting additional nodes. This avoids an unnecessary first-bootstrap race.

#### Pinned HA state

The HA state message is sent inside `ha-control-room` and is maintained only by
`reddit2tg`. Do not edit or delete it.

The Telegram Bot API `getChat` interface exposes the latest pinned message for
the chat, not a transactional topic-scoped key/value store. `reddit2tg`
therefore verifies that the latest pin belongs to the configured
`ha-control-room` and contains a valid HA document. While running, a node that
already knows the HA state message ID can re-pin it if another message
temporarily becomes the latest pin.

If all nodes restart while another message is the latest pin, HA intentionally
fails closed. Re-pin the reddit2tg HA state message in `ha-control-room` before
starting the cluster again.

This Telegram fencing scheme reduces write races with deterministic election,
generation checks, claim settling, process nonces and read-after-write
verification, but it is not equivalent to an atomic etcd/Raft-style consensus
store.

#### Important alpha limitation: SQLite bridge state is not replicated

HA currently coordinates **ownership**, but each host still has its own local
SQLite database containing:

- Matrix room -> Telegram topic mappings
- Matrix sync checkpoint
- Matrix event deduplication
- Reddit -> Telegram message mappings
- pending read markers
- short-lived Matrix authentication state
- per-chat settings

The Telegram data-bot offset is shared through the HA document in HA mode, but
the rest of this bridge state is not yet replicated.

For an initial controlled two-node test, start both nodes from a consistent
SQLite snapshot taken while the original bridge is stopped. After traffic
continues, the databases may diverge again.

Do not point several running instances at one SQLite file on NFS or another
network filesystem. Shared-network SQLite is not a supported HA backend.

Seamless unattended production failover still requires a shared or replicated
bridge-state design and remains future work.

### Authentication and recovery

- no Reddit password is required on the bridge host
- the bridge can reuse a valid saved Matrix session or current Reddit `token_v2`
- short-lived Reddit Matrix authentication is automatically refreshed from the saved browser cookie set through Reddit `/chat/`
- `/svc/shreddit/token` is retained as a fallback when a usable CSRF cookie is available
- Matrix HTTP 401 invalidates the cached session, performs one refresh, and retries the failed operation once
- short-lived Matrix credentials are persisted in SQLite while the long-lived Reddit browser session remains only in `config.toml`
- authentication failures generate one deduplicated warning in Telegram General; successful recovery generates one recovery notification

### Persistent state and reliability

- SQLite stores Reddit-room <-> Telegram-topic mappings, Matrix `/sync` checkpoints, Telegram `getUpdates` offsets, Matrix event deduplication, pending read markers, authentication state, and the short-lived Matrix session
- pending Reddit read markers survive restart and are retried without re-sending the Telegram message
- the SQLite database file is forced to mode `0600` on Unix
- the bridge uses Telegram long polling and requires no public IP address, inbound TCP port, or Telegram webhook
- the first Matrix `/sync` establishes a safe checkpoint without replaying old joined-room history, while events whose `origin_server_ts` is at or after bridge startup are still forwarded; a limited initial timeline is backfilled through `prev_batch` until the startup boundary is crossed
- explicit `/reload-history` or `/reset-chat` remains available when older history recovery is wanted

### Operations and deployment

- `reddit2tg check` validates Reddit/Matrix authentication and Telegram bot/forum configuration before the daemon is started
- `reddit2tg reconcile-names [--dry-run]` is available from the CLI in addition to the Telegram command
- release builds are static MUSL binaries for `x86_64`, `aarch64`, and ARMv7 hard-float (`armhf`)
- releases also include explicit Debian 9 Stretch legacy ARM builds for `arm64` and `armhf`; these use conservative generic CPU code generation and are smoke-tested with QEMU before packaging
- the legacy ARM builds remain statically linked with MUSL, so they do not depend on the target system's glibc; final validation on the actual legacy kernel/hardware is still recommended
- release packaging includes `.tar.gz` archives, Debian packages, and SHA256 checksums
- Devuan/SysV init is supported, with an optional systemd unit included in the package

### Current limitations

Not implemented yet: reactions, edits, typing indicators, starting a brand-new Reddit DM from Telegram, or reliable counterparty "seen" receipts. Reddit does not expose dependable remote-user read receipts through the Matrix sync stream, and the Telegram Bot API does not tell a bot when a human has opened a message.

Reddit's upload endpoint accepts images only (JPEG/PNG/WebP up to 20 MiB, GIF up to 100 MiB). The standard Telegram Bot API can download incoming user files only up to 20 MiB and upload at most 10 MiB as a photo or 50 MiB as another file, so larger media cannot be bridged through the hosted Bot API.

History recovery replays text/notice and image events. Historical images use the same Matrix media download and Telegram upload path as live Reddit images, while preserving chronological ordering and reply targets.

### Delete mirroring

Inside a mapped Telegram topic, `/allow-delete yes` (the default) makes a Reddit Matrix `m.room.redaction` delete the directly redacted Telegram copy created by the bridge. `/allow-delete no` preserves the copy and applies spoilers to the redacted message and recursively to bridge-created Telegram copies of every Matrix reply descendant below it. This keeps a deleted parent and its visible reply subtree consistently hidden. `/allow-delete` without an argument reports the current setting.

The bridge intentionally does not modify an operator-authored Telegram source message if Reddit later redacts the Matrix event created from it. Telegram's Bot API only permits `deleteMessage` for messages younger than 48 hours; if deletion of an older bridge-created message fails and a stored snapshot exists, reddit2tg falls back to spoiler preservation. Legacy messages mapped before snapshot support may not be recoverable for that fallback. JPEG/PNG/GIF images can be covered by Telegram media spoilers; Reddit WebP images are converted to PNG on the Telegram side so they have the same behavior. Generic documents/audio do not have equivalent Bot API media-spoiler support, so only their caption can be hidden.

## Configuration values

`reddit2tg` single-instance mode needs four account-specific values. HA mode additionally needs one shared HA control-bot token, the reserved `ha-control-room` thread ID, and a unique `ha.instance_id` per node:

| Config key | Meaning |
| --- | --- |
| `reddit.session` | Complete Cookie-header-shaped browser session applicable to `https://www.reddit.com/chat/`; it must contain `reddit_session`, and other applicable Reddit cookies should be retained rather than reduced to a small whitelist |
| `telegram.bot_token` | Telegram Bot API token issued by `@BotFather` |
| `telegram.chat_id` | numeric ID of the private Telegram forum supergroup used by the bridge |
| `telegram.operator_user_id` | numeric Telegram user ID of the only account allowed to relay messages to Reddit |

Treat `reddit.session` and `telegram.bot_token` as passwords. Never commit `config.toml`, paste either value into an issue, or send them to another user. The repository `.gitignore` already ignores `config.toml` and local SQLite files.

## Telegram setup

### 1. Register the bot with BotFather

1. Open Telegram and start a chat with the official `@BotFather` account.
2. Send `/newbot`.
3. Enter a display name, for example `reddit2tg`.
4. Enter a unique bot username. A normal bot username ends in `bot`, for example `my_reddit2tg_bot`.
5. BotFather returns an API token looking roughly like `123456789:AA...`. This is the value for `telegram.bot_token`.
6. Do not publish or commit this token. Anyone who has it controls the bot.

Group joining is enabled for normal new bots. If it was disabled previously, use `/setjoingroups` in BotFather, select the bot and enable joining groups.

`reddit2tg` does not require Telegram Group Privacy Mode to be disabled because the bot is required to be an administrator of the bridge supergroup. Telegram administrators receive group messages regardless of the normal privacy-mode restriction.

### 2. Create the private forum supergroup

Create a private Telegram group that will contain the bridged Reddit conversations. The exact menu labels vary slightly between Telegram Desktop, Android and iOS, but the required state is the same:

1. Create a new private group.
2. Open the group settings / Manage Group.
3. Enable **Topics** (forum mode). Telegram will use a supergroup for forum topics.
4. Add the newly created `reddit2tg` bot.
5. Promote the bot to **Administrator**.
6. Enable at least **Manage Topics** for the bot.
7. Make sure normal group permissions allow messages to be sent.

The bridge check command verifies that the configured chat is a supergroup with forum mode enabled and that the bot is an administrator with `can_manage_topics`.

### 3. Load the bot token into the shell without storing it in Bash history

The following command asks for the token interactively and does not echo it:

```sh
read -rsp 'Telegram bot token: ' TG_BOT_TOKEN; echo; export TG_BOT_TOKEN
```

Verify that the token belongs to the expected bot:

```sh
curl -sS "https://api.telegram.org/bot${TG_BOT_TOKEN}/getMe" | jq .
```

A successful response contains `"ok": true` and the username of the new bot.

If `jq` is not installed on the Debian 13 build machine:

```sh
sudo apt-get install -y jq
```

### 4. Verify that no Telegram webhook is configured

`reddit2tg` uses Bot API long polling (`getUpdates`), not a webhook. Telegram does not allow `getUpdates` while a webhook is active.

Check the current state:

```sh
curl -sS "https://api.telegram.org/bot${TG_BOT_TOKEN}/getWebhookInfo" | jq .result
```

For a new bot, `.url` should be an empty string. If an old webhook exists, remove it before using the bridge:

```sh
curl -sS "https://api.telegram.org/bot${TG_BOT_TOKEN}/deleteWebhook" | jq .
```

Do not use `drop_pending_updates=true` unless you intentionally want to discard pending Telegram updates.

### 5. Obtain `telegram.chat_id` and `telegram.operator_user_id`

Both values can be obtained from a single message sent by you in the new forum group.

1. Make sure the bot is already an administrator with **Manage Topics**.
2. In the group's **General** topic send this exact text from the Telegram account that will operate the bridge:

```text
reddit2tg-id-probe
```

3. Before starting `reddit2tg`, query the bot's pending updates:

```sh
curl -sS "https://api.telegram.org/bot${TG_BOT_TOKEN}/getUpdates" | jq -r '.result[] | select(.message.text == "reddit2tg-id-probe") | "chat_id = \(.message.chat.id)\noperator_user_id = \(.message.from.id)\nchat = \(.message.chat.title)\nuser = \(.message.from.username // .message.from.first_name)"'
```

Expected shape:

```text
chat_id = -1001234567890
operator_user_id = 123456789
chat = reddit2tg
user = your_username
```

Use the first number as `telegram.chat_id` and the second as `telegram.operator_user_id`.

A supergroup ID normally appears as a negative integer starting with `-100`. Do not remove the minus sign or the `-100` prefix.

If the command prints nothing, send `reddit2tg-id-probe` again and immediately repeat `getUpdates`. Also verify that the bot is in the correct group and is an administrator.

`operator_user_id` is deliberately checked by the bridge. Telegram messages from other human accounts in the group are ignored for the Telegram -> Reddit direction.

After obtaining the IDs, remove the temporary shell variable:

```sh
unset TG_BOT_TOKEN
```

## Reddit authentication

`reddit2tg` does not need your Reddit password and does not need Firefox/Chromium on the server. The bridge uses cookies copied from an already logged-in Reddit browser session. `reddit_session` must be present, but for reliable automatic refresh keep the **complete set of unexpired cookies that the browser would send to `https://www.reddit.com/chat/`**. Live testing showed that reducing the session to only a small cookie whitelist can make Reddit return HTTP 403. A current `token_v2` may be reused at startup, but automatic long-running refresh does not depend on it.

For automatic refresh, `reddit2tg` requests `https://www.reddit.com/chat/` with the saved Reddit cookie jar **after removing the `token_v2` cookie**. Reddit then server-renders a fresh short-lived Matrix JWT into the `<rs-app token="...">` bootstrap element. The bridge extracts that JWT, registers it with `https://matrix.redditspace.com/_matrix/client/v3/login` using Reddit's `com.reddit.token` login type, verifies it with Matrix `/account/whoami`, and stores only this short-lived Matrix session in SQLite. If `/chat/` minting fails and a `csrf_token` cookie is available, `/svc/shreddit/token` is retained as a fallback.

The long-lived `reddit_session` remains only in `config.toml`; it is not copied into SQLite.

### 1. Log in to Reddit normally

Open `https://www.reddit.com/` in the browser you normally use and log in to the Reddit account whose chats are to be bridged. Open `https://www.reddit.com/chat/` once and verify that Reddit Chat works in that browser.

### 2. Firefox on Linux: copy the required cookies automatically

For Firefox on the Debian build workstation this is the preferred method. It avoids manually copying long HttpOnly cookie values from Developer Tools. Install the two small helper tools once:

```sh
sudo apt-get install -y sqlite3 xclip
```

Then run from the repository root:

```sh
bash scripts/firefox-reddit-cookies.sh
```

The helper finds the first Firefox `cookies.sqlite`, makes a private temporary copy together with its WAL/SHM files, selects **all unexpired cookies whose domain and path apply to `https://www.reddit.com/chat/`**, and puts the resulting Cookie-header-shaped string directly into the X11 clipboard. It does not print the secret values. It prints only cookie names and their lengths.

If Firefox has more than one profile, pass the desired database explicitly:

```sh
bash scripts/firefox-reddit-cookies.sh /home/peva/.mozilla/firefox/h7v1hpbn.default-esr/cookies.sqlite
```

The clipboard contains the complete Cookie-header-shaped value. Some valid Reddit cookies can themselves contain JSON quotes, so **do not paste the raw clipboard contents directly between TOML quotes**. Insert it into an existing `config.toml` with TOML-safe escaping instead:

```sh
xclip -selection clipboard -o | python3 -c 'import sys,re,json; p="config.toml"; v=sys.stdin.read(); s=open(p,encoding="utf-8").read(); r="session = "+json.dumps(v); s,n=re.subn(r"(?m)^session\s*=.*$",lambda m:r,s,count=1); assert n==1,"session line not found"; open(p,"w",encoding="utf-8").write(s)'
```

Verify the file without printing the credential:

```sh
python3 -c 'import tomllib; tomllib.load(open("config.toml","rb")); print("config.toml: OK")'
```

`reddit_session` must be present. Keep all other applicable cookies captured by the helper. A current `token_v2` can speed up initial startup; during automatic refresh it is deliberately omitted from the `/chat/` request so Reddit mints a new Matrix JWT. `csrf_token` is used only by the fallback `/svc/shreddit/token` flow. Do not commit or share this value.

### 3. Manual browser method

If the Firefox helper cannot be used, copy the full set of unexpired cookies that the browser sends to `https://www.reddit.com/chat/` from a logged-in `www.reddit.com` session. `reddit_session` must be present; do not intentionally discard other applicable cookies. Construct a normal Cookie header, for example:

```text
reddit_session=VALUE1; token_v2=VALUE2
```

For Chrome, Chromium, Brave or Edge the values are under **F12 -> Application -> Storage -> Cookies -> https://www.reddit.com**. For Firefox they are under **F12 -> Storage -> Cookies -> https://www.reddit.com**. Cookie values are credentials; do not paste them into issues, screenshots, shell history or chat.

### 4. What reddit2tg does with the cookies

At startup, `reddit2tg` first tries a still-valid short-lived Matrix session saved in SQLite. If none is usable, it may probe the configured browser `token_v2`. Before expiry (controlled by `bridge.refresh_before_secs`), or after the first Matrix `401 M_UNKNOWN_TOKEN`, the bridge performs one automatic refresh:

1. remove `token_v2` from the outgoing Reddit cookie header,
2. `GET https://www.reddit.com/chat/`,
3. extract and HTML-decode the JSON from `<rs-app token="...">`,
4. register the returned JWT through Matrix `/login` with `type = "com.reddit.token"`,
5. verify `/account/whoami`,
6. save the short-lived Matrix token, user ID and expiry in SQLite.

If `/chat/` refresh fails and `csrf_token` exists, the bridge also tries `POST /svc/shreddit/token` as a fallback. A Matrix operation that receives HTTP 401 invalidates the cached token, performs one refresh, and retries the operation once. It does not loop infinitely on an invalid token.

When automatic refresh cannot recover, the daemon stays alive and continues retrying with backoff. The bot sends one warning to the Telegram **General** topic. Repeated failures are deduplicated through SQLite. After authentication works again, one recovery message is sent and the alert state is cleared. No cookie, Matrix token or HTTP response body containing credentials is copied into the Telegram alert.

When Reddit eventually expires or invalidates `reddit_session` itself, repeat the Firefox/browser cookie-extraction step, replace `reddit.session` in the configuration and restart the service. Logging out of Reddit sessions or changing account security settings may invalidate it earlier.

## Create a local test configuration

Before installing the daemon on Devuan, it is convenient to run the complete connectivity check on the Debian 13 build machine.

From the repository root:

```sh
cp config.example.toml config.toml
```

Protect the local file:

```sh
chmod 600 config.toml
```

For this local test, change the storage path from `/var/lib/reddit2tg/reddit2tg.sqlite3` to a writable file in the current directory:

```toml
[storage]
path = "./reddit2tg.sqlite3"
```

Fill the four values obtained above. The resulting structure is:

```toml
[reddit]
session = "PASTE_REDDIT_SESSION_HERE"
homeserver = "https://matrix.redditspace.com"

[telegram]
bot_token = "123456789:PASTE_BOT_TOKEN_HERE"
chat_id = -1001234567890
operator_user_id = 123456789

[storage]
path = "./reddit2tg.sqlite3"

[bridge]
matrix_timeout_ms = 10000
telegram_timeout_secs = 45
refresh_before_secs = 3600
```

Run the checks:

```sh
./target/x86_64-unknown-linux-musl/release/reddit2tg --config ./config.toml check
```

The check validates, among other things:

- the configured Reddit Cookie header contains `reddit_session`
- an existing short-lived Matrix session/token works, or Reddit `/chat/` can mint and register a fresh Matrix Chat token
- Matrix `/account/whoami` succeeds
- Telegram bot token is valid
- no Telegram webhook is active
- `telegram.chat_id` points to a forum supergroup
- the bot is an administrator with **Manage Topics**

Only after this command passes should the long-running bridge be started.

## Run locally

For a foreground test on the build machine:

```sh
./target/x86_64-unknown-linux-musl/release/reddit2tg --config ./config.toml run
```

Stop it with `Ctrl+C`.

Outside HA mode, do not simultaneously run another process using `getUpdates` with the same Telegram bot token. In HA mode the data-bot token is intentionally shared, but the lease gate guarantees that only the current active node polls it; the HA control bot has an independent cluster-elected poller.

### Runtime communication recovery

The long-running bridge continuously exercises both transports even when the chats are idle: Matrix `/sync` polls Reddit Chat and Telegram `getUpdates` polls Telegram. These normal polling cycles therefore also act as transport health checks.

Matrix session refresh is serialized between the Matrix polling worker and the Telegram-to-Reddit worker. If both workers notice an expiring or rejected Matrix token at the same time, only one worker refreshes it; a stale HTTP 401 from the other worker cannot invalidate the newer shared session.

Three consecutive recoverable HTTP-client failures ask the runtime supervisor for a full recovery. Message-specific application failures and Reddit authentication-refresh failures keep their normal retry/backoff path and do not recycle the whole runtime. The supervisor cancels the sibling worker, waits five seconds, creates fresh Reddit and Telegram HTTP clients, restores/revalidates the Matrix session, and resumes polling. This recovery is internal to `reddit2tg`, so it also works when the daemon is run under the supplied SysV init script, which does not provide automatic process respawn.

The Telegram configuration/permission probe is intentionally performed once before the recoverable runtime starts. A bad bot token, wrong chat, active webhook, or missing Manage Topics permission therefore remains a startup error instead of causing an endless internal restart loop.

Telegram update offsets are still persisted only after an accepted update has been processed successfully. Therefore a Telegram message whose Reddit delivery fails during a runtime recovery is fetched and retried after the clients are recreated instead of being acknowledged and lost.

### Reconcile existing Telegram topic names

Existing room mappings can be rechecked against current Reddit/Matrix membership without creating a new chat or sending a Reddit message. A dry run only reports differences:

```sh
cargo run --locked -- --config ./config.toml reconcile-names --dry-run
```

Apply the detected renames with:

```sh
cargo run --locked -- --config ./config.toml reconcile-names
```

The same operation is available from the configured Telegram operator account. Send either command in the bridge supergroup; the result is posted to the General topic:

```text
/reconcile-names dry-run
/reconcile-names
```

`/reconcile_names` is accepted as an alias for Telegram clients or bot-command tooling that prefers underscores. The reconciliation never creates rooms or topics: it only examines rows already present in the SQLite `rooms` mapping and renames a Telegram topic when the current Reddit counterpart name differs. Message-request topics retain their `REQUEST · ` prefix.

### Reload or reset a mapped chat from Reddit history

For a joined Reddit chat, the configured Telegram operator can replay recent Reddit/Matrix history directly from inside that chat's mapped Telegram topic. The default is 50 replayable events and the accepted range is 1-2000:

```text
/reload-history
/reload-history 100
/reload-history dry-run
/reload-history 100 dry-run
```

`/reload-history` fetches recent replayable `m.room.message` history through Matrix `/rooms/{roomId}/messages`, including text/notice and image events, renders it chronologically with the resolved Reddit sender names, and appends it to the current Telegram topic. It does not change the global Matrix `/sync` checkpoint and does not send read receipts for replayed history. Replayed event IDs are added to the normal dedup table only after Telegram delivery succeeds.

Each replayed Matrix event is sent as its own Telegram message. Text/notice events use the compact bold header `>> <name>:` for messages sent by the bridged Reddit account and `<< <name>:` for messages received from the other side. Historical images are uploaded as Telegram media with `[HH:MM:SS] >>/<< <name>:` in the caption and retain the Matrix `m.in_reply_to` target when it can be mapped.

For a damaged or badly reconstructed Telegram topic, use the stronger reset operation:

```text
/reset-chat dry-run
/reset-chat
/reset-chat 100
```

`/reset-chat` first downloads the requested history, creates a new Telegram topic using the currently resolved Reddit counterpart name, replays the history there, and only then atomically switches the SQLite room mapping to the new `message_thread_id` together with the replayed dedup event IDs. After that succeeds, the previous topic is renamed with an `ARCHIVED · ` prefix and closed. If history delivery or the SQLite switch fails, the old mapping remains active and reddit2tg marks the incomplete new topic with an `INCOMPLETE · ` prefix and closes it.

The underscore aliases `/reload_history` and `/reset_chat` are also accepted. Both commands must be sent inside an already mapped Telegram topic and only operate on rooms whose status is `joined`. `dry-run` performs the Matrix history lookup and reports the number of usable messages without changing Telegram or SQLite state.

### Read receipts

After an incoming Reddit text/notice has been successfully delivered to its Telegram topic, reddit2tg queues a Matrix read marker for that event and sends all three keys used by Reddit/Matrix clients: `m.fully_read`, `m.read` and `m.read.private`. The pending marker is persisted before the HTTP request, so a transient failure can be retried without sending the Telegram message twice.

This means the bridge can clear the Reddit-side unread state for messages it has forwarded. It does **not** mean the remote Reddit user has seen messages sent by you: the Reddit homeserver does not expose dependable counterparty read receipts in the `/sync` stream, and Telegram Bot API likewise does not tell a bot that a human opened a message. reddit2tg therefore does not invent a false "seen" state.

## Build on Debian 13

Install the build prerequisites:

```sh
sudo apt-get install -y build-essential musl-tools pkg-config ca-certificates curl
```

Install the Rust stable toolchain using rustup, then run:

```sh
bash scripts/build-static.sh
```

The resulting binary is:

```text
target/x86_64-unknown-linux-musl/release/reddit2tg
```

Verify it with:

```sh
file target/x86_64-unknown-linux-musl/release/reddit2tg
```

and:

```sh
ldd target/x86_64-unknown-linux-musl/release/reddit2tg
```

The expected result is a static/static-PIE x86-64 executable with no normal shared runtime-library dependency.

## Install on Devuan Excalibur

From the source directory, with the statically built binary available:

```sh
sudo sh scripts/install-devuan.sh target/x86_64-unknown-linux-musl/release/reddit2tg
```

Then edit `/etc/reddit2tg/config.toml`. On the installed service the normal database path is:

```toml
[storage]
path = "/var/lib/reddit2tg/reddit2tg.sqlite3"
```

Before starting the daemon:

```sh
sudo -u reddit2tg /usr/local/bin/reddit2tg --config /etc/reddit2tg/config.toml check
```

Then use the normal SysV service commands:

```sh
sudo service reddit2tg start
```

```sh
sudo service reddit2tg status
```

Runtime logs are written to `/var/log/reddit2tg.log`:

```sh
sudo tail -f /var/log/reddit2tg.log
```


## Remote build and GitHub releases

The repository contains `.github/workflows/build-release.yml`. Compilation and packaging therefore run on GitHub-hosted Ubuntu runners; the target Devuan/Linux host does not need Rust or build tools.

For every branch push, pull request and manual workflow run, GitHub Actions first runs `cargo test --locked`, then cross-compiles three static MUSL release targets in parallel with `cross`:

| Debian arch | Rust target | Typical use |
| --- | --- | --- |
| `amd64` | `x86_64-unknown-linux-musl` | x86-64 PC/server |
| `arm64` | `aarch64-unknown-linux-musl` | Raspberry Pi 3/4/5 64-bit, Orange Pi and newer SBCs |
| `armhf` | `armv7-unknown-linux-musleabihf` | 32-bit ARMv7 Debian/Devuan SBC installations |

Each architecture is verified to have no dynamic program interpreter and is packaged independently. The combined GitHub Actions artifact contains:

```text
reddit2tg-VERSION-linux-x86_64-musl.tar.gz
reddit2tg-VERSION-linux-aarch64-musl.tar.gz
reddit2tg-VERSION-linux-armv7-musleabihf.tar.gz
reddit2tg_VERSION_amd64.deb
reddit2tg_VERSION_arm64.deb
reddit2tg_VERSION_armhf.deb
SHA256SUMS
```

The ARMv7 target is hard-float and maps to Debian/Devuan `armhf`. It does not cover ARMv6-only boards. For current Raspberry Pi OS/Devuan 64-bit installations, prefer the `arm64` package.

For a pushed version tag such as `v0.1.4`, GitHub creates or updates one matching Release only after all three architecture builds succeed, and attaches the complete multi-architecture set. The workflow deliberately rejects a tag whose version does not match the `version` field in `Cargo.toml`.

A normal remote release is therefore:

```sh
git tag -a v0.1.4 -m 'reddit2tg v0.1.4'
```

```sh
git push origin HEAD
```

```sh
git push origin v0.1.4
```

The `.deb` packages install the static binary as `/usr/bin/reddit2tg`, the SysV init script as `/etc/init.d/reddit2tg`, and an optional systemd unit as `/lib/systemd/system/reddit2tg.service`. They create the unprivileged `reddit2tg` account and `/var/lib/reddit2tg`, and create `/etc/reddit2tg/config.toml` from the example only when that file does not already exist.

The Debian package intentionally does **not** enable or start the service automatically because the credentials in `/etc/reddit2tg/config.toml` must be configured first. After installation, verify the configuration with:

```sh
sudo -u reddit2tg /usr/bin/reddit2tg --config /etc/reddit2tg/config.toml check
```

On Devuan/SysV, enable and start it with:

```sh
sudo update-rc.d reddit2tg defaults && sudo service reddit2tg start
```

On a systemd host, the equivalent is:

```sh
sudo systemctl enable --now reddit2tg.service
```

The archive and Debian package are both produced by `scripts/package-release.sh`; the script exists mainly so the packaging logic is versioned with the application and shared by all GitHub Actions architecture jobs.

## Security notes

`reddit.session` (the Reddit cookie header) and the Telegram bot token are credentials. `config.toml` should be mode `0640` or stricter and readable only by root and the `reddit2tg` service group on the target system. A developer-local `config.toml` can simply use mode `0600`.

The SQLite database stores the short-lived Matrix access token, its user ID/expiry, pending read markers and the deduplicated authentication-alert state. It does **not** store the long-lived `reddit_session` or Telegram bot token. `reddit2tg` forces the SQLite database file to mode `0600`; the containing `/var/lib/reddit2tg` directory is installed as `0750`. Treat the database as sensitive runtime state.

The Reddit Chat protocol used here is not an official public Reddit API and can change without notice.

## AI-assisted development

A significant portion of the source code and documentation in this project has been generated or modified with the assistance of AI tools. Automated tests and successful builds do not guarantee correctness or security. Human review is strongly recommended before production deployment, especially for authentication, credential handling, networking, packaging and service-management changes.

## Verification status

The `amd64` MUSL build has been verified locally on Debian 13 as a static-PIE executable. `reddit2tg 0.1.4` has also been verified end-to-end with a real Reddit Chat DM mapped to a Telegram forum topic and replies relayed back to Reddit. The `amd64`, `arm64` and `armhf` targets all passed the first GitHub Actions multi-architecture build for v0.1.4, including static-link verification and `.deb`/`.tar.gz` packaging. The v0.1.5 authentication/read-receipt changes should be validated by the same remote workflow before tagging the release.

## References

- Telegram bots introduction: https://core.telegram.org/bots
- Telegram Bot API: https://core.telegram.org/bots/api
- Telegram bot FAQ / update delivery: https://core.telegram.org/bots/faq
- Telegram forum topics: https://core.telegram.org/api/forum

### Debugging Telegram topics

Version 0.1.5 retains the INFO-level diagnostics from 0.1.4 and adds authentication refresh/read-marker diagnostics. Version 0.1.4 keeps the INFO-level diagnostics for Telegram forum-topic routing and also resolves missing DM identities through Matrix `joined_members`. Existing generic `Reddit chat` topics are renamed automatically when the Reddit identity becomes available. Successful Telegram -> Reddit sends now log the resulting Matrix event ID. Version 0.1.3 added the original topic diagnostics. When a Reddit room creates a topic, the log prints both `matrix_room_id` and the returned `telegram_thread_id`. When the operator replies in Telegram, the log prints the incoming `message_thread_id` and `is_topic_message`. `sendMessage` also warns if Telegram returns a different or missing thread ID than the one requested.

This makes it possible to verify that the SQLite mapping and Telegram's actual topic IDs match.

<!-- ha-v3-replication-foundation -->
### HA protocol 3 state replication

The `feature/ha-replicated-state` development branch introduces a logical
replication layer for bridge state. It intentionally does **not** replicate the
SQLite file itself and does not replicate `matrix_auth` access tokens.

Replicated state includes room/topic mappings, Matrix event deduplication,
message mappings, reply edges, message snapshots, per-room delete policy,
profile intro message IDs, archived topics, pending read receipts and
`matrix_next_batch`.

SQLite triggers append those logical changes atomically to a local HA change
log. Standby nodes will consume published generations and apply them with
replication logging suppressed. Full logical snapshots provide bootstrap and
recovery without sharing SQLite over NFS.

Protocol 3 nodes must not be mixed with protocol 2 (`v0.4.0-alpha1`) nodes in
one running cluster.

<!-- ha-v3-sync-transport -->
### HA replication transport: `ha-sync`

HA protocol 3 keeps human administration and replication traffic separate.

The shared HA bot uses two reserved topics in the same Telegram forum:

- `ha-control-room` — operator commands and the pinned canonical HA state.
- `ha-sync` — encrypted replication delta/snapshot files only.

`ha-control-room` is created/configured by the operator as before. The HA bot
must now have both **Pin Messages** and **Manage Topics** administrator
permissions. The first protocol-3 control owner creates `ha-sync`
automatically and stores its `message_thread_id` in the canonical HA document;
there is no additional thread ID in `config.toml`.

Replication payloads are gzip-compressed and authenticated/encrypted with
ChaCha20-Poly1305. The per-cluster key is derived from the shared HA bot token
and `cluster_id`; the token itself is never stored in SQLite or Telegram.
Snapshot payloads are split according to `replication_snapshot_chunk_mib`
(default 8 MiB), safely below the official Bot API `getFile` download limit.

A standby node must apply the canonical snapshot/delta chain until its local
replication generation exactly matches the cluster generation before it is
eligible to claim the Reddit lease. Local Matrix authentication tokens are
never included in replicated state.

When upgrading from `v0.4.0-alpha1`/HA protocol 2, stop every alpha1 node first
and wait for its Reddit/control leases to expire. Protocol 3 can then migrate
the pinned protocol-2 document in place, preserving the shared Telegram data
and control update offsets before creating `ha-sync` and publishing the initial
logical database snapshot.
