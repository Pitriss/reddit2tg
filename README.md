# reddit2tg

Self-hosted Reddit Chat <-> Telegram bridge written in Rust.

The target deployment is Linux/Devuan without Rust, Cargo, OpenSSL, or a system SQLite library installed. Release builds are fully static MUSL binaries for `x86_64`, `aarch64`, and ARMv7 hard-float; HTTPS uses rustls and SQLite is compiled into the binary through `rusqlite`'s `bundled` feature.

## v0.1 scope

- Reddit Chat text DMs -> Telegram forum topics
- Telegram topic text -> Reddit Chat
- one Reddit room = one Telegram topic
- SQLite mapping, Matrix `/sync` checkpoint and Telegram `getUpdates` offset
- event deduplication
- Reddit message requests become `REQUEST · ...` topics with `/accept` and `/decline`
- Matrix JWT is automatically re-minted from the logged-in Reddit browser cookie set (`reddit_session`, with `token_v2`/CSRF refresh fallbacks)
- no inbound TCP port and no Telegram webhook
- first Matrix sync establishes a checkpoint and deliberately does not replay history

Not implemented yet: media, reactions, edits/deletes, read receipts, typing, starting a brand-new Reddit DM from Telegram.

## Configuration values

`reddit2tg` needs four account-specific values:

| Config key | Meaning |
| --- | --- |
| `reddit.session` | Cookie-header-shaped Reddit browser session containing `reddit_session`; `token_v2` and `csrf_token` are used when available |
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

`reddit2tg` does not need your Reddit password. Current Reddit Chat authentication is refreshed through `POST https://www.reddit.com/svc/shreddit/token`. The bridge uses cookies from an already logged-in Reddit browser session. `reddit_session` is required. If Firefox also has a current `token_v2`, the bridge can use it directly; otherwise it uses `csrf_token` when present or attempts to obtain CSRF from Reddit `/login/` before calling `/svc/shreddit/token`.

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

The helper finds the first Firefox `cookies.sqlite`, makes a private temporary copy together with its WAL/SHM files, selects the newest Reddit values for `reddit_session`, `token_v2`, `csrf_token`, `loid`, `session_tracker` and `edgebucket` that actually exist, and puts the resulting Cookie-header-shaped string directly into the X11 clipboard. It does not print the secret values. It prints only cookie names and their lengths.

If Firefox has more than one profile, pass the desired database explicitly:

```sh
bash scripts/firefox-reddit-cookies.sh /home/peva/.mozilla/firefox/h7v1hpbn.default-esr/cookies.sqlite
```

The clipboard will contain a value shaped like this:

```text
reddit_session=...; token_v2=...; loid=...; session_tracker=...; edgebucket=...
```

Paste that complete string between the quotes of `reddit.session`:

```toml
[reddit]
session = "reddit_session=...; token_v2=...; loid=...; session_tracker=...; edgebucket=..."
homeserver = "https://matrix.redditspace.com"
```

`reddit_session` must be present. `csrf_token` is not mandatory in Firefox: `reddit2tg` first tries a current `token_v2`, then the configured `csrf_token` if available, and finally attempts to obtain CSRF from Reddit `/login/`. Additional Reddit cookies are retained because they make the request resemble the established browser session. Do not commit or share this value.

### 3. Manual browser method

If the Firefox helper cannot be used, copy the available Reddit cookies from a logged-in `www.reddit.com` session. `reddit_session` is mandatory; `token_v2` or `csrf_token` are useful when present. Construct a normal Cookie header, for example:

```text
reddit_session=VALUE1; token_v2=VALUE2
```

For Chrome, Chromium, Brave or Edge the values are under **F12 -> Application -> Storage -> Cookies -> https://www.reddit.com**. For Firefox they are under **F12 -> Storage -> Cookies -> https://www.reddit.com**. Cookie values are credentials; do not paste them into issues, screenshots, shell history or chat.

### 4. What reddit2tg does with the cookies

At authentication time `reddit2tg` first probes a saved `token_v2` against Matrix when one exists. If Matrix rejects it or it is absent, the bridge obtains CSRF from the `csrf_token` cookie or from Reddit `/login/`, then sends it to `POST /svc/shreddit/token` together with the saved Reddit cookies and Reddit-style Origin/Referer headers. The resulting Matrix Chat JWT is verified with `https://matrix.redditspace.com/_matrix/client/v3/account/whoami`.

The Matrix token is short-lived and is refreshed automatically. When Reddit eventually expires or invalidates the browser session itself, repeat the cookie-extraction step and replace `reddit.session`. Logging out of Reddit sessions or changing account security settings may invalidate it earlier.

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
- an existing `token_v2` works, or Reddit `/svc/shreddit/token` can mint a Matrix Chat token
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

Do not simultaneously run another process using `getUpdates` with the same Telegram bot token. Bot API updates are a single stream and competing consumers will interfere with each other.

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

The SQLite database does not need to store either credential in v0.1.

The Reddit Chat protocol used here is not an official public Reddit API and can change without notice.

## Verification status

The `amd64` MUSL build has been verified locally on Debian 13 as a static-PIE executable. `reddit2tg 0.1.4` has also been verified end-to-end with a real Reddit Chat DM mapped to a Telegram forum topic and replies relayed back to Reddit. The `arm64` and `armhf` release targets are built remotely by GitHub Actions and should be validated from the first multi-architecture workflow run before publishing the release tag.

## References

- Telegram bots introduction: https://core.telegram.org/bots
- Telegram Bot API: https://core.telegram.org/bots/api
- Telegram bot FAQ / update delivery: https://core.telegram.org/bots/faq
- Telegram forum topics: https://core.telegram.org/api/forum

### Debugging Telegram topics

Version 0.1.4 keeps the INFO-level diagnostics for Telegram forum-topic routing and also resolves missing DM identities through Matrix `joined_members`. Existing generic `Reddit chat` topics are renamed automatically when the Reddit identity becomes available. Successful Telegram -> Reddit sends now log the resulting Matrix event ID. Version 0.1.3 added the original topic diagnostics. When a Reddit room creates a topic, the log prints both `matrix_room_id` and the returned `telegram_thread_id`. When the operator replies in Telegram, the log prints the incoming `message_thread_id` and `is_topic_message`. `sendMessage` also warns if Telegram returns a different or missing thread ID than the one requested.

This makes it possible to verify that the SQLite mapping and Telegram's actual topic IDs match.
