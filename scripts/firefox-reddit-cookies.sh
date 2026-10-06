#!/bin/sh
set -eu

DB=${1:-}
if [ -z "$DB" ]; then
  DB=$(find "$HOME/.mozilla/firefox" -name cookies.sqlite -type f -print 2>/dev/null | head -n 1 || true)
fi
[ -n "$DB" ] || { echo "Firefox cookies.sqlite not found" >&2; exit 1; }
[ -r "$DB" ] || { echo "Cannot read $DB" >&2; exit 1; }
command -v sqlite3 >/dev/null 2>&1 || { echo "sqlite3 is required" >&2; exit 1; }
command -v xclip >/dev/null 2>&1 || { echo "xclip is required" >&2; exit 1; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/reddit2tg-firefox.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
cp "$DB" "$TMP/cookies.sqlite"
[ ! -f "$DB-wal" ] || cp "$DB-wal" "$TMP/cookies.sqlite-wal"
[ ! -f "$DB-shm" ] || cp "$DB-shm" "$TMP/cookies.sqlite-shm"

SQL="WITH ranked AS (SELECT name,value,lastAccessed,ROW_NUMBER() OVER (PARTITION BY name ORDER BY lastAccessed DESC) AS rn FROM moz_cookies WHERE host LIKE '%reddit.com%' AND name IN ('reddit_session','token_v2','csrf_token','loid','session_tracker','edgebucket')) SELECT group_concat(name || '=' || value, '; ') FROM ranked WHERE rn=1;"
COOKIE_HEADER=$(sqlite3 "$TMP/cookies.sqlite" "$SQL")
printf '%s' "$COOKIE_HEADER" | grep -q 'reddit_session=' || { echo "reddit_session cookie not found" >&2; exit 1; }
printf '%s' "$COOKIE_HEADER" | xclip -selection clipboard

echo "Reddit cookie header copied to X11 clipboard from: $DB"
printf '%s' "$COOKIE_HEADER" | grep -q 'csrf_token=' || echo "note: csrf_token cookie is absent; reddit2tg will try token_v2 or obtain CSRF from Reddit /login/"
sqlite3 "$TMP/cookies.sqlite" "WITH ranked AS (SELECT name,value,lastAccessed,ROW_NUMBER() OVER (PARTITION BY name ORDER BY lastAccessed DESC) AS rn FROM moz_cookies WHERE host LIKE '%reddit.com%' AND name IN ('reddit_session','token_v2','csrf_token','loid','session_tracker','edgebucket')) SELECT name || ': ' || length(value) || ' chars' FROM ranked WHERE rn=1 ORDER BY name;"
