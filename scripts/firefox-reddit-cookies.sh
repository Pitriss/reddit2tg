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

# Build the Cookie header Firefox would use for https://www.reddit.com/chat/.
# The old helper copied only a small whitelist. Reddit's /chat/ bootstrap can
# require additional browser-session cookies, so keep every unexpired cookie
# whose domain and path are applicable to this request. Longer paths go first,
# matching normal browser cookie ordering. Duplicate cookie names are retained.
COOKIE_SQL="SELECT group_concat(pair, '; ') FROM (SELECT name || '=' || value AS pair FROM moz_cookies WHERE expiry > strftime('%s','now') AND (host = 'www.reddit.com' OR host = '.reddit.com' OR host = 'reddit.com') AND substr('/chat/', 1, length(path)) = path ORDER BY length(path) DESC, creationTime ASC);"
INFO_SQL="SELECT name || ': ' || length(value) || ' chars' FROM moz_cookies WHERE expiry > strftime('%s','now') AND (host = 'www.reddit.com' OR host = '.reddit.com' OR host = 'reddit.com') AND substr('/chat/', 1, length(path)) = path ORDER BY length(path) DESC, creationTime ASC;"

COOKIE_HEADER=$(sqlite3 "$TMP/cookies.sqlite" "$COOKIE_SQL")
printf '%s' "$COOKIE_HEADER" | grep -q 'reddit_session=' || { echo "reddit_session cookie not found for https://www.reddit.com/chat/" >&2; exit 1; }
printf '%s' "$COOKIE_HEADER" | xclip -selection clipboard

echo "Reddit /chat/ cookie header copied to X11 clipboard from: $DB"
echo "Included cookies (values are not printed):"
sqlite3 "$TMP/cookies.sqlite" "$INFO_SQL"
