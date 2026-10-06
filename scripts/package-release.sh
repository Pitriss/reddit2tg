#!/bin/sh
set -eu

BIN=${1:-target/x86_64-unknown-linux-musl/release/reddit2tg}
DIST=${DIST:-dist}
ARCH=${ARCH:-amd64}
TARGET_NAME=${TARGET_NAME:-linux-x86_64-musl}

[ -f "$BIN" ] || { echo "binary not found: $BIN" >&2; exit 1; }
command -v dpkg-deb >/dev/null 2>&1 || { echo "dpkg-deb not found" >&2; exit 1; }
command -v tar >/dev/null 2>&1 || { echo "tar not found" >&2; exit 1; }

VERSION=$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -n 1)
[ -n "$VERSION" ] || { echo "cannot read version from Cargo.toml" >&2; exit 1; }

WORK=$(mktemp -d "${TMPDIR:-/tmp}/reddit2tg-package.XXXXXX")
trap 'rm -rf "$WORK"' EXIT HUP INT TERM
mkdir -p "$DIST"

TAR_ROOT="reddit2tg-${VERSION}-${TARGET_NAME}"
mkdir -p "$WORK/$TAR_ROOT/etc/init.d" "$WORK/$TAR_ROOT/etc/systemd/system" "$WORK/$TAR_ROOT/scripts"
install -m 0755 "$BIN" "$WORK/$TAR_ROOT/reddit2tg"
install -m 0644 README.md "$WORK/$TAR_ROOT/README.md"
install -m 0644 LICENSE "$WORK/$TAR_ROOT/LICENSE"
install -m 0644 config.example.toml "$WORK/$TAR_ROOT/config.example.toml"
install -m 0755 etc/init.d/reddit2tg "$WORK/$TAR_ROOT/etc/init.d/reddit2tg"
install -m 0644 etc/systemd/system/reddit2tg.service "$WORK/$TAR_ROOT/etc/systemd/system/reddit2tg.service"
install -m 0755 scripts/install-devuan.sh "$WORK/$TAR_ROOT/scripts/install-devuan.sh"

SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct 2>/dev/null || date +%s)}
export SOURCE_DATE_EPOCH
TAR_FILE="$DIST/reddit2tg-${VERSION}-${TARGET_NAME}.tar.gz"
tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner -C "$WORK" -czf "$TAR_FILE" "$TAR_ROOT"

DEB_ROOT="$WORK/deb"
mkdir -p "$DEB_ROOT/DEBIAN" "$DEB_ROOT/usr/bin" "$DEB_ROOT/usr/share/doc/reddit2tg/examples" "$DEB_ROOT/etc/init.d" "$DEB_ROOT/lib/systemd/system"
install -m 0755 "$BIN" "$DEB_ROOT/usr/bin/reddit2tg"
install -m 0644 README.md "$DEB_ROOT/usr/share/doc/reddit2tg/README.md"
install -m 0644 LICENSE "$DEB_ROOT/usr/share/doc/reddit2tg/LICENSE"
install -m 0644 config.example.toml "$DEB_ROOT/usr/share/doc/reddit2tg/examples/config.toml"
install -m 0755 etc/init.d/reddit2tg "$DEB_ROOT/etc/init.d/reddit2tg"
install -m 0644 etc/systemd/system/reddit2tg.service "$DEB_ROOT/lib/systemd/system/reddit2tg.service"

cat > "$DEB_ROOT/DEBIAN/control" <<EOF_CONTROL
Package: reddit2tg
Version: $VERSION
Section: net
Priority: optional
Architecture: $ARCH
Maintainer: Pitriss <5106460+Pitriss@users.noreply.github.com>
Depends: adduser, lsb-base
Description: self-hosted Reddit Chat to Telegram bridge
 reddit2tg bridges Reddit Chat direct messages to Telegram forum topics
 and relays Telegram topic replies back to Reddit Chat.
EOF_CONTROL

cat > "$DEB_ROOT/DEBIAN/postinst" <<'EOF_POSTINST'
#!/bin/sh
set -e

if ! getent group reddit2tg >/dev/null 2>&1; then
  addgroup --system reddit2tg >/dev/null
fi
if ! getent passwd reddit2tg >/dev/null 2>&1; then
  adduser --system --ingroup reddit2tg --home /var/lib/reddit2tg --no-create-home --shell /usr/sbin/nologin reddit2tg >/dev/null
fi

install -d -o reddit2tg -g reddit2tg -m 0750 /var/lib/reddit2tg
install -d -o root -g reddit2tg -m 0750 /etc/reddit2tg
if [ ! -e /etc/reddit2tg/config.toml ]; then
  install -o root -g reddit2tg -m 0640 /usr/share/doc/reddit2tg/examples/config.toml /etc/reddit2tg/config.toml
fi
if [ ! -e /var/log/reddit2tg.log ]; then
  install -o reddit2tg -g reddit2tg -m 0640 /dev/null /var/log/reddit2tg.log
else
  chown reddit2tg:reddit2tg /var/log/reddit2tg.log
  chmod 0640 /var/log/reddit2tg.log
fi

if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi

echo "reddit2tg installed but not enabled or started."
echo "Edit /etc/reddit2tg/config.toml, run: reddit2tg --config /etc/reddit2tg/config.toml check"
echo "Then enable the service with either: update-rc.d reddit2tg defaults, or: systemctl enable reddit2tg.service"
exit 0
EOF_POSTINST
chmod 0755 "$DEB_ROOT/DEBIAN/postinst"

cat > "$DEB_ROOT/DEBIAN/prerm" <<'EOF_PRERM'
#!/bin/sh
set -e

if [ "$1" = remove ] || [ "$1" = deconfigure ]; then
  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemctl stop reddit2tg.service >/dev/null 2>&1 || true
  elif command -v invoke-rc.d >/dev/null 2>&1; then
    invoke-rc.d reddit2tg stop >/dev/null 2>&1 || true
  fi
fi
exit 0
EOF_PRERM
chmod 0755 "$DEB_ROOT/DEBIAN/prerm"

cat > "$DEB_ROOT/DEBIAN/postrm" <<'EOF_POSTRM'
#!/bin/sh
set -e

if [ "$1" = purge ]; then
  rm -f /etc/reddit2tg/config.toml /var/log/reddit2tg.log
  rmdir /etc/reddit2tg 2>/dev/null || true
fi
if command -v update-rc.d >/dev/null 2>&1 && [ "$1" = purge ]; then
  update-rc.d -f reddit2tg remove >/dev/null 2>&1 || true
fi
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi
exit 0
EOF_POSTRM
chmod 0755 "$DEB_ROOT/DEBIAN/postrm"

DEB_FILE="$DIST/reddit2tg_${VERSION}_${ARCH}.deb"
dpkg-deb --root-owner-group --build "$DEB_ROOT" "$DEB_FILE" >/dev/null

printf '%s\n' "$TAR_FILE"
printf '%s\n' "$DEB_FILE"
