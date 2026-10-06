#!/bin/sh
set -eu
BIN=${1:-./reddit2tg}
[ -f "$BIN" ] || { echo "binary not found: $BIN" >&2; exit 1; }
getent passwd reddit2tg >/dev/null 2>&1 || adduser --system --group --home /var/lib/reddit2tg --no-create-home reddit2tg
install -d -o reddit2tg -g reddit2tg -m 0750 /var/lib/reddit2tg
install -d -o root -g reddit2tg -m 0750 /etc/reddit2tg
if [ ! -e /var/log/reddit2tg.log ]; then
  install -o reddit2tg -g reddit2tg -m 0640 /dev/null /var/log/reddit2tg.log
else
  chown reddit2tg:reddit2tg /var/log/reddit2tg.log
  chmod 0640 /var/log/reddit2tg.log
fi
install -o root -g root -m 0755 "$BIN" /usr/local/bin/reddit2tg
install -o root -g root -m 0755 etc/init.d/reddit2tg /etc/init.d/reddit2tg
if [ ! -f /etc/reddit2tg/config.toml ]; then
  install -o root -g reddit2tg -m 0640 config.example.toml /etc/reddit2tg/config.toml
  echo "Edit /etc/reddit2tg/config.toml before starting the service."
fi
update-rc.d reddit2tg defaults
