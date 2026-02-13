#!/bin/sh
set -e

if command -v iol >/dev/null 2>&1; then
  iol snapshot catchup || true
fi

CRON_FILE=/etc/cron.d/iol-snapshot
CRON_TZ_VALUE=${IOL_MARKET_TZ:-America/Argentina/Buenos_Aires}
COPEN_TIME_VALUE=${IOL_MARKET_OPEN_TIME:-11:00}
CLOSE_TIME_VALUE=${IOL_MARKET_CLOSE_TIME:-18:00}
CRON_HOUR=$(echo "$CLOSE_TIME_VALUE" | cut -d: -f1)
CRON_MIN=$(echo "$CLOSE_TIME_VALUE" | cut -d: -f2)
INTERVAL_MIN=${IOL_SNAPSHOT_INTERVAL_MIN:-5}

{
  echo "SHELL=/bin/sh"
  echo "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
  echo "CRON_TZ=${CRON_TZ_VALUE}"
  # Intraday updates: one row per day (snapshot_date = today) updated every N minutes while market is open.
  echo "*/${INTERVAL_MIN} * * * 1-5 root iol snapshot run --mode live --only-market-open --source cron_intraday >> /var/log/cron.log 2>&1"
  # Close snapshot (keeps the strongest point near close if you also run manual snapshots).
  echo "${CRON_MIN} ${CRON_HOUR} * * 1-5 root iol snapshot run --mode close --source cron_close >> /var/log/cron.log 2>&1"
} > "$CRON_FILE"

chmod 0644 "$CRON_FILE"

touch /var/log/cron.log

cron -f
