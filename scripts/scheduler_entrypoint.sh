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
  # Open snapshot: capture opening prices at 11:05 AM
  OPEN_HOUR=$(echo "$COPEN_TIME_VALUE" | cut -d: -f1)
  OPEN_MIN=$(echo "$COPEN_TIME_VALUE" | cut -d: -f2)
  OPEN_MIN_PLUS5=$(( (OPEN_MIN + 5) % 60 ))
  OPEN_HOUR_FINAL=$(( OPEN_HOUR + (OPEN_MIN + 5) / 60 ))
  echo "${OPEN_MIN_PLUS5} ${OPEN_HOUR_FINAL} * * 1-5 root iol snapshot run --mode live --source cron_open >> /var/log/cron.log 2>&1"
  # Intraday updates: one row per day (snapshot_date = today) updated every N minutes while market is open.
  echo "*/${INTERVAL_MIN} * * * 1-5 root iol snapshot run --mode live --only-market-open --source cron_intraday >> /var/log/cron.log 2>&1"
  # Close snapshot (keeps the strongest point near close if you also run manual snapshots).
  echo "${CRON_MIN} ${CRON_HOUR} * * 1-5 root iol snapshot run --mode close --source cron_close >> /var/log/cron.log 2>&1"
  # Engine refresh: run regime+macro engines 15 min after market close (local DB only, no external calls).
  ENGINE_MIN=$(( (CRON_MIN + 15) % 60 ))
  ENGINE_HOUR_OFFSET=$(( (CRON_MIN + 15) / 60 ))
  ENGINE_HOUR=$(( CRON_HOUR + ENGINE_HOUR_OFFSET ))
  echo "${ENGINE_MIN} ${ENGINE_HOUR} * * 1-5 root iol engines run-all --skip-external --skip-smart-money >> /var/log/cron.log 2>&1"
  # Pivot detection: run 2 min after engine refresh (post-close only)
  PIVOT_MIN=$(( (ENGINE_MIN + 2) % 60 ))
  PIVOT_HOUR_OFFSET=$(( (ENGINE_MIN + 2) / 60 ))
  PIVOT_HOUR=$(( ENGINE_HOUR + PIVOT_HOUR_OFFSET ))
  echo "${PIVOT_MIN} ${PIVOT_HOUR} * * 1-5 root iol data detect-pivots >> /var/log/cron.log 2>&1"
  # Opportunities pipeline: run 5 min after engine refresh to populate advisor_opportunity_candidates
  OPP_MIN=$(( (ENGINE_MIN + 5) % 60 ))
  OPP_HOUR_OFFSET=$(( (ENGINE_MIN + 5) / 60 ))
  OPP_HOUR=$(( ENGINE_HOUR + OPP_HOUR_OFFSET ))
  echo "${OPP_MIN} ${OPP_HOUR} * * 1-5 root iol advisor opportunities run --mode both --budget-ars 200000 --top 15 >> /var/log/cron.log 2>&1"
  # Swing live-step: 5 min after opportunities (uses advisor_opportunity_candidates for symbol selection)
  SWING_MIN=$(( (OPP_MIN + 5) % 60 ))
  SWING_HOUR_OFFSET=$(( (OPP_MIN + 5) / 60 ))
  SWING_HOUR=$(( OPP_HOUR + SWING_HOUR_OFFSET ))
  echo "${SWING_MIN} ${SWING_HOUR} * * 1-5 root iol simulate swing live-step --bots all >> /var/log/cron.log 2>&1"
  # Event live-step: 5 min after swing
  EVENT_MIN=$(( (SWING_MIN + 5) % 60 ))
  EVENT_HOUR_OFFSET=$(( (SWING_MIN + 5) / 60 ))
  EVENT_HOUR=$(( SWING_HOUR + EVENT_HOUR_OFFSET ))
  echo "${EVENT_MIN} ${EVENT_HOUR} * * 1-5 root iol simulate event live-step --bots all >> /var/log/cron.log 2>&1"
  # Daily bots live-step: 5 min after event bots
  STEP_MIN=$(( (EVENT_MIN + 5) % 60 ))
  STEP_HOUR_OFFSET=$(( (EVENT_MIN + 5) / 60 ))
  STEP_HOUR=$(( EVENT_HOUR + STEP_HOUR_OFFSET ))
  echo "${STEP_MIN} ${STEP_HOUR} * * 1-5 root iol simulate live-step --bots all >> /var/log/cron.log 2>&1"
} > "$CRON_FILE"

chmod 0644 "$CRON_FILE"

touch /var/log/cron.log

cron -f
