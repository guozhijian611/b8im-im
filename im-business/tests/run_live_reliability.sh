#!/bin/sh
set -eu

mode="${1:-all}"
case "$mode" in
  websocket|audit|cleanup|all) ;;
  *)
    echo "usage: $0 [websocket|audit|cleanup|all]" >&2
    exit 64
    ;;
esac

if [ "$mode" = "all" ]; then
  case "${QA_CLEANUP_AFTER:-1}" in
    0|1) ;;
    *)
      echo "QA_CLEANUP_AFTER must be 0 or 1" >&2
      exit 64
      ;;
  esac
fi

case "$mode" in
  websocket|all)
    : "${A_PASSWORD:?A_PASSWORD is required}"
    : "${B_PASSWORD:?B_PASSWORD is required}"
    : "${X_PASSWORD:?X_PASSWORD is required}"
    export QA_RUN_ID="${QA_RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}"
    export QA_MANIFEST="${QA_MANIFEST:-/tmp/b8im-im-reliability-${QA_RUN_ID}.json}"
    ;;
  audit|cleanup)
    : "${QA_MANIFEST:?QA_MANIFEST is required for $mode}"
    ;;
esac

run_websocket() {
  node tests/live_phase1_websocket.mjs
}

run_audit() {
  php tests/live_reliability_audit.php
}

run_cleanup() {
  QA_ALLOW_CLEANUP=1 php tests/live_reliability_cleanup.php
}

case "$mode" in
  websocket) run_websocket ;;
  audit) run_audit ;;
  cleanup) run_cleanup ;;
  all)
    run_websocket
    run_audit
    case "${QA_CLEANUP_AFTER:-1}" in
      1) run_cleanup ;;
      0) ;;
    esac
    ;;
esac

echo "QA_MANIFEST=$QA_MANIFEST"
