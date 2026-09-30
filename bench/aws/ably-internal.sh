#!/usr/bin/env bash
# shellcheck shell=bash
# Ably internal helper, sourced by lib.sh. Anyone else exports their own AWS
# credentials (or sets AWS_PROFILE) and never reaches this code.
#
# When no credentials are in the environment and `ablyctl` is on PATH, mint
# them from the cached SSO sign-in:
#
#   eval "$(ablyctl aws env --account "$ABLYCTL_ACCOUNT" --aws-role "$AWS_SSO_ROLE")"
#
# ABLYCTL_ACCOUNT defaults to the dev account name; AWS_SSO_ROLE defaults to
# ablyctl's own default role (Operator). Neither value is an account id.
# If ablyctl cannot mint credentials (an expired sign-in asks for a browser
# device-code login, which cannot happen unattended) the script exits with
# one line saying what to run.

ensure_credentials() {
  if [ -n "${AWS_ACCESS_KEY_ID:-}" ] || [ -n "${AWS_PROFILE:-}" ]; then return 0; fi
  command -v ablyctl >/dev/null 2>&1 || return 0
  local account=${ABLYCTL_ACCOUNT:-dev} role=${AWS_SSO_ROLE:-Operator} out rc=0
  local -a limit=()
  if command -v timeout >/dev/null 2>&1; then limit=(timeout 60); fi
  out=$("${limit[@]}" ablyctl aws env --account "$account" --aws-role "$role" 2>/dev/null) || rc=$?
  if [ "$rc" != 0 ] || [ -z "$out" ]; then
    echo "ablyctl could not mint credentials (sign-in expired?): run 'ablyctl aws env --account $account --aws-role $role' in a terminal, finish the browser login, then re-run." >&2
    exit 1
  fi
  eval "$out"
}
