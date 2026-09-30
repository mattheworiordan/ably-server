#!/usr/bin/env bash
# shellcheck shell=bash
# Ably internal helper, sourced by lib.sh. Anyone else exports their own AWS
# credentials (or sets AWS_PROFILE) and never reaches this code.
#
# When no credentials are in the environment and `ablyctl` is on PATH, mint
# them from the cached SSO sign-in:
#
#   eval "$(ablyctl aws env --account "$ABLYCTL_ACCOUNT" [--aws-role "$AWS_SSO_ROLE"])"
#
# ABLYCTL_ACCOUNT defaults to the dev account name. --aws-role is passed only
# when AWS_SSO_ROLE is set; otherwise ablyctl uses its default role (Operator).
# Neither value is an account id.
#
# ablyctl detects agents (CLAUDECODE, CODEX_CI, CODEX_SANDBOX, CURSOR_AGENT,
# GEMINI_CLI): it then ignores --aws-role and chains into the AgentOperator
# role of the account, which the dev account does not have. This helper does
# not touch those variables and never tries to get around the detection.
# If ablyctl cannot mint credentials the script exits and names both supported
# paths.

ensure_credentials() {
  if [ -n "${AWS_ACCESS_KEY_ID:-}" ] || [ -n "${AWS_PROFILE:-}" ]; then return 0; fi
  command -v ablyctl >/dev/null 2>&1 || return 0
  local account=${ABLYCTL_ACCOUNT:-dev} out rc=0
  local -a tlimit=() role_args=()
  if command -v timeout >/dev/null 2>&1; then tlimit=(timeout 60); fi
  if [ -n "${AWS_SSO_ROLE:-}" ]; then role_args=(--aws-role "$AWS_SSO_ROLE"); fi
  out=$("${tlimit[@]}" ablyctl aws env --account "$account" "${role_args[@]}" 2>/dev/null) || rc=$?
  if [ "$rc" != 0 ] || [ -z "$out" ]; then
    {
      echo "ablyctl could not mint credentials for '$account'. Either the sign-in expired, or an AI agent is running this:"
      echo "ablyctl then chains into the AgentOperator role, which is not provisioned in that account (STS AccessDenied)."
      echo "Supported paths: (a) infrastructure provisions AgentOperator in the account; (b) mint the credentials in your"
      echo "own terminal ('eval \"\$(ablyctl aws env --account $account)\"', finish the browser login) and export"
      echo "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN in the shell that runs these scripts."
    } >&2
    exit 1
  fi
  eval "$out"
}
