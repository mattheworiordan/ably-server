# bench_load_secrets: no secret is written in this user-data. The script that
# launched the box copies the secrets it needs to /home/ec2-user/.bench-secrets.env
# over SSH once the box is up (lib.sh deliver_boot_secrets); this waits for that
# file, loads it into the environment without tracing it, and deletes it. Calling
# it again does nothing. The variables are BENCH_*; the role part passes them to
# containers by name, so no secret appears on a command line.
bench_load_secrets() {
  local f=${BENCH_SECRETS_FILE:-/home/ec2-user/.bench-secrets.env} traced=0
  if [ -n "${BENCH_SECRETS_LOADED:-}" ]; then return 0; fi
  case $- in *x*) traced=1 ;; esac
  set +x
  for _ in $(seq 1 600); do
    if [ -s "$f" ]; then break; fi
    sleep 2
  done
  if [ ! -s "$f" ]; then
    echo "no secrets were delivered to $f in 20 minutes: the launching script copies them over SSH" >&2
    return 1
  fi
  set -a
  # shellcheck disable=SC1090
  . "$f"
  set +a
  rm -f "$f"
  BENCH_SECRETS_LOADED=1
  if [ "$traced" = 1 ]; then set -x; fi
}
