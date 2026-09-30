#!/usr/bin/env bash
# build-push: build the ably-server and ably-loadgen images for linux/amd64,
# tag them with the git sha, smoke-test them, push them to the image registry
# (IMAGE_REGISTRY) and record the tags in STATE. Also mirrors the third party
# images to that registry.
#
#   bench/aws/build-push.sh             both images
#   bench/aws/build-push.sh server      one of server | loadgen | all
#   bench/aws/build-push.sh mirror      copy the third party images (postgres, nats,
#                                       prometheus, grafana, exporters) from Docker Hub to
#                                       IMAGE_REGISTRY; then set BASE_IMAGE_REGISTRY to it
#
# IMAGE_REGISTRY_KIND decides where the images go:
#   ghcr  (the default) ghcr.io/<github-owner>; with no IMAGE_REGISTRY the owner is your GitHub
#         login (gh api user). Login: gh auth token | docker login ghcr.io (the gh token
#         needs the write:packages scope: gh auth refresh -h github.com -s write:packages).
#         GHCR_USER is the login name (default: gh api user --jq .login).
#   ecr   an ECR registry. Login: aws ecr get-login-password. Missing repositories are
#         created (tagged when the role may tag, untagged when it may not).
#   none  no registry: the images are built locally and nothing is pushed
#         (IMAGE_REGISTRY_KIND=none).
# A new ghcr package starts private. Set each package to public once after the first push
# (https://github.com/users/<owner>/packages), or give the boxes GHCR_PULL_TOKEN.
#
# Without credentials for the registry, or with PUSH=0, the images are built locally and the
# push is skipped. A dirty working tree gets the tag <sha>-dirty and is never pushed unless
# ALLOW_DIRTY=1, so a published tag always names real commits.
#
# Optional: IMAGE_TAG, IMAGE_PLATFORM (linux/amd64), PUSH, ALLOW_DIRTY.
SCRIPT_NAME=build-push
if [ "${PUSH:-1}" = 0 ]; then export NO_AWS=1; fi
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq git
which_images=${1:-all}
case "$which_images" in server | loadgen | all | mirror) ;; *) die "usage: build-push.sh [server|loadgen|all|mirror]" ;; esac
: "${IMAGE_PLATFORM:=linux/amd64}"
: "${PUSH:=1}"
state_init
cd "$REPO_ROOT"

# registry_login: log the local Docker in to the registry. Dies when it cannot.
registry_login() {
  case "$IMAGE_REGISTRY_KIND" in
    ghcr)
      if is_dry; then
        printf 'DRYRUN %s\n' "gh auth token | docker login ghcr.io -u ${GHCR_USER:-<gh login>} --password-stdin" >&2
        return 0
      fi
      need_cmd gh docker
      : "${GHCR_USER:=$(gh api user --jq .login)}"
      [ -n "$GHCR_USER" ] || die "could not read the GitHub login (gh api user); set GHCR_USER or run gh auth login"
      # shellcheck disable=SC2005  # the token is read from gh and passed on stdin, never as an argument
      echo "$(gh auth token)" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
      ;;
    ecr)
      require_env AWS_ACCOUNT_ID AWS_REGION
      if is_dry; then
        printf 'DRYRUN %s\n' "aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $REGISTRY_HOST" >&2
      else
        aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY_HOST"
      fi
      ;;
  esac
}

push_hint() {
  case "$IMAGE_REGISTRY_KIND" in
    ghcr) echo "If the push was refused: the gh token needs write:packages (gh auth refresh -h github.com -s write:packages), and the owner in IMAGE_REGISTRY must be the login or an organisation you may push packages to." ;;
    ecr) echo "If the push was refused: the role needs ecr:BatchCheckLayerAvailability, InitiateLayerUpload, UploadLayerPart, CompleteLayerUpload and PutImage on the repository." ;;
  esac
}

# push_image <local ref> <remote ref>: tag and push; on failure say what to check.
push_image() {
  _ext "" docker tag "$1" "$2"
  _ext "" docker push "$2" || {
    push_hint >&2
    die "push of $2 failed"
  }
}

# ---------------------------------------------------------------- mirror
if [ "$which_images" = mirror ]; then
  require_registry
  [ "$BASE_IMAGE_REGISTRY" = docker.io ] ||
    die "BASE_IMAGE_REGISTRY=$BASE_IMAGE_REGISTRY is set: unset it (or set it to docker.io) for 'mirror', which copies from Docker Hub"
  if ! is_dry; then need_cmd docker; fi
  registry_login
  seen=" "
  n=0
  for src in "${BASE_IMAGE_SOURCES[@]}"; do
    case "$seen" in *" $src "*) continue ;; esac
    seen+="$src "
    name=${src##*/} # <name>:<tag>; the owner in the source path is dropped (flat layout)
    dest="$IMAGE_REGISTRY/$name"
    if [ "$IMAGE_REGISTRY_KIND" = ecr ]; then
      rc=0
      ecr_ensure_repo "${REGISTRY_PATH:+$REGISTRY_PATH/}${name%%:*}" || rc=$?
      [ "$rc" = 0 ] || die "cannot provide an ECR repository for $name"
    fi
    _ext "" docker pull --platform "$IMAGE_PLATFORM" "$src"
    push_image "$src" "$dest"
    n=$((n + 1))
  done
  log "mirrored $n images to $IMAGE_REGISTRY. Set BASE_IMAGE_REGISTRY=$IMAGE_REGISTRY for the fleet scripts."
  if [ "$IMAGE_REGISTRY_KIND" = ghcr ]; then
    log "each new ghcr package is private: set them to public (https://github.com/users/${IMAGE_REGISTRY#ghcr.io/}/packages) or give the boxes GHCR_PULL_TOKEN"
  fi
  log_line build-push "mirrored $n third party images to $IMAGE_REGISTRY ($IMAGE_PLATFORM)" "export BASE_IMAGE_REGISTRY=$IMAGE_REGISTRY before 20-postgres.sh and the fleet scripts"
  exit 0
fi

sha=$(git rev-parse --short=7 HEAD)
dirty=""
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then dirty="-dirty"; fi
tag=${IMAGE_TAG:-$sha$dirty}
log "building $which_images at $tag for $IMAGE_PLATFORM${dirty:+ (working tree is dirty)}"

build() { # <key> <dockerfile>
  local key=$1 file=$2
  _ext "" docker buildx build --platform "$IMAGE_PLATFORM" -f "$file" -t "$key:$tag" --load "$REPO_ROOT"
}

smoke() { # <key> <command...>
  local key=$1
  shift
  # The server and bench binaries print usage and exit 0 or 2 for --help; both mean "it starts".
  local rc=0 out
  out=$(_ext "" docker run --rm --platform "$IMAGE_PLATFORM" "$key:$tag" "$@" 2>&1) || rc=$?
  if is_dry; then return 0; fi
  if [ "$rc" -gt 2 ] || [ -z "$out" ]; then
    printf '%s\n' "$out" >&2
    die "smoke test failed for $key:$tag ($* exited $rc)"
  fi
  log "smoke ok: $key:$tag $* (exit $rc)"
}

can_push=0
if [ "$PUSH" = 1 ]; then
  case "$IMAGE_REGISTRY_KIND" in
    none) log "IMAGE_REGISTRY_KIND=none: building locally, nothing to push to" ;;
    ghcr)
      if is_dry; then
        can_push=1
      elif command -v gh >/dev/null 2>&1 && gh auth token >/dev/null 2>&1; then
        can_push=1
      else
        log "gh is missing or not signed in: building locally, push skipped (gh auth login)"
      fi
      ;;
    ecr)
      if is_dry; then
        can_push=1
      elif [ -n "${AWS_ACCOUNT_ID:-}" ] && [ -n "${AWS_REGION:-}" ] && command -v aws >/dev/null 2>&1 &&
        [ "$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)" = "$AWS_ACCOUNT_ID" ]; then
        can_push=1
      else
        log "no usable AWS credentials for the expected account: building locally, push skipped"
      fi
      ;;
  esac
  if [ "$can_push" = 1 ] && [ -n "$dirty" ] && [ "${ALLOW_DIRTY:-0}" != 1 ]; then
    log "working tree is dirty: push skipped (commit first, or ALLOW_DIRTY=1)"
    can_push=0
  fi
fi

push() { # <key> <ably-server|ably-loadgen>
  local key=$1 name=$2 uri
  uri=$(image_ref "$name" "$tag")
  if [ "$IMAGE_REGISTRY_KIND" = ecr ]; then
    local rc=0
    ecr_ensure_repo "$(registry_repo_path "$name")" || rc=$?
    [ "$rc" = 0 ] || die "cannot provide the ECR repository for $name"
  fi
  push_image "$key:$tag" "$uri"
  if [ "$IMAGE_REGISTRY_KIND" = ghcr ] && ! is_dry && [ -z "${GHCR_PULL_TOKEN:-}" ]; then
    if ghcr_is_public "$(registry_repo_path "$name")" "$tag"; then
      log "ghcr.io package $name is public: the boxes can pull it without a login"
    else
      log "WARNING: ghcr.io package $name is private (every new package starts private). Set it to public once: https://github.com/users/${IMAGE_REGISTRY#ghcr.io/}/packages (open $name, Package settings, Change visibility), or export GHCR_PULL_TOKEN for the boxes."
    fi
  fi
}

record() { # <key> <pushed 0|1> <ably-server|ably-loadgen>
  local key=$1 pushed=$2 name=$3 id ref=""
  if is_dry; then id=dry-run; else id=$(docker image inspect "$key:$tag" --format '{{.Id}}' 2>/dev/null || echo unknown); fi
  if [ "$IMAGE_REGISTRY_KIND" != none ] && { [ -n "$IMAGE_REGISTRY" ] || [ "$pushed" = 1 ]; }; then ref=$(image_ref "$name" "$tag"); fi
  _state_update '.images[$k] = {tag:$t, ref:$ref, registry_kind:$rk, git_sha:$sha, dirty:($d != ""), platform:$p, image_id:$id, pushed:($pu == "1"), at:$at}' \
    --arg k "$key" --arg t "$tag" --arg ref "$ref" --arg rk "$IMAGE_REGISTRY_KIND" --arg sha "$sha" --arg d "$dirty" --arg p "$IMAGE_PLATFORM" --arg id "$id" --arg pu "$pushed" --arg at "$(date -u +%FT%TZ)"
}

if [ "$can_push" = 1 ]; then
  require_registry # in this shell, so an owner taken from the GitHub login is kept
  registry_login
fi

if [ "$which_images" = server ] || [ "$which_images" = all ]; then
  build ably-server Dockerfile
  smoke ably-server --help
  pushed=0
  if [ "$can_push" = 1 ]; then
    push ably-server ably-server
    pushed=1
  fi
  record ably-server "$pushed" ably-server
fi
if [ "$which_images" = loadgen ] || [ "$which_images" = all ]; then
  build ably-loadgen bench/aws/Dockerfile.loadgen
  smoke ably-loadgen sh -c 'cat /usr/local/bin/BUILT'
  pushed=0
  if [ "$can_push" = 1 ]; then
    push ably-loadgen ably-loadgen
    pushed=1
  fi
  record ably-loadgen "$pushed" ably-loadgen
fi
log_line build-push "built $which_images at $tag ($IMAGE_PLATFORM); $([ "$can_push" = 1 ] && echo "pushed to $IMAGE_REGISTRY_KIND ($IMAGE_REGISTRY)" || echo "push skipped")" "40-nodes.sh with SERVER_TAG=$tag (after 10-network through 30-nats)"
