#!/usr/bin/env bash
# build-push: build the ably-server and ably-loadgen images for linux/amd64,
# tag them with the git sha, smoke-test them, push them to ECR when AWS
# credentials are present, and record the tags in STATE.
#
#   bench/aws/build-push.sh             both images
#   bench/aws/build-push.sh server      one of server | loadgen | all
#
# Without credentials (or with PUSH=0) the images are built locally and the
# push is skipped. A dirty working tree gets the tag <sha>-dirty and is never
# pushed unless ALLOW_DIRTY=1, so a published tag always names real commits.
#
# Needs for a push: AWS_ACCOUNT_ID, AWS_REGION (ECR_REGISTRY is derived).
# Optional: IMAGE_TAG, IMAGE_PLATFORM (linux/amd64), PUSH, ALLOW_DIRTY.
SCRIPT_NAME=build-push
if [ "${PUSH:-1}" = 0 ]; then export NO_AWS=1; fi
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq git
which_images=${1:-all}
case "$which_images" in server | loadgen | all) ;; *) die "usage: build-push.sh [server|loadgen|all]" ;; esac
: "${IMAGE_PLATFORM:=linux/amd64}"
: "${PUSH:=1}"
state_init
cd "$REPO_ROOT"

sha=$(git rev-parse --short=7 HEAD)
dirty=""
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then dirty="-dirty"; fi
tag=${IMAGE_TAG:-$sha$dirty}
log "building $which_images at $tag for $IMAGE_PLATFORM${dirty:+ (working tree is dirty)}"

build() { # <key> <ecr repo> <dockerfile>
  local key=$1 repo=$2 file=$3
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
  if is_dry; then
    can_push=1
  elif [ -n "${AWS_ACCOUNT_ID:-}" ] && [ -n "${AWS_REGION:-}" ] && command -v aws >/dev/null 2>&1 &&
    [ "$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)" = "$AWS_ACCOUNT_ID" ]; then
    can_push=1
  else
    log "no usable AWS credentials for the expected account: building locally, push skipped"
  fi
  if [ "$can_push" = 1 ] && [ -n "$dirty" ] && [ "${ALLOW_DIRTY:-0}" != 1 ]; then
    log "working tree is dirty: push skipped (commit first, or ALLOW_DIRTY=1)"
    can_push=0
  fi
fi

push() { # <key> <ecr repo>
  local key=$1 repo=$2 uri="$ECR_REGISTRY/$2:$tag"
  local have
  have=$(aws_r "" ecr describe-repositories --repository-names "$repo" --query 'repositories[0].repositoryName') || have=""
  if [ -z "$have" ]; then
    aws_w "" ecr create-repository --repository-name "$repo" --tags "Key=Project,Value=$PROJECT_TAG" "Key=Name,Value=$repo" >/dev/null
  fi
  _ext "" docker tag "$key:$tag" "$uri"
  _ext "" docker push "$uri"
}

record() { # <key> <pushed 0|1>
  local key=$1 pushed=$2 id
  if is_dry; then id=dry-run; else id=$(docker image inspect "$key:$tag" --format '{{.Id}}' 2>/dev/null || echo unknown); fi
  _state_update '.images[$k] = {tag:$t, git_sha:$sha, dirty:($d != ""), platform:$p, image_id:$id, pushed:($pu == "1"), at:$at}' \
    --arg k "$key" --arg t "$tag" --arg sha "$sha" --arg d "$dirty" --arg p "$IMAGE_PLATFORM" --arg id "$id" --arg pu "$pushed" --arg at "$(date -u +%FT%TZ)"
}

if [ "$can_push" = 1 ]; then
  require_env AWS_ACCOUNT_ID AWS_REGION
  if is_dry; then
    printf 'DRYRUN %s\n' "aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY" >&2
  else
    aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"
  fi
fi

if [ "$which_images" = server ] || [ "$which_images" = all ]; then
  build ably-server "$ECR_REPO_SERVER" Dockerfile
  smoke ably-server --help
  pushed=0
  if [ "$can_push" = 1 ]; then
    push ably-server "$ECR_REPO_SERVER"
    pushed=1
  fi
  record ably-server "$pushed"
fi
if [ "$which_images" = loadgen ] || [ "$which_images" = all ]; then
  build ably-loadgen "$ECR_REPO_LOADGEN" bench/aws/Dockerfile.loadgen
  smoke ably-loadgen sh -c 'cat /usr/local/bin/BUILT'
  pushed=0
  if [ "$can_push" = 1 ]; then
    push ably-loadgen "$ECR_REPO_LOADGEN"
    pushed=1
  fi
  record ably-loadgen "$pushed"
fi
log_line build-push "built $which_images at $tag ($IMAGE_PLATFORM); $([ "$can_push" = 1 ] && echo "pushed to ECR" || echo "push skipped")" "40-nodes.sh with SERVER_TAG=$tag (after 10-network through 30-nats)"
