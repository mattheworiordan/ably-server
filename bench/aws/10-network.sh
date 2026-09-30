#!/usr/bin/env bash
# 10-network: default VPC, one Availability Zone, one security group
# (all traffic inside the group, SSH from ADMIN_CIDR only) and, when images come from
# ECR (or CREATE_INSTANCE_PROFILE=1), an instance profile that may pull from ECR and
# use SSM. With ghcr or no registry no IAM entity is created. Optionally a cluster placement group
# (USE_PLACEMENT_GROUP=1; off by default, and skipped with a warning when the
# role may not create one). There is no EC2 key pair: every box authorises
# the public key in SSH_PUBLIC_KEY_PATH through its user-data. Create or
# reuse. Ids go into STATE.
#
# Needs: AWS_REGION, ADMIN_CIDR, SSH_PUBLIC_KEY_PATH (an OpenSSH public key).
# Optional: AZ, VPC_ID, SUBNET_ID, USE_PLACEMENT_GROUP, INSTANCE_PROFILE_NAME
#           (use an existing profile with ECR read access instead of creating one),
#           CREATE_INSTANCE_PROFILE (1 for ecr, else 0).
SCRIPT_NAME=10-network
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION ADMIN_CIDR SSH_PUBLIC_KEY_PATH
case "$ADMIN_CIDR" in
  0.0.0.0/0 | ::/0) die "ADMIN_CIDR must not be open to the world; use your own address, e.g. \$(curl -s https://checkip.amazonaws.com)/32" ;;
esac
ssh_pubkey >/dev/null # dies unless SSH_PUBLIC_KEY_PATH holds a usable public key
state_init
require_preflight

# VPC and subnet: the default VPC, the default subnet in $AZ.
vpc=${VPC_ID:-$(state_get '.network.vpc_id')}
if [ -z "$vpc" ]; then
  vpc=$(aws_r vpc-dryrun ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId')
  [ -n "$vpc" ] || die "no default VPC in $AWS_REGION; run 'aws ec2 create-default-vpc' or set VPC_ID and SUBNET_ID"
fi
subnet=${SUBNET_ID:-$(state_get '.network.subnet_id')}
cidr=$(state_get '.network.subnet_cidr')
if [ -z "$subnet" ] || [ -z "$cidr" ]; then
  if [ -n "${SUBNET_ID:-}" ]; then
    read -r subnet cidr < <(aws_r "$SUBNET_ID 172.31.0.0/20" ec2 describe-subnets --subnet-ids "$SUBNET_ID" --query 'Subnets[0].[SubnetId,CidrBlock]')
  else
    read -r subnet cidr < <(aws_r "subnet-dryrun 172.31.0.0/20" ec2 describe-subnets \
      --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$AZ" Name=default-for-az,Values=true \
      --query 'Subnets[0].[SubnetId,CidrBlock]')
  fi
  [ -n "$subnet" ] && [ "$subnet" != None ] || die "no default subnet in $AZ of $vpc; set AZ or SUBNET_ID"
fi
state_set '.network.vpc_id' "$vpc"
state_set '.network.subnet_id' "$subnet"
state_set '.network.subnet_cidr' "$cidr"
state_set '.network.az' "$AZ"
log "network: $vpc / $subnet ($cidr) in $AZ"

# Security group.
sg_name="${PROJECT_TAG}-sg"
sg=$(aws_r "" ec2 describe-security-groups --filters "Name=group-name,Values=$sg_name" "Name=vpc-id,Values=$vpc" --query 'SecurityGroups[0].GroupId')
if [ -z "$sg" ]; then
  sg=$(aws_w sg-dryrun ec2 create-security-group --group-name "$sg_name" \
    --description "ably-server scale proof ($PROJECT_TAG)" --vpc-id "$vpc" \
    --tag-specifications "$(tag_spec security-group "$sg_name" network)" \
    --query GroupId --output text)
  log "created security group $sg"
else
  log "reuse security group $sg"
fi
state_set '.network.sg_id' "$sg"
state_add_resource security-group "$sg" network "$sg_name"
aws_w_tolerate InvalidPermission.Duplicate "" ec2 authorize-security-group-ingress --group-id "$sg" \
  --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$sg}]" >/dev/null
aws_w_tolerate InvalidPermission.Duplicate "" ec2 authorize-security-group-ingress --group-id "$sg" \
  --protocol tcp --port 22 --cidr "$ADMIN_CIDR" >/dev/null

# Cluster placement group: optional. Off by default (the Operator role is not
# allowed to create one). With USE_PLACEMENT_GROUP=1 it is attempted, and a
# denial is a warning: the fleet then launches without it.
pg="${PROJECT_TAG}-cluster"
state_set '.network.placement_group' ""
if [ "$USE_PLACEMENT_GROUP" = 1 ]; then
  have=$(aws_r "" ec2 describe-placement-groups --filters "Name=group-name,Values=$pg" --query 'PlacementGroups[0].GroupName')
  if [ -z "$have" ]; then
    rc=0
    aws_w_soft "" ec2 create-placement-group --group-name "$pg" --strategy cluster \
      --tag-specifications "$(tag_spec placement-group "$pg" network)" >/dev/null || rc=$?
    case "$rc" in
      0)
        log "created placement group $pg"
        have=$pg
        ;;
      3) log "WARNING: ec2:CreatePlacementGroup is denied; launching without a placement group (nodes and NATS will not be packed onto one network segment)" ;;
      *) die "create-placement-group failed" ;;
    esac
  else
    log "reuse placement group $pg"
  fi
  if [ -n "$have" ]; then
    state_set '.network.placement_group' "$pg"
    state_add_resource placement-group "$pg" network "$pg"
  fi
else
  log "no placement group (USE_PLACEMENT_GROUP=0)"
fi

# Instance profile: lets instances pull from ECR (and use SSM Session Manager). Optional
# everywhere: INSTANCE_PROFILE_NAME set uses that profile; unset creates one only for ecr (or
# CREATE_INSTANCE_PROFILE=1); and when the role may not create it the fleet launches without one
# (a warning; with ecr the boxes then cannot pull, so use ghcr or INSTANCE_PROFILE_NAME).
# Every lookup is by exact name (iam get-role, get-instance-profile): never a listing.
if [ -z "${INSTANCE_PROFILE_NAME:-}" ] && [ "$CREATE_INSTANCE_PROFILE" != 1 ]; then
  if [ "$(state_get '.network.created_profile')" != true ]; then # keep one an earlier run made, so teardown still removes it
    state_set '.network.instance_profile' ""
    state_set_json '.network.created_profile' false
  fi
  log "no instance profile (IMAGE_REGISTRY_KIND=$IMAGE_REGISTRY_KIND needs none; CREATE_INSTANCE_PROFILE=1 adds one for SSM)"
elif [ -n "${INSTANCE_PROFILE_NAME:-}" ]; then
  rc=0
  AWS_R_NOTFOUND=NoSuchEntity aws_r_soft "$INSTANCE_PROFILE_NAME" iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --query InstanceProfile.InstanceProfileName >/dev/null || rc=$?
  case "$rc" in
    0) ;;
    3) log "WARNING: iam:GetInstanceProfile is denied, so INSTANCE_PROFILE_NAME=$INSTANCE_PROFILE_NAME cannot be checked; using it" ;;
    4) die "INSTANCE_PROFILE_NAME=$INSTANCE_PROFILE_NAME does not exist" ;;
    *) die "iam get-instance-profile failed for INSTANCE_PROFILE_NAME=$INSTANCE_PROFILE_NAME" ;;
  esac
  state_set '.network.instance_profile' "$INSTANCE_PROFILE_NAME"
  state_set_json '.network.created_profile' false
  log "using existing instance profile $INSTANCE_PROFILE_NAME"
else
  role="${PROJECT_TAG}-instance"
  iam_tags "$role"
  init_work_dir
  wd=$BENCH_WORK_DIR
  cat >"$wd/trust.json" <<'JSON'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
  profile_ok=1
  made_any=0
  iam_step() { # <aws iam args...>: one step of the optional profile; a failure ends the attempt, never the script
    [ "$profile_ok" = 1 ] || return 0
    local rc=0
    aws_w_soft "" iam "$@" >/dev/null || rc=$?
    if [ "$rc" != 0 ]; then
      log "WARNING: iam $1 failed (exit $rc; 3 means the role is denied it)"
      profile_ok=0
    fi
  }
  have=$(AWS_R_QUIET=1 aws_r "" iam get-role --role-name "$role" --query Role.RoleName) || have=""
  if [ -z "$have" ]; then
    # Tagging on create needs iam:TagRole; without it, create the role untagged (STATE finds it at teardown).
    rc=0
    aws_create_tagged "" "the IAM role" iam create-role --role-name "$role" --assume-role-policy-document "file://$wd/trust.json" \
      --tags "${IAM_TAGS[@]}" -- iam create-role --role-name "$role" --assume-role-policy-document "file://$wd/trust.json" >/dev/null || rc=$?
    if [ "$rc" = 0 ]; then
      log "created IAM role $role"
      made_any=1
      state_add_resource iam-role "$role" network "$role"
    else
      log "WARNING: iam create-role failed (exit $rc; 3 means the role is denied it)"
      profile_ok=0
    fi
  else
    log "reuse IAM role $role"
    made_any=1 # it carries the project name, so it is ours: teardown removes it
    state_add_resource iam-role "$role" network "$role"
  fi
  iam_step attach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
  iam_step attach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  if [ "$profile_ok" = 1 ]; then
    have=$(AWS_R_QUIET=1 aws_r "" iam get-instance-profile --instance-profile-name "$role" --query InstanceProfile.InstanceProfileName) || have=""
    if [ -z "$have" ]; then
      rc=0
      aws_create_tagged "" "the instance profile" iam create-instance-profile --instance-profile-name "$role" --tags "${IAM_TAGS[@]}" \
        -- iam create-instance-profile --instance-profile-name "$role" >/dev/null || rc=$?
      if [ "$rc" = 0 ]; then
        made_any=1
        state_add_resource iam-instance-profile "$role" network "$role"
        iam_step add-role-to-instance-profile --instance-profile-name "$role" --role-name "$role"
        is_dry || sleep 10 # IAM is eventually consistent
        log "created instance profile $role"
      else
        log "WARNING: iam create-instance-profile failed (exit $rc; 3 means the role is denied it)"
        profile_ok=0
      fi
    else
      state_add_resource iam-instance-profile "$role" network "$role"
    fi
  fi
  # Teardown removes what this run created, by name; a profile that could not be completed is not used.
  if [ "$made_any" = 1 ] || [ "$(state_get '.network.created_profile')" = true ]; then
    state_set_json '.network.created_profile' true
  else
    state_set_json '.network.created_profile' false
  fi
  if [ "$profile_ok" = 1 ]; then
    state_set '.network.instance_profile' "$role"
  else
    state_set '.network.instance_profile' ""
    log "WARNING: launching WITHOUT an instance profile. $([ "$IMAGE_REGISTRY_KIND" = ecr ] && echo "The boxes cannot pull from ECR without one: use IMAGE_REGISTRY_KIND=ghcr, or set INSTANCE_PROFILE_NAME to an existing profile." || echo "SSM Session Manager will not work; SSH does.")"
  fi
fi

resolve_ami >/dev/null
log_line 10-network "network ready: default VPC, one zone ($AZ), security group, instance profile '$(state_get '.network.instance_profile')' (empty: none); placement group '$(state_get '.network.placement_group')' (empty: none); SSH by key in user-data" "20-postgres.sh"
log "network complete"
