#!/usr/bin/env bash
# 10-network: default VPC, one Availability Zone, one security group
# (all traffic inside the group, SSH from ADMIN_CIDR only), a cluster
# placement group, an EC2 key pair and an instance profile that may pull
# from ECR. Create or reuse. Ids go into STATE.
#
# Needs: AWS_REGION, ADMIN_CIDR, SSH_PUBLIC_KEY_PATH.
# Optional: AZ, VPC_ID, SUBNET_ID, INSTANCE_PROFILE_NAME (use an existing profile
#           with ECR read access instead of creating one).
SCRIPT_NAME=10-network
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

need_cmd jq
is_dry || need_cmd aws
require_env AWS_REGION ADMIN_CIDR SSH_PUBLIC_KEY_PATH
case "$ADMIN_CIDR" in
  0.0.0.0/0 | ::/0) die "ADMIN_CIDR must not be open to the world; use your own address, e.g. \$(curl -s https://checkip.amazonaws.com)/32" ;;
esac
is_dry || [ -r "$SSH_PUBLIC_KEY_PATH" ] || die "cannot read SSH_PUBLIC_KEY_PATH=$SSH_PUBLIC_KEY_PATH"
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

# Cluster placement group.
pg="${PROJECT_TAG}-cluster"
have=$(aws_r "" ec2 describe-placement-groups --filters "Name=group-name,Values=$pg" --query 'PlacementGroups[0].GroupName')
if [ -z "$have" ]; then
  aws_w "" ec2 create-placement-group --group-name "$pg" --strategy cluster \
    --tag-specifications "$(tag_spec placement-group "$pg" network)" >/dev/null
  log "created placement group $pg"
else
  log "reuse placement group $pg"
fi
state_set '.network.placement_group' "$pg"
state_add_resource placement-group "$pg" network "$pg"

# Key pair.
key="${PROJECT_TAG}-key"
have=$(aws_r "" ec2 describe-key-pairs --filters "Name=key-name,Values=$key" --query 'KeyPairs[0].KeyName')
if [ -z "$have" ]; then
  aws_w "" ec2 import-key-pair --key-name "$key" --public-key-material "fileb://$SSH_PUBLIC_KEY_PATH" \
    --tag-specifications "$(tag_spec key-pair "$key" network)" >/dev/null
  log "imported key pair $key"
else
  log "reuse key pair $key"
fi
state_set '.network.key_name' "$key"
state_add_resource key-pair "$key" network "$key"

# Instance profile: lets instances pull from ECR (and use SSM Session Manager).
if [ -n "${INSTANCE_PROFILE_NAME:-}" ]; then
  aws_r "$INSTANCE_PROFILE_NAME" iam get-instance-profile --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --query InstanceProfile.InstanceProfileName >/dev/null ||
    die "INSTANCE_PROFILE_NAME=$INSTANCE_PROFILE_NAME does not exist"
  state_set '.network.instance_profile' "$INSTANCE_PROFILE_NAME"
  state_set_json '.network.created_profile' false
  log "using existing instance profile $INSTANCE_PROFILE_NAME"
else
  role="${PROJECT_TAG}-instance"
  iam_tags "$role"
  have=$(AWS_R_QUIET=1 aws_r "" iam get-role --role-name "$role" --query Role.RoleName) || have=""
  if [ -z "$have" ]; then
    init_work_dir
    wd=$BENCH_WORK_DIR
    cat >"$wd/trust.json" <<'JSON'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
    aws_w "" iam create-role --role-name "$role" --assume-role-policy-document "file://$wd/trust.json" \
      --tags "${IAM_TAGS[@]}" >/dev/null
    aws_w "" iam attach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
    aws_w "" iam attach-role-policy --role-name "$role" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
    log "created IAM role $role"
  else
    log "reuse IAM role $role"
  fi
  have=$(AWS_R_QUIET=1 aws_r "" iam get-instance-profile --instance-profile-name "$role" --query InstanceProfile.InstanceProfileName) || have=""
  if [ -z "$have" ]; then
    aws_w "" iam create-instance-profile --instance-profile-name "$role" --tags "${IAM_TAGS[@]}" >/dev/null
    aws_w "" iam add-role-to-instance-profile --instance-profile-name "$role" --role-name "$role"
    is_dry || sleep 10 # IAM is eventually consistent
    log "created instance profile $role"
  fi
  state_set '.network.instance_profile' "$role"
  state_set_json '.network.created_profile' true
  state_add_resource iam-role "$role" network "$role"
  state_add_resource iam-instance-profile "$role" network "$role"
fi

resolve_ami >/dev/null
log_line 10-network "network ready: default VPC, one zone ($AZ), security group, placement group, key pair, instance profile" "20-postgres.sh"
log "network complete"
