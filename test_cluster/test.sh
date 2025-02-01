#!/bin/bash

# Parse command line arguments
SKIP_SETUP=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip_setup) SKIP_SETUP=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Generate SSH key if it doesn't exist
SSH_KEY="$HOME/.ssh/comfyui-cluster"
if [ ! -f "$SSH_KEY" ]; then
    ssh-keygen -t ed25519 -f "$SSH_KEY" -N "" -C "comfyui-cluster"
fi

PUBKEY=$(cat "${SSH_KEY}.pub")
VOLUME="$HOME/comfyui-cluster-volume"

# Configuration
LEADER_PORT=8189
LEADER_NAME="comfyui-cluster-leader"
FOLLOWER_COUNT=1
FOLLOWER_BASE_PORT=8190
UDP_PORT=9988

NETWORK_NAME="comfyui-cluster-net"
if ! sudo docker network inspect $NETWORK_NAME >/dev/null 2>&1; then
    sudo docker network create $NETWORK_NAME
fi

# Helper function to get container IP
get_container_ip() {
    sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$1"
}

# Clean up existing containers
sudo docker rm -f "$LEADER_NAME" >/dev/null 2>&1
for i in $(seq 0 $((FOLLOWER_COUNT-1))); do
    sudo docker rm -f "comfyui-cluster-follower-$i" >/dev/null 2>&1
done

# Start leader container
sudo docker run -d --rm \
    --name "$LEADER_NAME" \
    --network $NETWORK_NAME \
    -p ${LEADER_PORT}:${LEADER_PORT} \
    -e COMFY_CLUSTER_SSH_PUBKEY="$PUBKEY" \
    -e COMFY_CLUSTER_COMFY_PORT=${LEADER_PORT} \
    -e COMFY_CLUSTER_SKIP_SETUP=${SKIP_SETUP} \
    -e COMFY_CLUSTER_UDP_BROADCAST="true" \
    -e COMFY_CLUSTER_INSTANCE_COUNT=$((FOLLOWER_COUNT + 1)) \
    -e COMFY_CLUSTER_INSTANCE_INDEX=0 \
    -e COMFY_CLUSTER_ROLE="LEADER" \
    -v "$VOLUME:/workspace" \
    comfyui-cluster

# Start follower containers
for i in $(seq 0 $((FOLLOWER_COUNT-1))); do
    port=$((FOLLOWER_BASE_PORT + i))
    container_name="comfyui-cluster-follower-$i"
    
    sudo docker run -d --rm \
        --name "$container_name" \
        --network $NETWORK_NAME \
        -p ${port}:${port} \
        -e COMFY_CLUSTER_SSH_PUBKEY="$PUBKEY" \
        -e COMFY_CLUSTER_COMFY_PORT=${port} \
        -e COMFY_CLUSTER_SKIP_SETUP=${SKIP_SETUP} \
        -e COMFY_CLUSTER_UDP_BROADCAST="true" \
        -e COMFY_CLUSTER_INSTANCE_COUNT=$((FOLLOWER_COUNT + 1)) \
        -e COMFY_CLUSTER_INSTANCE_INDEX=$((i + 1)) \
        -e COMFY_CLUSTER_ROLE="FOLLOWER" \
        -v "$VOLUME:/workspace" \
        comfyui-cluster
done

# Listen to leader container logs
sudo docker logs -f "$LEADER_NAME"
