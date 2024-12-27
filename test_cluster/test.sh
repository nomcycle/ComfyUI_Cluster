if [ ! -f "$HOME/.ssh/comfyui-cluster" ]; then
    ssh-keygen -t ed25519 -f $HOME/.ssh/comfyui-cluster -N "" -C "comfyui-cluster"
fi

PUBKEY=$(cat ~/.ssh/comfyui-cluster.pub)

sudo docker rm -f comfyui-cluster || true
sudo docker run \
  --rm \
  --name comfyui-cluster \
  -e COMFY_DEV_SSH_PUBKEY="$PUBKEY" \
  -e COMFY_DEV_PYTHON_VERSION="3.12.4" \
  comfyui-cluster