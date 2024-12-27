#!/bin/bash

if ! command -v conda &> /dev/null; then
    if [ ! -d "/workspace/miniconda" ]; then
        wget --no-check-certificate https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
        bash /tmp/miniconda.sh -b -p /workspace/miniconda && \
        rm /tmp/miniconda.sh

        if [ -f "/workspace/miniconda/etc/profile.d/conda.sh" ]; then
            source "/workspace/miniconda/etc/profile.d/conda.sh"
        fi

        conda init bash
    fi
fi

COMFY_ENV_VARS=$(env | grep "COMFY_CLUSTER_" | awk '{print $1}' | paste -sd " " -)

if [ ! -z "$COMFY_CLUSTER_SSH_PUBKEY" ]; then
    echo "$COMFY_CLUSTER_SSH_PUBKEY" > /home/comfy/.ssh/authorized_keys
    chmod 600 /home/comfy/.ssh/authorized_keys
    chown comfy:comfy /home/comfy/.ssh/authorized_keys
fi


source "/workspace/miniconda/etc/profile.d/conda.sh"
if ! conda info --envs | grep -q '^comfy\s'; then
    conda create --name comfy python=${COMFY_CLUSTER_PYTHON_VERSION:-3.12.4} -y
fi

conda activate comfy

cd /workspace
echo '. /workspace/miniconda/etc/profile.d/conda.sh' >> /home/comfy/.bashrc
echo 'conda activate comfy' >> /home/comfy/.bashrc

if [ ! -d "/workspace/ComfyUI" ]; then
    git clone ${COMFY_CLUSTER_GIT_FORK:-https://github.com/comfyanonymous/ComfyUI}
    cd ComfyUI

    if ! grep -q "cd /workspace/ComfyUI" /home/comfy/.bashrc; then
        echo 'cd /workspace/ComfyUI' >> /home/comfy/.bashrc
    fi

    cp -r /tmp/custom_nodes/* /workspace/ComfyUI/custom_nodes/
fi

cd /workspace/ComfyUI/
PYTHON=/workspace/miniconda/envs/comfy/bin/python
$PYTHON -m pip install --upgrade pip
$PYTHON -m pip install -r requirements.txt 

for req_file in $(find /workspace/ComfyUI/custom_nodes -name requirements.txt -type f); do
    echo "Installing requirements from: $req_file"
    $PYTHON -m pip install -r "$req_file"
done

$PYTHON main.py --cpu --listen "0.0.0.0" --port $COMFY_CLUSTER_PORT