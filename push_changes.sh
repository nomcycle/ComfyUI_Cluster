#!/bin/bash

HOME_DIR="$HOME"
sudo rm -rf "$HOME_DIR/comfyui-cluster-volume/ComfyUI/custom_nodes/ComfyUI_Cluster/"
sudo cp -rf ../ComfyUI_Cluster "$HOME_DIR/comfyui-cluster-volume/ComfyUI/custom_nodes/"