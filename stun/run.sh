#!/bin/bash
sudo docker run -t -p 443:443 --env-file .env nomcycle/comfyui-cluster-stun:latest