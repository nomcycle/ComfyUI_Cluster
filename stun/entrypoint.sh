#!/bin/sh
/app/venv/bin/python -m src.server &
caddy run --config /etc/caddy/Caddyfile