#/bin/bash
mkdir -p caddy/certs
openssl req -x509 -newkey rsa:4096 -keyout caddy/certs/key.pem -out caddy/certs/cert.pem -days 365 -nodes -subj "/CN=*"