#!/usr/bin/env bash

# PG
sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache
# Rust
sudo apt-get install cmake
# psycopg
sudo apt-get install libpq5

# redis
sudo apt-get install redis-server redis-tools
sudo chmod -R 777 /var/lib/redis

sudo mkdir -p /etc/systemd/system/redis-server.service.d/
{
  echo "[Service]";
  echo "UMask=000";
} | sudo tee /etc/systemd/system/redis-server.service.d/override.conf
sudo systemctl daemon-reload
sudo systemctl reenable redis-server.service
sudo systemctl restart redis-server

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "${HOME}/.cargo/env"
cargo install --force cbindgen

sudo apt install python3.10-venv
python3 -m venv venv
source ./venv/bin/activate
pip3 install torch==1.13.1+cpu torchvision==0.14.1+cpu -f https://download.pytorch.org/whl/cpu/torch_stable.html
pip3 install -r cmudb/setup/requirements.txt
