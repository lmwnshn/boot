#!/usr/bin/env bash

echo "Read the source for this one."

# PG
# sudo apt-get install build-essential libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc ccache
# Rust
# sudo apt-get install cmake

# redis
# sudo apt-get install redis-server redis-tools
# sudo chmod -R 777 /var/lib/redis

# sudo systemctl edit redis-server.service
# add this:
# [Service]
# UMask=000
# systemctl reenable redis-server.service
# systemctl restart redis-server

# curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# source "${HOME}/.cargo/env"
# cargo install --force cbindgen

# sudo apt install python3.10-venv
# python3 -m venv venv
# source ./venv/bin/activate
# pip3 install torch==1.13.1+cpu torchvision==0.14.1+cpu -f https://download.pytorch.org/whl/cpu/torch_stable.html
# pip3 install -r cmudb/setup/requirements.txt

# psycopg
# sudo apt install libpq5
