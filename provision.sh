#!/bin/bash

set -x

apt-get update
apt-get install -y python3 python3-pip tmux git vim visidata unzip jq wget

curl -L https://github.com/dolthub/dolt/releases/latest/download/install.sh > /tmp/install.sh && bash /tmp/install.sh
dolt config --global --add user.email rimantas@keyspace.lt
dolt config --global --add user.name "rl1987"

pip3 install --upgrade jupyterlab
pip3 install --upgrade requests lxml openpyxl pandas xlrd doltcli httpx "httpx[http2]" js2xml
pip3 install --upgrade mysql-connector-python sqlalchemy==1.4.16

curl -sSL https://repos.insights.digitalocean.com/install.sh -o /tmp/install.sh
bash /tmp/install.sh

mkdir /root/data

pushd /root/data || exit
dolt clone rl1987/standard-charge-files
dolt clone rl1987/transparency-in-pricing
popd || exit

curl -fsSL https://deb.nodesource.com/setup_18.x -o /tmp/install_node.sh
bash /tmp/install_node.sh
apt-get install -y gcc g++ make nodejs

npm install pm2 -g

jupyter notebook --generate-config 
sudo pm2 start "jupyter-lab --allow-root --ip=0.0.0.0"
pm2 save
pm2 startup systemd

swapoff -a
dd if=/dev/zero of=/swapfile bs=1G count=16
chmod 0600 /swapfile
mkswap /swapfile
swapon /swapfile
echo "/swapfile swap swap sw 0 0" >> /etc/fstab
