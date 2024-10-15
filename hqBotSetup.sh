#!/bin/bash 

git clone --single-branch --branch master https://github.com/3GDS-Charlie/hq-telegram-bot.git
sudo chown ubuntu hq-telegram-bot/
cd hq-telegram-bot
sudo apt update
sudo apt upgrade -y
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10 -y
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
sudo update-alternatives --config python3
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
sudo apt install tesseract-ocr -y
sudo apt install pkg-config libhdf5-dev -y
sudo apt install libffi7 libheif-dev libde265-dev -y
sudo apt-get install libgl1 -y
pip3 install scikit-learn
sudo apt-get install poppler-utils -y
sudo timedatectl set-timezone Asia/Singapore
sed '/tensorflow-metal/d' requirements.txt | pip3 install -r /dev/stdin
playwright install
playwright install-deps
screen -dmS my_script_session bash -c 'python3 main.py; exec bash'