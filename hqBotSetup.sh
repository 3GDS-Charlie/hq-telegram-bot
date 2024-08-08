#!/bin/bash 

git clone --single-branch --branch master https://github.com/3GDS-Charlie/hq-telegram-bot.git
cd hq-telegram-bot
sudo apt update
sudo apt upgrade -y
sudo snap install chromium
sudo apt install python3-pip -y
sudo apt install tesseract-ocr -y
sudo apt install pkg-config libhdf5-dev -y
sudo apt install libffi7 libheif-dev libde265-dev -y
sudo apt-get install libgl1 -y
pip3 install scikit-learn -y
sudo apt-get install poppler-utils -y
sudo timedatectl set-timezone Asia/Singapore
pip3 install -r requirements.txt -y
screen -dmS my_script_session bash -c 'python3 hq-telegram-bot/main.py; exec bash'