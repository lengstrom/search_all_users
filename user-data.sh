#!/bin/bash
sudo su
apt-get update -y -q
apt-get install -q -y python-pip python-dev libhdf5-serial-dev htop

export LC_ALL=C
pip install pandas
pip install --upgrade pip
pip install numpy
pip install numexpr
pip install cython
pip install tables
pip install boto3
pip install requests

mkdir ~/.aws
cd ~

curl https://raw.githubusercontent.com/lengstrom/search_all_users/master/search_for_range.py > search_for_range.py
