#!/bin/bash

cp /Volumes/CPSQL.1/business_automations/creds.py ./temp.py
awk '{gsub(/\/\/mainserver\//, "/Volumes/"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/\/\/MAINSERVER\//, "/Volumes/"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/ticket_location = r/, "ticket_location = "); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/..mainserver.Share.logs.tickets../, "/Volumes/Share/logs/tickets/"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/cp_api..cp_api_server../, "cp_api[\"cp_api_server\"].replace(\"//mainserver\", \"//192.168.1.10\")"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/cp_api..cp_api_order_server../, "cp_api[\"cp_api_order_server\"].replace(\"//mainserver\", \"//192.168.1.10\")"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/company..item_images../, "company[\"item_images\"].replace(\"//mainserver\", \"/Volumes\")"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/company..brand_images../, "company[\"brand_images\"].replace(\"//mainserver\", \"/Volumes\")"); print}' temp.py > creds.py
mv creds.py ./temp.py
awk '{gsub(/config_data..logs....main../, "config_data[\"logs\"][\"main\"].replace(\"//MAINSERVER/\", \"/Volumes/\")"); print}' temp.py > creds.py
rm temp.py
mv creds.py ./setup/creds.py