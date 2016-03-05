#!/bin/bash

echo Delete Buckets

Site=http://127.0.0.1:8091/pools/default/buckets/
Auth=Administrator:password
bucket=(CUSTOMER DISTRICT HISTORY ITEM NEW_ORDER ORDERS ORDER_LINE STOCK WAREHOUSE)

echo POST /pools/default/buckets

for i in "${bucket[@]}"
do
echo curl -u $Auth $Site$i
curl -X DELETE -u $Auth $Site$i
done

# echo rm -rf /run/data/
# rm -rf /run/data/

echo Creating Buckets

Site=http://127.0.0.1:8091/pools/default/buckets
Auth=Administrator:password
port=11224
low=3
high=8

#CUSTOMER
curl -X POST -u $Auth -d name=CUSTOMER -d ramQuotaMB=512 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#DISTRICT
curl -X POST -u $Auth -d name=DISTRICT -d ramQuotaMB=100 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1
#HISTORY
curl -X POST -u $Auth -d name=HISTORY -d ramQuotaMB=128 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#ITEM
curl -X POST -u $Auth -d name=ITEM -d ramQuotaMB=128 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#NEW_ORDER
curl -X POST -u $Auth -d name=NEW_ORDER -d ramQuotaMB=256 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#ORDERS
curl -X POST -u $Auth -d name=ORDERS -d ramQuotaMB=1024 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#ORDER_LINE
curl -X POST -u $Auth -d name=ORDER_LINE -d ramQuotaMB=1024 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#STOCK
curl -X POST -u $Auth -d name=STOCK -d ramQuotaMB=512 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$high 
let port\+=1

#WAREHOUSE
curl -X POST -u $Auth -d name=WAREHOUSE -d ramQuotaMB=128 -d authType=none -d proxyPort=$port $Site -d threadsNumber=$low 
let port\+=1

