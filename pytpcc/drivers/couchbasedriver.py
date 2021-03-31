#/ -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import sys
import os
import logging
import subprocess
from pprint import pprint,pformat
from datetime import datetime

import json
import requests
import time
import urllib3
from urllib3.poolmanager import PoolManager

import constants
from .abstractdriver import *
import random


QUERY_URL = "127.0.0.1:8093"
USER_ID = "Administrator"
PASSWORD = "password"

TXN_QUERIES = {
    "work": {
        "begin": "BEGIN WORK",
        "rollback":"ROLLBACK WORK",
        "commit":"COMMIT WORK"
    },
    "DELIVERY": {
        "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 AND NO_O_ID > -1 LIMIT 1", #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = $1 AND NO_W_ID = $2 AND NO_O_ID = $3", # d_id, w_id, no_o_id
        "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = $1 AND O_D_ID = $2 AND O_W_ID = $3", # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = $1 WHERE O_ID = $2 AND O_D_ID = $3 AND O_W_ID = $4", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = $1 WHERE OL_O_ID = $2 AND OL_D_ID = $3 AND OL_W_ID = $4", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM(OL_AMOUNT) AS SUM_OL_AMOUNT FROM ORDER_LINE WHERE OL_O_ID = $1 AND OL_D_ID = $2 AND OL_W_ID = $3", # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER USE KEYS [(to_string($4) || '.' || to_string($3) || '.' ||  to_string($2))] SET C_BALANCE = C_BALANCE + $1 ", # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = $1", # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = $1 AND D_W_ID = $2", # d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = $1 WHERE D_ID = $2 AND D_W_ID = $3", # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER USE KEYS [(to_string($1) || '.' ||  to_string($2) || '.' ||  to_string($3)) ] ", # w_id, d_id, c_id
        "createOrder": "INSERT INTO ORDERS (KEY, VALUE) VALUES (TO_STRING($3) || '.' ||  TO_STRING($2) || '.' ||  TO_STRING($1), {'O_ID':$1, 'O_D_ID':$2, 'O_W_ID':$3, 'O_C_ID':$4, 'O_ENTRY_D':$5, 'O_CARRIER_ID':$6, 'O_OL_CNT':$7, 'O_ALL_LOCAL':$8})", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO NEW_ORDER(KEY, VALUE) VALUES(TO_STRING($2)|| '.' || TO_STRING($3)|| '.' || TO_STRING($1), {'NO_O_ID':$1,'NO_D_ID':$2,'NO_W_ID':$3})",
        "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM USE KEYS [to_string($1)]", # ol_i_id
        "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK USE KEYS [TO_STRING($2)|| '.' || TO_STRING($1)]", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK USE KEYS [to_string($6) || '.' || to_string($5)] SET S_QUANTITY = $1, S_YTD = $2, S_ORDER_CNT = $3, S_REMOTE_CNT = $4 ", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO ORDER_LINE(KEY, VALUE) VALUES(TO_STRING($3)|| '.' || TO_STRING($2)|| '.' || TO_STRING($1)|| '.' || TO_STRING($4), { 'OL_O_ID':$1, 'OL_D_ID':$2, 'OL_W_ID':$3, 'OL_NUMBER':$4, 'OL_I_ID':$5, 'OL_SUPPLY_W_ID':$6, 'OL_DELIVERY_D':$7, 'OL_QUANTITY':$8, 'OL_AMOUNT':$9, 'OL_DIST_INFO':$10})" # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER USE KEYS [(to_string($1) || '.' ||  to_string($2) || '.' ||  to_string($3)) ]", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = $1 AND O_D_ID = $2 AND O_C_ID = $3 ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = $1 AND OL_D_ID = $2 AND OL_O_ID = $3", # w_id, d_id, o_id        
    },
    
    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = $1", # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + $1 WHERE W_ID = $2", # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2", # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + $1 WHERE D_W_ID  = $2 AND D_ID = $3", # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER USE KEYS [(to_string($1) || '.' ||  to_string($2) || '.' ||  to_string($3)) ]", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = $1 AND C_D_ID = $2 AND C_LAST = $3 ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER USE KEYS [(to_string($6) || '.' ||  to_string($6) || '.' ||  to_string($7)) ] SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3, C_DATA = $4 ", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER USE KEYS [(to_string($4) || '.' ||  to_string($5) || '.' ||  to_string($6)) ] SET C_BALANCE = $1, C_YTD_PAYMENT = $2, C_PAYMENT_CNT = $3 ", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY(KEY, VALUE) VALUES (TO_STRING($6), {'H_C_ID':$1, 'H_C_D_ID':$2, 'H_C_W_ID':$3, 'H_D_ID':$4, 'H_W_ID':$5, 'H_DATE':$6, 'H_AMOUNT':$7, 'H_DATA':$8})"
    },
    
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = $1 AND D_ID = $2",
        "getStockCount": " SELECT COUNT(DISTINCT(o.OL_I_ID)) AS CNT_OL_I_ID FROM  ORDER_LINE o INNER JOIN STOCK s ON KEYS (TO_STRING(o.OL_W_ID) || '.' ||  TO_STRING(o.OL_I_ID)) WHERE o.OL_W_ID = $1 AND o.OL_D_ID = $2 AND o.OL_O_ID < $3 AND o.OL_O_ID >= $4 AND s.S_QUANTITY < $6 "
    },
   "STOCK_LEVEL_ANSI": {
        "ansigetStockCount": " SELECT COUNT(DISTINCT(o.OL_I_ID)) AS CNT_OL_I_ID FROM  ORDER_LINE o INNER JOIN STOCK s ON (o.OL_W_ID == s.S_W_ID AND o.OL_I_ID ==  s.S_I_ID) WHERE o.OL_W_ID = $1 AND o.OL_D_ID = $2 AND o.OL_O_ID < $3 AND o.OL_O_ID >= $4 AND s.S_QUANTITY < $6 ",
        "getOrdersByDistrict": "SELECT * FROM  DISTRICT d INNER JOIN ORDERS o ON d.D_ID == o.O_D_ID where d.D_ID = $1",
        "getCustomerOrdersByDistrict": "SELECT COUNT(DISTINCT(c.C_ID)) FROM  CUSTOMER c INNER JOIN ORDERS o USE HASH(BUILD) ON c.C_ID == o.O_C_ID WHERE c.C_D_ID = $1" # d_ID
    },
}

KEYNAMES = {
        constants.TABLENAME_ITEM:         [0],  # INTEGER
        constants.TABLENAME_WAREHOUSE:         [0],  # INTEGER
        constants.TABLENAME_DISTRICT:         [1, 0],  # INTEGER
        constants.TABLENAME_CUSTOMER:         [2, 1, 0], # INTEGER
        constants.TABLENAME_STOCK:         [1, 0],  # INTEGER
        constants.TABLENAME_ORDERS:         [3, 2, 0], # INTEGER
        constants.TABLENAME_NEW_ORDER:         [1, 2, 0], # INTEGER
        constants.TABLENAME_ORDER_LINE: [2, 1, 0, 3], # INTEGER
        constants.TABLENAME_HISTORY:         [0, 5],  # INTEGER
}


TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
    ],
    constants.TABLENAME_WAREHOUSE: [
        "W_ID", # SMALLINT
        "W_NAME", # VARCHAR
        "W_STREET_1", # VARCHAR
        "W_STREET_2", # VARCHAR
        "W_CITY", # VARCHAR
        "W_STATE", # VARCHAR
        "W_ZIP", # VARCHAR
        "W_TAX", # FLOAT
        "W_YTD", # FLOAT
    ],
    constants.TABLENAME_DISTRICT: [
        "D_ID", # TINYINT
        "D_W_ID", # SMALLINT
        "D_NAME", # VARCHAR
        "D_STREET_1", # VARCHAR
        "D_STREET_2", # VARCHAR
        "D_CITY", # VARCHAR
        "D_STATE", # VARCHAR
        "D_ZIP", # VARCHAR
        "D_TAX", # FLOAT
        "D_YTD", # FLOAT
        "D_NEXT_O_ID", # INT
    ],
    constants.TABLENAME_CUSTOMER:   [
        "C_ID", # INTEGER
        "C_D_ID", # TINYINT
        "C_W_ID", # SMALLINT
        "C_FIRST", # VARCHAR
        "C_MIDDLE", # VARCHAR
        "C_LAST", # VARCHAR
        "C_STREET_1", # VARCHAR
        "C_STREET_2", # VARCHAR
        "C_CITY", # VARCHAR
        "C_STATE", # VARCHAR
        "C_ZIP", # VARCHAR
        "C_PHONE", # VARCHAR
        "C_SINCE", # TIMESTAMP
        "C_CREDIT", # VARCHAR
        "C_CREDIT_LIM", # FLOAT
        "C_DISCOUNT", # FLOAT
        "C_BALANCE", # FLOAT
        "C_YTD_PAYMENT", # FLOAT
        "C_PAYMENT_CNT", # INTEGER
        "C_DELIVERY_CNT", # INTEGER
        "C_DATA", # VARCHAR
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID", # INTEGER
        "S_W_ID", # SMALLINT
        "S_QUANTITY", # INTEGER
        "S_DIST_01", # VARCHAR
        "S_DIST_02", # VARCHAR
        "S_DIST_03", # VARCHAR
        "S_DIST_04", # VARCHAR
        "S_DIST_05", # VARCHAR
        "S_DIST_06", # VARCHAR
        "S_DIST_07", # VARCHAR
        "S_DIST_08", # VARCHAR
        "S_DIST_09", # VARCHAR
        "S_DIST_10", # VARCHAR
        "S_YTD", # INTEGER
        "S_ORDER_CNT", # INTEGER
        "S_REMOTE_CNT", # INTEGER
        "S_DATA", # VARCHAR
    ],
    constants.TABLENAME_ORDERS:     [
        "O_ID", # INTEGER
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_ENTRY_D", # TIMESTAMP
        "O_CARRIER_ID", # INTEGER
        "O_OL_CNT", # INTEGER
        "O_ALL_LOCAL", # INTEGER
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "NO_O_ID", # INTEGER
        "NO_D_ID", # TINYINT
        "NO_W_ID", # SMALLINT
    ],
    constants.TABLENAME_ORDER_LINE: [
        "OL_O_ID", # INTEGER
        "OL_D_ID", # TINYINT
        "OL_W_ID", # SMALLINT
        "OL_NUMBER", # INTEGER
        "OL_I_ID", # INTEGER
        "OL_SUPPLY_W_ID", # SMALLINT
        "OL_DELIVERY_D", # TIMESTAMP
        "OL_QUANTITY", # INTEGER
        "OL_AMOUNT", # FLOAT
        "OL_DIST_INFO", # VARCHAR
    ],
    constants.TABLENAME_HISTORY:    [
        "H_C_ID", # INTEGER
        "H_C_D_ID", # TINYINT
        "H_C_W_ID", # SMALLINT
        "H_D_ID", # TINYINT
        "H_W_ID", # SMALLINT
        "H_DATE", # TIMESTAMP
        "H_AMOUNT", # FLOAT
        "H_DATA", # VARCHAR
    ],
}

globpool = None
queryservices = None

def generate_prepared_query (name):
    return {'prepared': '"' + name + '"'}

def n1ql_execute(url, stmt, qcontext, creds, pool):
    stmt['creds'] = creds
    stmt['query_context'] = qcontext
    try:
        response = pool.request('POST', url, fields=stmt, encode_multipart=False)
        response.read(cache_content=False)
        body = json.loads(response.data.decode('utf8'))
#        if body['status'] != "success":
#            logging.info("%s --- %s" % (stmt, json.JSONEncoder().encode(body)))
        return body
    except:
        pass
    return {}

## ==============================================
## CouchbaseDriver
## ==============================================
class CouchbaseDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "uri":                ("The couchbase cluster URI", "http://localhost:8091" ),
        "name":               ("Database (query_context) name", "default:default.tpcc"),
        "user":               ("User name", "Administrator"),
        "password":           ("Password", "password"),
        "denormalize":        ("If True, data will be denormalized", False),
        "notransactions":     ("If True, run as non transaction workload", False),
        "durability_level":   ("durability_level (none, majority, majorityAndPersistActive, persistToMajority)", "majority"),
        "scan_consistency":   ("scan_consistency (not_bounded, scan_plus)", "not_bounded"),
        "allcommits":         ("If True, all commits only counted", True),
    }

    DENORMALIZED_TABLES = [
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE
    ]
    
    def __init__(self, ddl):
        global globpool

        super(CouchbaseDriver, self).__init__("couchbase", ddl)
        self.query_context = "default:default.tpcc"
        self.denormalize = False
        self.w_orders = {}
        self.notransactions = False
        self.allcommits = True
        self.ansiqueries = False
        self.globpool = globpool
        self.prepared_dict = {}
        self.query_node = ""
        self.tx_status = ""
        self.txtimeout = "3s"
        self.durability_level = "majority"
        self.scan_consistency = "not_bounded"
        self.batch_load = 1
        self.user = ""
        self.password = ""
        self.creds = ""

    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return CouchbaseDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        
        for key in CouchbaseDriver.DEFAULT_CONFIG.keys():
            if not key in config:
                logging.debug("'%s' is not in the config, set to default : %s", 
                              key, str(CouchbaseDriver.DEFAULT_CONFIG[key][1]))
                config[key] = str(CouchbaseDriver.DEFAULT_CONFIG[key][1])
 
        logging.debug("config values %s", pformat(config))
        
        self.user = config['user']
        self.password = config['password']

        self.creds = '[{"user":"' + self.user + '","pass":"' + self.password + '"}]'
        self.query_context = config['name']

        if 'denormalize' in config:
            self.denormalize = config['denormalize'] == 'True'

        if 'notransactions' in config:
            self.notransactions = config['notransactions'] == 'True'
        
        if 'allcommits' in config:
            self.allcommits = config['allcommits'] == 'True'

        global queryservices
        global globpool
  
        initalize = ('load' in config and not config['load']) and \
                      ('execute' in config and not config['execute'])

        if initalize:
            globpool = PoolManager(10, retries=urllib3.Retry(10), maxsize=60)
            self.globpool = globpool
            queryservices = []
            response = requests.get(config['uri']+"/pools/default/nodeServices", auth=(self.user, self.password))
            body = response.json()
            for r in body['nodesExt']:
                if 'n1ql' in r['services']:
                    if 'hostname' in r:
                        queryservices.append("http://" + r['hostname'] + ":" + str(r['services']['n1ql']) + "/query/service")
                    else:
                        sh = config['uri'].split(':')
                        queryservices.append(sh[0] + ":" + sh[1] + ":" + str(r['services']['n1ql']) + "/query/service")
        
        if len(queryservices) == 0:
            logging.error("No query services detected on the cluster")
            sys.exit(1)
        self.query_node = random.choice(queryservices)
        if not self.notransactions:
            if 'txtimeout' in config:
                self.txtimeout = config['txtimeout']
            if 'durability_level' in config:
                self.durability_level = config['durability_level']
            if 'scan_consistency' in config:
                self.scan_consistency = config['scan_consistency']
        return 

    def executeStart(self):
        if len(self.prepared_dict) > 0:
            return
        for txn in TXN_QUERIES:
            if self.notransactions and txn == "work":
                 continue
            elif (not self.ansiqueries) and (txn == "STOCK_LEVEL_ANSI"):
                 continue
            for query in TXN_QUERIES[txn]:
                if query == "getStockInfo":
                    for i in range(1,11):
                        converted_district = TXN_QUERIES[txn][query] % i
                        prepare_query = "PREPARE %s_%s_%s " % (txn, i, query) + "FROM %s" % converted_district
                        stmt = json.loads('{"statement" : "' + str(prepare_query) + '"}')
                        body = n1ql_execute(self.query_node, stmt, self.query_context, self.creds, self.globpool)
                        self.prepared_dict[txn + str(i) + query] = body['results'][0]['name']
                else:
                    prepare_query = "PREPARE %s_%s " % (txn, query) + "FROM %s" % TXN_QUERIES[txn][query]
                    stmt = json.loads('{"statement" : "' + str(prepare_query) + '"}')
                    body = n1ql_execute(self.query_node, stmt, self.query_context, self.creds, self.globpool)
                    self.prepared_dict[txn + query] = body['results'][0]['name']
        return

    def txStatus(self):
        return self.tx_status

    ## ----------------------------------------------
    ## loadTuples for Couchbase (Adapted from MongoDB implemenetation. Only normalized version is ported to Couchbase).
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        fullTableName = self.query_context + "." + tableName
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), fullTableName))
        
        assert tableName in TABLE_COLUMNS, "Unexpected table %s" % fullTableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))
        
        tuple_dicts = [ ]

        ## We want to combine all of a CUSTOMER's ORDERS, ORDER_LINE, and HISTORY records
        ## into a single document
        if self.denormalize and tableName in CouchbaseDriver.DENORMALIZED_TABLES:
            ## If this is the ORDERS table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_ORDERS:
                for t in tuples:
                    key = tuple(t[:1]+t[2:4]) # O_ID, O_C_ID, O_D_ID, O_W_ID
                    val = {}
                    for l, v in enumerate(t):
                        if isinstance(v,(datetime)):
                            v = str(v)
                        val[columns[l]] = v
                    self.w_orders[key] = val
                ## FOR
            ## IF

            ## If this is an ORDER_LINE record, then we need to stick it inside of the
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    assert o_key in self.w_orders, "Order Key: %s\nall Keys:\n%s" % (str(o_key), "\n".join(map(str, sorted(self.w_orders.keys()))))
                    o = self.w_orders[o_key]
                    if not tableName in o:
                        o[tableName] = []
                    val = {}
                    for l in num_columns[4:]:
                        v = t[l]
                        if isinstance(v,(datetime)):
                            v = str(v)
                        val[columns[l]] = v
                    o[tableName].append(val)
                ## FOR
        elif self.batch_load == 1:
            for t in tuples:
                self.loadOneDoc(tableName, t, False)
        ## Otherwise just shove the tuples straight to the target collection
        else:
            i = 0
            sql = 'INSERT INTO %s(KEY, VALUE) ' % fullTableName
            for t in tuples:
                key = ""
                kpart = len(KEYNAMES[tableName]) - 1
                l = 0
                for k in KEYNAMES[tableName]:
                        if (l < kpart):
                                key = key + str(t[k]) + '.'
                        else:
                                key = key + str(t[k])
                        l = l + 1
                #sql = 'INSERT INTO %s(KEY, VALUE) VALUES (\\"%s\\", {' % (fullTableName, key)
                if i != 0:
                    sql = sql + ',VALUES (\\"%s\\", {' % key
                else:
                    sql = sql + 'VALUES (\\"%s\\", {' % key
                j=0
                for x in t:
                    if isinstance(t[j],(int, float)):
                            sql = sql + '\\"%s\\":%s  ' % (columns[j],  str(t[j]))
                    else:
                            sql = sql + '\\"%s\\":\\"%s\\"  ' % (columns[j],  t[j])
                    j = j + 1
                    if j < len(t):
                            sql = sql + ","
                if ( i == self.batch_load ):
                    sql = sql + "})"
                    nsql = '{"statement": "' + sql + '"}'
                    jsql = json.loads(nsql)
                    n1ql_execute(self.query_node, jsql, self.query_context, self.creds, self.globpool)
                    sql = 'INSERT INTO %s(KEY, VALUE) ' % fullTableName
                    i = 0
                else:
                    sql = sql + "})"
                    i = i + 1
            if i > 0:
                 nsql = '{"statement": "' + sql + '"}'
                 jsql = json.loads(nsql)
                 n1ql_execute(self.query_node, jsql, self.query_context, self.creds, self.globpool)

        logging.debug("LoadTuples:%s" %  (fullTableName))
        return
        
    def loadOneDoc(self, tableName, tuple, denorm):
         columns = TABLE_COLUMNS[tableName]
         fullTableName = self.query_context + "." + tableName
         args = []
         args.append(fullTableName)
         args.append("")
         args.append("")
         args.append({})
         key = ""
         if denorm:
             for l, k in enumerate(KEYNAMES[tableName]):
                 if l == 0:
                     key = str(tuple[columns[k]])
                 else:
                     key = key + '.' + str(tuple[columns[k]])
             val = tuple
         else:
             for l, k in enumerate(KEYNAMES[tableName]):
                 if l == 0:
                     key = str(tuple[k])
                 else:
                     key = key + '.' + str(tuple[k])
             val = {}
             for l, v in enumerate(tuple):
                 v1 = tuple[l]
                 if isinstance(v1,(datetime)):
                     v1 = str(v1)
                 val[columns[l]] = v1

         args[1] = key
         args[2] = val
         self.doQueryParam("__insert", args, "")

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Finished loading tables")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            for name in constants.ALL_TABLES:
                if self.denormalize and name in CouchbaseDriver.DENORMALIZED_TABLES[1:]: return
        #Nothing to commit for N1QL
        return

    def loadFinishDistrict(self, w_id, d_id):
        if self.denormalize and len(self.w_orders) > 0:
            logging.debug("Inserting %d denormalized ORDERS records for WAREHOUSE %d DISTRICT %d ", len(self.w_orders), w_id, d_id)
            for w_ord_key in self.w_orders:
                self.loadOneDoc(constants.TABLENAME_ORDERS, self.w_orders[w_ord_key], self.denormalize)
            self.w_orders.clear()
        ## IF

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):

        txn = "DELIVERY"
        q = TXN_QUERIES[txn]
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        result = []
        txid = ""

        try:
            txid, status =  self.doTranStatement("begin", self.prepared_dict["workbegin"], "")
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                newOrder, status = self.doQueryParam(self.prepared_dict[ txn + "getNewOrder"], [d_id, w_id], txid)
                if len(newOrder) == 0:
                    assert len(newOrder) > 0
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                no_o_id = newOrder[0]['NO_O_ID']
                
                rs, status = self.doQueryParam(self.prepared_dict[ txn + "getCId"], [no_o_id, d_id, w_id], txid)
                c_id = rs[0]['O_C_ID']
                
                rs2, status = self.doQueryParam(self.prepared_dict[ txn + "sumOLAmount"], [no_o_id, d_id, w_id], txid)
                ol_total = rs2[0]['SUM_OL_AMOUNT']
    
                self.doQueryParam(self.prepared_dict[ txn + "deleteNewOrder"], 
                                                   [d_id, w_id, no_o_id], txid)
                self.doQueryParam(self.prepared_dict[ txn + "updateOrders"], 
                                                   [o_carrier_id, no_o_id, d_id, w_id], txid)
                self.doQueryParam(self.prepared_dict[ txn + "updateOrderLine"], 
                                                   [ol_delivery_d, no_o_id, d_id, w_id], txid)
                
                # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                # them out
                # If there are no order lines, SUM returns null. There should always be order lines.
    
                assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
                assert ol_total > 0.0
    
                self.doQueryParam(self.prepared_dict[ txn + "updateCustomer"], 
                                                   [ol_total, c_id, d_id, w_id], txid)
    
                result.append((d_id, no_o_id))
            ## FOR

        except Exception as e:
            self.doTranStatement("rollback", self.prepared_dict["workrollback"], txid)
            raise
        else:
            trs, self.tx_status = self.doTranStatement("commit", self.prepared_dict["workcommit"], txid)

        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):

        txn = "NEW_ORDER"
        q = TXN_QUERIES[txn]
        d_next_o_id = 0
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]

        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True
        items = []
        txid = ""

        try:
            txid, status =  self.doTranStatement("begin", self.prepared_dict["workbegin"], "")
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                rs, status = self.doQueryParam(self.prepared_dict[ txn + "getItemInfo"], [i_ids[i]], txid)
                assert len(rs) > 0
                items.append(rs[0])

            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                assert len(item) != 0
            
            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            rs, status = self.doQueryParam(self.prepared_dict[ txn + "getWarehouseTaxRate"], [w_id], txid)
            if len(rs) > 0:
                w_tax = rs[0]['W_TAX']
            
            district_info, status = self.doQueryParam(self.prepared_dict[ txn +"getDistrict"], [d_id, w_id], txid)
            if len(district_info) != 0:
                d_tax = district_info[0]['D_TAX']
                d_next_o_id = district_info[0]['D_NEXT_O_ID']
            
            rs, status = self.doQueryParam(self.prepared_dict[ txn + "getCustomer"], [w_id, d_id, c_id], txid)
            customer_info = rs
            if len(rs) != 0:
                c_discount = rs[0]['C_DISCOUNT']
    
            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID
            
            self.doQueryParam(self.prepared_dict[ txn + "incrementNextOrderId"], 
                              [d_next_o_id + 1, d_id, w_id], txid)
            self.doQueryParam(self.prepared_dict[ txn + "createOrder"], 
                              [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local], txid)
            self.doQueryParam(self.prepared_dict[ txn + "createNewOrder"], 
                              [d_next_o_id, d_id, w_id], txid)
            
            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = [ ]
            total = 0
            for i in range(len(i_ids)):
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]
                itemInfo = items[i]
    
                i_name = itemInfo["I_NAME"]
                i_data = itemInfo["I_DATA"]
                i_price = itemInfo["I_PRICE"]
    
                stockInfo, status = self.doQueryParam(self.prepared_dict[ txn + str(d_id) + "getStockInfo"], 
                                                      [ol_i_id, ol_supply_w_id], txid)
                if len(stockInfo) == 0:
                    logging.warning("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                    continue
    
                s_quantity = stockInfo[0]["S_QUANTITY"]
                s_ytd = stockInfo[0]["S_YTD"]
                s_order_cnt = stockInfo[0]["S_ORDER_CNT"]
                s_remote_cnt = stockInfo[0]["S_REMOTE_CNT"]
                s_data = stockInfo[0]["S_DATA"]
                distxx = "S_DIST_" + str(d_id).zfill(2)
                s_dist_xx = stockInfo[0][distxx] # Fetches data from the s_dist_[d_id] column
    
                ## Update stock
                s_ytd += ol_quantity
                if s_quantity >= ol_quantity + 10:
                    s_quantity = s_quantity - ol_quantity
                else:
                    s_quantity = s_quantity + 91 - ol_quantity
                s_order_cnt += 1
                
                if ol_supply_w_id != w_id: s_remote_cnt += 1
    
                self.doQueryParam(self.prepared_dict[ txn + "updateStock"], 
                                  [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id], txid)
    
                if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                    brand_generic = 'B'
                else:
                    brand_generic = 'G'
    
                ## Transaction profile states to use "ol_quantity * i_price"
                ol_amount = ol_quantity * i_price
                total += ol_amount
    
                self.doQueryParam(self.prepared_dict[ txn + "createOrderLine"], 
                                  [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx], 
                                  txid)
                
    
                ## Add the info to be returned
                item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
            ## FOR

        except Exception as e:
            self.doTranStatement("rollback", self.prepared_dict["workrollback"], txid)
            raise
        else:
            trs, self.tx_status = self.doTranStatement("commit", self.prepared_dict["workcommit"], txid)
    
        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]
            
        # print "//end of NewOrder"
        return [ customer_info, misc, item_data ]
    
    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        txn = "ORDER_STATUS"
        q = TXN_QUERIES[txn]
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)
        txid = ""

        try:
            txid, status =  self.doTranStatement("begin", self.prepared_dict["workbegin"], "")
            if c_id != None:
                customerlist, status = self.doQueryParam(self.prepared_dict[ txn + "getCustomerByCustomerId"], 
                                                         [w_id, d_id, c_id], txid)
                assert len(customerlist) > 0
                customer = customerlist[0]
            else:
                # Get the midpoint customer's id
                all_customers, status = self.doQueryParam(self.prepared_dict[ txn + "getCustomersByLastName"], 
                                                          [w_id, d_id, c_last], txid)
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = int((namecnt-1)/2)
                customer = all_customers[index]
                c_id = customer['C_ID']
    
            assert (len(customer) > 0 or c_id != None)
    
            order, status = self.doQueryParam(self.prepared_dict[ txn + "getLastOrder"], 
                                              [w_id, d_id, c_id], txid)
            if len(order) > 0:
                orderLines, status = self.doQueryParam(self.prepared_dict[ txn + "getOrderLines"], 
                                                       [w_id, d_id, order[0]['O_ID']], txid)
            else:
                orderLines = [ ]

        except Exception as e:
            self.doTranStatement("rollback", self.prepared_dict["workrollback"], txid)
            raise
        else:
            trs, self.tx_status = self.doTranStatement("commit", self.prepared_dict["workcommit"], txid)

        return [ customer, order, orderLines ]

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------
    def doPayment(self, params):
        txn = "PAYMENT"
        q = TXN_QUERIES[txn]
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]
        txid = ""

        try:
            txid, status =  self.doTranStatement("begin", self.prepared_dict["workbegin"], "")
            if c_id != None:
                customerlist, status = self.doQueryParam(self.prepared_dict[ txn + "getCustomerByCustomerId"], 
                                                         [w_id, d_id, c_id], txid)
                assert len(customerlist) > 0
                customer = customerlist[0]
            else:
                # Get the midpoint customer's id
                all_customers,status = self.doQueryParam(self.prepared_dict[ txn + "getCustomersByLastName"], 
                                                         [w_id, d_id, c_last], txid)
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = int((namecnt-1)/2)
                customer = all_customers[index]
                c_id = customer['C_ID']
            assert (len(customer) > 0 or c_id != None)
    
            c_balance = customer['C_BALANCE'] - h_amount
            c_ytd_payment = customer['C_YTD_PAYMENT'] + h_amount
            c_payment_cnt = customer['C_PAYMENT_CNT'] + 1
            c_data = customer['C_DATA']
    
            warehouse, status = self.doQueryParam(self.prepared_dict[ txn + "getWarehouse"], [w_id], txid)
            district, status = self.doQueryParam(self.prepared_dict[ txn + "getDistrict"], [w_id, d_id], txid)
            self.doQueryParam(self.prepared_dict[ txn + "updateWarehouseBalance"], [h_amount, w_id], txid)
            self.doQueryParam(self.prepared_dict[ txn + "updateDistrictBalance"], [h_amount, w_id, d_id], txid)
    
            # Customer Credit Information
            if customer['C_CREDIT'] == constants.BAD_CREDIT:
                newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                c_data = (newData + "|" + c_data)
                if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
                self.doQueryParam(self.prepared_dict[ txn + "updateBCCustomer"], 
                                  [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id], txid)
            else:
                c_data = ""
                self.doQueryParam(self.prepared_dict[ txn + "updateGCCustomer"], 
                                  [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id], txid)
                
            # Concatenate w_name, four spaces, d_name
            # print "warehouse %s" % (str(warehouse))
            # print "district %s" % (str(district))
            h_data = "%s    %s" % (warehouse[0]['W_NAME'], district[0]['D_NAME'])
            # Create the history record
            self.doQueryParam(self.prepared_dict[ txn + "insertHistory"], 
                              [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data], txid)
            
        except Exception as e:
            self.doTranStatement("rollback", self.prepared_dict["workrollback"], txid)
            raise
        else:
            trs, self.tx_status = self.doTranStatement("commit", self.prepared_dict["workcommit"], txid)

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # print "doPayment: Stage5"
        # Hand back all the warehouse, district, and customer data
        return [ warehouse, district, customer ]

    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------
    def doStockLevel(self, params):

        txn = "STOCK_LEVEL"
        q = TXN_QUERIES[txn]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        txid = ""

        try:
            # txid, status =  self.doTranStatement("begin", self.prepared_dict["workbegin"], "")
             result, self.tx_status = self.doQueryParam(self.prepared_dict[ txn + "getOId"], [w_id, d_id], txid)
             assert result
             o_id = result[0]['D_NEXT_O_ID']

             result, self.tx_status = self.doQueryParam(self.prepared_dict[ txn + "getStockCount"], 
                                                        [w_id, d_id, o_id, (o_id - 20), w_id, threshold], txid)
        except Exception as e:
            self.doTranStatement("rollback", self.prepared_dict["workrollback"], txid)
            raise
        else:
            trs, self.tx_status = self.doTranStatement("commit", self.prepared_dict["workcommit"], txid)
        return int(result[0]['CNT_OL_I_ID'])


    ## ----------------------------------------------
    ## BEGIN, COMMIT, ROLLBACK
    ## ----------------------------------------------
    def doTranStatement(self, prefix, query, txid):
        if self.notransactions or (prefix != "begin" and txid == ""):
            return "", "success"
        stmt = generate_prepared_query(query)

        stmt['durability_level'] = self.durability_level
        stmt['scan_consistency'] = self.scan_consistency
        if self.txtimeout != "":
            stmt['txtimeout'] = self.txtimeout
        if txid != "":
            stmt['txid'] = txid

        body = n1ql_execute(self.query_node, stmt, self.query_context, self.creds, self.globpool)
        rs, status = self.queryResults(prefix, body)
        if prefix == "begin":
            self.tx_status = ""
            return rs[0]['txid'], status
        elif not self.allcommits and prefix == "commit" and status != "success":
            raise Exception(status)
        return rs, status

    ## ----------------------------------------------
    ## runNQueryParam
    ## ----------------------------------------------
    def doQueryParam(self, query, param, txid):
        stmt = generate_prepared_query(query)
        if txid != "":
            stmt['txid'] = txid

        if (len(param) > 0):
             qparam = []
             for p in param:
                 if isinstance(p, (datetime)):
                     qparam.append(str(p))
                 else:
                     qparam.append(p)
             stmt['args'] = json.JSONEncoder().encode(qparam)

        body = n1ql_execute(self.query_node, stmt, self.query_context, self.creds, self.globpool)
        return self.queryResults("", body)

    ## ----------------------------------------------
    ## process query results
    ## ----------------------------------------------
    def queryResults(self, prefix, rj):

        if 'status' not in rj:
            return rj, "assert"
        status = rj['status']
        if status != "success" :
            if rj['errors'][0]["code"] == 17010 :
                status = "timeout"
            elif ( (rj['errors'][0]["code"] == 17007) and
                 ("cause" in rj['errors'][0]) and
                 ("cause" in rj['errors'][0]['cause']) ) :
                 if rj['errors'][0]['cause']['cause'] == "found existing document: document already exists" :
                      status = "duplicates"
                 elif (("msg" in rj['errors'][0]['cause']['cause']) and 
                        (rj['errors'][0]['cause']['cause']['msg'] == "write write conflict")):
                       status = "wwconflict"  
                 elif ("error_description" in rj['errors'][0]['cause']['cause'])  :
                      status = rj['errors'][0]['cause']['cause']['error_description']
                      if status == "key already exists, or CAS mismatch" :
                          status = "casmismatch"
    
            #if status != "casmismatch" and status != "timeout" and status != "duplicates" and status != wwconflict" :
            #    print rj
            if prefix != "" :
                status = prefix + "-" + status

        return rj['results'], status

## CLASS
