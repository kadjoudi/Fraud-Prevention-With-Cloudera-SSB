#!/usr/bin/python
""" 
  The implementation of the synthetic financial transaction stream,
  used in the main script of gess.

@author: Michael Hausenblas, http://mhausenblas.info/#i
@since: 2013-11-08
@status: init
"""
import sys
import os
import socket
import logging
import string
import datetime
import random
import uuid
import csv
import json
from time import sleep
from random import randint
import os
import random
import math
from random import uniform
import time
from time import sleep

#The target host Ip address need to be updated with the current UDP Server  IP address.
TARGET_HOST = "10.10.1.203"
UDP_PORT = 6900
DELAY = 1.5

# defines the sampling interval (in seconds) for reporting runtime statistics
SAMPLE_INTERVAL = 10

# lower range for randomly emitted frauds (min. tick between trans)
FRAUD_TICK_MIN = 5

# upper range for randomly emitted frauds (max. tick between trans)
FRAUD_TICK_MAX = 15


# Add some data = Amounts and Cities.
AMOUNTS = [20, 50, 100, 200, 300, 400,500]
CITIES = [                                                                                                                                                                                                                                                     
    {"lat": 48.8534, "lon": 2.3488, "city": "Paris"},                                                                                                                                                                                                    
    {"lat": 43.2961743, "lon": 5.3699525, "city": "Marseille"},                                                                                                                                                                                                 
    {"lat": 45.7578137, "lon": 4.8320114, "city": "Lyon"},                                                                                                                                                                                                      
    {"lat": 50.6365654, "lon": 3.0635282, "city": "Lille"},
    {"lat": 44.841225, "lon": -0.5800364, "city": "Bordeaux"}
]        

# Define geo functions
def create_random_point(x0, y0, distance):
    r = distance/111300
    u = random.random()
    v = random.random()
    w = r * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    x1 = x / math.cos(y0)
    y = w * math.sin(t)
    return (x0+x1, y0 +y)

def create_geopoint(lat, lon):
    return create_random_point(lat, lon, 50000)

def get_latlon():                                                                                                                                                                                                                                              
    geo = random.choice(CITIES)
    return create_geopoint(geo['lat'], geo['lon']),geo['city']        


# creates a single financial transaction  using the following
  # format:
#    {
#       'ts': '2013-11-08T10:58:19.668225',
#       'account_id': 'a335',
#       'transaction_id': '636adacc-49d2-11e3-a3d1-a820664821e3'
#       'amount': 100,
#       'lat': '36.7220096',
#       'lon': '-4.4186772'
#     }

def create_fintran():
 
    latlon,city = get_latlon()
    tsbis=(datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S ")
    date = str(datetime.datetime.strptime(tsbis, "%Y-%m-%d %H:%M:%S "))
    fintran = {
      'ts': date,
      'account_id' : str(random.randint(1, 1000)),
      'transaction_id' : str(uuid.uuid1()),
      'amount' : random.randrange(1,2000),  
      'lat' : latlon[0],
      'lon' : latlon[1]
    }    

    return (fintran)

# creates a single fraudulent financial transaction
  # based on an existing transaction, using the following format:
  # {
  #   'ts': '2013-11-08T12:28:39.466325',
  #    'account_id': 'a335',
  #   'transaction_id': 'xxx636adacc-49d2-11e3-a3d1-a820664821e3'
  #   'amount': 200,
  #   'lat': '39.5655472',
  #   'lon': '-0.530058'
  # }
  # Note: the fraudulent transaction will have the same account ID as
  #       the original transaction but different location and ammount.

def create_fraudtran(fintran):
   
    latlon,city = get_latlon()
    tsbis = str((datetime.datetime.now() - datetime.timedelta(seconds=random.randint(60,600))).strftime("%Y-%m-%d %H:%M:%S "))
    
    fraudtran = {
      'ts' : tsbis,
      'account_id' : fintran['account_id'],
      'transaction_id' : 'xxx' + str(fintran['transaction_id']),
      'amount' : random.randrange(1,2000),      
      'lat' : latlon[0],
      'lon' : latlon[1]
    }    
    return (fraudtran)


def send_fintran(out_socket, fintran):
    out_socket.sendto(str.encode(fintran), (TARGET_HOST, UDP_PORT))

# Main programm 

out_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # use UDP
ticks = 0 # ticks (virtual time basis for emits)
fraud_tick = random.randint(FRAUD_TICK_MIN, FRAUD_TICK_MAX) 
while True:
 ticks += 1
 fintran = create_fintran()   
 fintransaction =  json.dumps(fintran)
 send_fintran(out_socket, json.dumps(fintran))
 sleep(DELAY)
 if ticks > fraud_tick:
    fraudtran = create_fraudtran(fintran)
    fraudfintransaction=json.dumps(fraudtran)
    send_fintran(out_socket, json.dumps(fraudtran))
    ticks = 0
    fraud_tick = random.randint(FRAUD_TICK_MIN, FRAUD_TICK_MAX)

    

  
  

