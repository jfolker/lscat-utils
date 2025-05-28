#!/usr/bin/python3

#
# This script is effectively a shim between EPICs PVs and a Redis store,
# designed for language runtimes that do not have an EPICS library,
# e.g. nodejs. It is designed to be run only on internal networks, with the
# sole exception of Argonne APS networks protected by their firewall and IT
# staff; the network we run on is assumed secure.
#
# The config file is a set of mappings of EPICS PVs to some Redis hashes, and
# the default config file which serves as an example uses the names
# historically used by other programs written by LS-CAT's previous lone
# software developer from 2006-2021, Keith Brister. You may notice some colons
# in the Redis hash names: that is not kosher, and this script substitutes
# them for dots.
#
# -Jory Folker, May 2025
#

import json
import os
import redis
import threading
import time
from redis.retry import Retry
from redis.exceptions import (TimeoutError, ConnectionError)
from redis.backoff import ExponentialBackoff

print("NOTICE: lspvmonitor is starting")
# Unless the caller of this script sets the env var, set it with every
# broadcast address and every single IOC we can think of on LS-CAT's network.
if not os.environ.get('EPICS_CA_ADDR_LIST'):
    apsIocIps     = '164.54.252.2'

    vmeCrateIps   = '10.1.0.3'
    monoIps       = '10.1.17.1 10.1.17.2 10.1.17.15'
    beamStsIps    = '10.1.17.4 10.1.17.6 10.1.17.8 10.1.17.10'
    detectorIps   = '10.1.252.248 10.1.252.249 10.1.252.250'
    camIocIps     = '164.54.252.19 10.1.253.14 10.1.253.18 10.1.253.19 10.1.253.164'
    bimorphIps    = '10.1.17.31 10.1.17.32'
    liberaIps     = '10.1.17.33 10.1.17.34'

    broadcastIps  = '164.54.253.255 10.255.255.255 10.1.255.255'

    os.environ['EPICS_CA_ADDR_LIST'] = f'{broadcastIps} {apsIocIps} {vmeCrateIps} {monoIps} {beamStsIps} {detectorIps} {camIocIps} {bimorphIps} {liberaIps}'
    print(f"NOTICE: using EPICS_CA_ADDR_LIST={os.environ['EPICS_CA_ADDR_LIST']}")
    
else:
    print(f"NOTICE: using EPICS_CA_ADDR_LIST inherited from environment:\n\nEPICS_CA_ADDR_LIST={os.environ['EPICS_CA_ADDR_LIST']}")

import epics

f = open(os.environ.get('LSPVMONITOR_CONFIG_FILE', 'lspvmonitor.config.json'))
config = json.load(f)

# Indexed by station letter, each station contains 2 attributes:
# "stnNum" (integer) and "redis" (a redis connection)
stations = {}

# Mapping of an EPICS PV name to the redis hash for each station where it
# exists.
#
# Example:
# { "PA:21ID:A_BEAM_PRESENT" : {
#     "f": "stns.3.pss.a_beam_present",
#     "g": "stns.4.pss.a_beam_present"
#   }
# }
pvMappings = {}
for stn, stnConfig in config.items():
    stnNum = stnConfig["stnNum"]
    retryStrategy = Retry(ExponentialBackoff(cap=10,base=1), 10)
    retryableErrors = [ConnectionError, TimeoutError, ConnectionResetError]
    redisConn = redis.Redis(stnConfig["iocRedis"], retry=retryStrategy,
                            retry_on_error=retryableErrors) 
    stations[stn] = {"stnNum": stnNum, "redis": redisConn}
    for pvInfo in stnConfig["pvs"]:
        pvName = pvInfo["INP"]
        if pvName not in pvMappings:
            pvMappings[pvName] = {}
        
        subHash = pvInfo['IN_PV'].replace(':', '.')
        redisHash = f"stns.{stnNum}.{subHash}"
        pvMappings[pvName][stn] = redisHash
        pvInfo["lspvmonitor_status"] = "inactive"
        redisConn.hset(redisHash, mapping=pvInfo)

retryQueue = []
queueLock = threading.Lock()
def lsPvCallback(pvname=None, value=None, char_value='', status=0, **kw):
    global retryQueue, queueLock
    if pvname is None or value is None:
        raise ValueError(f"Unexpected behavior in pyepics, the camonitor callback was called with one or more missing arguments: pvname={pvname}, value={value}")
    
    for stn, redisHash in pvMappings[pvname].items():
        m = {"lspvmonitor_status": "inactive"}
        if status == 0:
            m = {"VALUE": value, "lspvmonitor_status": "active"}
        else:
            print(f"ERROR: lost connection to {pvname}, adding to retry queue")
            epics.camonitor_clear(pvname)
            with queueLock:
                retryQueue.append(pvname)
            
        redisConn.hset(redisHash, mapping=m)

def registerPvs(pvNames, cagetTimeout=1.0):
    failedPvs = []
    pvValues = epics.caget_many(pvNames, connection_timeout=cagetTimeout)
    if len(pvValues) != len(pvNames):
        raise ValueError("caget_many did not return the expected number of results.")
    
    for i in range(len(pvValues)):
        pvName = pvNames[i]
        pvValue = pvValues[i]
        if pvValue is None:
            print(f"ERROR: could not connect to {pvName}, adding to retry queue")
            failedPvs.append(pvName)
        for stn, redisHash in pvMappings[pvName].items():
            if pvValue is None:
                redisConn.hset(redisHash, "lspvmonitor_status", "inactive")
            else:
                print(f"INFO: connected to {pvName}, publishing value to {redisHash}")
                m = {"VALUE": pvValue, "lspvmonitor_status": "active"}
                redisConn.hset(redisHash, mapping=m)
                epics.camonitor(pvName, writer=None, callback=lsPvCallback)
            
    return failedPvs

def retryQueueServicer():
    global retryQueue, queueLock
    while True:
        # 5 seconds of sleep for servicing rare broken connections is
        # reasonable, but we can make this an env var later if necessary.
        time.sleep(5)
        with queueLock:
            retryQueue = registerPvs(retryQueue)
        

print("NOTICE: lspvmonitor is registering PVs for CA monitor")
retryQueue = registerPvs(list(pvMappings.keys()), cagetTimeout=5.0)

print("NOTICE: lspvmonitor is ready")
#
# We service the retry queue in a separate thread because pyepics doesn't
# have a non-blocking way to try reconnecting to PVs on error. Python's GIL
# is not a problem for our concurrency model because we are only trying to
# avoid being blocked on polling for I/O, not parallelizing compute-intensive
# work.
#
retryerThread = threading.Thread(target=retryQueueServicer)
retryerThread.start()
while True:
    # We use the minimum polling interval that pyepics will actually honor.
    # This is supposed to be a float parameter and allow us to specify
    # millisecond granularity, but in practice anything less than 1 will result
    # in pyepics polling longer than 1 second and then reporting a timeout
    # after some negative number of seconds.
    #
    # It's worth fixing in pyepics, but for now we must work around it.
    epics.ca.pend_event(timeout=1.0)
retryerThread.join()
print("NOTICE: lspvmonitor is terminating")
