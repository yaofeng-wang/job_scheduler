import subprocess
import pickle
import numpy as np
import shlex
import time
import os
from collections import defaultdict

NUM_TESTCASES = 2
CLIENT_PICKLE = "client.pickle"
SERVER_PICKLE = "server.pickle"
READ_BINARY_MODE = "rb"

def portGenerator():
    port = 12344

    def nextPort():
        nonlocal port
        port += 1
        return port
    return nextPort

def loadPickle(filename):
    with open(filename, READ_BINARY_MODE) as file:
        map_ts = pickle.load(file)
    return {k: float(v) for k, v in map_ts.items()}

def calcJCTs(map_ts_begin, map_ts_complete):
    assert(set(map_ts_begin.keys()) == set(map_ts_complete.keys()))
    list_tsdiff = []
    for filename in map_ts_begin.keys():
        list_tsdiff.append(round(2 * (map_ts_complete[filename] - map_ts_begin[filename]), 1)/2.0)
    return list_tsdiff

def calcStat(list_tsdiff):
    p50 = np.median(list_tsdiff)
    p95 = np.percentile(list_tsdiff, 95)
    return p50, p95

def processPickles(tcDir):
    map_ts_begin = loadPickle(os.path.join(tcDir, CLIENT_PICKLE))
    map_ts_complete = loadPickle(os.path.join(tcDir, SERVER_PICKLE))
    return calcJCTs(map_ts_begin, map_ts_complete)

def calcAverageStat(stat):
    avgTimings = dict()
    for k in stat.keys():
        avgTimings[k] = np.mean(stat[k])
    return avgTimings

def printStat(avgTimings, start):
    end = time.time()
    print(f"Total time taken for {NUM_TESTCASES} test: {end-start}s")
    keys = sorted(avgTimings.keys())
    for k in keys:
        print(f'{k}: {avgTimings[k]}')

def removePickles(path_to_client_pickle, path_to_server_pickle):
    if os.path.exists(path_to_client_pickle):
        os.remove(path_to_client_pickle)
    if os.path.exists(path_to_server_pickle):
        os.remove(path_to_server_pickle)

def startScheduler(tcDir, getNextPort, prob):
    print(f"***Start test in {tcDir}")
    port = getNextPort()
    print(f"Using port: {port}")
    command = f"cd {tcDir}; ./server_client -port {port} -prob {prob}"
    sc = subprocess.Popen(command, shell=True)
    time.sleep(2)
    command = f"/usr/bin/python3 jobScheduler.py -port {port}"
    scheduler = subprocess.Popen(shlex.split(command))
    while sc.poll() is None:
        time.sleep(2)
    scheduler.terminate()
    print(f"***End test in {tcDir}")

if __name__ == "__main__":
    stat = defaultdict(list)
    getNextPort = portGenerator()
    start = time.time()
    for prob in [0, 50, 100]:
        for tcIndex in range(NUM_TESTCASES):
            tcDir = f"/home/yfwang/assignment2/testcases/{tcIndex}"
            path_to_client_pickle = os.path.join(tcDir, CLIENT_PICKLE)
            path_to_server_pickle = os.path.join(tcDir, SERVER_PICKLE)
            removePickles(path_to_client_pickle, path_to_server_pickle)
            startScheduler(tcDir, getNextPort, prob)
            list_tsdiff = processPickles(tcDir)
            p50, p95 = calcStat(list_tsdiff)
            stat[f'prob{prob}_p50s'].append(p50)
            stat[f'prob{prob}_p95s'].append(p95)
            removePickles(path_to_client_pickle, path_to_server_pickle)
    avgTimings = calcAverageStat(stat)
    printStat(avgTimings, start)
