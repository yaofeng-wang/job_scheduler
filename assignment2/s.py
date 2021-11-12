import subprocess
import pickle
import shlex
import time
import os
from collections import defaultdict
import statistics
import math

NUM_TESTCASES = 1
CLIENT_PICKLE = "client.pickle"
SERVER_PICKLE = "server.pickle"
READ_BINARY_MODE = "rb"

def my_percentile(data, percentile):
    n = len(data)
    p = n * percentile / 100
    if p.is_integer():
        return sorted(data)[int(p)]
    else:
        return sorted(data)[int(math.ceil(p)) - 1]

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
    p50 = statistics.median(list_tsdiff)
    p95 = my_percentile(list_tsdiff, 95)
    return p50, p95

def processPickles(tcDir):
    map_ts_begin = loadPickle(os.path.join(tcDir, CLIENT_PICKLE))
    map_ts_complete = loadPickle(os.path.join(tcDir, SERVER_PICKLE))
    return calcJCTs(map_ts_begin, map_ts_complete)

def calcAverageStat(stat):
    avgTimings = dict()
    for k in stat.keys():
        avgTimings[k] = statistics.mean(stat[k])
    return avgTimings

def printStat(avgTimings, start, stat):
    for k, v in stat.items():
        print(f"{k}:{v}")
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
    command = f"./server_client -port {port} -prob {prob}"
    sc = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, cwd=tcDir)
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
    retry = True
    while retry:
        try:
            for prob in [50]:
                for tcIndex in range(NUM_TESTCASES):

                    tcDir = f"/home/y/yaofengw/assignment2/testcases/{tcIndex}"
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
            printStat(avgTimings, start, stat)
            retry = False
        except OSError as e:
            print(e)
            retry = True
