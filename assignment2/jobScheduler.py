from datetime import datetime, timedelta
import socket
import sys
import argparse
import signal
import time
from typing import Tuple

JOBS = "JOBS"
LAST_UPDATE_TIME = "LAST_UPDATE_TIME"
NUM_ACTIVE_JOBS = "NUM_ACTIVE_JOBS"
BANDWIDTH = "BANDWIDTH"
NBPAJ = "NBPAJ"
NUM_ASSIGNED_JOBS = "NUM_ASSIGNED_JOBS"
DEFAULT_BANDWIDTH = 50.0
ESTIMATING_BW = "ESTIMATING_BW"
NPBJ_BONUS_ZERO_CONNECTION = -10.0
MODE_DECREASE = "MODE_DECREASE"
MODE_INCREASE = "MODE_INCREASE"

class serverQueue:

    def __init__(self, servernames, startTime):
        """
        contains details for each server:
        - JOBS([]): stores information for all jobs with known sizes
        that were allocated to the server. For each job, stores
         bandwidth allocation (as a proportion of bandwidth).
        - LAST_UPDATE_TIME(startTime): time since last update.
        - NUM_ACTIVE_JOBS(0): number of active jobs in server.
        - BANDWIDTH(None): effective bandwidth for each job.
        - NBPAJ(0): negative bandwidth per active job
        """
        self.serverDetails = {name: {
            JOBS: dict(),
            LAST_UPDATE_TIME: startTime,
            NUM_ACTIVE_JOBS: 0,
            BANDWIDTH: DEFAULT_BANDWIDTH,
            NBPAJ: - DEFAULT_BANDWIDTH + NPBJ_BONUS_ZERO_CONNECTION,
            NUM_ASSIGNED_JOBS: 0,
            ESTIMATING_BW: True
            } for name in servernames}

        # maps each job with known size to the servername with
        # unkown job size that it was allocated to and its size
        self.jobDetails = {}

        self.allBandwidthsKnown = False


    def _findServerWithLeast(self, attribute):
        serverDetail = self.serverDetails
        leastVal = float('inf')
        server = None

        for key in serverDetail.keys():
            if serverDetail[key][attribute] < leastVal:
                server = key
                leastVal = serverDetail[key][attribute]
        return server


    def _isUnknownJobSize(self, jobSize):
        return jobSize == '-1' or jobSize == -1.0


    def _updateNBPAJ(self, server):
        divisor = max(1, self.serverDetails[server][NUM_ACTIVE_JOBS])
        dividend = - self.serverDetails[server][BANDWIDTH]
        if self.serverDetails[server][NUM_ACTIVE_JOBS] == 0:
            dividend += NPBJ_BONUS_ZERO_CONNECTION
        self.serverDetails[server][NBPAJ] = round(dividend / divisor, 3)


    def _addJobToServerDetails(self, server, jobName, jobSize):
        serverDetail = self.serverDetails[server]

        if not serverDetail[ESTIMATING_BW]:
            _ = self._updateNumActiveJobs(server, MODE_INCREASE)
            _ = self._updateNumAssignedJobs(server, MODE_INCREASE)
            self._updateNBPAJ(server)
            return
        else:
            prevNumActiveJobs = self._updateNumActiveJobs(server, MODE_INCREASE)
            _ = self._updateNumAssignedJobs(server, MODE_INCREASE)
            self._updateNBPAJ(server)
            self._updateJOBS(server, prevNumActiveJobs)
            if not self._isUnknownJobSize(jobSize):
                serverDetail[JOBS][jobName] = 0


    def _removeJobFromServerDetails(self, server, jobName, jobSize):
        if not self.serverDetails[server][ESTIMATING_BW]: # already know true BW
            _ = self._updateNumActiveJobs(server, MODE_DECREASE)
            self._updateNBPAJ(server)
            return
        elif self._isUnknownJobSize(jobSize):
            prevNumActiveJobs = self._updateNumActiveJobs(server, MODE_DECREASE)
            self._updateJOBS(server, prevNumActiveJobs)
            self._updateNBPAJ(server)
            return
        else: # probe job
            prevNumActiveJobs = self._updateNumActiveJobs(server, MODE_DECREASE)
            self._updateJOBS(server, prevNumActiveJobs)
            self._updateBANDWIDTH(server, jobName, jobSize)
            self._updateNBPAJ(server)
            self._serverStopEstimatingBW(server)


    def _addJobToJobDetails(self, server: str, jobName: str, jobSize: str) -> None:
        self.jobDetails[jobName] = [server, float(jobSize)]

    def _removeJobFromJobDetails(self, jobName: str) -> Tuple[str, float]:
         return self.jobDetails.pop(jobName)

    def _printServerDetails(self):
        for k,v in self.serverDetails.items():
            print(f"{k}:{v}")


    def _printJobDetails(self):
        for k,v in self.jobDetails.items():
            print(f"{k}:{v}")


    def _serverStopEstimatingBW(self, server):
        serverDetail = self.serverDetails[server]
        serverDetail.pop(JOBS)
        serverDetail.pop(LAST_UPDATE_TIME)
        serverDetail[ESTIMATING_BW] = False


    def _updateJOBS(self, server, prevNumActiveJobs):
        serverDetail = self.serverDetails[server]
        lut = serverDetail[LAST_UPDATE_TIME]
        now = datetime.now()
        e = ((now - lut) / timedelta(microseconds=1))

        for job in serverDetail[JOBS].keys():
            serverDetail[JOBS][job] += e / prevNumActiveJobs
        serverDetail[LAST_UPDATE_TIME] = now


    def _updateBANDWIDTH(self, server, jobName, jobSize):
        serverDetail = self.serverDetails[server]
        serverDetail[BANDWIDTH] = round(jobSize / (serverDetail[JOBS][jobName] / 1_000_000), 3)


    def _updateNumAssignedJobs(self, server, mode):
        serverDetail = self.serverDetails[server]
        prevNumAssignedJobs = serverDetail[NUM_ASSIGNED_JOBS]
        if mode == MODE_DECREASE:
            serverDetail[NUM_ASSIGNED_JOBS] -= 1
        elif mode == MODE_INCREASE:
            serverDetail[NUM_ASSIGNED_JOBS] += 1
        return prevNumAssignedJobs


    def _updateNumActiveJobs(self, server, mode):
        serverDetail = self.serverDetails[server]
        prevNumActiveJobs = serverDetail[NUM_ACTIVE_JOBS]
        if mode == MODE_DECREASE:
            serverDetail[NUM_ACTIVE_JOBS] -= 1
        elif mode == MODE_INCREASE:
            serverDetail[NUM_ACTIVE_JOBS] += 1
        return prevNumActiveJobs


    def getServer(self, jobName, jobSize):
        server = self._findServerWithLeast(NBPAJ)
        self._addJobToServerDetails(server, jobName, jobSize)
        self._addJobToJobDetails(server, jobName, jobSize)
        return server


    def printServerStatus(self):
        self._printServerDetails()
        self._printJobDetails()


    def removeJob(self, jobName):
        server, jobSize = self._removeJobFromJobDetails(jobName)
        self._removeJobFromServerDetails(server, jobName, jobSize)


# KeyboardInterrupt handler
def sigint_handler(signal, frame):
    print('KeyboardInterrupt is caught. Close all sockets :)')
    sys.exit(0)

# send trigger to printAll at servers
def sendPrintAll(serverSocket):
    serverSocket.send(b"printAll\n")

# Parse available severnames
def parseServernames(binaryServernames):
    return binaryServernames.decode().split(',')[:-1]

# get the completed file's name, what you want to do?
def getCompletedFilename(filename):
    ####################################################
    #                      TODO                        #
    # You should use the information on the completed  #
    # job to update some statistics to drive your      #
    # scheduling policy. For example, check timestamp, #
    # or track the number of concurrent files for each #
    # server?                                          #
    ####################################################
    sq.removeJob(filename)
    print(f"[JobScheduler] Filename {filename} is finished.")


# formatting: to assign server to the request
def scheduleJobToServer(servername, request):
    return (servername + "," + request + "\n").encode()

# main part you need to do
def assignServerToRequest(servernames, request, sq):
    ####################################################
    #                      TODO                        #
    # Given the list of servers, which server you want #
    # to assign this request? You can make decision.   #
    # You can use a global variables or add more       #
    # arguments.                                       #

    request_name = request.split(",")[0]
    request_size = request.split(",")[1]
    server_to_send = sq.getServer(request_name, request_size)

    ####################################################

    # Schedule the job
    scheduled_request = scheduleJobToServer(server_to_send, request)
    return scheduled_request


def parseThenSendRequest(clientData, serverSocket, servernames, sq):
    # print received requests
    print(f"*******************")
    print(f"[JobScheduler] Received binary messages:\n{clientData}\n")

    # parsing to "filename, jobsize" pairs
    requests = clientData.decode().split("\n")[:-1]
    sendToServers = b""
    for request in requests:
        if request[0] == "F":
            # if completed filenames, get the message with leading alphabet "F"
            filename = request.replace("F", "")
            getCompletedFilename(filename)
        else:
            # if requests, add "servername" front of the pairs -> "servername, filename, jobsize"
            sendToServers = sendToServers + \
                assignServerToRequest(servernames, request, sq)

    # send "servername, filename, jobsize" pairs to servers
    if sendToServers != b"":
        serverSocket.send(sendToServers)

    sq.printServerStatus()
    print(f"--------------------")

if __name__ == "__main__":
    # catch the KeyboardInterrupt error in Python
    signal.signal(signal.SIGINT, sigint_handler)

    # parse arguments and get port number
    parser = argparse.ArgumentParser(description="JobScheduler.")
    parser.add_argument('-port', '--server_port', action='store', type=str, required=True,
                        help='port to server/client')
    args = parser.parse_args()
    server_port = int(args.server_port)

    # open socket to servers
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.connect(('127.0.0.1', server_port))

    # IMPORTANT: for 50ms granularity of emulator
    serverSocket.settimeout(0.0001)

    # receive preliminary information: servernames (can infer the number of servers)
    binaryServernames = serverSocket.recv(4096)
    servernames = parseServernames(binaryServernames)
    print(f"Servernames: {servernames}")

    currSeconds = -1
    now = datetime.now()
    sq = serverQueue(servernames, now)
    while (True):
        try:
            # receive the completed filenames from server
            completeFilenames = serverSocket.recv(4096)
            if completeFilenames != b"":
                parseThenSendRequest(
                    completeFilenames, serverSocket, servernames, sq)
        except socket.timeout:

            # IMPORTANT: catch timeout exception, DO NOT REMOVE
            pass

        # Example printAll API : let servers print status in every seconds
        # if (datetime.now() - now).seconds > currSeconds:
        #     currSeconds = currSeconds + 1
        #     sendPrintAll(serverSocket)
