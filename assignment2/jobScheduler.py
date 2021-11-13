from datetime import datetime, timedelta
import socket
import sys
import argparse
import signal
import time
from typing import Tuple

J = "Jobs"
LUT = "Last Update Time"
NACJ = "Num Active Jobs"
ACL = "Active Load"
B = "Bandwidth"
P = "Priority"
NASJ = "Num Assigned Jobs"
ASL = "Assigned Load"
DB = 50.0  # Default Bandwidth
EB = "Estimating Bandwidth"
FF = "Force Fed"

MODE_D = "MODE_DECREASE"
MODE_I = "MODE_INCREASE"

class serverQueue:

    def __init__(self, servernames, startTime):
        self.serverDetails = {name: {
            J: dict(),
            LUT: startTime,
            NACJ: 0,
            B: DB,
            P: 0,
            NASJ: 0,
            EB: 0,
            ACL: 0,
            ASL: 0,
            FF: 0,
            } for name in servernames}
        self.jobDetails = {}
        self.numForceFed = 0
        self.TFL = 0 # Total Finished Load
        self.DL = 200.0  # Default Load
        self._updatePs()


    def _findServerWithMost(self, attribute):
        serverDetail = self.serverDetails
        maxVal = 0
        server = None

        for key in serverDetail.keys():
            if serverDetail[key][attribute] > maxVal:
                server = key
                maxVal = serverDetail[key][attribute]
        return server


    def _isUnknownJobSize(self, jobSize):
        return jobSize == '-1' or jobSize == -1.0


    def _updatePs(self):
        for server in self.serverDetails.keys():
            divisor = max(1, self.serverDetails[server][ACL])
            dividend = self.serverDetails[server][B]
            self.serverDetails[server][P] = dividend / divisor
            if self.TFL >= 1000:
                finishedLoadForServer = self.serverDetails[server][ASL] - self.serverDetails[server][ACL]
                self.serverDetails[server][P] *= (finishedLoadForServer + 0.1) / self.TFL


    def _addJobToServerDetails(self, server, jobName, jobSize):
        serverDetail = self.serverDetails[server]
        load = self.DL if self._isUnknownJobSize(jobSize) else float(jobSize)
        if EB not in serverDetail:
            _ = self._updateNACJ(server, MODE_I)
            _ = self._updateNASJ(server, MODE_I)
            self._updateACL(server, MODE_I, load)
            self._updateASL(server, MODE_I, load)
            self._updatePs()
            return
        else:
            prevNACJ = self._updateNACJ(server, MODE_I)
            _ = self._updateNASJ(server, MODE_I)
            self._updateACL(server, MODE_I, load)
            self._updateASL(server, MODE_I, load)
            self._updatePs()
            self._updateJ(server, prevNACJ)
            if not self._isUnknownJobSize(jobSize):
                serverDetail[J][jobName] = 0


    def _removeJobFromServerDetails(self, server, jobName, jobSize):
        load = self.DL if self._isUnknownJobSize(jobSize) else float(jobSize)
        if EB not in self.serverDetails[server]: # already know true BW
            _ = self._updateNACJ(server, MODE_D)
            self._updateACL(server, MODE_D, load)
            self._updatePs()
        elif self._isUnknownJobSize(jobSize):
            prevNACJ = self._updateNACJ(server, MODE_D)
            self._updateACL(server, MODE_D, load)
            self._updateJ(server, prevNACJ)
            self._updatePs()
        else: # probe job
            prevNACJ = self._updateNACJ(server, MODE_D)
            self._updateACL(server, MODE_D, load)
            self._updateJ(server, prevNACJ)
            self._updateB(server, jobName, jobSize)
            self._updatePs()
            self._serverStopEB(server)


    def _increaseTFL(self, jobSize):
        load = self.DL if self._isUnknownJobSize(jobSize) else float(jobSize)
        self.TFL += load


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


    def _serverStopEB(self, server):
        serverDetail = self.serverDetails[server]
        serverDetail.pop(J)
        serverDetail.pop(LUT)
        serverDetail.pop(EB)


    def _updateJ(self, server, prevNACJ):
        serverDetail = self.serverDetails[server]
        lut = serverDetail[LUT]
        now = datetime.now()
        e = ((now - lut) / timedelta(microseconds=1))

        for job in serverDetail[J].keys():
            serverDetail[J][job] += e / prevNACJ
        serverDetail[LUT] = now


    def _updateB(self, server, jobName, jobSize):
        serverDetail = self.serverDetails[server]
        serverDetail[B] = round(jobSize / (serverDetail[J][jobName] / 1_000_000), 3)


    def _updateACL(self, server, mode, load):
        serverDetail = self.serverDetails[server]
        if mode == MODE_D:
            serverDetail[ACL] -= load
        elif mode == MODE_I:
            serverDetail[ACL] += load


    def _updateASL(self, server, mode, load):
        serverDetail = self.serverDetails[server]
        if mode == MODE_D:
            serverDetail[ASL] -= load
        elif mode == MODE_I:
            serverDetail[ASL] += load


    def _updateNASJ(self, server, mode):
        serverDetail = self.serverDetails[server]
        prevNASJ = serverDetail[NASJ]
        if mode == MODE_D:
            serverDetail[NASJ] -= 1
        elif mode == MODE_I:
            serverDetail[NASJ] += 1
        return prevNASJ


    def _updateNACJ(self, server, mode):
        serverDetail = self.serverDetails[server]
        prevNACJ = serverDetail[NACJ]
        if mode == MODE_D:
            serverDetail[NACJ] -= 1
        elif mode == MODE_I:
            serverDetail[NACJ] += 1
        return prevNACJ


    def _hasForceFedAll(self):
        return self.numForceFed == len(self.serverDetails)


    def _forceFeed(self):
        for server in self.serverDetails.keys():
            if FF in self.serverDetails[server]:
                self.numForceFed += 1
                self.serverDetails[server].pop(FF)
                return server
        return None


    def printServerStatus(self):
        self._printServerDetails()
        self._printJobDetails()


    def getServer(self, jobName, jobSize):
        if self._hasForceFedAll() or self._isUnknownJobSize(jobSize):
            server = self._findServerWithMost(P)
        else:
            server =  self._forceFeed()
        self._addJobToServerDetails(server, jobName, jobSize)
        self._addJobToJobDetails(server, jobName, jobSize)
        return server


    def _updateDefaultLoad(self, jobSize):
        if self._isUnknownJobSize(jobSize):
            return
        self.DL = 0.5 * self.DL + 0.5 * float(jobSize)

    def removeJob(self, jobName):
        server, jobSize = self._removeJobFromJobDetails(jobName)
        self._removeJobFromServerDetails(server, jobName, jobSize)
        self._increaseTFL(jobSize)


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
