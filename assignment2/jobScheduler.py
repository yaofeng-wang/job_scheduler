from datetime import datetime, timedelta
import socket
import sys
import argparse
import signal

class serverQueue:

    def __init__(self, servernames):
        self.s = {name:0 for name in servernames}
        self.l = {}

    def getServer(self, filename):
        minVal = float('inf')
        minServer = None
        for key in self.s.keys():
            if self.s[key] < minVal:
                minServer = key
                minVal = self.s[key]
        self.s[minServer] += 1
        self.l[filename] = minServer
        print(f"Sending {filename} to {minServer}.")
        return minServer

    def removeLoad(self, filename):
        server = self.l[filename]
        self.l.pop(filename)
        self.s[server] -= 1




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

    print(f"[JobScheduler] Filename {filename} is finished.")
    sq.removeLoad(filename)

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

    # Example. just assign the first server
    server_to_send = sq.getServer(request_name)

    ####################################################

    # Schedule the job
    scheduled_request = scheduleJobToServer(server_to_send, request)
    return scheduled_request


def parseThenSendRequest(clientData, serverSocket, servernames, sq):
    # print received requests
    print(f"[JobScheduler] Received binary messages:\n{clientData}")
    print(f"--------------------")
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

    sq = serverQueue(servernames)

    currSeconds = -1
    now = datetime.now()
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

        # # Example printAll API : let servers print status in every seconds
        # if (datetime.now() - now).seconds > currSeconds:
        #     currSeconds = currSeconds + 1
        #     sendPrintAll(serverSocket)
