#!/usr/bin/python3
from socket import *
import os
import threading
import hashlib
import sys
import time
import random

#use this to receive strings that are newline terminated
def recvString(conn):
    string = ''
    char = conn.recv(1).decode()
    while char != '\n':
        string += char
        char = conn.recv(1).decode()

    return string

#take the string with the chunk information and return that info as a tuple
def parseChunkInfo(string):
    info = string.split(',')
    info = (int(info[0]), info[1])
    return info

#sends data to the client that requested a chunk
def sendData(clientInfo):
    clientConn, clientAddr = clientInfo
    chunkIndex = recvString(clientConn)
    chunkIndex = int(chunkIndex)
    clientConn.send(chunkBytes[chunkIndex])
    clientConn.close()

running = True

##handles if a connection is made, if so, creates a thread for send data
def handleClient(listener):
    while running:
        clientInfo = listener.accept()
        threading.Thread(target=sendData, args = (clientInfo,), daemon=True).start()



#Init variables we'll need
fileName = ''
maxChunkSize = 0
numChunks = 0
chunkInfo = []
myChunkMask = ''
listenerPort = int(input('Enter a port number to listen on: '))

#connect to the server
serverIP = '10.92.21.15'
serverPort = 42425
serverConn = socket(AF_INET, SOCK_STREAM)
serverConn.connect( (serverIP, serverPort) )

"""Threading"""
# Set up listening socket
listener = socket(AF_INET, SOCK_STREAM)
listener.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
listener.bind(('', listenerPort))
listener.listen(4) # Support up to 4 simultaneous connections

##begin threading function to listen for new connection
threading.Thread(target=handleClient, args = (listener,), daemon=True).start()


#receive fileName
fileName = recvString(serverConn)

#receive maxChunkSize
maxChunkSize = int(recvString(serverConn))

#recveive numChunks
numChunks = int(recvString(serverConn))
chunkBytes = [None] * numChunks

#receive "chunkSize,chunkDigest\n" for each chunk
for i in range (0,numChunks):
    currChunkInfo = recvString(serverConn)
    currChunkInfo = parseChunkInfo(currChunkInfo)
    chunkInfo.append(currChunkInfo)

#send "listenerPort,myChunkMask\n"
myChunkMask = '0' * numChunks
sendStr = str(listenerPort) + ',' + myChunkMask + '\n'
serverConn.send(sendStr.encode())


wroteToFile = False
while running:
    try:
        ##this section receives data from other clients
        while '0' in myChunkMask:
            #randomly choose a chunk to get from our chunk mask to maintain swarm health
            chunkIndToGet = random.randint(0, len(myChunkMask)-1)
            while myChunkMask[chunkIndToGet] != '0':
                chunkIndToGet = random.randint(0, len(myChunkMask)-1)

            #grab client list from server to see who's connected
            #each loop to make sure we don't grab from a disconnected client
            cmd = "CLIENT_LIST\n"
            serverConn.send(cmd.encode())
            numClients = int(recvString(serverConn))
            clientList = {}
            clientKey = ""
            for i in range(0, numClients):
                newClient = recvString(serverConn)
                newClient = newClient.split(",")
                clientList.update({newClient[0]:newClient[1]})

            #find a client with the chunk we need and add them to a relevant clients list
            relevantClients = []
            for key in clientList:
                if clientList[key][chunkIndToGet] == '1':
                    relevantClients.append(key)
                    break

            #randomly select a client with the chunk we need from relevant list so we dont always grab from the same client to promote swarm health
            relevantIndex = random.randint(0, len(relevantClients)-1)
            clientKey = relevantClients[relevantIndex]


            #connect to that client and send request for the randomly
            #selected chunk index
            clientKey = clientKey.split(':')
            clientIP = clientKey[0]
            clientPort = clientKey[1]
            clientConn = socket(AF_INET, SOCK_STREAM)
            clientConn.connect( (clientIP, int(clientPort)) )

            chunkIndToGetStr = str(chunkIndToGet) + '\n'
            clientConn.send(str(chunkIndToGetStr).encode())

            #recv byte array from client
            data = b''
            bytesExpected = chunkInfo[chunkIndToGet][0]
            while len(data) < bytesExpected:
                recvChunk = clientConn.recv(bytesExpected - len(data))
                data += recvChunk
            clientConn.close()

            #compare byte array to its digest
            digest = hashlib.sha224(data).hexdigest()

            #add byte array to our chunks if it matches the digest we have
            if digest == chunkInfo[chunkIndToGet][1]:
                chunkBytes[chunkIndToGet] = data


                #update our chunkMask and send that update to server
                #turn string of chunkMask into a list, change character from 0 to 1 by
                #index, change it back to a string
                myChunkMask = list(myChunkMask)
                myChunkMask[chunkIndToGet] = '1'
                myChunkMask = "".join(myChunkMask)

                sendStr = myChunkMask+'\n'

                #send updated mask to server every time to make sure all other clients know what we have, should increase swarm health
                serverConn.send('UPDATE_MASK\n'.encode())
                serverConn.send(sendStr.encode())
        #write to file after all bytes have been received
        if wroteToFile==False:
            print("Writing...")
            wroteToFile = True
            with open(fileName, 'wb') as fin:
                for i in range(0, len(chunkBytes)):
                    fin.write(chunkBytes[i])
            fin.close()

    except KeyboardInterrupt:
        print('\n[Shutting down]')
        running = False

#shutting down
serverConn.send('DISCONNECT!'.encode())
serverConn.close()
