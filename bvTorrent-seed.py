#!/usr/bin/python3
from socket import *
import os
import threading
import hashlib
import sys
import time

"""
Things to still do:
    Init:
        myChunkMask -- how?
    Send last string to Server
    Client
        serve content
        receive content
"""

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

def sendData(clientInfo):
    clientConn, clientAddr = clientInfo
    chunkIndex = recvString(clientConn)
    chunkIndex = int(chunkIndex)
    clientConn.send(chunkBytes[chunkIndex])
    clientConn.close()

#Init variables we'll need
fileName = ''
maxChunkSize = 0
numChunks = 0
chunkInfo = []
myChunkMask = ''
listenerPort = int(input('Enter a port number to listen on: '))
chunkSize = 2**20

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
listener.listen(4) # Support up to 32 simultaneous connections


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

#TODO how do we figure out our own chunkMask?
#send "listenerPort,myChunkMask\n"

#open file
fin = open(fileName, "rb")
fileData = fin.read()
fin.close()

myChunkMask = ''

#getting chunks from file data and seeing if theyre legit by checking digest
j=0
for i in range(0, len(fileData), chunkSize):
    sz = min(maxChunkSize, len(fileData)-i)
    digest = hashlib.sha224(fileData[i:i+sz]).hexdigest()
    if digest == chunkInfo[j][1]:
        chunkBytes[j] = fileData[i:i+sz]
        myChunkMask += '1'
    else:
        myChunkMask += '0'

    j+=1

sendStr = str(listenerPort) + ',' + myChunkMask + '\n'
serverConn.send(sendStr.encode())

running = True
while running:
    try:
        print('past thread')
        threading.Thread(target=sendData, args=(listener.accept(),), daemon=True).start()
        if '0' in myChunkMask:
            print('past check')
            chunkIndToGet = myChunkMask.find('0')
            #figure update out client list
            serverConn.send("CLIENT_LIST".encode())
            numClients = int(recvString(serverConn))
            clientList = {}
            clientKey = ""
            for i in range(0, numClients):
                newClient = recvString(serverConn)
                newClient = newClient.split(",")
                clientList[newClient[0]] = newClient[1]

            #find a client with the chunk we need
            for key in clientList:
                if clientList[key][chunkIndToGet] == 1:
                    clientKey = key
                    break

            #connect to that client and send request them that chunk index
            clientKey = clientKey.split(":")
            clientIP = clientKey[0]
            clientPort = clientKey[1]
            clientConn = socket(AF_INET, SOCK_STREAM)
            clientConn.connect( (clientIP, clientPort) )

            chunkIndToGetStr = str(chunkIndToGet) + '\n'
            clientConn.send(str(chunkIndToGetStr).encode())

            #recv byte array
            data = []
            bytesExpected = chunkInfo[chunkIndToGet][0]
            while len(data) < bytesExpected:
                recvChunk = clientConn.recv(bytesExpected - len(data))
                data += recvChunk

            #compare byte array to its digest
            digest = hashlib.sha224(data).hexdigest()
            #add byte array to our chunks if it works
            if digest == chunkInfo[chunkIndToGet][1]:
                chunkBytes[chunkIndToGet] = data

                #update our chunkMask and send that update to server
                myChunkMask = myChunkMask[:chunkIndToGet] + '1' + [myChunkMask[chunkIndToGet+1:]]

                sendStr = myChunkMask+'\n'
                #send updated mask to server
                serverConn.send('UPDATE_MASK'.encode())
                serverConn.send(sendStr.encode())

    except KeyboardInterrupt:
        print('\n[Shutting down]')
        running = False

#shutting down
serverConn.send('DISCONNECT!'.encode())
serverConn.close()
