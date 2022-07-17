#!/usr/bin/env python3
# ipc_server.py

import socket
from time import sleep

HOST = '0.0.0.0'   # Standard loopback interface address (localhost)
PORT = 9898        # Port to listen on (non-privileged ports are > 1023)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()    
    print('\nListening for a client at',HOST , PORT)
    conn, addr = s.accept()

    with conn:
        print('\nConnected by', addr)
        while True:
            with open('/data/g1059783_Data.dat','r') as f:
                for line in f:
                    out = line.encode('utf-8')
                    print('Sending line',line)
                    sleep(5)
                    conn.sendall(out)
            print('End Of Stream.')
            break
            
