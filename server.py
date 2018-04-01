#!/usr/bin/env python
import socket
import time, threading
import random 
import sys
import signal
from datetime import datetime
import select
import struct
from collections import defaultdict
from logger import write_to_file

# server thread will constantly poll for new connections/writes from clients 
# if there is a new connection, accept it and associate that pid with a socket and address 
# otherwise try to read the message from the client and choose a receive handler based on the message's encoding

class listener_thread (threading.Thread):
    def __init__(self):
        super(listener_thread, self).__init__()
        self.running = True
    def stop(self):
        self.running = False
    def run(self):
        global inputs, pid_to_socket, address_to_pid
        while(self.running):
            readable, writable, exceptional = select.select(inputs, [], [], 0) 
            for s in readable:
                if s is server:
                    connection, client_address = s.accept()
                    client_pid = int(connection.recv(recv_limit))
                    pid_to_socket[client_pid] = connection
                    address_to_pid[client_address] = client_pid
                    inputs.append(connection)
                else:        
                    data = s.recv(recv_limit)
                    if(data):
                        for fmt_string in [lamport_fmt_string, unicast_fmt_string]:
                            try:
                                decoded_msg = decode_message(fmt_string, data)      # decode will fail if the encoding doesn't match the format string
                                break
                            except struct.error:
                                continue
                        if fmt_string == unicast_fmt_string:
                            unicast_receive(s, decoded_msg)
                        elif fmt_string == lamport_fmt_string:
                            multicast_receive(s, decoded_msg)


# initialize the server and listen the port specified by the config file

def server_init():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # server.bind(('', port_value))
    server.bind(('', pid_to_address[server_pid][1]))
    server.listen(pid_count)
    return server 

# client code ----------------------------------------------------------------------------------------------------------------------------

# the client thread will read a command from stdin and perform a unicast or multicast send 
# unicast format: send destination message
# casual format: msend message casual

class client_thread (threading.Thread):
    def __init__(self, listener_thread):
        super(client_thread, self).__init__()
        self.listener_thread = listener_thread
        self.running = True
    def run(self):
        client_init()
        while(self.running):
            raw_argument = raw_input("Enter command line argument for client: \n\t")
            cli_arg = raw_argument.strip().split(' ')
            if(cli_arg[0] == 'put'):
                key = cli_arg[1]
                value = int(cli_arg[2])
                if(consistency_model == 'E'):
                    ec_put(key, value)
                elif(consistency_model == 'L'):
                    linear_put_request(key, value)
            elif(cli_arg[0] == 'get'):
                key = cli_arg[1]
                if(consistency_model == 'E'):
                    ec_get(key)
                elif(consistency_model == 'L'):
                    linear_get(key)
            elif(cli_arg[0] in ["q", "quit", "exit"]):
                self.running = False
                self.listener_thread.stop()
            else:
                print("Invalid CLI argument. Please follow this format.")
                print("\tsend destination message")
                print ("\tmsend message [casual, total]")
                print("\t\"[s]witch\" to switch between total and casual ordering")
                print("\t[q]uit to disconnect")

# establish a connection between every pair of processes 
# send own pid after connection, so the server can map the client address/connection to the pid

def client_init():
    global inputs, pid_to_socket
    clients = []
    for i in range(pid_count):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clients.append(sock)
    for client, pid, address in zip(clients, pid_to_address.keys(), pid_to_address.values()):        # establish a connection between every pair of processes 
            result = client.connect_ex(address)
            if(not result):
                print("Client connected to process " + str(pid))
                pid_to_socket[pid] = client
                inputs.append(client)
                client.send(str(server_pid)) # send process id to every other process  
            else:
                print("Could not connect to process " + str(pid))


# unicast functions ----------------------------------------------------------------------------------------------------------------------

# basic tcp send given a socket and a message  

def tcp_send(send_socket, message):
    send_socket.sendall(message)

# delay the unicast send by a random delay specified in the config file 
def delayed_send(send_socket, message, delay=None):
    if not delay:
        delay = random.uniform((float(min_delay)/1000), (float(max_delay)/1000))
    sender_thread = threading.Timer(delay, tcp_send, [send_socket, message])
    sender_thread.start()

# parse the command line arguments for message, and destination
# encode the message and send it to the destination process
# also print the time that the message was sent 

def unicast_send(destination, message):
    destination = int(destination)
    send_socket = pid_to_socket[destination]
    encoded_msg = encode_unicast_message(message)
    if(destination != server_pid):
        delayed_send(send_socket, encoded_msg)
    else:
        tcp_send(send_socket, encoded_msg)

# printing the time when the unicasted message was received 

def unicast_receive(server, decoded_msg):
    data = decoded_msg[0].strip()
    pid_from = address_to_pid[server.getpeername()]
    msg_handler(data, pid_from, decoded_msg)

# encode the unicasted message as binary data
# msg format is the extended_message

def encode_unicast_message(message):
    extended_message = extend_message(message)
    buf = struct.pack(unicast_fmt_string, extended_message)
    return buf 

# ensure that the messsage is length msg_limit for encoding purposes 

def extend_message(message):
    extended_message = message 
    for i in range(len(extended_message), msg_limit):
        extended_message += ' '
    return extended_message

# unpack decoded message and return a tuple 

def decode_message(format_string, encoded_string):
    return struct.unpack(format_string, encoded_string)

# return the unicast format string for struct.pack

def get_unicast_fmt_string():
    string_lim = str(msg_limit)
    return (string_lim + 's')

# multicast functions -------------------------------------------------------------------------------------------------------------------------------

# increment the server's vector element corresponding to its own pid
# encode the message and send it to all connected processes

def multicast_send(message):
    global pid_to_socket, server_pid 
    print("At server number: " + str(server_pid) + "the timestamp is: " + str(pid_to_timestamp[server_pid]) + '\n')
    encoded_msg = encode_vector(message)
    for pid, socket in pid_to_socket.iteritems():
        socket = pid_to_socket[pid]
        if(pid != server_pid):
            delayed_send(socket, encoded_msg)
        else:
            tcp_send(socket, encoded_msg)

def lamport_send(message):
    pid_to_timestamp[server_pid] += 1
    multicast_send(message)

# receive handler for casual ordering, which checks if the received message can be delivered 
# if client == server, print the received time 
# if client != server, deliver message if its ready and then recursively deliver queued messages
# if message can't be delivered, put the vector in the hold back queue  

def multicast_receive(server, decoded_vec): 
    client_pid = address_to_pid[server.getpeername()]
    if(client_pid != server_pid):
        message = decoded_vec[0].strip()
        msg_handler(message, client_pid, decoded_vec)

# return a format string for casual ordering 
# used by struct module to encode/decode data 

def get_lamport_fmt_string():
    string_lim = str(msg_limit)
    fmt_string =  string_lim + 's ' + '1i'
    return fmt_string 

# encode the server's vector as binary data 
# casual message format will be (extended_message, timestamp)

def encode_vector(message):
    lamport_timestamp = pid_to_timestamp[server_pid]
    extended_message = extend_message(message)
    buf = struct.pack(lamport_fmt_string, extended_message, lamport_timestamp)
    return buf 

# key value store functions ---------------------------------------------------------------------------------------------------------------
def msg_handler(message, sender_pid, decoded_vec):
    split_msg = message.split(",")
    if(split_msg[0] == 'eput'):
        # sender_pid is the sender's pid 
        ec_receive_put(split_msg, sender_pid, int(decoded_vec[1]))
    elif(split_msg[0] == 'eput_ack' and key_to_write_counter[split_msg[1].strip()] != 0): # if the counter is 0, that means the write has completed 
        receive_put_ack(split_msg)
    elif(split_msg[0] == 'eget'):
        ec_receive_get(split_msg, sender_pid)
    elif(split_msg[0] == 'eget_ack' and key_to_read_counter[split_msg[1].strip()] != 0): 
        receive_read_ack(split_msg)
    elif(split_msg[0] == 'lputrequest'):
        linear_put_request_receive(split_msg)
    elif(split_msg[0] == 'lreply'):
        linear_receive_ack(split_msg)
    elif(split_msg[0] == 'lput'):
        linear_put_response(split_msg)

def ec_put(key, value):
    timestamp = int(time.time())
    write_to_file(server_pid, 'put', key, timestamp, 'req', value)
    message = 'eput, ' + str(key) + ', ' + str(value) 
    key_value[key] = value
    key_to_write_counter[key] = 1 
    print('Client calling PUT operation\n')
    lamport_send(message)

# message format is 'put', key, value delimited by commas
# for put, ask whether we log the last written timestamp from a replica 

def ec_receive_put(message, sender_pid, timestamp):
    key = message[1].strip()
    old_timestamp = pid_to_timestamp[server_pid]
    if(key not in pid_to_timestamp or old_timestamp < timestamp): # if local time < timestamp, last writer wins 
        value = int(message[2])
        key_value[key] = value 
        pid_to_timestamp[server_pid] = timestamp # if their timestamp is bigger, our timestamp = theirs for last writer
        print('Replica sending PUT operation ack success. Old timestamp was: ' + str(old_timestamp) + ' New timestamp is: ' + str(pid_to_timestamp[server_pid]) + '\n')
    else:
        print('Replica sending PUT operation ack failure. Old timestamp was: ' + str(old_timestamp) + ' New timestamp is: ' + str(pid_to_timestamp[server_pid]) + '\n')
    put_ack_msg = 'eput_ack, ' + str(key) 
    unicast_send(sender_pid, put_ack_msg) # ack for ignore write


# message format is ['put', key, 'success/failure']

def receive_put_ack(message):
    global key_to_write_counter
    key = message[1].strip()
    key_to_write_counter[key] += 1
    if(key_to_write_counter[key] == write_to_count):
        print('Finished writing to ' + str(write_to_count) + 'replicas\n')
        write_to_file(server_pid, 'put', key, int(time.time()), 'req', key_value[key])
        key_to_write_counter[key] = 0

# multicast the get operation to all replicas  
def ec_get(key):
    message = 'eget, ' + str(key)
    timestamp = str(datetime.now())
    write_to_file(server_pid, 'get', key, timestamp, 'resp', None)
    print('Client calling GET operation\n')
    key_to_read_counter[key] = 1 
    if(key in get_key_to_timestamp):
        get_key_to_timestamp[key] = pid_to_timestamp[server_pid]
        get_last_writer_value[key] = key_value[key]
    else:
        get_key_to_timestamp[key] = 0
        get_last_writer_value[key] = None
    multicast_send(message)

# each replica that receives the get operation sends its value back
# message format is 'get, key'

def ec_receive_get(message, sender_pid):
    key = message[-1].strip()
    value = key_value[key]
    timestamp = pid_to_timestamp[server_pid]
    replica_message = 'eget_ack, ' + str(key) + ', ' + str(value) + ', ' + str(timestamp)
    print('Replica sending GET operation ack with key: ' + key + 'and value: ' + str(value) + '\n')
    unicast_send(sender_pid, replica_message)

def receive_read_ack(message):
    global key_to_read_counter
    key = message[1].strip()
    value = int(message[2])
    timestamp = int(message[-1])
    key_to_read_counter[key] += 1
    if(key not in get_key_to_timestamp or timestamp > get_key_to_timestamp[key]):
        get_key_to_timestamp[key] = timestamp
        get_last_writer_value[key] = value 

    if(key_to_read_counter[key] == read_from_count):
        print('Client GET has read from key: ' + key + ' the value: ' + str(get_last_writer_value[key]) + '\n')
        write_to_file(server_pid, 'get', key, int(time.time()), 'resp', get_last_writer_value[key])
        key_to_read_counter[key] = 0

# --------------------------------------------------------------------------------------------------------------------------------------

def linear_put_request(key, value):
    global requested
    requested = 1
    message = 'lputrequest, ' + str(pid_to_timestamp[server_pid]+1) + ', ' + str(server_pid) + ', ' + key + ', ' + str(value)
    key_to_write_counter[key] = 1 # include write to itself  
    timestamp = int(time.time())
    write_to_file(server_pid, 'put', key, timestamp, 'req', value)
    lamport_send(message)

def linear_put_request_receive(split_msg):
    if(held == 1 or (requested == 1 and lexicographical_comparison([int(split_msg[1]), int(split_msg[2])], [pid_to_timestamp[server_pid],server_pid]))):
        print('Queued up request\n')
        request_queue.append(split_msg)
    else:
        reply_to_request(split_msg)


def reply_to_request(split_msg):
    request_pid = int(split_msg[2])
    request_key = split_msg[3].strip()
    request_value = int(split_msg[4])
    reply_msg = 'lreply, ' + request_key + ', ' + str(request_value) 
    print('Replying to request from server: ' + str(request_pid) + '\n')
    unicast_send(request_pid, reply_msg) 

def lexicographical_comparison(vector1, vector2):
    if(vector1[0] == vector2[0]):
        return lexicographical_comparison(vector1[1:], vector2[1:])
    elif(vector1[0] > vector2[0]):
        return 1 
    else:
        return 0

def linear_receive_ack(split_msg):
    global reply_counter
    key = split_msg[1].strip()
    value = int(split_msg[2])
    key_to_write_counter[key] += 1
    if(key_to_write_counter[key] == len(pid_to_socket)):
        print('Process' + str(server_pid) + 'is now in state HELD\n')
        held = 1
        requested = 0
        key_value[key] = value
        linear_put(key, value)
        # exiting now   
        held = 0 
        for request in request_queue:
            reply_to_request(request)

def linear_put(key, value):
    multicast_send('lput, ' + key + ', ' + str(value) + ', ' + str(pid_to_timestamp[server_pid]))
    timestamp = int(time.time())
    write_to_file(server_pid, 'put', key, timestamp, 'resp', value)

def linear_put_response(split_msg):
    key = split_msg[1].strip()
    value = split_msg[2]
    lamport_timestamp = int(split_msg[-1])
    if(lamport_timestamp > pid_to_timestamp[server_pid]):
        pid_to_timestamp[server_pid] = lamport_timestamp + 1
    else:
        pid_to_timestamp[server_pid] += 1
    key_value[key] = value

def linear_get(key):
    write_to_file(server_pid, 'get', key, int(time.time()), 'req', None)
    return_value = key_value[key] if key in key_value else None
    write_to_file(server_pid, 'get', key, int(time.time()), 'resp', return_value)
    print('Received the value: ' + str(return_value) + '\n')
    return return_value

# entry point --------------------------------------------------------------------------------------------------------------------------

# read the config file and initialize various dictionaries that map between pid and info about the server
# initialize format strings and initialize any global variables that are commonly accessed 
# start the client and server threads 

pid_to_timestamp = {}
pid_to_address = {}
address_to_pid = {}
pid_to_socket = {}
socket_to_pid = {}
key_value = {}
key_to_timestamp = {}
request_queue = []

get_key_to_timestamp = {}
get_last_writer_value = {}

pid_count = 0
msg_limit = 128
recv_limit = 1024
key_to_write_counter = {}
key_to_read_counter = {}
requested = 0
held = 0
reply_counter = 0

server_pid = int(sys.argv[1])
consistency_model = sys.argv[2]
if len(sys.argv) == 5:
    write_to_count = int(sys.argv[3])
    read_from_count = int(sys.argv[4])

inputs = []
port_value = 0 

with open("config1.txt", "r") as file:
    delay = file.readline()
    for row in file:
        if(row not in ['\n', '\r\n']):
            stripped = row.strip()
            pid, hostname, port = stripped.split(' ')
            port_value = int(port)+pid_count # change this to port_value = int(port) when using config.txt 
            pid = int(pid)
            ip = socket.gethostbyname(hostname)
            client_address = (ip, port_value)
            pid_to_address[pid] = client_address
            address_to_pid[client_address] = pid
            pid_count = pid_count + 1 

pid_to_timestamp[server_pid] = 0
lamport_fmt_string = get_lamport_fmt_string()
unicast_fmt_string = get_unicast_fmt_string()
server_binding_addr = pid_to_address[server_pid]
min_delay, max_delay = delay.strip().split(' ')
server = server_init()
inputs = [server]
listener = listener_thread()
client = client_thread(listener)
listener.start()
client.start()

try:
    listener.join()
    client.join()
except KeyboardInterrupt as e:
    pass
