#!/usr/bin/env python3

# Import the configurations for the kv nodes
import time

import clients_config
import nodes_config as cfg

# Import the client functionality for testing
import client

# Import the kv nodes initialization method to start the processes
from kv_node import init_kv_node

# Import Process to startup the kv nodes
from multiprocessing import Process

# Import for argparse for command line arguments
import argparse

# Array of the currently running kv nodes
kv_nodes = []

# Array of processes running the clients for testing
clients = []


# Main to start the driver program, clients, and nodes to run
def main():

    # Parser to get the command line arguments to run either of the consistency models. Can declare all
    parser = argparse.ArgumentParser(description="Distributed KV")
    parser.add_argument("-e", "--eventual", action="store_true", default=False, help="Run eventual consistency")
    parser.add_argument("-s", "--sequential", action="store_true", default=False, help="Run sequential consistency")
    parser.add_argument("-l", "--linearizable", action="store_true", default=False, help="Run linearizable")
    parser.add_argument("-a", "--all", action="store_true", default=False, help="Run all of the consistencies")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Verbose logging option")
    args = parser.parse_args()

    # Arguments to check for eventual
    if args.eventual or args.all:
        init_kv_nodes("eventual", args.verbose)
        time.sleep(1)
        init_clients("eventual")
        kill_clients("eventual")
        kill_kv_nodes("eventual")

    # Arguments to check for sequential
    if args.sequential or args.all:
        init_kv_nodes("sequential", args.verbose)
        time.sleep(1)
        init_clients("sequential")
        kill_clients("sequential")
        kill_kv_nodes("sequential")

    # Arguments to check for linearizable
    if args.linearizable or args.all:
        init_kv_nodes("linearizable", args.verbose)
        time.sleep(1)
        init_clients("linearizable")
        kill_clients("linearizable")
        kill_kv_nodes("linearizable")


# Initialize the clients to run tests on the nodes
def init_clients(mode):

    # For each client in the clients config
    for i in range(len(clients_config.clients)):
        # If the specified is eventual
        if mode == "eventual":
            # Start a process for the client and append it to the client list
            p = Process(target=client.eventual_consistency, args=(i,))
            p.start()
            clients.append(p)
        # If the specified is sequential
        elif mode == "sequential":
            # Start a process for the client and append it to the client list
            p = Process(target=client.sequential_consistency, args=(i,))
            p.start()
            clients.append(p)
        # If the specified is linearizable
        elif mode == "linearizable":
            # Start a process for the client and append it to the client list
            p = Process(target=client.linearizable, args=(i,))
            p.start()
            clients.append(p)

    # Join all the client processes to the main. This will allow all clients to test before continuing
    for c in clients:
        c.join()


# Kill the clients processes after running the tests from them
def kill_clients(mode):
    # Counter for the clients
    count = 1
    # While there are clients in the client list
    while clients:
        # Pop client from the list and get the object
        p = clients.pop()
        # Kill the process
        p.terminate()
        print("Killed Client {} with {} consistency".format(count, mode))
        count += 1


# Start the KV nodes processes with their respective "mode" which is their consistency
def init_kv_nodes(mode, verbose):
    # For each node in the range of the configured clients
    for i in range(len(cfg.nodes)):
        # Get the address
        address = cfg.nodes[i].get("address")
        # Get the port
        port = cfg.nodes[i].get("port")
        # Get the node id
        node_id = cfg.nodes[i].get("node_id")
        # Create a process to initialize the node with the arguments
        p = Process(target=init_kv_node, args=(address, port, node_id, mode, verbose,))
        # Start the process
        p.start()
        # Append the process to the the list of nodes
        kv_nodes.append(p)


# Kill the currently running KV nodes with parameter of type of consistency
def kill_kv_nodes(mode):
    # counter for the nodes
    count = 1
    # While there are nodes in the list
    while kv_nodes:
        # Get each nodes object by popping it from the list
        p = kv_nodes.pop()
        # Terminate the process
        p.terminate()
        print("Killed KV Node {} with {} consistency...".format(count, mode))
        count += 1


if __name__ == "__main__":
    main()
