#!/usr/bin/env python3

# Import XML RPC client to connect to kv nodes and test
import random

# Import xml rpc client
import xmlrpc.client

# Import the configurations for the kv nodes
import nodes_config

# Import the configuration for the clients
import clients_config

# Create artificial delays
import time

"""
This file will have the methods for testing the key-value stores.
The tests have been configured for the current clients_config.py and nodes_config.py
    - 3 nodes
    - 2 clients
"""


# Method to run the eventual consistency client
def eventual_consistency(client_id):
    print("Client {} started with eventual consistency".format(client_id))

    # Create a connection to node 1
    node1 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[0].get("address")
                                      + ":" + str(nodes_config.nodes[0].get("port")))
    # Create a connection to node 2
    node2 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[1].get("address")
                                      + ":" + str(nodes_config.nodes[1].get("port")))
    # Create a connection to node 3
    node3 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[2].get("address")
                                      + ":" + str(nodes_config.nodes[2].get("port")))

    # Create a dictionary to put test data into
    input_data = {}
    # Open the file with the tests
    with open(clients_config.clients[client_id].get("test_file"), "r") as f:
        # For each line
        for line in f:
            # Get a key, value by splitting at the :
            key, value = line.split(":")
            # Set the dictionary value and strip off the \n at the end of the line
            input_data[key] = value.strip("\n")

    time.sleep(1)

    # Loop though the items in the dictionary for puts
    for k, v in input_data.items():
        # Put the key/value into the first node
        node1.put(str(k), str(v))
        # Print out the statement confirming
        print("Client {} Node 1 - PUT {}, {}".format(client_id, k, v))
        # Create a random delay
        delay = random.uniform(0.5, 1.2)
        # Print out specified delay
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep delay time
        time.sleep(delay)
        # Do get values for each of the three nodes with key and value then confirm with expected value
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), v))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), v))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), v))

    # Loop through some items in the dictionary for removes
    for k, v in input_data.items():
        # Put the key/value into the first node
        print("Client {} Node 1 - REMOVE {} -> Result {}".format(client_id, k, node1.remove(k)))
        # Create delay for waiting
        delay = random.uniform(0.5, 1.2)
        # Print out delay to show
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep for desired delay
        time.sleep(delay)
        # Print out more get statements that should show that values were removed
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), "NULL"))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), "NULL"))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), "NULL"))


# Method to run the sequential consistency client
def sequential_consistency(client_id):
    print("Client {} started with sequential consistency".format(client_id))

    # Create a connection to node 1
    node1 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[0].get("address")
                                      + ":" + str(nodes_config.nodes[0].get("port")))
    # Create a connection to node 2
    node2 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[1].get("address")
                                      + ":" + str(nodes_config.nodes[1].get("port")))
    # Create a connection to node 3
    node3 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[2].get("address")
                                      + ":" + str(nodes_config.nodes[2].get("port")))

    # Create a dictionary to put test data into
    input_data = {}
    # Open the file with the tests
    with open(clients_config.clients[client_id].get("test_file"), "r") as f:
        # For each line
        for line in f:
            # Get a key, value by splitting at the :
            key, value = line.split(":")
            # Set the dictionary value and strip off the \n at the end of the line
            input_data[key] = value.strip("\n")

    time.sleep(1)

    # Loop though the items in the dictionary
    for k, v in input_data.items():
        # Put the key/value into the first node
        node1.put(str(k), str(v))
        # Print out the statement confirming
        print("Client {} Node 1 - PUT {}, {}".format(client_id, k, v))
        # Create a random delay
        delay = random.uniform(0.5, 1.2)
        # Print out specified delay
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep delay time
        time.sleep(delay)
        # Do get values for each of the three nodes with key and value then confirm with expected value
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), v))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), v))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), v))

    print("Waiting for update queues to finish for {} seconds".format(30))
    time.sleep(30)

    # Loop through some items in the dictionary for removes
    for k, v in input_data.items():
        # Put the key/value into the first node
        print("Client {} Node 1 - REMOVE {} -> Result {}".format(client_id, k, node1.remove(k)))
        # Create delay for waiting
        delay = random.uniform(0.5, 1.2)
        # Print out delay to show
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep for desired delay
        time.sleep(delay)
        # Print out more get statements that should show that values were removed
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), "NULL"))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), "NULL"))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), "NULL"))


def linearizable(client_id):
    print("Client {} started with linearizable consistency".format(client_id))

    # Create a connection to node 1
    node1 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[0].get("address")
                                      + ":" + str(nodes_config.nodes[0].get("port")))
    # Create a connection to node 2
    node2 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[1].get("address")
                                      + ":" + str(nodes_config.nodes[1].get("port")))
    # Create a connection to node 3
    node3 = xmlrpc.client.ServerProxy("http://" + nodes_config.nodes[2].get("address")
                                      + ":" + str(nodes_config.nodes[2].get("port")))

    # Create a dictionary to put test data into
    input_data = {}
    # Open the file with the tests
    with open(clients_config.clients[client_id].get("test_file"), "r") as f:
        # For each line
        for line in f:
            # Get a key, value by splitting at the :
            key, value = line.split(":")
            # Set the dictionary value and strip off the \n at the end of the line
            input_data[key] = value.strip("\n")

    time.sleep(1)

    # Loop though the items in the dictionary
    for k, v in input_data.items():
        # Put the key/value into the first node
        node1.put(str(k), str(v))
        # Print out the statement confirming
        print("Client {} Node 1 - PUT {}, {}".format(client_id, k, v))
        # Create a random delay
        delay = random.uniform(0.5, 1.2)
        # Print out specified delay
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep delay time
        time.sleep(delay)
        # Do get values for each of the three nodes with key and value then confirm with expected value
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), v))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), v))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), v))

    time.sleep(10)

    # Loop through some items in the dictionary for removes
    for k, v in input_data.items():
        # Put the key/value into the first node
        print("Client {} Node 1 - REMOVE {} -> Result {}".format(client_id, k, node1.remove(k)))
        # Create delay for waiting
        delay = random.uniform(0.5, 1.2)
        # Print out delay to show
        print("Client {} Delay: {} seconds".format(client_id, delay))
        # Sleep for desired delay
        time.sleep(delay)
        # Print out more get statements that should show that values were removed
        print("Client {} Node 1 - GET {} -> Result {} --> Expected {}".format(client_id, k, node1.get(k), "NULL"))
        print("Client {} Node 2 - GET {} -> Result {} --> Expected {}".format(client_id, k, node2.get(k), "NULL"))
        print("Client {} Node 3 - GET {} -> Result {} --> Expected {}\n".format(client_id, k, node3.get(k), "NULL"))
