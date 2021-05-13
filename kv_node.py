#!/usr/bin/env python3

# Import queue for sequential and linearizable to keep updates in order
import queue

# Import the configuration file for the nodes to start up connections
import nodes_config as cfg

# Import XML RPC server to receive data
from xmlrpc.server import SimpleXMLRPCServer

# Import XML RPC client to send data
import xmlrpc.client

# Import threading to update other without waiting
import threading

# Import time and random to generate random delay between updates. To get some "real" tests
import time
import random


# Initialize the kv node with an address and port number
def init_kv_node(address, port, node_id, mode, verbose):
    # Create the XML RPC server object
    server = SimpleXMLRPCServer((address, port), allow_none=True, logRequests=False,)

    # If the mode is eventual then start the eventual instance with the arguments for XML RPC
    if mode == "eventual":
        server.register_instance(EventualNode(address, port, node_id, verbose, ))

    # If the mode is sequential then start the eventual instance with the arguments for XML RPC
    elif mode == "sequential":
        server.register_instance(SequentialNode(address, port, node_id, verbose, ))

    # If the mode is linearizable then start the eventual instance with the arguments for XML RPC
    elif mode == "linearizable":
        server.register_instance(LinearizableNode(address, port, node_id, verbose, ))

    print("Node {} started on {}:{}! Using {} consistency...".format(node_id, address, port, mode))

    if verbose:
        print("Node {} Verbose Logging Enabled!".format(node_id))
    else:
        print("Node {} Verbose Logging Disabled!".format(node_id))

    # Start the thread that runs the XML RPC server listener
    server.serve_forever()


# Class functionality for eventual consistency kv
# Running everything in an instance will allow instance variables and running everything in-memory
class EventualNode:
    # Initialize the object
    def __init__(self, address, port, node_id, verbose):
        # Set the address
        self.address = address
        # Set the port
        self.port = port
        # Set the node id
        self.node_id = node_id
        # This is what is going to be the node's Key/Value store in memory
        self.data = {}
        # This will store a list of the other nodes running
        self.other_nodes = []
        # Verbose logging option for more data to be printed
        self.verbose = verbose

        # Create connections to the other nodes but skip self
        for node in cfg.nodes:
            # If the node id matches itself then skip
            if node.get("node_id") == self.node_id:
                continue

            # Otherwise create a connection to the XML RPC server for the other node
            else:
                self.other_nodes.append(xmlrpc.client.ServerProxy("http://" + node.get("address") + ":"
                                                                  + str(node.get("port"))))

    # Put method for the key node's key/value store
    def put(self, key, value):
        # Set the key and value for the dictionary from the passed arguments
        self.data[key] = value
        # Create a new worker thread to send out the updates to the other nodes
        t = threading.Thread(target=update_others_eventual, args=(self.other_nodes, key, value,))
        # Start the thread in the background of the node
        t.start()

        if self.verbose:
            print("Node {} -> Key {}, Value {}".format(self.node_id, key, value))

    # Get and return the value by passing the key to the node
    def get(self, key):
        if self.verbose:
            print("Node {} GET -> Key {}".format(self.node_id, key))
        # If the value does exist then return it
        if self.data.get(key):
            return self.data.get(key)
        # Else return null value because there is nothing
        else:
            return "NULL"

    # Remove the value by key from the node
    def remove(self, key):
        if self.verbose:
            print("Node {} Remove! -> Key {}".format(self.node_id, key))
        # If the key exists
        if self.data.get(key):
            # Start a new background worker thread to remove the keys from the other nodes
            t = threading.Thread(target=update_remove_eventual, args=(self.other_nodes, key,))
            # Start the worker thread
            t.start()
            # Return the value after popping
            return self.data.pop(key)
        # Else return null because the value does not exist
        else:
            return "NULL"

    # Used for updates from other nodes to update the key, value pair
    def update(self, key, value):
        # set the specified key to the value
        self.data[key] = value

        if self.verbose:
            print("Node {} Updated! -> Key {}, Value {}".format(self.node_id, key, value))

    # Used for removals from other nodes
    def update_remove(self, key):
        # Pop the key/value from the in memory dictionary
        if self.data.get(key):
            self.data.pop(key)
        if self.verbose:
            print("Node {} Updated Remove! -> Key {}".format(self.node_id, key))


# Static method to update the other nodes after getting a new put
def update_others_eventual(other_nodes, key, value):
    # For each nodes in the other_nodes array
    for node in other_nodes:
        # This loop will keep trying to update even with errors
        while True:
            # Create an artificial delay to mimic actual distribution
            delay = random.uniform(0.2, 1)
            # Sleep the thread for the delayed time
            time.sleep(delay)
            # Try to update the other node
            try:
                # Update the other node
                node.update(key, value)
                # Break the while look
                break
            # When there is an error updating the key, value in the other node
            except:
                # Tell the loop to continue and try value again
                continue


# Static method to remove the other nodes after getting a new put
def update_remove_eventual(other_nodes, key):
    # For each nodes in the other_nodes array
    for node in other_nodes:
        # This loop will keep trying to update even with errors
        while True:
            # Create an artificial delay to mimic actual distribution
            delay = random.uniform(0.2, 1)
            # Sleep the thread for the delayed time
            time.sleep(delay)
            # Try to update the other node
            try:
                # Update the other node
                node.update_remove(key)
                # Break the while look
                break
            # When there is an error updating the key, value in the other node
            except:
                # Tell the loop to continue and try value again
                continue


# Class functionality for eventual sequential kv
# Running everything in an instance will allow instance variables and running everything in-memory
class SequentialNode:
    def __init__(self, address, port, node_id, verbose):
        # Set the address
        self.address = address
        # Set the port
        self.port = port
        # Set the node id
        self.node_id = node_id
        # This is what is going to be the node's Key/Value store in memory
        self.data = {}
        # This will store a list of the other nodes running
        self.other_nodes = []
        # Queue of the keys and values that will have to be updated
        self.update_queue = queue.Queue()
        # Verbose logging option for more data to be printed
        self.verbose = verbose

        # Create connections to the other nodes but skip self
        for node in cfg.nodes:
            # If the node id matches itself then skip
            if node.get("node_id") == self.node_id:
                continue

            # Otherwise create a connection to the XML RPC server for the other node
            else:
                self.other_nodes.append(xmlrpc.client.ServerProxy("http://" + node.get("address") + ":"
                                                                  + str(node.get("port"))))

        # Create and start the worker thread to update the other nodes
        threading.Thread(target=update_sequential, args=(self.other_nodes, self.update_queue,)).start()

    # Put method for the key node's key/value store
    def put(self, key, value):
        # Set the key and value for the dictionary from the passed arguments
        self.data[key] = value
        # Add the new put value to the queue to update other nodes
        self.update_queue.put("PUT" + ":" + key + ":" + value)

        if self.verbose:
            print("Node {} -> Key {}, Value {}".format(self.node_id, key, value))

    # Get and return the value by passing the key to the node
    def get(self, key):
        if self.verbose:
            print("Node {} GET -> Key {}".format(self.node_id, key))
        # If the value does exist then return it
        if self.data.get(key):
            return self.data.get(key)
        # Else return null value because there is nothing
        else:
            return "NULL"

    # Remove the value by key from the node
    def remove(self, key):
        if self.verbose:
            print("Node {} Remove! -> Key {}".format(self.node_id, key))
        # If the value exists
        if self.data.get(key):
            self.update_queue.put("REMOVE" + ":" + key)
            # Return the value and pop it from the dictionary
            return self.data.pop(key)
        # Else return null when nothing happens because the value does not exist
        else:
            return "NULL"

    # Used for updates from other nodes to update the key, value pair
    def update(self, key, value):
        # set the specified key to the value
        self.data[key] = value

        if self.verbose:
            print("Node {} Updated! -> Key {}, Value {}".format(self.node_id, key, value))

    # Used for removals from other nodes
    def update_remove(self, key):
        # Pop the key/value from the in memory dictionary
        if self.data.get(key):
            self.data.pop(key)
        if self.verbose:
            print("Node {} Updated Remove! -> Key {}".format(self.node_id, key))


# Static worker thread to update the other nodes from a queue
def update_sequential(other_nodes, update_queue):
    # While loop to run continuously as a background worker for the update queue
    while True:
        # Create an artificial delay to mimic actual distribution
        delay = random.uniform(0.2, 1)
        # Sleep the thread for the delayed time
        time.sleep(delay)
        # Only run update when the queue is not empty
        if update_queue.empty() is False:
            # Get the value from the queue to update, in order FIFO of course
            update_value = update_queue.get()
            # For each of the other nodes
            for node in other_nodes:
                # While loop to keep trying until update succeeds
                while True:
                    # Try
                    try:
                        # Split the queue values back into key-val
                        key_val = update_value.split(":")
                        if key_val[0] == "PUT":
                            # Update with the key value pair with the other node
                            node.update(key_val[1], key_val[2])
                        elif key_val[0] == "REMOVE":
                            node.update_remove(key_val[1])
                        # Break the sub-while loop to go back to the other
                        break
                    # If there is some issue updating, try again
                    except:
                        continue


# Class functionality for eventual linearizable kv
# Running everything in an instance will allow instance variables and running everything in-memory
class LinearizableNode:
    def __init__(self, address, port, node_id, verbose):
        # Set the address
        self.address = address
        # Set the port
        self.port = port
        # Set the node id
        self.node_id = node_id
        # This is what is going to be the node's Key/Value store in memory
        self.data = {}
        # This will store a list of the other nodes running
        self.other_nodes = []
        # Queue of the keys and values that will have to be updated
        self.update_queue = queue.Queue()
        # Verbose logging option for more data to be printed
        self.verbose = verbose

        # Create connections to the other nodes but skip self
        for node in cfg.nodes:
            # If the node id matches itself then skip
            if node.get("node_id") == self.node_id:
                continue

            # Otherwise create a connection to the XML RPC server for the other node
            else:
                self.other_nodes.append(xmlrpc.client.ServerProxy("http://" + node.get("address") + ":"
                                                                  + str(node.get("port"))))

        # Create and start the worker thread to update the other nodes
        threading.Thread(target=update_linearizable, args=(self.other_nodes, self.update_queue,)).start()

    # Put method for the key node's key/value store
    def put(self, key, value):
        # Set the key and value for the dictionary from the passed arguments
        self.data[key] = value
        # Add the new put value to the queue to update other nodes
        self.update_queue.put("PUT" + ":" + key + ":" + value)

        if self.verbose:
            print("Node {} -> Key {}, Value {}".format(self.node_id, key, value))

    # Get and return the value by passing the key to the node
    def get(self, key):
        if self.verbose:
            print("Node {} GET -> Key {}".format(self.node_id, key))
        # If the value does exist then return it
        if self.data.get(key):
            return self.data.get(key)
        # Else return null value because there is nothing
        else:
            return "NULL"

    # Remove the value by key from the node
    def remove(self, key):
        if self.verbose:
            print("Node {} Remove! -> Key {}".format(self.node_id, key))
        # If the value exists
        if self.data.get(key):
            self.update_queue.put("REMOVE" + ":" + key)
            # Return the value and pop it from the dictionary
            return self.data.pop(key)
        # Else return null when nothing happens because the value does not exist
        else:
            return "NULL"

    # Used for updates from other nodes to update the key, value pair
    def update(self, key, value):
        # set the specified key to the value
        self.data[key] = value

        if self.verbose:
            print("Node {} Updated! -> Key {}, Value {}".format(self.node_id, key, value))

    # Used for removals from other nodes
    def update_remove(self, key):
        # Pop the key/value from the in memory dictionary
        if self.data.get(key):
            self.data.pop(key)
        if self.verbose:
            print("Node {} Updated Remove! -> Key {}".format(self.node_id, key))


# Static worker thread to update the other nodes from a queue
def update_linearizable(other_nodes, update_queue):
    # While loop to run continuously as a background worker for the update queue
    while True:
        # Create an artificial delay to mimic actual distribution
        delay = random.uniform(0.2, 1)
        # Sleep the thread for the delayed time
        time.sleep(delay)
        # Only run update when the queue is not empty
        if update_queue.empty() is False:
            # Get the value from the queue to update, in order FIFO of course
            update_value = update_queue.get()
            # For each of the other nodes
            for node in other_nodes:
                # While loop to keep trying until update succeeds
                while True:
                    # Try
                    try:
                        # Split the queue values back into key-val
                        key_val = update_value.split(":")
                        if key_val[0] == "PUT":
                            # Update with the key value pair with the other node
                            node.update(key_val[1], key_val[2])
                            # Verify that the value of the node has been properly updated then break to the next
                            if node.get(key_val[1] == key_val[2]):
                                break
                        elif key_val[0] == "REMOVE":
                            node.update_remove(key_val[1])
                            # Verify that the value of the node has been properly removed then break to the next
                            if node.get(key_val[1] == "NULL"):
                                break
                    # If there is some issue updating, try again
                    except:
                        continue
