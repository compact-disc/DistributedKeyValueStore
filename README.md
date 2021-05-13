# Distributed Key Value Store

This is my own Distributed Key Value Store implementation written in Python 3. It includes functionality for different consistency models including Eventual Consistency, Sequential Consistency, and Linearizable Consistency.

The tests are written in the client.py and the configuration for each of them is in clients_config.py.

The lab report and output from tests are included.

There is also two test data text files both cleaned and raw. There is a script included that cleaned the files.

### Command Line Arguments Available:
- -e (--eventual)
  - Run eventual consistency model on the nodes
- -s (--sequential)
  - Run sequential consistency model on the nodes
- -l (--linearizable)
  - Run linearizable consistency model on the nodes
- -a (--all)
  - Run all of the consistency models
- -v (--verbose)
  - Enable verbose logging. Which includes print statements from the nodes.
