# IoT-lab to MQTT bridge

This module provides an easy way to redirect the input/output of the serial ports of iot-lab nodes to a MQTT broker. It only creates these bridges for the last experiment started.


The core of the module is a script intended to be run on an iotlab ssh frontend. Utilities are provided to help you run that script on the server automatically when the experiment start.

## Installation

`pip3 install iotlab_mqtt_bridge`

On your own computer you will not be able to launch the module directly, but you will be able to use the module in scripts (see Examples).

On iotlab ssh frontends, the script may already be installed by your local admin.

## Configurations

The following parameters of the bridge can be configured :
  * broker IP
  * broker port
  * verbosity
  * broker authentification username
  * broker authentification password
  * topic root (`topic_root` hereafter) is used to construct individual node topics.
  * ID dictionnary (json file) used to convert iotlab IDs to a custom set of ids (each must be unique).
  
### Node topics

Each individual node topic root is constructed as follow :
  1. The local node name is exctracted from its address. Ex: `dwm1001-1.toulouse.iot-lab.info` becomes `dwm1001-1`
  2. If an ID dictionnary was provided, this local node name (`dwm1001-1` in our example) is used as a key in the dictionnary to determine the new identifier, otherwise the local name is used as identifier.
  3. `topic_root` is prepended to this identifier to form the `node_topic`
  

## Node output handling

The serial output if each node is split into substrings at every '\n' character, then each substring is published in a single message (containing also the node id and a timestamp) on a specific topic (`<node_topic>/out`) for each node. 

If this string can be parsed as a JSON object, it is also published on a second topic (`<node_topic>/out_json`). The payload of messages on the latter topic are thus guaranteed to be valid json objects.

## Node input handling

Each message received on `<node_topic>/in` gets written directly on the serial port of the associated node.


## Examples 
### CLI

Run on iotlab ssh frontend :

`python3 -m iotlab_mqtt_bridge -b <x.x.x.x> -u <username> -p <password> -t "topic_root/"`

### In python script
See examples/script_launcher.py in module directory.

## Improvements doable if there is demand
Contact me if you need them, I'll see what I can do.
  * certificate handling
  * older experiment handling
  
