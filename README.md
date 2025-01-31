# IoT-lab to MQTT bridge

This module provides an easy way to redirect the input/output of the serial ports of iot-lab nodes to a MQTT broker.

The core of the module is a script intended to be run on an iotlab ssh frontend. Utilities are provided to help you run that script on the server automatically when the experiment start.

## Installation

`pip3 install iotlab_mqtt_bridge`

On your own computer you will not be able to launch the module directly, but you will be able to use the module in scripts (see Examples).

On iotlab ssh frontends, the script may already be installed by your local admin (toulouse.iot-lab.info for example). If not, you can ask them or install it in your home directory.

## Options

See `iotlab-mqtt-bridge --help` for detailed options. Notably, the following parameters of the bridge can be configured :
  * broker (IP, broker port, encryption certificate, authentification)
  * verbosity
  * topic root (`topic_root` hereafter) is used to construct individual node topics.
  * ID dictionnary (json file) used to convert iotlab IDs to a custom set of ids (each must be unique).

The arguments not described in `iotlab-mqtt-bridge --help` are passed down the `serial_aggregator` (on which this project is based).

## Node topics

Each individual node topic root is constructed as follow :
  1. The local node name is extracted from its address. Ex: `dwm1001-1.toulouse.iot-lab.info` becomes `dwm1001-1`
  2. If an ID dictionnary was provided, this local node name (`dwm1001-1` in our example) is used as a key in the dictionnary to determine the new identifier, otherwise the local name is used as identifier.
  3. `topic_root` is prepended to this identifier to form the `node_topic`
  

## Node output handling

The serial output of each node is split into substrings at every '\n' character, then each substring is published in a single message (containing also the node id and a timestamp) on a specific topic (`<node_topic>/out`) for each node. 

If this substring can be parsed as a JSON object, it is also published on a second topic (`<node_topic>/out_json`). The payload of messages on the latter topic are thus guaranteed to be valid json objects.

## Node input handling

Each message published on `<node_topic>/in` gets written directly on the serial port of the associated node.


## Examples 
### On site ssh frontend

Run on iotlab ssh frontend :
`python3 -m iotlab_mqtt_bridge -b <x.x.x.x> -u <broker_username> -p <broker_password> -t "topic_root/" `

If TLS is used on the broker, it may be necessary to use the argument `-C <ca_cert>`.

If iotlab-mqtt-bridge is already installed, running `iotlab-mqtt-bridge` without arguments will bridge the serial ports to mqtt4.iot-lab.info (it requires that you run `iotlab-auth` at least once before).


### In python script

See examples/script_launcher.py in module directory.


## Known issues

  * The serial port can be opened only once. Consquently :
    * A given node can be bridged by only one iotlab\_mqtt\_bridge at a time
    * Web serial consoles must be closed before starting the MQTT bridge
    * `serial_aggregator` can not access a given node at the same time as `iotlab\_mqtt\_bridge`
  * Serial ports can be disturbed when nodes are flashed, so it may be necessary to restart `iotlab\_mqtt\_bridge` after re-flashing the nodes.
  
