#! /usr/bin/env python3
from iotlabaggregator.serial import SerialAggregator
import paho.mqtt.client as mqtt
import time
import json
import argparse
import os


class mqttSerialBridge(mqtt.Client) :
    def __init__(self, nodeList, brokerAddress, username=None, password=None, IDMap=None, port=1883, experimentID = None, clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp") :
        super().__init__(client_id="mqttSerialBridge", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.brokerAddress = brokerAddress
        self.port = port
        self.nodeList = nodeList
        self.serialAggregator = SerialAggregator(nodeList, line_handler=self.line_handler)
        if username is not None :
            self.username_pw_set(username, password)
        self.IDMap  = IDMap
        self.rIDMap = {v:k for k,v in IDMap.items()} if not IDMap is None else None
        
    def start(self):
        # MQTT connect
        self.connect_async(self.brokerAddress, self.port)
        # MQTT loop start
        self.loop_start()
        # serial aggregator start
        self.serialAggregator.start()
        
    def loop_forever(self):
        # MQTT connect
        self.connect_async(self.brokerAddress, self.port)
        # serial aggregator start
        self.serialAggregator.start()
        # forever
        super().loop_forever()
        
    def stop(self):
        # MQTT loop stop
        self.loop_stop()
        # serial aggregator stop
        self.serialAggregator.stop()
        
        
    def on_connect(self, client, userdata, flags, rc):
        # subscribe on specific node topic
        for node in self.nodeList :
            self.subscribe('testbed_dev/node/{}/in'.format(node))
        
    def on_message(self, client, userdata, msg) :
        # parse/convert node id from topic and create node identifier
        node = msg.topic.split('/')[2]
        if not self.rIDMap is None and node in self.rIDMap :
            node = self.rIDMap[node]
        # decode data
        data = msg.payload.decode()
        # send it to node
        print(time.time(), node,'<-', msg.payload, data)
        self.serialAggregator.send_nodes([node,], data)
        
    def line_handler(self, identifier, line):
        now = time.time()
        identifier2 = identifier
        if not self.IDMap is None and identifier in self.IDMap :
            identifier2 = self.IDMap[identifier]
        
        # publish as raw data on testbed/node/+/out
        rawDict = {
            'timestamp':    now,
            'node_id':      identifier2,
            'payload':      line.strip('\r')
            }
        self.publish('testbed_dev/node/{}/out'.format(identifier), json.dumps(rawDict))
        # attempt to json-ify the data, publish it on testbed/node/+/json_out
        try :
            jsonDict = {
                'timestamp':    now,
                'node_id':      identifier2,
                'payload':      json.loads(line)
                }
            self.publish('testbed_dev/node/{}/out_json'.format(identifier), json.dumps(rawDict))
        except json.decoder.JSONDecodeError :
            pass
        print(time.time(), "{} -> {}".format(identifier2, line))
            
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'LocuURa<->iotlab bridge')
    parser.add_argument('-f','--idFile', action='store', required=True,
                    help='json dictionnary file with iotlab IDs ans keys and locura IDs as values.')
    parser.add_argument('-b','--broker', action='store', required=True,
                    help='Broker address')
    parser.add_argument('-P','--port', action='store', default=1883,
                    help='Broker port')
    parser.add_argument('-u','--username', action='store', default=None,
                    help='username on the broker. Notice : LC_LIBRIDGE_USER environment variable has the same effect, though this argument will override the environment variable')
    parser.add_argument('-p','--password', action='store', default=None,
                    help='password on the broker. Advice : use LC_LIBRIDGE_PWD environment variable instead. This argument will override the environment variable')
    args = parser.parse_args()

    d = ''
    with open(args.idFile,'r') as f :
        for l in f.readlines() :
            d += l
    mapping = json.loads(d)

    # let's exploit automatic things from serialaggregator
    opts = SerialAggregator.parser.parse_args("")
    nodes_list = SerialAggregator.select_nodes(opts)
    
    # get the username/pwd for environment variables
    if args.username is None and 'LC_LIBRIDGE_USER' in os.environ:
        args.username = os.environ['LC_LIBRIDGE_USER']
    if args.password is None and 'LC_LIBRIDGE_PWD' in os.environ:
        args.password = os.environ['LC_LIBRIDGE_PWD']

    bridge = mqttSerialBridge(nodes_list, args.broker, username=args.username, password=args.password, IDMap=mapping, port=args.port)
    bridge.loop_forever()
    
    
