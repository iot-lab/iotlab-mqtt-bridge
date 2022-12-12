#! /usr/bin/env python3
from iotlabaggregator.serial import SerialAggregator
import paho.mqtt.client as mqtt
import time
import json
import argparse
import os, sys

class mqttSerialBridge(mqtt.Client) :
    def __init__(self, nodeList, brokerAddress, username=None, password=None, verbose = None, IDMap=None, port=1883, experimentID = None, clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp") :
        super().__init__(client_id="mqttSerialBridge", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.brokerAddress = brokerAddress
        self.port = port
        self.nodeList = nodeList
        self.serialAggregator = SerialAggregator(nodeList, line_handler=self.line_handler)
        if username is not None :
            self.username_pw_set(username, password)
        self.IDMap  = IDMap
        self.rIDMap = {v:k for k,v in IDMap.items()} if not IDMap is None else None
        self.looping = False
        self.verbose = verbose if verbose else 0
        
    def start(self):
        # MQTT connect
        self.connect_async(self.brokerAddress, self.port)
        # MQTT loop start
        self.loop_start()
        # serial aggregator start
        self.serialAggregator.start()
        self.looping = True
        
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
        self.looping = False
        
        
    def on_connect(self, client, userdata, flags, rc):
        if self.verbose >= 1 : 
            print("Return code",rc,"on MQTT connect")
        if rc != 0 :
            print("Error return code",rc,"on MQTT connect")
            if rc == 5 :
                print("Check MQTT credentials")
            self.looping = False
            
        # subscribe on specific node topic
        for node in self.nodeList :
            topic = 'testbed/node/{}/in'.format(node)
            self.subscribe(topic)
            if self.verbose >= 1 : 
                print("subscribed to", topic)
        
    def on_message(self, client, userdata, msg) :
        # parse/convert node id from topic and create node identifier
        node = msg.topic.split('/')[2]
        if not self.rIDMap is None and node in self.rIDMap :
            node = self.rIDMap[node]
        # decode data
        data = msg.payload.decode()
        # send it to node
        self.serialAggregator.send_nodes([node,], data)
        if self.verbose >= 2 : 
            print(time.time(), node,'<-', data)
        
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
        self.publish('testbed/node/{}/out'.format(identifier2), json.dumps(rawDict))
#        print('testbed/node/{}/out'.format(identifier),self.IDMap)
        # attempt to json-ify the data, publish it on testbed/node/+/json_out
        try :
            jsonDict = {
                'timestamp':    now,
                'node_id':      identifier2,
                'payload':      json.loads(line)
                }
            self.publish('testbed/node/{}/out_json'.format(identifier2), json.dumps(rawDict))
        except json.decoder.JSONDecodeError :
            pass
        if self.verbose >= 2 : 
            print(time.time(), "{} -> {}".format(identifier2, line))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog = 'LocuURa<->iotlab bridge')
    parser.add_argument('-f','--idFile', action='store', default=None, required=False,
                    help='json dictionnary file with iotlab IDs ans keys and locura IDs as values.')
    parser.add_argument('-b','--broker', action='store', default=os.environ['LI_BRIDGE_HOST'],
                    help='Broker address')
    parser.add_argument('-v','--verbose', action='count', default=int(os.environ['LI_BRIDGE_VERBOSE']),
                    help='Verbosity. Specify multiple times for more noise. LI_BRIDGE_VERBOSE environment variable can be used with the same effect.')
    parser.add_argument('-P','--port', action='store', default=int(os.environ['LI_BRIDGE_PORT']),
                    help='Broker port')
    parser.add_argument('-u','--username', action='store', default=os.environ['LI_BRIDGE_USER'],
                    help='username on the broker. Notice : LI_BRIDGE_USER environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-p','--password', action='store', default=os.environ['LI_BRIDGE_PWD'],
                    help='password on the broker. Advice : use LI_BRIDGE_PWD environment variable instead. This argument will override the environment variable')
    args = parser.parse_args()

    if args.idFile is not None :
        d = ''
        with open(args.idFile,'r') as f :
            for l in f.readlines() :
                d += l
        mapping = json.loads(d)
    else :
        mapping = None

    # Let's exploit automatic things from serialaggregator
    # We don't care about allowing the user to supply their username/password
    # because this script is only ever to be used directly on 
    # (dev)toulouse.iot-lab.info SSH frontend, where these are supplied as
    # environment variables
    opts = SerialAggregator.parser.parse_args("")
    nodes_list = SerialAggregator.select_nodes(opts)
    

    bridge = mqttSerialBridge(nodes_list, args.broker, username=args.username, password=args.password, IDMap=mapping, port=args.port, verbose = args.verbose)
    bridge.loop_forever()
    
    
