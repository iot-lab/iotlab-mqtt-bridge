#! /usr/bin/env python3
from iotlabaggregator.serial import SerialAggregator
import paho.mqtt.client as mqtt
import time
import json


class mqttSerialBridge(mqtt.Client) :

    def __init__(self, nodeList, brokerAddress, port=1883, experimentID = None, clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp") :
        super().__init__(client_id="mqttSerialBridge", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
        self.brokerAddress = brokerAddress
        self.port = port
        self.nodeList = nodeList
        self.serialAggregator = SerialAggregator(nodeList, line_handler=self.line_handler)
        
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
        # decode data
        data = msg.payload.decode()
        # send it to node
        print(node,'<-', msg.payload, data)
        self.serialAggregator.send_nodes([node,], data)
        
    def line_handler(self, identifier, line):
        now = time.time()
        print("{} -> {}".format(identifier, line))
        # publish as raw data on testbed/node/+/out
        rawDict = {
            'timestamp':    now,
            'node_id':      identifier,
            'payload':      line.strip('\r')
            }
        self.publish('testbed_dev/node/{}/out'.format(identifier), json.dumps(rawDict))
        # attempt to json-ify the data, publish it on testbed/node/+/json_out
        try :
            jsonDict = {
                'timestamp':    now,
                'node_id':      identifier,
                'payload':      json.loads(line)
                }
            self.publish('testbed_dev/node/{}/out_json'.format(identifier), json.dumps(rawDict))
        except json.decoder.JSONDecodeError :
            pass
            
        
if __name__ == '__main__':
    # test debug draft dirty config with hack bridge on 10.254.253.1
    opts = SerialAggregator.parser.parse_args(None)
    nodes_list = SerialAggregator.select_nodes(opts)
    bridge = mqttSerialBridge(nodes_list, '10.254.253.1')
    bridge.loop_forever()
    
    
