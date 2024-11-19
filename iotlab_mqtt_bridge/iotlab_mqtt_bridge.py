#! /usr/bin/env python3
from iotlabaggregator.serial import SerialAggregator
import paho.mqtt.client as mqtt
import time
import json
import argparse
import os, sys

class mqttSerialBridge(mqtt.Client) :
    def __init__(
            self, nodeList, IDMap=None, topicRoot = '/', experimentID = None,
            brokerAddress='127.0.0.1', username=None, password=None, port=1883,
            verbose = None,
            clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp",
            ca_certs=None, certfile=None, keyfile=None) :
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
        self.topicRoot = topicRoot if topicRoot[-1] != '/' else topicRoot[:-1]
        # TLS setup if required
        if any((ca_certs, certfile, keyfile)):
            self.tls_set(ca_certs, certfile, keyfile)
        
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
            print("Return code",rc,"on MQTT connect", file=sys.stderr)
        if rc != 0 :
            print("Error return code",rc,"on MQTT connect", file=sys.stderr)
            if rc == 5 :
                print("Check MQTT credentials", file=sys.stderr)
            self.looping = False
            
        # subscribe on specific node topic
        for node in self.nodeList :
            topic = '{}/{}/in'.format(self.topicRoot, node)
            self.subscribe(topic, 2)
            if self.verbose >= 1 : 
                print("subscribed to", topic, file=sys.stderr)
        
    def on_message(self, client, userdata, msg) :
        # parse/convert node id from topic and create node identifier
        node = msg.topic.replace(self.topicRoot,'',1).split('/')[1]
        if not self.rIDMap is None and node in self.rIDMap :
            node = self.rIDMap[node]
        # decode data
        data = msg.payload.decode()
        # send it to node
        self.serialAggregator.send_nodes([node,], data)
        if self.verbose >= 2 : 
            print(time.time(), node,'<-', data, file=sys.stderr)
        
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
        self.publish('{}/{}/out'.format(self.topicRoot,identifier2), json.dumps(rawDict),0)
        # attempt to json-ify the data, publish it on testbed/node/+/json_out
        try :
            jsonDict = {
                'timestamp':    now,
                'node_id':      identifier2,
                'payload':      json.loads(line)
                }
            self.publish('{}/{}/out_json'.format(self.topicRoot,identifier2), json.dumps(jsonDict),0)
        except json.decoder.JSONDecodeError :
            pass
        if self.verbose >= 2 : 
            print(time.time(), "{} -> {}".format(identifier2, line), file=sys.stderr)

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog = 'LocuURa<->iotlab bridge')
    parser.add_argument('-f','--idFile', action='store', default=None, required=False,
                    help='json dictionnary file with iotlab IDs ans keys and target IDs as values.')
    parser.add_argument('-b','--broker', action='store', default=os.environ['LI_BRIDGE_HOST'] if 'LI_BRIDGE_HOST' in os.environ else '127.0.0.1',
                    help='Broker address. Notice : LI_BRIDGE_HOST environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-v','--verbose', action='count', default=int(os.environ['LI_BRIDGE_VERBOSE'] if 'LI_BRIDGE_VERBOSE' in os.environ else False),
                    help='Verbosity. Specify multiple times for more noise. LI_BRIDGE_VERBOSE environment variable can be used with the same effect.')
    parser.add_argument('-P','--port', action='store', default=int(os.environ['LI_BRIDGE_PORT'] if 'LI_BRIDGE_PORT' in os.environ else 1883),
                    help='Broker port', type=int)
    parser.add_argument('-u','--username', action='store', default=os.environ['LI_BRIDGE_USER'] if 'LI_BRIDGE_USER' in os.environ else '',
                    help='username on the broker. Notice : LI_BRIDGE_USER environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-p','--password', action='store', default=os.environ['LI_BRIDGE_PWD'] if 'LI_BRIDGE_PWD' in os.environ else '',
                    help='password on the broker. Advice : use LI_BRIDGE_PWD environment variable instead. This argument will override the environment variable')
    parser.add_argument('-V','--username_iotlab', action='store', default=os.environ['LI_IOTLAB_USER'] if 'LI_IOTLAB_USER' in os.environ else '',
                    help='username for iot-lab API. Notice : LI_IOTLAB_USER environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-Q','--password_iotlab', action='store', default=os.environ['LI_IOTLAB_PWD'] if 'LI_IOTLAB_PWD' in os.environ else '',
                    help='password for iotl-ab API. Advice : use LI_IOTLAB_PWD environment variable instead. This argument will override the environment variable')
    parser.add_argument('-t','--topic_root', action='store', default=os.environ['LI_BRIDGE_TOPIC'] if 'LI_BRIDGE_TOPIC' in os.environ else '',
                    help='root of the topics. Topics used will be <topic_root>/node-id/out[_json] and <topic_root>/node-id/in. Notice : LI_BRIDGE_TOPIC environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-C','--ca_certs', action='store', default=os.environ['LI_BRIDGE_CA_CERTS'] if 'LI_BRIDGE_CA_CERTS' in os.environ else None,
                    help='a string path to the Certificate Authority certificate files that are to be treated as trusted by this client. Notice : LI_BRIDGE_CA_CERTS environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-c','--certfile', action='store', default=os.environ['LI_BRIDGE_CERTFILE'] if 'LI_BRIDGE_CERTFILE' in os.environ else None,
                    help='PEM encoded client certificate filename. Notice : LI_BRIDGE_CERTFILE environment variable has the same effect. This argument will override the environment variable')
    parser.add_argument('-k','--keyfile', action='store', default=os.environ['LI_BRIDGE_KEYFILE'] if 'LI_BRIDGE_KEYFILE' in os.environ else None,
                    help='PEM encoded client private keys filename. Notice : LI_BRIDGE_KEYFILE environment variable has the same effect. This argument will override the environment variable')
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
    iotlab_args =  []
    if 'EXP_ID' in os.environ :
        iotlab_args += ['--id', os.environ['EXP_ID']]
    if args.username_iotlab :
        iotlab_args += ['--user', args.username_iotlab ]
    if args.password_iotlab :
        iotlab_args += ['--password', args.password_iotlab ]
    if len(iotlab_args) == 0 :
        iotlab_args = ''

    opts = SerialAggregator.parser.parse_args(iotlab_args)
    
    if args.verbose :
        print(time.time(), "Started with verbosity {}".format(args.verbose), file=sys.stderr)
        print("broker", args.broker, file=sys.stderr)
        print("port", args.port, file=sys.stderr)
        print("username", args.username, file=sys.stderr)
        print("password", args.password, file=sys.stderr)
        print("topicRoot", args.topic_root, file=sys.stderr)
        
    
    nodes_list = SerialAggregator.select_nodes(opts)

    bridge = mqttSerialBridge(
                nodes_list, brokerAddress=args.broker, username=args.username, password=args.password, 
                IDMap=mapping, port=args.port, verbose = args.verbose, topicRoot=args.topic_root,
                ca_certs=args.ca_certs, certfile=args.certfile, keyfile=args.keyfile)
    bridge.loop_forever()
    
    
