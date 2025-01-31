#!/usr/bin/python3

# This example shows how to look if the nodes are already reserved by the user. If so, it reflashes them and (re)start the bridge. Otherwise it reserve nodes, wait until they are available, launch an experiment and start the bridge.

import sys
from iotlabcli import experiment, get_user_credentials, Api
import iotlab_mqtt_bridge.helpers as helpers
import json
from urllib.error import HTTPError
import signal
import time
from iotlabcli.node import node_command


# experiment config
node1 = 'dwm1001-1.toulouse.iot-lab.info'
node2 = 'dwm1001-2.toulouse.iot-lab.info'
node3 = 'dwm1001-5.toulouse.iot-lab.info'
allNodes = set([node1, node2, node3])

firmwareA = '/tmp/firwmare_A.elf'
firmwareB = '/tmp/firmware_B.elf'

site = 'toulouse'
experimentName = "testLauncherAlreadyRunning" # only [a-zA-Z0-9] characters
duration = 10   # minutes

# broker config
bridgeIP    = 'xxx.xxx.xxx.xxx'
bridgeUser  = 'username'
bridgePW    = 'password'
baseTopic   = 'basetopic/'


user, passwd = get_user_credentials(None, None)  # read from ~/.iotlabrc, 
                                                 # requeries to have executed 
                                                 # iotlab-atch at least once
# user, passwd = get_user_credentials("username", "password") # directly in file
api = Api(user, passwd)


# Selects node to use and which firmwares.
# Here, node1 and node2 will be flashed with firmware_A.elf, and node3 with 
# firmware_B.elf
nodes_resource_iotlab = [   
                            experiment.exp_resources([node1, node2], firmware_path=firmwareA),
                            experiment.exp_resources([node3,], firmware_path=firmwareB)
                        ]
# get bridge script location
script = helpers.getScriptPath()
# configure the bridge (creates a config file)
scriptconfig = helpers.makeScriptConfig(bridgeIP, bridgeUser, bridgePW, baseTopic)

# Prepare the site association, i.e. information to run the experiment. Here, 
# it contains the script to execute and its config.
site_assocs = [ experiment.site_association(site, script=script,scriptconfig=scriptconfig),]

try:
    # get existing experiments
    print('Downloading experiments list')
    experiments = experiment.get_experiments_list(api, 'Running', 100, 0)
    
    foundAll = False
    # browse list to find if our nodes are already available
    for e in experiments['items'] :
        nodes = experiment.get_experiment(api, e['id'], 'nodes')
        found = set()
        for n in nodes['items'] :
            if n['network_address'] in allNodes :
                found.add(n['network_address'])
        if len(found) == len(allNodes) :
            exp_id = e['id']
            print("All nodes are already reserved in experiment #{}".format(e['id']))
            foundAll = True
            break
    
    if foundAll :
        # (re)-starting bridge
        for site_assoc in site_assocs :
            try :
                print("Attempting to stop previous script")
                experiment.script_experiment(api, exp_id, 'kill', site_assoc.sites)
                print("Previous script stopped")
            except :
                print("Failed to stop previous script. It was likely not running.")
                pass
            print("Starting new script")
            experiment.script_experiment(api, exp_id, 'run', site_assoc)
        
        # flashing these nodes
        print("Flashing all nodes. Deployment statuses :")
        for resource in nodes_resource_iotlab :
            deployment_status_r = node_command(api, 'flash', exp_id, nodes_list= resource['nodes'], cmd_opt=str(resource['firmware']))
            print('\t', deployment_status_r)
    else :
        # reserve nodes as usual
        print("Nodes not found in any experiment already running. Reserving and flashing them")
        exp_id = experiment.submit_experiment(  api,
                                                experimentName, 
                                                duration,
                                                nodes_resource_iotlab,
                                                sites_assocs=site_assocs)['id'] # the API gives us an 'id'
        experiment.wait_experiment(api, exp_id) # waits until sate "running"
        print('Experiment {} is running ...'.format(exp_id))
        deployment_status = experiment.get_experiment(api, exp_id, 'deployment')
        print("Deployment statuses:")
        for s in sorted(deployment_status) :
            if s == '0' :
                print('\t', s, '(success): ', deployment_status[s])
            else :
                print('\t', s, ': ', deployment_status[s])
                for n in deployment_status[s] :
                    nodesResultString[n] += 'deploy failed ({}).'.format(s)
except HTTPError as err:
    json_err = json.loads(err.reason)
    print('{}'.format(json_err['message']))
    sys.exit(-1)
except Exception as e:
    raise e
    sys.exit(-1)



# define handler to catch CTRL-C to end the experiment early    
def signal_handler(sig, frame):
    print('Requested exit')
    #stop the experiment only if it was not already running
    if not foundAll :
        experiment.stop_experiment(api, exp_id)
        print('Experiment stopped')
    else :
        print('Experiment and remote script kept running')
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

print('press CTRL+C to quit')

# wait until experiment end
experiments = experiment.get_active_experiments(api, True)
while 'Running' in experiments and exp_id in experiments['Running'] :
    time.sleep(5)
    experiments = experiment.get_active_experiments(api, True)
print('Experiment finished')
