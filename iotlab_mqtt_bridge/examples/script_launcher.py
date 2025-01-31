#!/usr/bin/python3

# This example shows how to reserve nodes, wait until they are available, launch an experiment and start the bridge.

import sys
from iotlabcli import experiment, get_user_credentials, Api
import iotlab_mqtt_bridge.helpers as helpers
import json
from urllib.error import HTTPError
import signal
import time

# experiment config
node1 = 'dwm1001-1.toulouse.iot-lab.info'
node2 = 'dwm1001-2.toulouse.iot-lab.info'
node3 = 'dwm1001-5.toulouse.iot-lab.info'

firmwareA = '/tmp/firwmare_A.elf'
firmwareB = '/tmp/firmware_B.elf'

site = 'toulouse'
experimentName = "testLauncher" # only [a-zA-Z0-9] characters
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
scriptconfig = helpers.makeScriptConfig(bridgeIP, bridgeUser, bridgePW, baseTopic))

# Prepare the site association, i.e. information to run the experiment. Here, 
# it contains the script to execute and its config.
site_assocs = [ experiment.site_association(site, script=script,scriptconfig=scriptconfig),]

try:
    # submit the experiment
    print('submitting experiment', experimentName)
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
    print(e)
    sys.exit(-1)


# define handler to catch CTRL-C to end the experiment early    
def signal_handler(sig, frame):
    print('Requested exit')
    #stop the experiment early (if requested)
    experiment.stop_experiment(api, exp_id)
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

print('press CTRL+C to quit')

# wait until experiment end
experiments = experiment.get_active_experiments(api, True)
while 'Running' in experiments and exp_id in experiments['Running'] :
    time.sleep(5)
    experiments = experiment.get_active_experiments(api, True)
print('Experiment finished')
