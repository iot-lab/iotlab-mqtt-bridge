import os, tempfile

def getScriptPath():
    return os.path.dirname(os.path.abspath(__file__))+'/locura_iotlab_bridge.py'
    
    
__hidden_file_list__ = []   # keep the files in memory so they are not deleted 
                            # util the end of the script.
    
    
def makeScriptConfig(host, username, password, port=1883, verbose=0):
    """Generate a temporary scriptconfig file which contain the appropriate 
    environment variables. This file is deleted when the script ends.
    Returns the absolute path of this file."""
    f = tempfile.NamedTemporaryFile(mode='w', 
                                    prefix="locura_iotlab_bridge", 
                                    delete=True
                                    )
    f.write("LI_BRIDGE_HOST={}\n".format(host))
    f.write("LI_BRIDGE_PORT={}\n".format(port))
    f.write("LI_BRIDGE_VERBOSE={}\n".format(verbose))
    f.write("LI_BRIDGE_USER={}\n".format(username))
    f.write("LI_BRIDGE_PWD={}\n".format(password))
    f.flush()
    
    __hidden_file_list__.append(f)
        
        
    return f.name
    
    
