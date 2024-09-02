import keyring 
import pyesgf.logon

from .constants import AUTH_KEYS
        
def set_credentials(hostname, username, password):
    for key, value in {"hostname": hostname, 
                "username": username, 
                "password": password}.items():
        keyring.set_password("ESGF", key, value)
    logon()
    
def logon(verbose=True):
    manager = pyesgf.logon.LogonManager()
    # auth_config
    auth_config = {}
    for key in AUTH_KEYS:
        value = keyring.get_password("ESGF", key)
        if value is None:
            raise KeyError(f"{key} not set in keyring")
        auth_config[key] = value
    # logon
    manager.logon(**auth_config)
    if manager.is_logged_on():
        if verbose: print("Logged on to ESGF")
    else:
        raise ValueError("Failed to log on to ESGF.")