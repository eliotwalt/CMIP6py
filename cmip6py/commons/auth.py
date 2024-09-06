import os
import yaml
import keyring 
import pyesgf.logon
import logging 
logger = logging.getLogger()

from .constants import AUTH_KEYS, CRED_FILE
        
def set_credentials(hostname, username, password, verbose=False):
    """
    Writes credentials to keyring or locally
    """
    for key, value in {"hostname": hostname, 
                "username": username, 
                "password": password}.items():
        try:
            # write to keyring using OS backend
            keyring.set_password("ESGF", key, value)
        except Exception as e:
            # write to user-only file
            logger.warning(f"keyring not available. writing credentials to file: {CRED_FILE}")
            with open(CRED_FILE, mode="w", encoding="utf-8") as f:
                yaml.safe_dump({"hostname": hostname,
                                "username": username,
                                "password": password},
                               f, default_flow_style=False, sort_keys=False)
            os.chmod(CRED_FILE, 0o700)
    logon(verbose)
    
def logon(verbose=True):
    """
    Reads credentials from keyring or locally
    """
    manager = pyesgf.logon.LogonManager()
    # auth_config
    auth_config = {}
    for key in AUTH_KEYS:
        try:
            # retrieve from keyring
            value = keyring.get_password("ESGF", key)
        except Exception as e:
            # retrieve locally
            with open(CRED_FILE, mode="r", encoding="utf-8") as f:
                value = yaml.safe_load(f)[key]
        if value is None:
            raise KeyError(f"{key} not found")
        auth_config[key] = value
    # logon
    try:
        manager.logon(**auth_config)
    except:
        manager.logon(**auth_config, bootstrap=True)
        
    if manager.is_logged_on():
        if verbose: logger.info("Logged on to ESGF")
    else:
        raise ValueError("Failed to log on to ESGF.")