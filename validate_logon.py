import pyesgf.logon

manager = pyesgf.logon.LogonManager()
if manager.is_logged_on():
    print("Logged on to ESGF")
else:
    print("Not logged on to ESGF")