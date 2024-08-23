import argparse
import getpass

from cmip6py.commons.auth import set_credentials

p = argparse.ArgumentParser()
p.add_argument("--username", required=True, help="ESGF username", type=str)
p.add_argument("--hostname", required=True, help="ESGF hostname", type=str)

args = p.parse_args()
pwd = getpass(f"Enter password ({args.username}@{args.hostname}): ")

set_credentials(args.hostname, args.username, pwd)