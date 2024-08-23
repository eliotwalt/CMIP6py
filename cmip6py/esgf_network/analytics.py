import os
from pathlib import Path
import time
import json
import playwright.sync_api
import logging

from ..commons.constants import CACHE_DIR

ESGF_NODES_STATUS_CACHE_FILE = CACHE_DIR / 'esgf-nodes-status.json'
ESGF_NODES_STATUS_CACHE_TTL = 600 # 10 minutes time-to-live
ESGF_NODES_STATUS_URL = 'https://aims2.llnl.gov/nodes'
os.makedirs(os.path.dirname(ESGF_NODES_STATUS_CACHE_FILE), exist_ok=True)

logger = logging.getLogger(__name__)

class ESGFNodesStatusError(Exception):
    pass

def get_esgf_nodes_status():
	# utilities
	def cache_is_valid():
		if not os.path.exists(ESGF_NODES_STATUS_CACHE_FILE):
			return False
		file_mod_time = os.path.getmtime(ESGF_NODES_STATUS_CACHE_FILE)
		current_time = time.time()
		return (current_time - file_mod_time) < ESGF_NODES_STATUS_CACHE_TTL
	def load_cache():
		if os.path.exists(ESGF_NODES_STATUS_CACHE_FILE):
			with open(ESGF_NODES_STATUS_CACHE_FILE, "r") as f:
				return json.load(f)
		return None
	def write_cache(nodes_status):
		with open(ESGF_NODES_STATUS_CACHE_FILE, "w") as f:
			return json.dump(nodes_status, f)
	def fetch_nodes_status():
		# see Documents/phd/code/esgf-network-analytics/node_status.py
		# return nodes_status = {$url: $status}
		nodes_status = {}
		with playwright.sync_api.sync_playwright() as p:
			browser = p.chromium.launch(headless=True)  # Run in headless mode
			page = browser.new_page()
			# Navigate to the URL
			page.goto(ESGF_NODES_STATUS_URL)
			# Wait for network to be idle (no ongoing network requests)
			page.wait_for_load_state('networkidle')
			# Wait for the tbody element to be present and visible
			try:
				page.wait_for_selector('tbody.ant-table-tbody', timeout=60000)  # Wait up to 60 seconds
			except Exception as e:
				browser.close()
				raise e
			# Extract rows from tbody
			tbody = page.query_selector('tbody.ant-table-tbody')
			if not tbody:
				browser.close()
				raise ESGFNodesStatusError(f"malformed HTML at {ESGF_NODES_STATUS_URL}. Table body (tbody) not found")
			rows = tbody.query_selector_all('tr.ant-table-row')
			for row in rows:
				cells = row.query_selector_all('td.ant-table-cell')
				if len(cells) > 1:
					node = cells[0].inner_text().strip()
					status = True if cells[1].inner_text().strip().lower() == "yes" else False
					nodes_status[node] = status
				else:
					logger.error(f'Expected cells not found in row: {cells}')
			# Close the browser
			browser.close()
		return nodes_status
	# caching routine
	if cache_is_valid():
		try:
			return load_cache()
		except Exception as e:
			logger.info(f"could not load esgf nodes status from cache, fetching again")
	nodes_status = fetch_nodes_status()
	write_cache(nodes_status)
	return nodes_status