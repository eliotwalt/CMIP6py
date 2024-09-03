import os
import time
import json
from playwright.sync_api import sync_playwright, expect
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
		nodes_status = {}
		with sync_playwright() as p:
			browser = p.chromium.launch(headless=True)  # Run in headless mode
			page = browser.new_page()
			# Navigate to the URL
			page.goto(ESGF_NODES_STATUS_URL)
			try:
				tbody = page.locator('tbody.ant-table-tbody')
				# Wait for the table body to be visible
				expect(tbody).to_be_visible(timeout=60000)
				rows = tbody.locator('tr.ant-table-row')
				# Wait until at least one row is present
				expect(rows.first).to_be_visible(timeout=60000)
				# Iterate over rows
				for i in range(rows.count()):
					row = rows.nth(i)
					cells = row.locator('td.ant-table-cell')
					if cells.count() > 1:
						node = cells.nth(0).inner_text().strip()
						status = True if cells.nth(1).inner_text().strip().lower() == "yes" else False
						nodes_status[node] = status
					else:
						logger.error(f'Expected cells not found in row: {cells}')
			except Exception as e:
				browser.close()
				raise e
			browser.close()
		return nodes_status
	# caching routine
	if cache_is_valid():
		try:
			return load_cache()
		except Exception as e:
			logger.error(f"could not load esgf nodes status from cache, fetching again")
	nodes_status = fetch_nodes_status()
	write_cache(nodes_status)
	return nodes_status