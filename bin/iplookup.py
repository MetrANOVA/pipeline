"""
Command-line script that reads pickle file into pytricia trie for IP lookups.
Usage: iplookup.py [--file <file_path>] IP_ADDRESS
"""
import argparse
import os
import pickle
import sys
import time

#Parse args
parser = argparse.ArgumentParser(description="IP Lookup Tool")
parser.add_argument("--file", type=str, help="Path to the pickle file")
parser.add_argument("ip_address", type=str, help="IP address to look up")
parser.add_argument("--verbose", action='store_true', default=False, help="Enable verbose output")
args = parser.parse_args()

# Determine filename
if args.file:
    filename = args.file
else:
    filename = os.path.join(os.getcwd(), "ip_trie.pickle")
# Check if file exists
if os.path.exists(filename):
    if args.verbose:
        print(f"Found IP trie cache file: {filename}")
else:
    print(f"No IP trie cache file found: {filename}")
    sys.exit(1)

# Load the trie from the pickle file
try:
    with open(filename, 'rb') as f:
        trie = pickle.load(f)
except Exception as e:
    print(f"Error loading IP trie from {filename}: {e}")
    sys.exit(1)
if args.verbose:
    print(len(trie), "entries loaded.")

start = time.perf_counter()
print(trie.get(args.ip_address, "IP address not found")) 
end = time.perf_counter()

if args.verbose:
    print(f"Lookup took {end - start:.6f} seconds")