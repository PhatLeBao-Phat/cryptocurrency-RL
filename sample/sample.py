import sys
import os

# Handle import
grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(grandparent_dir)
# from etl_basic.crypto_pipeline import *

print(__name__)

print(sys.path)