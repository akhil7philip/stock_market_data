import os, sys
from pathlib import Path
print(Path(__file__).resolve().parent.parent)
sys.path.insert(0,Path(__file__).resolve().parent.parent)