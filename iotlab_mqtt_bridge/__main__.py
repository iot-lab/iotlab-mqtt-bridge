#! /usr/bin/env python3
from .helpers import getScriptPath
with open(getScriptPath()) as f:
    exec(f.read())
