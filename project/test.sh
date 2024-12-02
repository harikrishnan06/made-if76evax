#!/bin/bash

# Exit
set -e

try_command() {
    "$@" || return 1
}

# Multi agent support for pack insatllation
if try_command pip install -r project/requirements.txt; then
    echo "used windows runner"
elif try_command pip3 install -r project/requirements.txt; then
    echo "used linux/mac runner"
else
    echo "Failed"
    exit 1
fi

# Multi agent support for Runner python -m unittest test_fetch_data.py  python project/main.py
if try_command python -m pytest project/test/; then
   echo "used windows runner"
elif try_command python -m pytest project/test/ ; then
    echo "used linux/mac runner"
else
    echo "Failed"
    exit 1
fi