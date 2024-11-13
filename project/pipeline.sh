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

# Multi agent support for Runner
if try_command python project/main.py; then
    echo "used windows runner"
elif try_command python3 project/main.py; then
    echo "used linux/mac runner"
else
    echo "Failed"
    exit 1
fi