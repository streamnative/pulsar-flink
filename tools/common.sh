#!/usr/bin/env bash

SCRIPTS_DIR=${SCRIPTS_DIR:-"`dirname "$0"`"}

GIT=$(which git)
if [ $? != 0 ]; then
    echo "Error: git is not found"
    exit 1
fi