#!/bin/bash

set -e

export TOP_DIR=`pwd`

export WORK_DIR=$HOME/workflows
if ls /local-scratch/ >/dev/null 2>&1; then
    export WORK_DIR=/local-scratch/$USER/workflows
fi
mkdir -p $WORK_DIR

export RUN_ID=test-workflow-`date +'%s'`

export INPUT_DIR=$(pwd)/../

#envsubst < tc.txt.template > tc.txt
envsubst < sites.xml.template > sites.xml
envsubst < ../input.yml.template > ../input.yml

# generate workflow
../../pegasus-cwl-converter.py ../workflow.cwl ../input.yml ./workflow.xml

# plan and submit the  workflow
pegasus-plan \
    --conf pegasus.conf \
    --force \
    --dir $WORK_DIR \
    --relative-dir $RUN_ID \
    --sites condorpool \
    --output-site local \
    --dax workflow.xml \
    --cluster horizontal \
    --submit
