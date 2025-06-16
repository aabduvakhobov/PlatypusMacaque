#!/bin/bash

# IoTDB_HOME= # Recommended use: Define IoTDB_HOME env var in your path.

bash $IoTDB_HOME/sbin/start-confignode.sh -d
sleep 5
bash $IoTDB_HOME/sbin/start-datanode.sh -d
sleep 5
echo "All node started"
