#!/bin/bash

# IoTDB_HOME= # Recommended use: Define IoTDB_HOME env var in your path.
bash $IoTDB_HOME/sbin/stop-confignode.sh
bash $IoTDB_HOME/sbin/stop-datanode.sh
