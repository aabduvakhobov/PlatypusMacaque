#!/bin/bash

sudo systemctl stop clickhouse
sudo rm -rf \
   /usr/bin/clickhouse* \
   /etc/clickhouse-server \
   /var/log/clickhouse-server \
   /var/lib/clickhouse-server \
   /var/run/clickhouse-server \
   /etc/clickhouse-server \
   /etc/security/limits.d/clickhouse.conf
sudo groupdel clickhouse
sudo userdel -r clickhouse
