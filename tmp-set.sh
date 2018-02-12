#!/bin/bash
HOME_PATH=${1}
DATA_PATH=${2}
COMM_PATH=${3}
ROOT_PATH=${4}
RECOVER_FILE=$HOME/cassandra4slurm/cassandra-recover-file.txt
CFG_FILE=$HOME/cassandra4slurm/conf/cassandra4slurm.cfg
export CASS_HOME=$(cat $CFG_FILE | grep "CASS_HOME=" | sed 's/CASS_HOME=//g' | sed 's/"//g')

# Cleaning saved caches
rm -rf $CASS_HOME/data/saved_caches/

# Building directory tree
mkdir -p $ROOT_PATH
chmod g+w $ROOT_PATH

mkdir -p $ROOT_PATH/c4j
chmod g+w $ROOT_PATH/c4j

#If the data path exists, cleans the content, otherwise it is created
#It gives group write permissions by default 
if [ -d $DATA_PATH ]; then
    rm -rf $DATA_PATH/*
fi
mkdir -p $DATA_PATH
chmod g+w $DATA_PATH

#Commit Log folder reset
#It gives group write permissions by default
#By default it is /tmp/cassandra-commitlog, if you change it you should also change the cassandra.yaml file
if [ -d $COMM_PATH ]; then
    rm -rf $COMM_PATH/*
fi
mkdir -p $COMM_PATH
chmod g+w $COMM_PATH

#set the data path in the config file (safely)
sed 's/.*data_file_directories.*/data_file_directories:/' $HOME_PATH/conf/cassandra-cfg.yaml | sed "/data_file_directories:/!b;n;c     - $DATA_PATH" | sed "s+.*commitlog_directory:.*+commitlog_directory: $COMM_PATH+" > $HOME_PATH/conf/aux.yaml-$(hostname)
mv $HOME_PATH/conf/aux.yaml-$(hostname) $HOME_PATH/conf/cassandra-cfg.yaml-$(hostname)

#check & set of the symlinks to cassandra.yaml file for this hostname
if [ -L $ROOT_PATH/cassandra.yaml ]; then
    rm $ROOT_PATH/cassandra.yaml
fi
ln -s $HOME_PATH/conf/cassandra-cfg.yaml-$(hostname) $ROOT_PATH/cassandra.yaml
