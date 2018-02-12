#!/bin/bash
###############################################################################################################
#                                                                                                             #
#                                  Cassandra Node Snapshot Recovery for Slurm                                 #
#                                          Eloy Gil - eloy.gil@bsc.es                                         #
#                                                                                                             #
#                                     Barcelona Supercomputing Center 2018                                    #
#                                                    .-.--_                                                   #
#                                                  ,´,´.´   `.                                                #
#                                                  | | | BSC |                                                #
#                                                  `.`.`. _ .´                                                #
#                                                    `·`··                                                    #
#                                                                                                             #
###############################################################################################################

CFG_FILE=$HOME/cassandra4slurm/conf/cassandra4slurm.cfg
CASS_HOME=$(cat $CFG_FILE | grep "CASS_HOME=" | sed 's/CASS_HOME=//g' | sed 's/"//g')
SNAP_ORIG=$CASS_HOME/snapshots/
ROOT_PATH=${1}
DATA_HOME=$ROOT_PATH/c4j/cassandra-data
NODEFILE=$HOME/cassandra4slurm/hostlist-$SLURM_JOBID.txt
RECOVER_FILE=cassandra-recover-file.txt

#get & set the token list (safely)
RECOVERY=$(cat $RECOVER_FILE)
if [ "$RECOVERY" != "" ]
then
    TKNFILE_LIST=$(find $SNAP_ORIG -type f -name $RECOVERY-ring.txt)
    filecounter=0
    for tokens in $TKNFILE_LIST
    do  
        ((filecounter++))
        if [ "$(cat $NODEFILE | sed -n ""$filecounter"p")" == "$(hostname)" ]
        then
            echo "Restoring snapshot in node #"$filecounter": "$(hostname)
            sed "s/.*initial_token:.*/initial_token: $(cat $tokens)/" $CASS_HOME/conf/cassandra-cfg.yaml-$(hostname) > $CASS_HOME/conf/aux-$(hostname).yaml
            mv $CASS_HOME/conf/aux-$(hostname).yaml $CASS_HOME/conf/cassandra-cfg.yaml-$(hostname)

            orig_host=$(echo $tokens | sed "s+$CASS_HOME++g" | sed 's+/+ +g' | awk '{ print $2 }')
            clean_token=$RECOVERY
            for folder in $(find $SNAP_ORIG/$orig_host/$clean_token -maxdepth 1 -type d | sed -e "1d")
            do
                clean_folder=$(echo $folder | sed 's+/+ +g' | awk '{ print $NF }') 
                mkdir $DATA_HOME/$clean_folder
                for subfolder in $(find $SNAP_ORIG/$orig_host/$clean_token/$clean_folder -maxdepth 1 -type d | sed -e "1d")
                do
                    clean_subfolder=$(echo $subfolder | sed 's+/+ +g' | awk '{ print $NF }')
                    mkdir $DATA_HOME/$clean_folder/$clean_subfolder
                    cp $SNAP_ORIG/$orig_host/$clean_token/$clean_folder/$clean_subfolder/snapshots/$RECOVERY/* $DATA_HOME/$clean_folder/$clean_subfolder/
                done
            done
            break
        fi  
    done
fi
