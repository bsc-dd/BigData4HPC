#!/bin/bash
#SBATCH --job-name cassandr 
#SBATCH --time 06:00:00
#SBATCH -o log-cass-%j.out
#SBATCH -e log-cass-%j.err
#TO BE REMOVED#SBATCH --qos=debug
###############################################################################################################
#                                                                                                             #
#                                    Cassandra4Slurm Job for Marenostrum IV                                   #
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

iface="ib0" # will be deprecated soon, now getting it from env. var. CASS_IFACE
iface=$(echo "$CASS_IFACE" | sed 's/-//g')

CFG_FILE=$HOME/cassandra4slurm/conf/cassandra4slurm.cfg
NODEFILE=$HOME/cassandra4slurm/hostlist-$SLURM_JOBID.txt
srun --ntasks-per-node=1 /bin/hostname >> $NODEFILE
sleep 5
export CASS_HOME=$(cat $CFG_FILE | grep "CASS_HOME=" | sed 's/CASS_HOME=//g' | sed 's/"//g')
THETIME=$(date "+%Y%m%dD%H%Mh%Ss")
export DATA_PATH=$(cat $CFG_FILE | grep "DATA_PATH=" | sed 's/DATA_PATH=//g' | sed 's/"//g') 
ROOT_PATH=$DATA_PATH/$THETIME
DATA_HOME=$ROOT_PATH/c4j/cassandra-data
COMM_HOME=$ROOT_PATH/c4j/cassandra-commitlog
N_NODES=$(cat $NODEFILE | wc -l)
SNAPSHOT_FILE=cassandra-snapshot-file.txt
RECOVER_FILE=cassandra-recover-file.txt
RETRY_MAX=50
TEST_BASE_FILENAME=SD_NN3_RL3S_100M_WR_SN1

function exit_killjob () {
    # Traditional harakiri
    scancel $SLURM_JOBID
}

function exit_bad_node_status () {
    # Exit after getting a bad node status. 
    echo "Cassandra Cluster Status: ERROR"
    echo "It was expected to find $N_NODES nodes UP nodes, found "$NODE_COUNTER"."
    echo "Exiting..."
    exit_killjob
}

function get_nodes_up () {
    NODE_STATE_LIST=`$CASS_HOME/bin/nodetool status | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
    if [ "$NODE_STATE_LIST" != "" ]
    then
        NODE_COUNTER=0
        for state in $NODE_STATE_LIST
        do  
            if [ $state == "UN" ]
            then
                ((NODE_COUNTER++))
            fi  
        done
    fi 
}

if [ ! -f $CASS_HOME/bin/cassandra ]; then
    echo "ERROR: Cassandra executable is not placed where it was expected. ($CASS_HOME/bin/cassandra)"
    echo "Exiting..."
    exit
fi

if [ ! -f $SNAPSHOT_FILE ]; then
    echo "ERROR: The file that sets the snapshot/not snapshot option is not placed where it was expected ($SNAPSHOT_FILE)"
    echo "Exiting..."
    exit
fi

if [ "$(cat $RECOVER_FILE)" != "" ]; then
    RECOVERING=$(cat $RECOVER_FILE)
    echo "INFO: Recovering snapshot: $RECOVERING"
fi

echo "STARTING UP CASSANDRA..."
echo "I am $(hostname)."
# JAVA module load in Marenostrum 4:
#srun --ntasks=$SLURM_NNODES module load java/8u131
export REPLICA_FACTOR=2

sleep 10

hostlist=`cat $NODEFILE`
seeds=`echo $hostlist | sed "s/ /-$iface,/g"`
seeds=$seeds-$iface #using only infiniband atm, will change later
sed "s/.*seeds:.*/          - seeds: \"$seeds\"/" $CASS_HOME/conf/cassandra-cfg.yaml | sed "s/.*rpc_interface:.*/rpc_interface: $iface/" | sed "s/.*listen_interface:.*/listen_interface: $iface/" | sed "s/.*listen_address:.*/#listen_address: localhost/" | sed "s/.*rpc_address:.*/#rpc_address: localhost/" > $CASS_HOME/conf/cassandra-aux.yaml
sed "s/.*initial_token:.*/#initial_token:/" $CASS_HOME/conf/cassandra-aux.yaml > $CASS_HOME/conf/cassandra-aux2.yaml
mv $CASS_HOME/conf/cassandra-aux2.yaml $CASS_HOME/conf/cassandra-cfg.yaml


TIME_START=`date +"%T.%3N"`
echo "Launching in the following hosts: $hostlist"

# Setting symlink to future symlink to each cassandra.yaml config file per node
if [ -L $CASS_HOME/conf/cassandra.yaml ]; then
    rm -f $CASS_HOME/conf/cassandra.yaml
fi
ln -s $ROOT_PATH/cassandra.yaml $CASS_HOME/conf/cassandra.yaml

# Clearing data from previous executions and checking symlink coherence
srun --ntasks-per-node=1 $HOME/cassandra4slurm/tmp-set.sh $CASS_HOME $DATA_HOME $COMM_HOME $ROOT_PATH
sleep 5

if [ "$(cat $RECOVER_FILE)" != "" ]
then
    RECOVERTIME1=`date +"%T.%3N"`
    # Moving data to each datapath
    srun --ntasks-per-node=1 $HOME/cassandra4slurm/smart-recover.sh $ROOT_PATH
    RECOVERTIME2=`date +"%T.%3N"`

    echo "[STATS] Recover process initial datetime: $RECOVERTIME1"
    echo "[STATS] Recover process final datetime: $RECOVERTIME2"

    MILL1=$(echo $RECOVERTIME1 | cut -c 10-12)
    MILL2=$(echo $RECOVERTIME2 | cut -c 10-12)
    TIMESEC1=$(date -d "$RECOVERTIME1" +%s)
    TIMESEC2=$(date -d "$RECOVERTIME2" +%s)
    TIMESEC=$(( TIMESEC2 - TIMESEC1 ))
    MILL=$(( MILL2 - MILL1 ))

    # Adjusting seconds if necessary
    if [ $MILL -lt 0 ]
    then
        MILL=$(( 1000 + MILL ))
        TIMESEC=$(( TIMESEC - 1 ))
    fi

    echo "[STATS] Cluster recover process (copy files and set tokens for all nodes) took: "$TIMESEC"s. "$MILL"ms."    
fi       

# Launching Cassandra in every node
srun --ntasks-per-node=1 cass_node.sh &
sleep 5

# Checking cluster status until all nodes are UP (or timeout)
echo "Waiting 20 seconds until all Cassandra nodes are launched..."
sleep 20
echo "Checking..."
RETRY_COUNTER=0
get_nodes_up
while [ "$NODE_COUNTER" != "$N_NODES" ] && [ $RETRY_COUNTER -lt $RETRY_MAX ]; do
    echo "Retry #$RETRY_COUNTER"
    echo "Checking..."
    sleep 5
    get_nodes_up
    ((RETRY_COUNTER++))
done
if [ "$NODE_COUNTER" == "$N_NODES" ]
then
    TIME_END=`date +"%T.%3N"`
    echo "Cassandra Cluster with "$N_NODES" nodes started successfully."
    MILL1=$(echo $TIME_START | cut -c 10-12)
    MILL2=$(echo $TIME_END | cut -c 10-12)
    TIMESEC1=$(date -d "$TIME_START" +%s)
    TIMESEC2=$(date -d "$TIME_END" +%s)
    TIMESEC=$(( TIMESEC2 - TIMESEC1 ))
    MILL=$(( MILL2 - MILL1 ))

    # Adjusting seconds if necessary
    if [ $MILL -lt 0 ]
    then
        MILL=$(( 1000 + MILL ))
        TIMESEC=$(( TIMESEC - 1 ))
    fi

    echo "[STATS] Cluster launching process took: "$TIMESEC"s. "$MILL"ms."
else
    echo "[STATS] ERROR: Cassandra Cluster RUN timeout. Check STATUS."
    exit_bad_node_status
fi

# THIS IS THE APPLICATION CODE EXECUTING SOME TASKS USING CASSANDRA DATA, ETC
echo "CHECKING CASSANDRA STATUS: "
$CASS_HOME/bin/nodetool status

sleep 12
firstnode=$(echo $hostlist | awk '{ print $1 }')
CNAMES=$(sed ':a;N;$!ba;s/\n/,/g' $HOME/cassandra4slurm/hostlist-$(squeue | grep $JOBNAME | awk '{ print $1 }').txt)$CASS_IFACE
CNAMES=$(echo $CNAMES | sed "s/,/$CASS_IFACE,/g")
export CONTACT_NAMES=$CNAMES
echo "CONTACT_NAMES="$CONTACT_NAMES

#TO BE UNCOMMENTED#$HOME/wordcount_yolanda/./compss_launcher.sh # Launches PyCOMPSs wordcount

#N_OP=10000000
#N_OP=100000000
#while [ "$IT_COUNTER" -lt "$N_TESTS" ]; do
#    if [ "$IT_COUNTER" == "0" ] || [ "$(tail -n 1 stress/$TEST_FILENAME)" == "END" ]; then
#        ((IT_COUNTER++))
#        TEST_FILENAME="$TEST_BASE_FILENAME"_"$IT_COUNTER".log
#        #../cassandra/tools/bin/cassandra-stress write n=$N_OP -schema replication\(factor=3\) -node $firstnode-$iface -log file=stress/$TEST_FILENAME  
#        ../cassandra/tools/bin/cassandra-stress write n=$N_OP -schema replication\(strategy=SimpleStrategy, factor=3\) -node $firstnode-$iface -log file=stress/$TEST_FILENAME  
#        #../cassandra/tools/bin/cassandra-stress write n=$N_OP -schema replication\(strategy=SimpleStrategy, factor=3\) -node $firstnode-$iface  
#    fi
#    sleep 10
#done
# END OF THE APPLICATION EXECUTION CODE

# Wait for a couple of minutes to assure that the data is stored
while [ "$(cat $HOME/stop.txt)" != "1" ]; do
    echo "Sleeping until ~/stop.txt has value \"1\"."
    sleep 5
done
#sleep 72000

# Don't continue until the status is stable
while [ "$NDT_STATUS" != "$($CASS_HOME/bin/nodetool status)" ]
do
    NDT_STATUS=$($CASS_HOME/bin/nodetool status)
    #sleep 60
    sleep 20
done

# If an snapshot was ordered, it is done
if [ "$(cat $SNAPSHOT_FILE)" == "1" ]
then 
    TIME1=`date +"%T.%3N"`
    SNAP_NAME=$THETIME
    # Looping over the assigned hosts until the snapshots are confirmed
    srun --ntasks-per-node=1 bash snapshot.sh $SNAP_NAME $ROOT_PATH

    SNAP_CONT=0
    while [ "$SNAP_CONT" != "$N_NODES" ]
    do
        SNAP_CONT=0
        for u_host in $hostlist
        do
            if [ -f snap-status-$SNAP_NAME-$u_host-file.txt ]
            then
                SNAP_CONT=$(($SNAP_CONT+1))
            fi
        done
    done
    
    TIME2=`date +"%T.%3N"`

    echo "[STATS] Snapshot initial datetime: $TIME1"
    echo "[STATS] Snapshot final datetime: $TIME2" 

    MILL1=$(echo $TIME1 | cut -c 10-12)
    MILL2=$(echo $TIME2 | cut -c 10-12)
    TIMESEC1=$(date -d "$TIME1" +%s)
    TIMESEC2=$(date -d "$TIME2" +%s)
    TIMESEC=$(( TIMESEC2 - TIMESEC1 ))
    MILL=$(( MILL2 - MILL1 ))

    # Adjusting seconds if necessary
    if [ $MILL -lt 0 ]
    then
        MILL=$(( 1000 + MILL ))
        TIMESEC=$(( TIMESEC - 1 ))
    fi

    echo "[STATS] Snapshot process took: "$TIMESEC"s. "$MILL"ms."
    #echo "Snapshot process took: "$TIMESEC"s. "$MILL"ms." > stress/"$TEST_BASE_FILENAME"_0.log

    # Cleaning status files
    rm snap-status-$$-*-file.txt
fi
sleep 10
srun --ntasks-per-node=1 bash killer.sh


# Kills the job to shutdown every cassandra service
exit_killjob
