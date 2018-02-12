#!/bin/bash
###############################################################################################################
#                                                                                                             #
#                                     Cassandra Cluster Launcher for Slurm                                    #
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
ACTION=${1}
OPTION=${2}
SNAPSH=${3}

export JOBNAME="cassandr"
export CASS_IFACE="-ib0"
CFG_FILE=$HOME/cassandra4slurm/conf/cassandra4slurm.cfg
CASS_HOME=$(cat $CFG_FILE | grep "CASS_HOME=" | sed 's/CASS_HOME=//g' | sed 's/"//g')
HOST_LIST=/tmp/cassandra-host-list-$(whoami).txt
N_NODES_FILE=cassandra-num-nodes.txt
SNAPSHOT_FILE=cassandra-snapshot-file.txt
RECOVER_FILE=cassandra-recover-file.txt
RETRY_MAX=12

function usage () {
    # Prints a help message
    echo "Usage: . ./launcher.sh [ -h | RUN [ -s ] [ N ] | RECOVER [ -s ] | STATUS | KILL ]"
    echo "       ^ "
    echo "IMPORTANT: The leading dot is mandatory since this launcher sets some environment variables."
    echo " "
    echo "       -h:"
    echo "       Prints this usage help."
    echo " "
    echo "       RUN:"
    echo "       Starts new a Cassandra Cluster. Starts N nodes, if given. Default is 4."
    echo "       Using the optional parameter -s it will save a snapshot after the execution."
    echo " "
    echo "       RECOVER:"
    echo "       Shows a list of snapshots from previous Cassandra Clusters and restores the chosen one."
    echo "       Using the optional parameter -s it will save a snapshot after the execution."
    echo " "
    echo "       STATUS:"
    echo "       Gets the status of the Cassandra Cluster."
    echo " "
    echo "       KILL:"
    echo "       If a Cassandra Cluster is running, it is killed, aborting the process."
    echo " "
}

function get_job_info () {
    # Gets the ID of the job that runs the Cassandra Cluster
    JOB_INFO=$(squeue | grep $JOBNAME) 
    JOB_ID=$(echo $JOB_INFO | awk '{ print $1 }')
    JOB_STATUS=$(echo $JOB_INFO | awk '{ print $5 }')   
}

function get_cluster_node () {
    # Gets the ID of the first node
    NODE_ID=$(head -n 1 $HOME/cassandra4slurm/hostlist-$(squeue | grep $JOBNAME | awk '{ print $1 }').txt)
}

function get_cluster_ips () {
    # Gets the IP of every node in the cluster
    NODE_IPS=$(ssh $NODE_ID "$CASS_HOME/bin/nodetool -h $NODE_ID$CASS_IFACE status" | awk '/Address/{p=1;next}{if(p){print $2}}')
}

function exit_no_cluster () {
    # Any Cassandra cluster is running. Exit.
    echo "There is not a Cassandra cluster running. Exiting..."
    exit
}

function exit_bad_node_status () {
    # Exit after getting a bad node status. 
    echo "Cassandra Cluster Status: ERROR"
    echo "One or more nodes are not up (yet?) - It was expected to find ""$(cat $N_NODES_FILE)"" UP nodes."
    echo "Exiting..."
    exit
}

function test_if_cluster_up () {
    # Checks if other Cassandra Cluster is running, aborting if it is happening
    if [ "$(squeue | grep $JOBNAME)" != "" ] 
    then
        echo "Another Cassandra Cluster is running and could collide with a new execution. Aborting..."
        squeue
        exit
    fi
}

function get_nodes_up () {
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
        if [ "$JOB_STATUS" == "R" ]
        then    
            get_cluster_node 
            NODE_STATE_LIST=`ssh -q $NODE_ID "$CASS_HOME/bin/nodetool -h $NODE_ID$CASS_IFACE status" | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
            if [ "$NODE_STATE_LIST" != "" ]
            then
                NODE_COUNTER=0
                for state in $NODE_STATE_LIST
                do  
                    if [ $state != "UN" ]
                    then
                        RETRY_COUNTER=$(($RETRY_COUNTER+1))
                        break
                    else
                        NODE_COUNTER=$(($NODE_COUNTER+1))
                    fi
                done
            fi
        fi
    fi
}

function set_snapshot_value () {
    # Writes snapshot option into file
    if [ "$SNAPSH" == "-s" ]
    then
        echo "1" > $SNAPSHOT_FILE
    else
        echo "0" > $SNAPSHOT_FILE
    fi
    echo "$N_NODES" > $N_NODES_FILE
}

if [ "$OPTION" == "-s" ] || [ "$OPTION" == "-S" ]
then
    # Swap just in case the "-s" parameter (for snapshot) comes in the second parameter
    OPTION=$SNAPSH
    SNAPSH="-s"
fi

if [ "$ACTION" == "RUN" ] || [ "$ACTION" == "run" ]
then
    test_if_cluster_up
    # Starts a (default) Cassandra Cluster
    echo "Starting Cassandra Cluster..."
    if [ "$OPTION" != "" ]
    then
        N_NODES=$OPTION
    else
        N_NODES=4 # Default if not given
    fi
    echo $N_NODES > $N_NODES_FILE

    # Since this is a fresh launch, it assures that the recover file is empty
    echo "" > $RECOVER_FILE

    # Enables/Disables the snapshot option after the execution
    set_snapshot_value

    sbatch < cass.sh --ntasks=$N_NODES --ntasks-per-node=1 --exclusive #--nodelist=s12r2b68
    echo "Please, be patient. It may take a while until it shows a correct STATUS (and it may show some harmless errors during this process)."
    RETRY_COUNTER=0
    sleep 15
    while [ "$NODE_COUNTER" != "$N_NODES" ] && [ $RETRY_COUNTER -lt $RETRY_MAX ]; do
        echo "Checking..."
        sleep 10
	get_nodes_up
	#echo "NODE_COUNTER: $NODE_COUNTER | N_NODES: $N_NODES | RETRY_COUNTER: $RETRY_COUNTER" #debug only
    done
    if [ "$NODE_COUNTER" == "$N_NODES" ]
    then
	while [ ! -f "$HOME/cassandra4slurm/hostlist-$(squeue | grep $JOBNAME | awk '{ print $1 }').txt" ]; do
            sleep 3
	done
	sleep 3
        echo "Cassandra Cluster with "$N_NODES" nodes started successfully."
	CNAMES=$(sed ':a;N;$!ba;s/\n/,/g' $HOME/cassandra4slurm/hostlist-$(squeue | grep $JOBNAME | awk '{ print $1 }').txt)$CASS_IFACE
	CNAMES=$(echo $CNAMES | sed "s/,/$CASS_IFACE,/g")
	export CONTACT_NAMES=$CNAMES
	echo $CNAMES | tr , '\n' > $HOME/bla.txt # Set list of nodes (with interface) in PyCOMPSs file
	echo "Contact names environment variable (CONTACT_NAMES) should be set to: $CNAMES"
    else
        echo "ERROR: Cassandra Cluster RUN timeout. Check STATUS."
    fi 
elif [ "$ACTION" == "STATUS" ] || [ "$ACTION" == "status" ]
then
    # If there is a running Cassandra Cluster it prints the information of the nodes
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
    	if [ "$JOB_STATUS" == "PEND" ]
        then
            echo "The job is still pending. Wait for a while and try again."
            exit
        fi 
        get_cluster_node 
        NODE_STATE_LIST=`ssh $NODE_ID "$CASS_HOME/bin/nodetool -h $NODE_ID$CASS_IFACE status" | sed 1,5d | sed '$ d' | awk '{ print $1 }'`
	if [ "$NODE_STATE_LIST" == "" ]
	then
            echo "ERROR: No status found. The Cassandra Cluster may be still bootstrapping. Try again later."
            exit
        fi
        NODE_COUNTER=0
        for state in $NODE_STATE_LIST
        do
            if [ $state != "UN" ]
            then
                echo "E1"
                exit_bad_node_status
            else
                NODE_COUNTER=$(($NODE_COUNTER+1))
            fi
       	done
        if [ "$(cat $N_NODES_FILE)" == "$NODE_COUNTER" ]
        then
            echo "Cassandra Cluster Status: OK"
       	    ssh $NODE_ID "$CASS_HOME/bin/nodetool -h $NODE_ID$CASS_IFACE status"
        else
            echo "E2"
            echo "N_NODES_FILE: "$(cat $N_NODES_FILE)
            echo "NODE_COUNTER: "$NODE_COUNTER
            exit_bad_node_status
        fi
    else
        exit_no_cluster
    fi
elif [ "$ACTION" == "RECOVER" ] || [ "$ACTION" == "recover" ]
then
    test_if_cluster_up
    # Launches a new Cluster to recover a previous snapshot
    SNAP_LIST=""
    for node in $(ls $CASS_HOME/snapshots)
    do 
        for snap in $(ls $CASS_HOME/snapshots/$node/*-ring.txt)
        do
            snap_name=`echo $snap | sed 's+/+ +g' | awk '{ print $NF }' | rev | cut -c 10- | rev`
            if [ "$(echo $SNAP_LIST | grep $snap_name)" == "" ]
            then
                SNAP_LIST="$SNAP_LIST $snap_name"
            fi 
        done
    done
    if [ "$SNAP_LIST" == "" ]
    then
        echo "There are no available snapshots to restore."
        echo "Exiting..."
        exit
    else
        echo "The following snapshots are available to be restored:"
    fi
    for snap in $SNAP_LIST
    do
        echo -e $snap
    done
    echo "Introduce a snapshot to restore: "
    read input_snap
    if [ "$(echo $SNAP_LIST | grep $input_snap)" == "" ]
    then
        echo "ERROR: Wrong snapshot input. Exiting..."
        exit
    fi
    TKNFILE_LIST=$(find $CASS_HOME/snapshots/ -type f -name $input_snap-ring.txt)
    N_NODES=0
    for token_file in $TKNFILE_LIST
    do
        ((N_NODES++))
    done

    # Set snapshot name to recover into file
    echo $input_snap > $RECOVER_FILE

    # Enables/Disables the snapshot option after the execution
    set_snapshot_value

    sbatch < cass.sh --ntasks=$N_NODES --ntasks-per-node=1 --exclusive
    echo "Launching $N_NODES nodes to recover snapshot $input_snap"
    sleep 15
    echo "Launch still in progress. You can check it later with:"
    echo "    bash launcher.sh STATUS"
elif [ "$ACTION" == "KILL" ] || [ "$ACTION" == "kill" ]
then
    # If there is a running Cassandra Cluster it kills it
    get_job_info
    if [ "$JOB_ID" != "" ]
    then
        scancel $JOB_ID
        echo "It will take a while to complete the shutdown..." 
        sleep 5
        echo "Done."
    else
        exit_no_cluster
    fi
elif [ "$ACTION" == "-H" ] || [ "$ACTION" == "-h" ]
then
    # Shows the help information
    usage
    exit
else
    # There may be an error with the arguments used, also prints the help
    echo "Input argument error. Only an ACTION must be specified."
    usage
    echo "Exiting..."
    exit
fi
