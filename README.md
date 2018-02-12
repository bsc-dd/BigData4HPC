# cassandra4slurm

Note: These scripts are also available for LSF scheduler based clusters under petition (eloy.gil[at]bsc[dot]es)

### Usage: 
. ./launcher.sh [ **-h** | **RUN** [ **-s** ] [ **N** ] | **RECOVER** [ **-s** ] | **STATUS** | **KILL** ]

IMPORTANT: The leading dot is needed for Hecuba (https://github.com/bsc-dd/hecuba) since this launcher sets some environment variables.

- `bash launcher.sh -h`	
	Prints this usage help.

- `bash launcher.sh RUN`
	Starts new a Cassandra Cluster. Starts `N` nodes, if given. Default is 4.
	Using the optional parameter `-s` it will save a snapshot after the execution.

- `bash launcher.sh RECOVER`
	Shows a list of snapshots from previous Cassandra Clusters and restores the chosen one.
	Using the optional parameter `-s` it will save a snapshot after the execution.

- `bash launcher.sh STATUS`
	Gets the status of the Cassandra Cluster.

- `bash launcher.sh KILL`
	If a Cassandra Cluster is running, it is killed, aborting the process.
