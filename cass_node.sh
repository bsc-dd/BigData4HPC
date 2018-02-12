#!/bin/bash
###############################################################################################################
#													      #
#                                  Cassandra Node Launcher for Marenostrum IV                                 #
#                                          Eloy Gil - eloy.gil@bsc.es                                         #
#													      #
#                                     Barcelona Supercomputing Center 2018                                    #
#		                                     .-.--_                                       	      #
#                    			           ,´,´.´   `.                                     	      #
#              			                   | | | BSC |                                     	      #
#                   			           `.`.`. _ .´                                     	      #
#                        		             `·`··                                         	      #
#													      #
###############################################################################################################
echo "JAVA_HOME="$JAVA_HOME
echo "Cassandra node $(hostname) is starting now..."

echo "CASS_HOME="$CASS_HOME
$CASS_HOME/bin/cassandra -f

echo "Cassandra has stopped in node $(hostname)"

