#!/bin/bash
#need first arg to be the username
if [ -z "$1" ]; then
    echo "Error: username not provided."
    exit 1
fi

# Define the remote server and user
REMOTE_USER="$1"
REMOTE_HOST_1="svm-11.cs.helsinki.fi"
REMOTE_HOST_2="svm-11-2.cs.helsinki.fi"
REMOTE_HOST_3="svm-11-3.cs.helsinki.fi"
SCRIPT_NAME="setup_script.sh"

#assert called from path
REPO_NAME="distributed_systems"
DIR_PATH=$(pwd)
if [ "$DIR_PATH" != "$HOME/$REPO_NAME" ]; then
    echo "Error: script should be called from $HOME/$REPO_NAME."
    exit 1
fi


# Copy the local script to the remote server
#$HOME in ssh is interpreted correctly as the remote user's home directory
setup_script() {
    remote_host=$1
    echo -e "\nRunning setup for $remote_host...\n"
    echo "Copying script $SCRIPT_NAME to $remote_host..."
    scp "$SCRIPT_NAME" "$REMOTE_USER@$remote_host:~/$SCRIPT_NAME" ||{
        echo "Error: failed to copy script to $remote_host."
        exit 1
    }
    echo "Making setup script executable on $remote_host..."
    ssh "$REMOTE_USER@$remote_host" "chmod +x ~/$SCRIPT_NAME" ||{
        echo "Error: failed to make script executable on $remote_host."
        exit 1
    }
    echo "Executing setup script on $remote_host..."
    ssh "$REMOTE_USER@$remote_host" "bash ~/$SCRIPT_NAME" ||{
        echo "Error: failed to execute script on $remote_host."
        exit 1
    }
    echo -e "\nSetup complete for $remote_host.\n"
}
setup_script $REMOTE_HOST_2
get_ip() {
    remote_host=$1
    OUT=$(ssh "$REMOTE_USER@$remote_host" python3 ~/remote_exec/get_ip.py)
    RETVAL=$?
    if [ $RETVAL -ne 0 ]; then
        echo "Error: failed to get IP for $remote_host."
        exit 1
    fi
    echo "$OUT"
}
get_ports() {
    remote_host=$1
    num_ports=$2
    OUT=$(ssh "$REMOTE_USER@$remote_host" python3 ~/remote_exec/get_free_ports.py $num_ports)
    RETVAL=$?
    if [ $RETVAL -ne 0 ]; then
        echo "Error: failed to get ports for $remote_host."
        exit 1
    fi
    echo "$OUT"
}
NUM_PORTS=3
# PORTS=$(ssh "$REMOTE_USER@$REMOTE_HOST_2" python3 ~/remote_exec/get_free_ports.py $NUM_PORTS)
IP=$(get_ip $REMOTE_HOST_2)
PORTS=$(get_ports $REMOTE_HOST_2 $NUM_PORTS)
echo "IP for $REMOTE_HOST_2: $IP"
echo "Ports for $REMOTE_HOST_2:"
i=0
for port in $PORTS; do
    echo "Port $i: $port"
    i=$((i+1))
done


# Optionally, you can remove the script from the remote server after execution
# ssh "$REMOTE_USER@$REMOTE_HOST" "rm $REMOTE_SCRIPT_PATH"