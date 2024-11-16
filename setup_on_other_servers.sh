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
    echo "Copying script $SCRIPT_NAME to $REMOTE_HOST_1..."
    scp "$SCRIPT_NAME" "$REMOTE_USER@$REMOTE_HOST_1:~/$SCRIPT_NAME" ||{
        echo "Error: failed to copy script to $REMOTE_HOST_1."
        exit 1
    }
    echo "Making setup script executable on $REMOTE_HOST_1..."
    ssh "$REMOTE_USER@$REMOTE_HOST_1" "chmod +x ~/$SCRIPT_NAME" ||{
        echo "Error: failed to make script executable on $REMOTE_HOST_1."
        exit 1
    }
    echo "Executing setup script on $REMOTE_HOST_1..."
    ssh "$REMOTE_USER@$REMOTE_HOST_1" "bash ~/$SCRIPT_NAME" ||{
        echo "Error: failed to execute script on $REMOTE_HOST_1."
        exit 1
    }
}


# Optionally, you can remove the script from the remote server after execution
# ssh "$REMOTE_USER@$REMOTE_HOST" "rm $REMOTE_SCRIPT_PATH"