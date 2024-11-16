#!/bin/bash
# Ensure called from distributed_systems folder
REPO_NAME="distributed_systems"


REPO_URL="https://github.com/AntonBogun/distributed_systems.git"
REPO_DIR="$HOME/$REPO_NAME"
# Navigate to the repository directory
mkdir -p "$REPO_DIR" || {
    echo "Error: failed to create repository directory: $REPO_DIR."
    exit 1
}
cd "$REPO_DIR" || {
    echo "Error: failed to navigate to repository directory: $REPO_DIR."
    exit 1
}

# Ensure repository is cloned
if [ ! -d ".git" ]; then
    echo "Cloning repository from $REPO_URL..."
    git clone "$REPO_URL" || {
        echo "Error: failed to clone repository."
        exit 1
    }
fi


#stash changes if any
echo "Stashing any local changes..."
git stash push -m "Auto-stash before updating to latest upstream master" || {
    echo "Error: failed to stash local changes."
    exit 1
}

# Ensure repository is at the latest upstream master
echo "Fetching latest changes from upstream..."
git fetch origin || {
    echo "Error: failed to fetch latest changes."
    exit 1
}

echo "Resetting local master to match upstream master..."
git reset --hard origin/master || {
    echo "Error: failed to reset to upstream master."
    exit 1
}

echo "Repository is now at the latest upstream master."