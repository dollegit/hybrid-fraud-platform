#!/bin/bash

# This script installs all prerequisites for the project on a Debian-based Linux system (like Ubuntu).
# It should be run with sudo privileges.

# First, ensure the script is run with sudo privileges.
if [ "$EUID" -ne 0 ]; then
  echo "Please run this script with sudo: sudo ./install_prerequisites.sh"
  exit
fi

# Then, check if the calling user is already in the docker group.
if groups "$SUDO_USER" | grep -q '\bdocker\b'; then
    echo "User $SUDO_USER is already in the docker group."
else
    echo "Adding current user ($SUDO_USER) to the docker group..."
    usermod -aG docker "$SUDO_USER"
fi
# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Cleaning up old repository configurations (if any) ---"
rm -f /etc/apt/sources.list.d/docker.list
rm -f /etc/apt/sources.list.d/kubernetes.list
rm -f /etc/apt/sources.list.d/helm-stable-debian.list

echo "--- 1. Installing Docker ---"
apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io
echo "Adding current user to the docker group..."
usermod -aG docker $SUDO_USER

echo "--- 2. Installing Minikube ---"
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64

echo "--- 3. Installing kubectl and Helm ---"
apt-get install -y apt-transport-https

echo "Downloading Kubernetes signing key..."
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key | gpg --dearmor --yes -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' | tee /etc/apt/sources.list.d/kubernetes.list

# Update apt after adding the Kubernetes repository
apt-get update
apt-get install -y kubectl

echo "Installing Helm using alternative script method..."
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
rm -f get_helm.sh

echo ""
echo "--- Prerequisite installation complete! ---"
echo "IMPORTANT: Please log out and log back in, or run 'newgrp docker' in your terminal for the Docker group changes to take effect."
