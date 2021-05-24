#!/usr/bin/env bash

if [ -f /etc/redhat-release ]; then
  yum install openssh-server
fi

if [ -f /etc/lsb-release ]; then
  apt-get update
  apt-get install openssh-server -y
fi

mkdir /var/run/sshd
adduser --disabled-password --gecos "" dockeruser

echo "docker\ndocker\n" | passwd dockeruser

printf "dockeruser ALL=NOPASSWD: ALL\n" >> /etc/sudoers

su dockeruser -c "mkdir -m 700 -p /home/dockeruser/.ssh"
su dockeruser -c "umask 077; touch /home/dockeruser/.ssh/authorized_keys"

/usr/sbin/sshd

echo "sshd started!"
