#!/bin/sh -e

# ssh-agent support on MacOS requires some extra work; see
# https://github.com/docker/for-mac/issues/410#issuecomment-536531657
# for the beginning of the solution (note that the thread gets _very_ confused
# and later on starts talking about using SSH_AUTH_SOCK at build-time, which
# is _not_ what we're interested in).

# Ensure access to SSH_AUTH_SOCK, then switch user before executing the
# remainder of the arguments.  Note that on docker-for-mac this will change
# the permissions on the proxied socket (/run/host-services/ssah-auth.sock)
# in the Moby VM that hosts all the docker containers.

if [ -n "$SSH_AUTH_SOCK" ] && [ -S "$SSH_AUTH_SOCK" ]; then
    echo "Ensuring $SSH_AUTH_SOCK is accessible"
    chmod a+rw "$SSH_AUTH_SOCK"
fi

exec gosu "$@"
