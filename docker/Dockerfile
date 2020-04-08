# To build a docker image for fallout, use './gradlew dockerTagLatest` in the
# project root directory.  This Dockerfile won't do anything useful with a
# direct `docker build` invocation.

FROM openjdk:11

# Taken from https://help.github.com/en/github/authenticating-to-github/testing-your-ssh-connection
ARG github_host_key_fingerprint="SHA256:nThbg6kXUpJWGl7E1IGOCspRomTxdCARLviKw6E5SY8"

# Paths
ENV FALLOUT_HOME /home/fallout
ENV PATH "${FALLOUT_HOME}/bin:${PATH}"

# Cassandra
ENV FALLOUT_CASSANDRA_HOST "cassandra"
ENV FALLOUT_CASSANDRA_PORT "9042"

# Install wait-for-it, so we can wait for cassandra to start up, and wget/curl
# so we can get tools

# _Not_ an ENV, because we don't want to export this to the running container,
# just to the build commands
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get upgrade -y
# install apt-utils to make debian be quiet about it not being there in later
# installs
RUN apt-get install -y apt-utils
RUN apt-get install -y wait-for-it gosu

RUN useradd --create-home fallout
WORKDIR ${FALLOUT_HOME}

# Everything above will have been created as root; fallout should not change
# any of it so this is the right thing to do.  Only now do we switch to the
# fallout user to create the tests dir which fallout _does_ need to write to
# (and this also means that if we mount a named volume on it, the mount point
# will have the correct owner).
USER fallout
RUN mkdir -p tests

# Add the github key to known_hosts so we can clone repos
RUN mkdir -m=0700 .ssh
RUN touch .ssh/known_hosts
RUN chmod 0600 .ssh/known_hosts
# Mostly copied from https://serverfault.com/a/971922/41102
RUN if [ "$(ssh-keyscan -H -t rsa github.com 2>/dev/null | tee -a .ssh/known_hosts | ssh-keygen -lf - | cut -d' ' -f 2)" != "${github_host_key_fingerprint}" ]; then echo "Bad github host key" 1>&2; exit 1; fi

# Do the copy of the fallout distribution _last_: that way we make the most
# of docker's inbuilt build caching, and don't have to keep downloading
# the external-tool artifacts.
COPY . .

# Switch to root so that entrypoint can do the things it needs to do; it will
# switch back to fallout to run CMD
USER root
ENTRYPOINT ["./entrypoint.sh"]

CMD wait-for-it \
  --host=${FALLOUT_CASSANDRA_HOST} \
  --port=${FALLOUT_CASSANDRA_PORT} \
  --strict --timeout=600 -- \
  bin/fallout standalone
