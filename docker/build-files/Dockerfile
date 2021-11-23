# syntax=docker/dockerfile:1

# To build a docker image for fallout, use './gradlew dockerBuild` in the
# project root directory.  This Dockerfile won't do anything useful with a
# direct `docker build` invocation.

FROM openjdk:17-buster

# Make apt and debconf be quiet
RUN --mount=type=bind,source=build-files,target=build-files \
    build-files/configure-apt.sh

RUN apt-get update
# install apt-utils to make debian be quiet about it not being there in later
# installs
RUN apt-get install apt-utils
RUN apt-get upgrade

# Tools to support ENTRYPOINT and CMD
RUN apt-get install wait-for-it gosu

# Additional packages needed to run python-based tools
RUN apt-get install \
    virtualenv \
    python3.7 python3-pip python3.7-venv

# Add the github key to known_hosts so we can clone repos as root
RUN mkdir -m=0700 ~/.ssh
RUN touch ~/.ssh/known_hosts
RUN chmod 0600 ~/.ssh/known_hosts

# Taken from https://help.github.com/en/github/authenticating-to-github/testing-your-ssh-connection
ARG github_host_key_fingerprint="SHA256:nThbg6kXUpJWGl7E1IGOCspRomTxdCARLviKw6E5SY8"

# Mostly copied from https://serverfault.com/a/971922/41102
RUN if [ "$(ssh-keyscan -H -t rsa github.com 2>/dev/null | \
            tee -a ~/.ssh/known_hosts | \
            ssh-keygen -lf - | \
            cut -d' ' -f 2)" != "${github_host_key_fingerprint}" ]; then \
        echo "Bad github host key" 1>&2; \
        exit 1; \
    fi

ARG uid=1000
ARG gid=1000
ARG user_home="/home/fallout"
RUN groupadd --gid ${gid} fallout && \
    useradd --create-home fallout --uid ${uid} --gid ${gid}

# Make sure the fallout user also has the github key
RUN cp -a ~/.ssh ~fallout/.ssh && \
    chown fallout:fallout ~fallout/.ssh

ENV FALLOUT_HOME="${user_home}"

WORKDIR ${FALLOUT_HOME}

USER fallout

# Ensure ~/.local/bin is in the path; some OS's don't do this
ENV PATH="${user_home}/.local/bin:${PATH}"

# Install python support
RUN --mount=type=cache,target=${user_home}/.cache/pip,uid=${uid},gid=${gid} \
    --mount=type=bind,source=pip-conf,target=${user_home}/.config/pip \
    --mount=type=bind,source=build-files,target=build-files \
    python3 -m pip install --user --upgrade pip && \
    python3 -m pip install --user poetry

COPY --chown=fallout:fallout image-files .

RUN mkdir -p tests run

RUN --mount=type=cache,target=${user_home}/.cache/pip,uid=${uid},gid=${gid} \
    --mount=type=cache,target=${user_home}/.cache/pypoetry,uid=${uid},gid=${gid} \
    --mount=type=bind,source=pip-conf,target=${user_home}/.config/pip \
    "${FALLOUT_HOME}/lib/tools/support/install-python-tools" run

# Switch to root so that gosu-with-access-to-ssh-auth-sock can do the
# things it needs to do; it will switch back to fallout to run commands
USER root

# Cassandra
ENV FALLOUT_CASSANDRA_HOST="cassandra"
ENV FALLOUT_CASSANDRA_PORT="9042"
ENV PATH="${FALLOUT_HOME}/bin:${PATH}"

ENTRYPOINT ["./gosu-with-access-to-ssh-auth-sock.sh", "fallout"]

CMD wait-for-it \
  --host=${FALLOUT_CASSANDRA_HOST} \
  --port=${FALLOUT_CASSANDRA_PORT} \
  --strict --timeout=600 -- \
  bin/fallout standalone
