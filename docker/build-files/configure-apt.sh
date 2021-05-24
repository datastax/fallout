#!/bin/sh -ex

# Make apt quiet, non-interactive and minimalist in its dependency selections

dd of="/etc/apt/apt.conf.d/99-setup-apt" <<EOF
# Suppress dpkg progress messages: https://askubuntu.com/a/668859
Dpkg::Use-Pty "0";
# -qq
quiet "2";
# -y
APT::Get::Assume-yes "true";
# --no-install-recommends
APT::Install-Recommends "0";
# --no-install-suggests
APT::Install-Suggests "0";
EOF

# Make debconf non-interactive

debconf-set-selections << EOF
debconf debconf/frontend select Noninteractive
debconf debconf/priority select critical
EOF
