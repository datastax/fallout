# Set PATH to include likely locations of external tools

# Sourcetree sets TERM, but it's not exported
export TERM
if [[ -z $TERM ]]; then
    export TERM=dumb
fi

if ((SHLVL == 1)); then
    # We're executing outside a terminal environment => on macOS, various env
    # vars won't be setup correctly, including PATH, so we set them up here.
    # We don't use
    #
    #   eval "$(bash --login -c './git-hooks/print-env' 2>/dev/null)"
    #
    # because there's a possibility of users printing
    # things to stdout in their shell startup files; to protect against that
    # we send print-env output to a temporary file and source it.
    env_file="$(mktemp /tmp/fallout.git-hooks.env_file.XXXXXX)"
    bash --login -c "./git-hooks/print-env > $env_file" >/dev/null 2>&1
    # shellcheck disable=SC1090
    source "$env_file"
    rm -f "$env_file"
fi

function stage()
{
    echo "$(tput smul; tput bold)$*$(tput rmul; tput sgr0)"
}

function checking()
{
    echo "$(tput bold)"$'\xE2\x96\xB6\xEF\xB8\x8F'"  $*$(tput sgr0)"
}

function skipping()
{
    echo "$(tput bold)"$'\xE2\x8F\xA9'"  Skipping $*$(tput sgr0)"
}

function ok()
{
    echo "$(tput cuu1)"$'\xE2\x9C\x85'
}

function check_if_installed()
{
    local name="$1"

    local failed=0
    local not_installed=""

    while [[ -n "$1" ]]; do
        if ! type "$1" >/dev/null 2>&1; then
            not_installed="${not_installed}$1 "
            failed=1
        fi
        shift
    done

    if ((failed)); then
        skipping "$name ($not_installed not installed)"
    else
        checking "$name"
    fi

    return $failed
}

# GNU xargs requires --no-run-if-empty to disable always running at least once;
# macOS does not.  See https://unix.stackexchange.com/a/61823
function _xargs()
{
    if xargs --no-run-if-empty < /dev/null > /dev/null 2>&1; then
        xargs --no-run-if-empty "$@"
    else
        xargs "$@"
    fi
}
