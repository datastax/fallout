# shellcheck shell=bash

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
    # shellcheck disable=SC2064
    trap "_fail '$* failed'; info Override checks using --no-verify; exit 1" 0
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

function check_or_skip()
{
    local check="$1"
    shift

    if ((check)); then
        checking "$@"
    else
        skipping "$@"
    fi

    ((check))
}

function ok()
{
    echo "$(tput cuu1)"$'\xE2\x9C\x85'
}

function _fail()
{
    echo "$(tput bold)"$'\xE2\x9B\x94\xEF\xB8\x8F'"  $*$(tput sgr0)" 1>&2
}

function fail()
{
    _fail "$@"
    exit 1
}

function info()
{
    echo $'\xE2\x84\xB9\xEF\xB8\x8F'"  $*" 1>&2
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

function check_git_secrets()
{
    if check_if_installed git-secrets; then
        git secrets "$@"
        ok
    elif [[ -z "$ALLOW_SKIPPED_SECRETS_CHECK" ]]; then
        fail "git-secrets (https://github.com/awslabs/git-secrets) must be " \
            "installed; to bypass this check prefix the git command with " \
            "ALLOW_SKIPPED_SECRETS_CHECK=1"
    fi
}

# GNU xargs requires --no-run-if-empty to disable always running at least once;
# macOS does not.  See https://unix.stackexchange.com/a/61823
if xargs --no-run-if-empty < /dev/null > /dev/null 2>&1; then
    function _xargs()
    {
        xargs --no-run-if-empty "$@"
    }
else
    function _xargs()
    {
        xargs "$@"
    }
fi

function all_checks_passed()
{
    trap "" 0
}

# shellcheck disable=SC2034
# Allow including files to override this
if [ -z "$LINT_TRIGGERS" ]; then
    LINT_TRIGGERS="$(echo {src,*.gradle{,.kts}} \
        {src,*.gradle{,.kts}} \
        build-logic/*/{src,*.gradle{,.kts}})"
fi
