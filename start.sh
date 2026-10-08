#!/usr/bin/env bash
# Build and ensure the systemd service is installed, enabled and running.
set -Eeuo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

if [[ $(uname -s) != Linux ]]; then
    echo 'This script requires Linux with systemd.' >&2
    exit 1
fi
for command in go systemctl install cmp mktemp; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "Required command not found: $command" >&2
        exit 1
    fi
done
if [[ ! -d /run/systemd/system ]]; then
    echo 'systemd is not running on this system.' >&2
    exit 1
fi

sudo_cmd=()
if (( EUID != 0 )); then
    if ! command -v sudo >/dev/null 2>&1; then
        echo 'Installing the service requires root or sudo.' >&2
        exit 1
    fi
    sudo_cmd=(sudo)
    sudo -v
fi

service=docker-aggregator.service
binary=/usr/local/bin/docker-aggregator
unit=/etc/systemd/system/$service
mkdir -p bin
build=$(mktemp ./bin/.docker-aggregator.XXXXXX)
staged=''
cleanup() {
    rm -f -- "$build"
    if [[ -n $staged ]]; then
        "${sudo_cmd[@]}" rm -f -- "$staged"
    fi
}
trap cleanup EXIT

echo 'Building docker-aggregator...'
# Build for the current Linux host, even if GOOS/GOARCH were set for cross-building.
GOOS=linux GOARCH="$(go env GOHOSTARCH)" go build -o "$build" ./src
chmod 0755 "$build"
mv -f -- "$build" bin/docker-aggregator

changed=false
if ! "${sudo_cmd[@]}" cmp -s bin/docker-aggregator "$binary"; then
    "${sudo_cmd[@]}" install -d /usr/local/bin
    staged=$("${sudo_cmd[@]}" mktemp /usr/local/bin/.docker-aggregator.XXXXXX)
    "${sudo_cmd[@]}" install -m 0755 bin/docker-aggregator "$staged"
    "${sudo_cmd[@]}" mv -f -- "$staged" "$binary"
    staged=''
    changed=true
fi
if ! "${sudo_cmd[@]}" cmp -s "$service" "$unit"; then
    staged=$("${sudo_cmd[@]}" mktemp /etc/systemd/system/.docker-aggregator.XXXXXX)
    "${sudo_cmd[@]}" install -m 0644 "$service" "$staged"
    "${sudo_cmd[@]}" mv -f -- "$staged" "$unit"
    staged=''
    changed=true
fi

"${sudo_cmd[@]}" systemctl daemon-reload
"${sudo_cmd[@]}" systemctl enable "$service"
"${sudo_cmd[@]}" systemctl reset-failed "$service"
if [[ $changed == true ]]; then
    "${sudo_cmd[@]}" systemctl restart "$service"
else
    "${sudo_cmd[@]}" systemctl start "$service"
fi

# Type=simple may briefly be active even if initialization fails. Observe a stable
# PID for a few seconds to catch startup failures and automatic restart loops.
pid=''
for ((i=0; i<5; i++)); do
    sleep 1
    current=$(systemctl show "$service" --property=MainPID --value)
    if ! systemctl is-active --quiet "$service" || [[ $current == 0 || -z $current ]] ||
        [[ -n $pid && $pid != "$current" ]]; then
        echo 'Service failed to remain running. Recent logs:' >&2
        "${sudo_cmd[@]}" journalctl -u "$service" -n 40 --no-pager >&2 || true
        exit 1
    fi
    pid=$current
done

echo "Service is enabled and running (PID $pid)."
echo "Logs: sudo journalctl -u $service -f"
