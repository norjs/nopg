#!/usr/bin/env bash
self="$(node -e 'console.log(require("fs").realpathSync(process.argv[1]))' "$0")"
self_dir="$(dirname "$self")"
node="$(command -v node || command -v nodejs)"

if test "x$node" = x; then
    echo "ERROR: Could not find node or nodejs command" >&2
    exit 1
fi

exec "$node" "$self_dir/../dist/bin/$(basename "$self" ".sh").js" "$@"
