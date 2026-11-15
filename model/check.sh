#!/bin/sh
set -eu

model_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
check_dir=$(mktemp -d "${TMPDIR:-/tmp}/puddle-model.XXXXXXXX")
trap 'rm -rf "$check_dir"' EXIT HUP INT TERM

# Keep TLC's state graph and generated counterexample files out of the tree.
cp "$model_dir/Pool.tla" "$model_dir/Pool.cfg" "$check_dir/"
cd "$check_dir"

tlc_java=${TLC_JAVA:-java}
if ! command -v "$tlc_java" >/dev/null 2>&1; then
    echo "Java 11 or newer is required; set TLC_JAVA to its java executable." >&2
    exit 1
fi

if [ -n "${TLA2TOOLS_JAR:-}" ]; then
    # Supply an absolute path for offline runs.
    jar=$TLA2TOOLS_JAR
else
    jar="$check_dir/tla2tools.jar"
    curl --fail --location --silent --show-error --retry 3 \
        --output "$jar" \
        https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
fi
# Pin the checker as well as the model. Updating TLC requires updating this hash.
printf '%s  %s\n' \
    936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88 \
    "$jar" | sha256sum --check --status

check() {
    "$tlc_java" -XX:+UseParallelGC -Xmx1g -jar "$jar" \
        -workers 1 -config "$1" Pool.tla
}

echo "Checking capacity 1 (safety and Close liveness)..."
check Pool.cfg
sed 's/Capacity = 1/Capacity = 2/' Pool.cfg > PoolTwo.cfg
echo "Checking capacity 2 (safety and Close liveness)..."
check PoolTwo.cfg

reject_mutation() {
    bug=$1
    invariant=$2
    sed "s/Bug = \"none\"/Bug = \"$bug\"/" Pool.cfg > Mutant.cfg
    if check Mutant.cfg > "$bug.log" 2>&1; then
        cat "$bug.log"
        echo "ERROR: model accepted mutation $bug" >&2
        exit 1
    fi
    # A parser error, timeout, or unrelated invariant failure is not a pass.
    if ! grep -F "Invariant $invariant is violated" "$bug.log"; then
        cat "$bug.log"
        echo "ERROR: mutation $bug did not fail the expected invariant" >&2
        exit 1
    fi
    echo "Rejected mutation: $bug"
}

reject_mutation publish-invalid IdleValid
reject_mutation leak-failure-permit PermitAccounting
