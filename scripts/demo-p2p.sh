#!/usr/bin/env bash
#
# Demo: federated learning in P2P mode (full mesh, no central coordinator).
# Starts N peer nodes as separate processes; each trains locally, gossips
# updates via PeerSync (CRDT-gated rounds), and aggregates independently.
# At the end every peer must hold the SAME model — the script verifies the
# saved model files are byte-identical.
#
# Usage:
#   scripts/demo-p2p.sh [ROUNDS] [PEERS] [EPOCHS] [/path/to/paysim.csv]
#
# Without a CSV path, nodes fall back to synthetic data (fast; the metrics are
# meaningless, but the gossip protocol and aggregation are fully exercised).
set -euo pipefail

ROUNDS="${1:-3}"
PEERS="${2:-3}"
EPOCHS="${3:-2}"
DATA="${4:-}"

BASE_PORT=9600
OUT="demo-out/p2p"

cd "$(dirname "$0")/.."
echo "==> Building..."
cabal build exe:fl-actors >/dev/null
BIN="$(cabal list-bin fl-actors)"

mkdir -p "$OUT"
rm -f "$OUT"/*.log fl_model_peer-*.bin

DATA_ARGS=()
[ -n "$DATA" ] && DATA_ARGS=(--data "$DATA")

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

for i in $(seq 0 $((PEERS - 1))); do
  PORT=$((BASE_PORT + i))
  ARGS=(--mode p2p --role peer --id "$i" --port "$PORT"
        --rounds "$ROUNDS" --trainers "$PEERS" --epochs "$EPOCHS")
  for j in $(seq 0 $((PEERS - 1))); do
    [ "$j" = "$i" ] && continue
    ARGS+=(--peer "localhost:$((BASE_PORT + j))")
  done
  echo "==> Starting peer-$i on port $PORT"
  "$BIN" "${ARGS[@]}" "${DATA_ARGS[@]}" > "$OUT/peer$i.log" 2>&1 &
  PIDS+=($!)
done

echo "==> Federation running; waiting for all $PEERS peers to finish..."
echo "    (live log: tail -f $OUT/peer0.log)"
FAIL=0
for pid in "${PIDS[@]}"; do
  wait "$pid" || FAIL=1
done
PIDS=()
if [ "$FAIL" -ne 0 ]; then
  echo "!!  A peer exited abnormally; check $OUT/ logs"
  exit 1
fi

echo
echo "==> Evaluation results (peer-0's view):"
grep "Eval round=" "$OUT/peer0.log" || true
echo

MODELS=(fl_model_peer-*.bin)
if [ "${#MODELS[@]}" -ne "$PEERS" ]; then
  echo "!!  Expected $PEERS model files, found ${#MODELS[@]}; check $OUT/ logs"
  exit 1
fi

FIRST="${MODELS[0]}"
for m in "${MODELS[@]:1}"; do
  if ! cmp -s "$FIRST" "$m"; then
    echo "!!  Divergence: $m differs from $FIRST"
    md5sum "${MODELS[@]}"
    exit 1
  fi
done
echo "==> Success: all $PEERS peers converged to a byte-identical model:"
ls -la "${MODELS[@]}"
echo "    Full logs in $OUT/"
