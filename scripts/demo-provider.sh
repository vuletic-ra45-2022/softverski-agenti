#!/usr/bin/env bash
#
# Demo: federated learning in PROVIDER mode (star topology).
# Starts one coordinator and N trainer nodes as separate processes, runs the
# federation, then prints the evaluation log and the saved model file.
#
# Usage:
#   scripts/demo-provider.sh [ROUNDS] [TRAINERS] [EPOCHS] [/path/to/paysim.csv]
#
# Without a CSV path, nodes fall back to synthetic data (fast; the metrics are
# meaningless, but the message flow and aggregation are fully exercised).
set -euo pipefail

ROUNDS="${1:-3}"
TRAINERS="${2:-3}"
EPOCHS="${3:-2}"
DATA="${4:-}"

BASE_PORT=9500
OUT="demo-out/provider"

cd "$(dirname "$0")/.."
echo "==> Building..."
cabal build exe:fl-actors >/dev/null
BIN="$(cabal list-bin fl-actors)"

mkdir -p "$OUT"
rm -f "$OUT"/*.log fl_model_coordinator.bin

DATA_ARGS=()
[ -n "$DATA" ] && DATA_ARGS=(--data "$DATA")

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "==> Starting coordinator on port $BASE_PORT (rounds=$ROUNDS, trainers=$TRAINERS, epochs=$EPOCHS)"
"$BIN" --mode provider --role coordinator \
  --port "$BASE_PORT" --rounds "$ROUNDS" --trainers "$TRAINERS" --epochs "$EPOCHS" \
  "${DATA_ARGS[@]}" > "$OUT/coordinator.log" 2>&1 &
COORD=$!
PIDS+=("$COORD")
sleep 1

for i in $(seq 0 $((TRAINERS - 1))); do
  echo "==> Starting trainer-$i on port $((BASE_PORT + i + 1))"
  "$BIN" --mode provider --role trainer --id "$i" \
    --base-port "$BASE_PORT" --coord-host localhost --coord-port "$BASE_PORT" \
    --rounds "$ROUNDS" --trainers "$TRAINERS" --epochs "$EPOCHS" \
    "${DATA_ARGS[@]}" > "$OUT/trainer$i.log" 2>&1 &
  PIDS+=($!)
done

echo "==> Federation running; waiting for the coordinator to finish..."
echo "    (live log: tail -f $OUT/coordinator.log)"
if ! wait "$COORD"; then
  echo "!!  Coordinator exited abnormally; last log lines:"
  tail -20 "$OUT/coordinator.log"
  exit 1
fi

echo
echo "==> Evaluation results:"
grep "Eval round=" "$OUT/coordinator.log" || true
echo
if [ -f fl_model_coordinator.bin ]; then
  echo "==> Success: model saved to fl_model_coordinator.bin ($(stat -c %s fl_model_coordinator.bin) bytes)"
  echo "    Full logs in $OUT/"
else
  echo "!!  No model file was produced; check $OUT/ logs"
  exit 1
fi
