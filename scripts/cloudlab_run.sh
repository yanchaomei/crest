#!/bin/bash
# CloudLab CREST experiment runner
# Usage: ./cloudlab_run.sh <workload> <threads> <coros> <num_cn> [txn_num] [duration]

set -e

# CloudLab nodes (via skv-node1 jump)
SKV_KEY="/Users/yanchaomei/Downloads/ssh/id_rsa"
CL_KEY="~/.ssh/id_ed25519_cloudlab"
SKV_HOST="kvgroup@222.195.68.87"
SKV_PORT=6666

MN_HOST="amd107.utah.cloudlab.us"  # 128.110.219.18
CN_HOSTS=("amd103.utah.cloudlab.us" "amd118.utah.cloudlab.us" "amd112.utah.cloudlab.us")
# CN IPs: 128.110.219.14, 128.110.219.29, 128.110.219.23

CREST_DIR="/users/chaomei/CREST-Opensource-0007"
BENCH_BIN="$CREST_DIR/build/benchmark/bench_runner"

# Parameters
WORKLOAD=${1:-tpcc}
THREADS=${2:-28}
COROS=${3:-8}
NUM_CN=${4:-1}
TXN_NUM=${5:-1000000}
DURATION=${6:-10}

CONFIG="$CREST_DIR/config/cloudlab_${WORKLOAD}_config.json"

ssh_cmd() {
    local host=$1
    shift
    ssh -i $SKV_KEY -p $SKV_PORT $SKV_HOST \
        "ssh -i $CL_KEY -o StrictHostKeyChecking=no chaomei@$host '$@'" 2>&1 | grep -v WARNING | grep -v vulnerable | grep -v upgraded
}

echo "=== CREST CloudLab Experiment ==="
echo "Workload: $WORKLOAD, Threads: $THREADS, Coros: $COROS, CNs: $NUM_CN"
echo "TxnNum: $TXN_NUM, Duration: $DURATION"
echo ""

# Step 1: Kill any existing processes
echo "[1/4] Cleaning up old processes..."
ssh_cmd $MN_HOST "pkill -9 bench_runner 2>/dev/null; pkill -9 memcached 2>/dev/null; sleep 1; echo done"
for ((i=0; i<NUM_CN; i++)); do
    ssh_cmd ${CN_HOSTS[$i]} "pkill -9 bench_runner 2>/dev/null; echo done" &
done
wait

# Step 2: Start memcached on MN
echo "[2/4] Starting memcached on MN..."
ssh_cmd $MN_HOST "memcached -d -p 11211 -u chaomei -m 256 -c 1024; sleep 1; echo memcached started"

# Step 3: Start MN
echo "[3/4] Starting MN (id=0)..."
ssh_cmd $MN_HOST "cd $CREST_DIR && nohup $BENCH_BIN \
    --type=mn --id=0 --config=$CONFIG --workload=$WORKLOAD \
    --threads=$THREADS --coro=$COROS --duration=$DURATION \
    > /tmp/mn_output.log 2>&1 &
echo 'MN started, waiting for population...'
# Wait for MN to be ready (check for 'waits for' in log)
for i in \$(seq 1 120); do
    if grep -q 'waits for' /tmp/mn_output.log 2>/dev/null; then
        echo 'MN ready after '\$i's'
        break
    fi
    sleep 1
done
tail -5 /tmp/mn_output.log"

# Step 4: Start CNs
echo "[4/4] Starting $NUM_CN CN(s)..."
for ((i=0; i<NUM_CN; i++)); do
    echo "  Starting CN$i on ${CN_HOSTS[$i]}..."
    ssh_cmd ${CN_HOSTS[$i]} "cd $CREST_DIR && $BENCH_BIN \
        --type=cn --id=$i --config=$CONFIG --workload=$WORKLOAD \
        --threads=$THREADS --coro=$COROS --txn_num=$TXN_NUM --duration=$DURATION \
        --output=/tmp/cn${i}_output 2>&1 | tee /tmp/cn${i}_log.txt | tail -30" &
done
wait

echo ""
echo "=== Experiment Done ==="
echo "Collecting results..."
for ((i=0; i<NUM_CN; i++)); do
    echo "--- CN$i (${CN_HOSTS[$i]}) ---"
    ssh_cmd ${CN_HOSTS[$i]} "cat /tmp/cn${i}_output* 2>/dev/null; echo; tail -20 /tmp/cn${i}_log.txt 2>/dev/null"
done

# Cleanup
echo ""
echo "Cleaning up..."
ssh_cmd $MN_HOST "pkill -9 bench_runner 2>/dev/null; pkill -9 memcached 2>/dev/null; echo done"
for ((i=0; i<NUM_CN; i++)); do
    ssh_cmd ${CN_HOSTS[$i]} "pkill -9 bench_runner 2>/dev/null; echo done" &
done
wait
echo "=== All done ==="
