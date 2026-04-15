#!/bin/bash
# CloudLab CREST experiment runner v3
# Run FROM skv-node1 (which has CloudLab SSH key)
#
# Usage: bash cloudlab_exp.sh [rebuild|run|cleanup|status] [workload] [num_cn] [threads] [coro] [duration] [txn_num]
# Example: bash cloudlab_exp.sh run tpcc 1 28 3 10 100000

set -e

ACTION=${1:-run}
WORKLOAD=${2:-tpcc}
NUM_CN=${3:-1}
THREADS=${4:-28}
CORO=${5:-3}
DURATION=${6:-10}
TXN_NUM=${7:-100000}

# CloudLab SSH config
CL_KEY=~/.ssh/id_ed25519_cloudlab
CL_USER=chaomei
CREST_DIR=/users/chaomei/CREST-Opensource-0007
BIN=$CREST_DIR/build/benchmark/bench_runner

# Node assignments: MN=amd112, CN0=amd118, CN1=amd103, CN2=amd107
MN_HOST=amd112.utah.cloudlab.us
CN_HOSTS=(amd118.utah.cloudlab.us amd103.utah.cloudlab.us amd107.utah.cloudlab.us)

ssh_cl() {
    ssh -i $CL_KEY -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o ServerAliveInterval=30 $CL_USER@$1 "$2" 2>/dev/null
}

do_cleanup() {
    echo "=== Cleanup ==="
    for node in $MN_HOST ${CN_HOSTS[@]}; do
        # SIGTERM first for graceful RDMA cleanup, then SIGKILL as fallback
        ssh_cl $node "pkill -TERM bench_runner 2>/dev/null; sleep 2; pkill -9 bench_runner 2>/dev/null; pkill -9 memcached 2>/dev/null; sudo systemctl stop memcached 2>/dev/null" || true
    done
    sleep 1
    echo "  Done"
}

do_status() {
    echo "=== Node Status ==="
    for node in $MN_HOST ${CN_HOSTS[@]}; do
        echo -n "  $node: "
        result=$(ssh_cl $node "hostname; pgrep -c bench_runner 2>/dev/null || echo 0; pgrep -c memcached 2>/dev/null || echo 0")
        if [ -n "$result" ]; then
            echo "UP | $result"
        else
            echo "DOWN"
        fi
    done
}

do_rebuild() {
    echo "=== Rebuilding CREST on all nodes ==="
    for node in $MN_HOST ${CN_HOSTS[@]}; do
        echo "  Building on $node..."
        ssh_cl $node "cd $CREST_DIR && rm -rf build && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. >/dev/null 2>&1 && make -j40 >/dev/null 2>&1 && echo BUILD_OK || echo BUILD_FAILED" &
    done
    wait
    echo "=== Verify binaries ==="
    for node in $MN_HOST ${CN_HOSTS[@]}; do
        echo -n "  $node: "
        ssh_cl $node "test -f $BIN && echo OK || echo MISSING"
    done
}

do_run() {
    local CONFIG=$CREST_DIR/config/cloudlab_${WORKLOAD}_${NUM_CN}cn.json

    echo "============================================"
    echo "  CREST Experiment"
    echo "  Workload: $WORKLOAD | CNs: $NUM_CN"
    echo "  Threads: $THREADS | Coro: $CORO"
    echo "  Duration: ${DURATION}s | TxnNum: $TXN_NUM"
    echo "  Config: $CONFIG"
    echo "============================================"
    echo ""

    # 0. Cleanup
    do_cleanup

    # 1. Start memcached on MN (MUST bind 0.0.0.0, port 11211)
    echo "=== Step 1: Start memcached on MN ==="
    ssh_cl $MN_HOST "sudo systemctl stop memcached 2>/dev/null; pkill -9 memcached 2>/dev/null; sleep 1; memcached -d -p 11211 -u $CL_USER -m 256 -c 1024 -l 0.0.0.0"
    # Verify memcached is listening on 0.0.0.0
    local mc_check=$(ssh_cl $MN_HOST "ss -tlnp 2>/dev/null | grep 11211 | head -1")
    if echo "$mc_check" | grep -q "0.0.0.0:11211"; then
        echo "  OK: memcached listening on 0.0.0.0:11211"
    else
        echo "  WARNING: memcached may not be binding correctly: $mc_check"
        echo "  Attempting fix..."
        ssh_cl $MN_HOST "pkill -9 memcached; sleep 1; /usr/bin/memcached -d -p 11211 -u $CL_USER -m 256 -c 1024 -l 0.0.0.0"
        mc_check=$(ssh_cl $MN_HOST "ss -tlnp 2>/dev/null | grep 11211 | head -1")
        echo "  After fix: $mc_check"
    fi

    # 2. Start MN
    echo ""
    echo "=== Step 2: Start MN on $MN_HOST ==="
    ssh_cl $MN_HOST "cd $CREST_DIR/build/benchmark && nohup ./bench_runner \
        --type=MN --id=0 --config=$CONFIG --workload=$WORKLOAD \
        --threads=$THREADS --coro=$CORO --duration=$DURATION --txn_num=$TXN_NUM \
        > /tmp/mn.log 2>&1 &"

    # Wait for MN ready (poll for "waits for incomming connection")
    echo "  Waiting for MN to initialize..."
    for i in $(seq 1 90); do
        sleep 2
        local ready=$(ssh_cl $MN_HOST "grep -c 'waits for incomming' /tmp/mn.log 2>/dev/null || echo 0")
        if [ "$ready" -gt 0 ] 2>/dev/null; then
            echo "  MN ready after $((i*2))s"
            break
        fi
        # Check for fatal errors
        local err=$(ssh_cl $MN_HOST "grep -iE 'assert|abort|Segmentation|FATAL' /tmp/mn.log 2>/dev/null | head -1")
        if [ -n "$err" ]; then
            echo "  MN FATAL: $err"
            echo "  === Full MN log ==="
            ssh_cl $MN_HOST "cat /tmp/mn.log"
            do_cleanup
            return 1
        fi
        if [ $i -eq 90 ]; then
            echo "  MN TIMEOUT after 180s"
            echo "  === MN log tail ==="
            ssh_cl $MN_HOST "tail -20 /tmp/mn.log"
            do_cleanup
            return 1
        fi
    done

    # 3. Start CNs
    echo ""
    echo "=== Step 3: Start $NUM_CN CN(s) ==="
    for ((i=0; i<NUM_CN; i++)); do
        local cn_host=${CN_HOSTS[$i]}
        echo "  Starting CN$i on $cn_host..."
        ssh_cl $cn_host "cd $CREST_DIR/build/benchmark && nohup ./bench_runner \
            --type=CN --id=$i --config=$CONFIG --workload=$WORKLOAD \
            --threads=$THREADS --coro=$CORO --duration=$DURATION --txn_num=$TXN_NUM \
            > /tmp/cn${i}.log 2>&1 &"
        sleep 2
    done

    # 4. Wait for all CNs to finish
    echo ""
    echo "=== Step 4: Waiting for experiment to complete ==="
    local cn0_host=${CN_HOSTS[0]}
    for i in $(seq 1 90); do
        sleep 10
        local running=$(ssh_cl $cn0_host "pgrep -c bench_runner 2>/dev/null || echo 0")
        if [ "$running" = "0" ] 2>/dev/null; then
            echo "  CN0 finished after $((i*10))s"
            break
        fi
        # Show progress
        local last=$(ssh_cl $cn0_host "tail -1 /tmp/cn0.log 2>/dev/null")
        echo "  [$((i*10))s] $last"
        if [ $i -eq 90 ]; then
            echo "  TIMEOUT after 900s"
        fi
    done
    sleep 5

    # 5. Collect results
    echo ""
    echo "============================================"
    echo "  RESULTS: $WORKLOAD ${NUM_CN}CN t=${THREADS} c=${CORO}"
    echo "============================================"
    echo ""
    echo "--- MN log (tail) ---"
    ssh_cl $MN_HOST "tail -10 /tmp/mn.log"
    for ((i=0; i<NUM_CN; i++)); do
        echo ""
        echo "--- CN$i log ---"
        ssh_cl ${CN_HOSTS[$i]} "cat /tmp/cn${i}.log"
    done

    # 6. Cleanup (SIGTERM for graceful RDMA release)
    echo ""
    do_cleanup
}

case $ACTION in
    rebuild)  do_rebuild ;;
    run)      do_run ;;
    cleanup)  do_cleanup ;;
    status)   do_status ;;
    *)        echo "Usage: $0 [rebuild|run|cleanup|status] [workload] [num_cn] [threads] [coro] [duration] [txn_num]" ;;
esac
