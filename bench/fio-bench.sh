#!/usr/bin/env sh
set -eu

MOUNTPOINT="${MOUNTPOINT:-/mnt/arustio}"

# arustio fuse --server http://arustio:50052 --mountpoint $MOUNTPOINT
arustio \
  fs --server http://arustio:50052 \
  mount /data/ s3a://test-bucket \
  -o access_key_id=minioadmin \
  -o secret_access_key=minioadmin \
  -o endpoint=http://minio:9000

arustio \
  fs --server http://arustio:50052 \
  set-conf /data writetype=NO_CACHE



WORKDIR="${WORKDIR:-$MOUNTPOINT/data}"
SIZE="${SIZE:-128M}"
NUMJOBS="${NUMJOBS:-4}"
IODEPTH="${IODEPTH:-16}"
RUNTIME="${RUNTIME:-60}"
BS="${BS:-4k}"
IOENGINE="${IOENGINE:-libaio}"
DIRECT="${DIRECT:-0}"

if [ ! -d "$MOUNTPOINT" ]; then
  echo "mountpoint not found: $MOUNTPOINT" >&2
  exit 1
fi

mkdir -p "$WORKDIR"

run_job() {
  name="$1"
  rw="$2"
  fio --name="$name" \
    --directory="$WORKDIR" \
    --rw="$rw" \
    --bs="$BS" \
    --size="$SIZE" \
    --numjobs="$NUMJOBS" \
    --iodepth="$IODEPTH" \
    --runtime="$RUNTIME" \
    --time_based=1 \
    --ioengine="$IOENGINE" \
    --direct="$DIRECT" \
    --group_reporting=1
}

echo "FIO on $MOUNTPOINT"
echo "  size=$SIZE bs=$BS numjobs=$NUMJOBS iodepth=$IODEPTH runtime=${RUNTIME}s ioengine=$IOENGINE direct=$DIRECT"

run_job seq_write write
run_job seq_read read
run_job rand_write randwrite
run_job rand_read randread
run_job rand_rw randrw

echo "done"
