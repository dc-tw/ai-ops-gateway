#!/bin/sh
set -eu

# Standalone disposable PostgreSQL 17 restore-point PITR. This verifies WAL
# archive/recovery semantics separately from the application's pg_dump test.
suffix="$$"
prefix="ai-ops-m9-pitr-${suffix}"
network="${prefix}"
primary="${prefix}-primary"
restore="${prefix}-restore"
primary_volume="${prefix}-primary"
base_volume="${prefix}-base"
restore_volume="${prefix}-restore"
wal_volume="${prefix}-wal"
image="postgres:17-alpine"

cleanup() {
  docker rm -f "${primary}" "${restore}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  docker volume rm "${primary_volume}" "${base_volume}" \
    "${restore_volume}" "${wal_volume}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

wait_ready() {
  container="$1"
  for _ in $(seq 1 60); do
    if docker exec "${container}" pg_isready -U gateway -d gateway >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs "${container}" >&2 || true
  return 1
}

docker network create "${network}" >/dev/null
docker volume create "${primary_volume}" >/dev/null
docker volume create "${base_volume}" >/dev/null
docker volume create "${restore_volume}" >/dev/null
docker volume create "${wal_volume}" >/dev/null
docker run --rm -u root -v "${wal_volume}:/archive" "${image}" \
  sh -ec 'chown postgres:postgres /archive'

docker run -d --name "${primary}" --network "${network}" \
  -e POSTGRES_USER=gateway -e POSTGRES_PASSWORD=m9-pitr-only -e POSTGRES_DB=gateway \
  -v "${primary_volume}:/var/lib/postgresql/data" \
  -v "${base_volume}:/base" -v "${wal_volume}:/archive" \
  "${image}" \
  -c wal_level=replica -c archive_mode=on \
  -c "archive_command=test ! -f /archive/%f && cp %p /archive/%f" \
  -c archive_timeout=1s >/dev/null
wait_ready "${primary}"
docker exec "${primary}" psql -X -v ON_ERROR_STOP=1 -U gateway -d gateway -c \
  "CREATE TABLE m9_pitr(id integer PRIMARY KEY, value text NOT NULL); INSERT INTO m9_pitr VALUES (1, 'base');" >/dev/null
docker exec -u root "${primary}" sh -ec \
  'mkdir -p /base/data && chown postgres:postgres /base/data'
docker exec -u postgres "${primary}" pg_basebackup \
  -U gateway -D /base/data -Fp -Xs -P >/dev/null

docker exec "${primary}" psql -X -v ON_ERROR_STOP=1 -U gateway -d gateway -c \
  "INSERT INTO m9_pitr VALUES (2, 'before-target')" >/dev/null
target_wal="$(docker exec "${primary}" psql -X -q -A -t -v ON_ERROR_STOP=1 \
  -U gateway -d gateway -c \
  "SELECT pg_walfile_name(pg_create_restore_point('m9_verified_target'))")"
test -n "${target_wal}"
docker exec "${primary}" psql -X -v ON_ERROR_STOP=1 -U gateway -d gateway <<'SQL' >/dev/null
SELECT pg_switch_wal();
INSERT INTO m9_pitr VALUES (3, 'after-target-must-not-return');
SELECT pg_switch_wal();
CHECKPOINT;
SQL

# archive_timeout is one second; require the exact segment containing the target.
for _ in $(seq 1 30); do
  wal_count="$(docker exec "${primary}" sh -ec "find /archive -type f | wc -l")"
  if docker exec "${primary}" test -f "/archive/${target_wal}"; then break; fi
  sleep 1
done
docker exec "${primary}" test -f "/archive/${target_wal}"
docker stop "${primary}" >/dev/null

docker run --rm -u root -v "${base_volume}:/from" -v "${restore_volume}:/to" \
  "${image}" sh -ec '
    cp -a /from/data/. /to/
    touch /to/recovery.signal
    printf "%s\n" \
      "restore_command = '\''cp /archive/%f %p'\''" \
      "recovery_target_name = '\''m9_verified_target'\''" \
      "recovery_target_action = '\''promote'\''" >> /to/postgresql.auto.conf
    chown -R postgres:postgres /to
  '

docker run -d --name "${restore}" --network "${network}" \
  -e POSTGRES_PASSWORD=m9-pitr-only \
  -v "${restore_volume}:/var/lib/postgresql/data" -v "${wal_volume}:/archive:ro" \
  "${image}" >/dev/null
wait_ready "${restore}"

rows="$(docker exec "${restore}" psql -X -q -A -t -v ON_ERROR_STOP=1 \
  -U gateway -d gateway -c "SELECT id || ':' || value FROM m9_pitr ORDER BY id")"
expected="$(printf '1:base\n2:before-target')"
test "${rows}" = "${expected}"
printf '{"postgres":"17-alpine","recovery_target":"m9_verified_target","target_wal":"%s","rows":["1:base","2:before-target"],"excluded_after_target":true,"archived_wal_segments":%s}\n' \
  "${target_wal}" "${wal_count}"
echo "PostgreSQL restore-point PITR passed"
