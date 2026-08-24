#!/usr/bin/env bash

# 100 * 3s = 5 minutes. Bumped from 40 (2 minutes) when the broker/schema-registry moved to
# Confluent Platform 8.3.1 in KRaft (combined broker+controller) mode -- first boot has to format
# the KRaft log dir and start Confluent Balancer/Metrics Reporter on top of that, which can take
# noticeably longer than the old zookeeper-backed setup, especially on a shared CI runner.
for i in $(seq 1 100); do
  running=$(docker inspect -f '{{ .State.Running }}' test-app)
  exit_code=$(docker inspect -f '{{ .State.ExitCode }}' test-app)
  # A freshly started container also reports ExitCode 0 (its unset default)
  # while still Running, so we must wait for it to have actually stopped
  # before trusting the exit code - otherwise this check is a no-op that
  # always "succeeds" on the very first loop.
  if [[ "$running" == "false" && "$exit_code" == "0" ]]; then
    echo -e "Successful load java data in loop ${i}"
    exit 0
  fi
  # Fail fast on a genuine crash rather than waiting out the full timeout -- a non-zero exit
  # here never turns into a 0 on a later loop -- and dump the logs while they're still around.
  if [[ "$running" == "false" && "$exit_code" != "0" ]]; then
    echo "test-app exited with code ${exit_code} after loop ${i}, logs:"
    docker logs test-app
    exit 1
  fi
  sleep 3
done
echo "test-app still running after $((100 * 3))s, giving up. Logs from test-app/schema-registry/broker:"
docker logs test-app
docker logs schema-registry
docker logs broker
exit 1
