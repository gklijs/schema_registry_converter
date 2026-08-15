#!/usr/bin/env bash

for i in $(seq 1 40); do
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
  sleep 3
done
exit 1