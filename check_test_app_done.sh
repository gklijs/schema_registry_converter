#!/usr/bin/env bash

for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  failures=$(docker inspect -f '{{ .State.ExitCode }}' test-app)
  if [[ "$failures" == "0" ]]; then
    echo -e "Successful load java data in loop ${i}"
    exit 0
  fi
  sleep 3
done
exit 1