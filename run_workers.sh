#! /bin/bash

trap "kill 0" EXIT

./mr_worker localhost:50051 &
./mr_worker localhost:50052 &
./mr_worker localhost:50053 &
./mr_worker localhost:50054 &
./mr_worker localhost:50055 &
./mr_worker localhost:50056 &

wait
