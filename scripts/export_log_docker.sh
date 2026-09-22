#!/bin/bash

# Exit immediately for non zero status
set -e

log_dir=${1:-"logs"}
array=($(docker compose ps -a|awk 'NR == 1 {next} {print $1}'))
echo ${array[@]}
if [ ! -d $log_dir ];
then
    mkdir -p $log_dir
fi
echo "export logs start"
docker compose ps -a > "$log_dir/compose_ps.txt" 2>&1 || true
for container in ${array[*]}
do
if [[ $container == milvus-* ]];
then
    echo "export logs for container $container "
    docker logs $container > $log_dir/$container.log 2>&1 || echo "export logs for container $container failed"
    # A SIGKILLed process leaves no stack in its log; the exit code and OOM
    # flag only live in container state, so capture them into the artifact.
    docker inspect --format 'status={{.State.Status}} exitcode={{.State.ExitCode}} oomkilled={{.State.OOMKilled}} finishedat={{.State.FinishedAt}}' "$container" 2>&1 | tee "$log_dir/$container.state" || echo "inspect container $container failed"
fi
done
echo "export logs done"
