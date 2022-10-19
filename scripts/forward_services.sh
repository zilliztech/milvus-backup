#!/bin/bash
set -e
set -x
instance_name=$1
ns_name=${2:-"chaos-testing"}
array=("${instance_name}-etcd")

array=("etcd" "minio" "pulsar" "rootcoord" "querycoord" "indexcoord" "datacoord")

for pod in ${array[*]}
do
    echo "update $pod host"
    if [ $pod == "etcd" ]; then
        host=$(kubectl get svc/${instance_name}-etcd -n ${ns_name} -o jsonpath='{.spec.clusterIP}')

    elif [ $pod == "minio" ]; then
        host="$(kubectl get svc/${instance_name}-minio -n ${ns_name} -o jsonpath='{.spec.clusterIP}')"

    elif [ $pod == "pulsar" ]; then
        host="$(kubectl get svc/${instance_name}-pulsar-proxy -n ${ns_name} -o jsonpath='{.spec.clusterIP}')"

    else
        if [[ $pod =~ "milvus" ]]; then 
            host="$(kubectl get svc/${instance_name}-$pod -n ${ns_name} -o jsonpath='{.spec.clusterIP}')"
        else
            host="$(kubectl get svc/${instance_name}-milvus-$pod -n ${ns_name} -o jsonpath='{.spec.clusterIP}')"
        fi
        pod=${pod/c/C}
         

    fi
    echo "$pod host: $host"
    pushd ../configs/
    if [ $pod == "etcd" ]; then
        pwd
        echo "update $pod host"
        endpoint="${host}:2379"
        yq -i '.etcd.endpoints[0] = "'${endpoint}'"' milvus.yaml
    else
        yq -i '."'${pod}'".address = "'${host}'"' milvus.yaml
    fi
    popd
    echo "update $pod host done"
done