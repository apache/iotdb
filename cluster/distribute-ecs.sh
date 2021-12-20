src_lib_path=/e/codestore/incubator-iotdb2/cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/lib/iotdb*

ips=(ecs1 ecs2 ecs3 ecs4 ecs5)
target_lib_path=/root/iotdb_expr/lib

for ip in ${ips[*]}
  do
    ssh root@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path root@$ip:$target_lib_path
  done