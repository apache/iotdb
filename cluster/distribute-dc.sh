src_lib_path=/e/codestore/incubator-iotdb2/cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/lib/iotdb*

ips=(dc11 dc12 dc13 dc14 dc15 dc16 dc17 dc18)
target_lib_path=/home/jt/iotdb_expr/lib

for ip in ${ips[*]}
  do
    ssh jt@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path jt@$ip:$target_lib_path
  done