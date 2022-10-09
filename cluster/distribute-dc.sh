src_lib_path=/d/CodeRepo/iotdb/cluster/target/iotdb-cluster-0.14.0-SNAPSHOT/lib/iotdb*

ips=(dc15 dc16 dc17 dc18)
#ips=(dc11 dc12 dc13 dc14 dc11 dc12)
target_lib_path=/home/jt/iotdb_expr_vg/lib

for ip in ${ips[*]}
  do
    ssh jt@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path jt@$ip:$target_lib_path
  done