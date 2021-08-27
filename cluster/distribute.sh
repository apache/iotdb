src_lib_path=/e/codestore/incubator-iotdb2/cluster/target/iotdb-cluster-0.13.0-SNAPSHOT/lib/*

ips=(fit36 fit38 fit39)
target_lib_path=/data/iotdb_expr/lib

for ip in ${ips[*]}
  do
    ssh fit@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path fit@$ip:$target_lib_path
  done

ips=(fit31 fit33 fit34)
target_lib_path=/disk/iotdb_expr/lib

for ip in ${ips[*]}
  do
    ssh fit@$ip "mkdir $target_lib_path"
    scp -r $src_lib_path fit@$ip:$target_lib_path
  done