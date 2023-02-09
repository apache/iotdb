src_path=/home/jt/iotdb_expr_vg/data/system/*.flow

ips=(dc16)
#ips=(dc11 dc12 dc13 dc14 dc11 dc12)
target_path=/d/CodeRepo/iotdb/cluster/target/flow

rm $target_path/*
for ip in ${ips[*]}
  do
    mkdir $target_path
    scp -r jt@$ip:$src_path $target_path
  done