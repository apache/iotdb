src_path=/home/jt/iotdb_expr_vg/logs/*

ips=(dc16 dc17 dc18)
#ips=(dc11 dc12 dc13 dc14 dc11 dc12)
target_path=/d/CodeRepo/iotdb/cluster/target/logs

mkdir $target_path
rm $target_path/*
for ip in ${ips[*]}
  do
    mkdir $target_path/$ip
    scp -r jt@$ip:$src_path $target_path/$ip
  done