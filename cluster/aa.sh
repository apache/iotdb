chmod -R 777 ./target/
nohup ./target/cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./target/test-classes/node1conf/ >node1.log 2>&1 &
nohup ./target/cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./target/test-classes/node2conf/ >node2.log 2>&1 &
nohup ./target/cluster-0.12.0-SNAPSHOT/sbin/start-node.sh ./target/test-classes/node3conf/ >node3.log 2>&1 &

