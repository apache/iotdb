package org.apache.iotdb.db.sql.nodes5;

// read and the write statements are on the different nodes, and maybe in the same raft group.
public class FiveNodeCluster2IT extends AbstractFiveNodeClusterIT {

  protected String getWriteRpcIp() {
    return getContainer().getServiceHost("iotdb-server_2", 6667);
  }
}
