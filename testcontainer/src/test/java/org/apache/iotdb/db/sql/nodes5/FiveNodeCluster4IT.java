package org.apache.iotdb.db.sql.nodes5;

// read and the write statements are on the different nodes, and maybe in the different raft groups.
public class FiveNodeCluster4IT extends AbstractFiveNodeClusterIT {

  protected String getWriteRpcIp() {
    return getContainer().getServiceHost("iotdb-server_4", 6667);
  }
}
