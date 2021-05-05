package org.apache.iotdb.db.sql.nodes3;

// read and the write statements are on the different nodes.
public class ThreeNodeCluster2IT extends AbstractThreeNodeClusterIT {

  protected String getWriteRpcIp() {
    return getContainer().getServiceHost("iotdb-server_2", 6667);
  }

  protected int getWriteRpcPort() {
    return getContainer().getServicePort("iotdb-server_2", 6667);
  }
}
