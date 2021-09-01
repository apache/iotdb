package org.apache.iotdb.cluster.client;

public enum ClientCategory {
  META("MetaClient"),
  META_HEARTBEAT("MetaHeartbeatClient"),
  DATA("DataClient"),
  DATA_HEARTBEAT("DataHeartbeatClient"),
  SINGLE_MASTER("SingleMasterClient");

  private String name;

  ClientCategory(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
