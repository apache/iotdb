package org.apache.iotdb.db.queryengine.common;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class ConnectionInfo {
  private final int dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private final long userId;
  private final String userName;
  private final long sessionId;
  private final long lastActiveTime;
  private final String clientAddress;

  public ConnectionInfo(
      long userId, String userName, long sessionId, long lastActiveTime, String clientAddress) {
    this.userId = userId;
    this.userName = userName;
    this.sessionId = sessionId;
    this.lastActiveTime = lastActiveTime;
    this.clientAddress = clientAddress;
  }

  public int getDataNodeId() {
    return dataNodeId;
  }

  public long getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public long getSessionId() {
    return sessionId;
  }

  public long getLastActiveTime() {
    return lastActiveTime;
  }

  public String getClientAddress() {
    return clientAddress;
  }
}
