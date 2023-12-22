package org.apache.iotdb.session.mq;

import org.apache.iotdb.tsfile.write.record.Tablet;

public class TabletWrapper {
  private final long commitId;
  private final IoTDBWebSocketClient websocketClient;
  private final Tablet tablet;

  public TabletWrapper(long commitId, IoTDBWebSocketClient websocketClient, Tablet tablet) {
    this.commitId = commitId;
    this.websocketClient = websocketClient;
    this.tablet = tablet;
  }

  public long getCommitId() {
    return commitId;
  }

  public IoTDBWebSocketClient getWebSocketClient() {
    return websocketClient;
  }

  public Tablet getTablet() {
    return tablet;
  }
}
