package org.apache.iotdb.tool.core.model;

/**
 * ChunkGroupInfo
 *
 * @author shenguanchu
 */
public class ChunkGroupInfo {
  private String deviceName;
  private long offset;

  public ChunkGroupInfo() {}

  public ChunkGroupInfo(String deviceName, long offset) {
    this.deviceName = deviceName;
    this.offset = offset;
  }

  public String getDeviceName() {
    return deviceName;
  }

  public void setDeviceName(String deviceName) {
    this.deviceName = deviceName;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }
}
