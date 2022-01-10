package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.newsync.sender.conf.SenderConf;

import java.nio.ByteBuffer;

public class IoTDBPipeSink implements PipeSink {
  private String name;
  private String ip;
  private int port;

  public IoTDBPipeSink(String name) {
    ip = SenderConf.defaultPipeSinkIP;
    port = SenderConf.defaultPipeSinkPort;
    this.name = name;
  }

  @Override
  public void setAttribute(String attr, String value) {
    if (attr.equals("ip")) {
      ip = value;
    } else if (attr.equals("port")) {
      port = Integer.parseInt(value);
    } else {
      throw new UnsupportedOperationException(
          "there is No attribute " + attr + " in " + Type.IoTDB);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String showAttributes() {
    return null;
  }

  @Override
  public ByteBuffer serialize() {
    return null;
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {}
}
