package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.exception.PipeSinkException;
import org.apache.iotdb.db.newsync.sender.conf.SenderConf;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

public class IoTDBPipeSink implements PipeSink {
  private final PipeSink.Type type;

  private String name;
  private String ip;
  private int port;

  public IoTDBPipeSink(String name) {
    ip = SenderConf.defaultPipeSinkIP;
    port = SenderConf.defaultPipeSinkPort;
    this.name = name;
    type = Type.IoTDB;
  }

  @Override
  public void setAttribute(String attr, String value) throws PipeSinkException {
    if (attr.equals("ip")) {
      if (!value.startsWith("'") || !value.endsWith("'"))
        throw new PipeSinkException(attr, value, TSDataType.TEXT.name());
      ip = value.substring(1, value.length() - 1);
    } else if (attr.equals("port")) {
      try {
        port = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new PipeSinkException(attr, value, TSDataType.INT32.name());
      }
    } else {
      throw new PipeSinkException("there is No attribute " + attr + " in " + Type.IoTDB);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String showAllAttributes() {
    return String.format("ip='%s',port=%d", ip, port);
  }

  @Override
  public ByteBuffer serialize() {
    return null;
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {}
}
