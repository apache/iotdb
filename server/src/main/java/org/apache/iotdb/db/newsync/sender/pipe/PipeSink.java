package org.apache.iotdb.db.newsync.sender.pipe;

import java.nio.ByteBuffer;

public interface PipeSink {
  void setAttribute(String attr, String value);

  String getName();

  String showAttributes();

  ByteBuffer serialize();

  void deserialize(ByteBuffer byteBuffer);
}
