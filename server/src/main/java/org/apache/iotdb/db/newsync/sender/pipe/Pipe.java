package org.apache.iotdb.db.newsync.sender.pipe;

import java.nio.ByteBuffer;

public interface Pipe {
  void start();

  void pause();

  void drop();

  String getName();

  ByteBuffer serialize();

  void deserialize(ByteBuffer byteBuffer);
}
