package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.exception.PipeSinkException;

import java.nio.ByteBuffer;

public interface PipeSink {
  void setAttribute(String attr, String value) throws PipeSinkException;

  String getName();

  Type getType();

  String showAllAttributes();

  ByteBuffer serialize();

  void deserialize(ByteBuffer byteBuffer);

  enum Type {
    IoTDB;
  }
}
