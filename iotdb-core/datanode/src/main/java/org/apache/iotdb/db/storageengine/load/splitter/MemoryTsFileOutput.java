/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

final class MemoryTsFileOutput implements TsFileOutput {
  private final PublicBAOS output = new PublicBAOS();

  @Override
  public void write(byte[] bytes) throws IOException {
    output.write(bytes);
  }

  @Override
  public void write(byte value) {
    output.write(value);
  }

  @Override
  public void write(ByteBuffer buffer) {
    final int length = buffer.remaining();
    if (buffer.hasArray()) {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
      buffer.position(buffer.position() + length);
    } else {
      final byte[] bytes = new byte[length];
      buffer.get(bytes);
      output.write(bytes, 0, bytes.length);
    }
  }

  @Override
  public long getPosition() {
    return output.size();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return output;
  }

  @Override
  public void flush() {}

  @Override
  public void truncate(long size) {
    output.reset();
  }

  @Override
  public void force() {}

  byte[] copyRange(int offset, int length) {
    final byte[] result = new byte[length];
    System.arraycopy(output.getBuf(), offset, result, 0, length);
    return result;
  }
}
