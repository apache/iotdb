/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.common.object.io;

import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class SegmentedByteInputStream extends InputStream {

  private final List<ByteBuffer> bufferList;

  private int index = 0;

  private ByteBuffer workingBuffer;

  public SegmentedByteInputStream(List<ByteBuffer> bufferList) {
    this.bufferList = bufferList;
    this.workingBuffer = bufferList.get(0);
  }

  @Override
  public int read() throws IOException {
    if (!workingBuffer.hasRemaining()) {
      if (index == bufferList.size() - 1) {
        throw new EOFException();
      } else {
        index++;
        workingBuffer = bufferList.get(index);
      }
    }
    return workingBuffer.get();
  }

  @Override
  public int read(@NotNull byte[] b, int off, int len) throws IOException {
    int count = 0;
    int position;
    int delta;
    while (len > 0) {
      if (workingBuffer.remaining() > len) {
        position = workingBuffer.position();
        workingBuffer.get(b, off, len);
        delta = workingBuffer.position() - position;
      } else {
        delta = workingBuffer.remaining();
        workingBuffer.get(b, off, delta);
      }
      count += delta;
      off += delta;
      len -= delta;
      if (workingBuffer.remaining() <= 0) {
        if (index == bufferList.size() - 1) {
          break;
        } else {
          index++;
          workingBuffer = bufferList.get(index);
        }
      }
    }
    return count;
  }

  @Override
  public int available() throws IOException {
    int remaining = 0;
    for (int i = index; i < bufferList.size(); i++) {
      remaining += bufferList.get(i).remaining();
    }
    return remaining;
  }
}
