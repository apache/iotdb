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
package org.apache.iotdb.os.cache;

import org.apache.iotdb.os.fileSystem.OSFile;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

public class OSFileCacheKey implements Serializable {
  private OSFile file;
  private long startPosition;
  private int length;

  public OSFileCacheKey(OSFile file, long startPosition, int length) {
    this.file = file;
    this.startPosition = startPosition;
    this.length = length;
  }

  public ByteBuffer serializeToByteBuffer() {
    byte[] pathBytes = file.toString().getBytes();
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + pathBytes.length + Integer.BYTES);
    buffer.putInt(pathBytes.length);
    buffer.put(pathBytes);
    buffer.putInt(length);
    return buffer;
  }

  public OSFile getFile() {
    return file;
  }

  public long getStartPosition() {
    return startPosition;
  }

  public int getLength() {
    return length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(file, startPosition, length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    OSFileCacheKey that = (OSFileCacheKey) obj;
    return file.equals(that.file) && startPosition == that.startPosition && length == that.length;
  }
}
