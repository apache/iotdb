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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

public class OSFileCacheKey implements Serializable {
  /** remote TsFile */
  private final OSFile file;
  /** start position in the remote TsFile */
  private final long startPosition;

  public OSFileCacheKey(OSFile file, long startPosition) {
    this.file = file;
    this.startPosition = startPosition;
  }

  public int serializeSize() {
    return Integer.BYTES + file.toString().getBytes().length + Long.BYTES;
  }

  public ByteBuffer serialize() {
    ByteBuffer buffer = ByteBuffer.allocate(serializeSize());
    ReadWriteIOUtils.write(file.toString(), buffer);
    ReadWriteIOUtils.write(startPosition, buffer);
    return buffer;
  }

  public static OSFileCacheKey deserialize(InputStream inputStream) throws IOException {
    String filePath = ReadWriteIOUtils.readString(inputStream);
    long startPosition = ReadWriteIOUtils.readLong(inputStream);
    return new OSFileCacheKey(new OSFile(filePath), startPosition);
  }

  public OSFile getFile() {
    return file;
  }

  public long getStartPosition() {
    return startPosition;
  }

  @Override
  public int hashCode() {
    return Objects.hash(file, startPosition);
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
    return file.equals(that.file) && startPosition == that.startPosition;
  }
}
