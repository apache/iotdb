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

package org.apache.iotdb.commons.memory;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class IoTDBRuntimeOutOfMemoryException extends RuntimeException {

  private final long timestamp;
  private static final long VERSION = 1L;

  public IoTDBRuntimeOutOfMemoryException(final String message) {
    super(message);
    this.timestamp = System.currentTimeMillis();
  }

  public IoTDBRuntimeOutOfMemoryException(final String message, final long timeStamp) {
    super(message);
    this.timestamp = timeStamp;
  }

  public IoTDBRuntimeOutOfMemoryException(final String message, final Throwable cause) {
    super(message, cause);
    this.timestamp = System.currentTimeMillis();
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof IoTDBRuntimeOutOfMemoryException
        && Objects.equals(getMessage(), ((IoTDBRuntimeOutOfMemoryException) obj).getMessage())
        && Objects.equals(getTimestamp(), ((IoTDBRuntimeOutOfMemoryException) obj).getTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getTimestamp());
  }

  public void serialize(final ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(VERSION, byteBuffer);
    ReadWriteIOUtils.write(getMessage(), byteBuffer);
    ReadWriteIOUtils.write(getTimestamp(), byteBuffer);
  }

  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(VERSION, stream);
    ReadWriteIOUtils.write(getMessage(), stream);
    ReadWriteIOUtils.write(getTimestamp(), stream);
  }

  @Override
  public String toString() {
    return "IoTDBRuntimeOutOfMemoryException{"
        + "message='"
        + getMessage()
        + "', timestamp="
        + getTimestamp()
        + "}";
  }
}
