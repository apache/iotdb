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

package org.apache.iotdb.commons.exception.pipe;

import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMetaVersion;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeRuntimeNonCriticalException extends PipeRuntimeException {

  public PipeRuntimeNonCriticalException(String message) {
    super(message);
  }

  public PipeRuntimeNonCriticalException(String message, long timeStamp) {
    super(message, timeStamp);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeRuntimeNonCriticalException
        && Objects.equals(getMessage(), ((PipeRuntimeNonCriticalException) obj).getMessage())
        && Objects.equals(getTimeStamp(), ((PipeRuntimeException) obj).getTimeStamp());
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    PipeRuntimeExceptionType.NON_CRITICAL_EXCEPTION.serialize(byteBuffer);
    ReadWriteIOUtils.write(getMessage(), byteBuffer);
    ReadWriteIOUtils.write(getTimeStamp(), byteBuffer);
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    PipeRuntimeExceptionType.NON_CRITICAL_EXCEPTION.serialize(stream);
    ReadWriteIOUtils.write(getMessage(), stream);
    ReadWriteIOUtils.write(getTimeStamp(), stream);
  }

  public static PipeRuntimeNonCriticalException deserializeFrom(
      PipeRuntimeMetaVersion version, ByteBuffer byteBuffer) {
    final String message = ReadWriteIOUtils.readString(byteBuffer);
    switch (version) {
      case VERSION_1:
        return new PipeRuntimeNonCriticalException(message);
      case VERSION_2:
        return new PipeRuntimeNonCriticalException(message, ReadWriteIOUtils.readLong(byteBuffer));
      default:
        throw new UnsupportedOperationException(String.format("Unsupported version %s", version));
    }
  }

  public static PipeRuntimeNonCriticalException deserializeFrom(
      PipeRuntimeMetaVersion version, InputStream stream) throws IOException {
    final String message = ReadWriteIOUtils.readString(stream);
    switch (version) {
      case VERSION_1:
        return new PipeRuntimeNonCriticalException(message);
      case VERSION_2:
        return new PipeRuntimeNonCriticalException(message, ReadWriteIOUtils.readLong(stream));
      default:
        throw new UnsupportedOperationException(String.format("Unsupported version %s", version));
    }
  }

  @Override
  public String toString() {
    return "PipeRuntimeNonCriticalException{"
        + "message='"
        + getMessage()
        + "', timeStamp="
        + getTimeStamp()
        + "}";
  }
}
