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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMetaVersion;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public enum PipeRuntimeExceptionType {
  NON_CRITICAL_EXCEPTION((short) 1),
  CRITICAL_EXCEPTION((short) 2),
  CONNECTOR_CRITICAL_EXCEPTION((short) 3),
  OUT_OF_MEMORY_CRITICAL_EXCEPTION((short) 4),
  ;

  private final short type;

  PipeRuntimeExceptionType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(type, byteBuffer);
  }

  public void serialize(OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(type, stream);
  }

  public static PipeRuntimeException deserializeFrom(
      PipeRuntimeMetaVersion version, ByteBuffer byteBuffer) {
    final short type = ReadWriteIOUtils.readShort(byteBuffer);
    switch (type) {
      case 1:
        return PipeRuntimeNonCriticalException.deserializeFrom(version, byteBuffer);
      case 2:
        return PipeRuntimeCriticalException.deserializeFrom(version, byteBuffer);
      case 3:
        return PipeRuntimeConnectorCriticalException.deserializeFrom(version, byteBuffer);
      case 4:
        return PipeRuntimeOutOfMemoryCriticalException.deserializeFrom(version, byteBuffer);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported PipeRuntimeException type %s.", type));
    }
  }

  public static PipeRuntimeException deserializeFrom(
      PipeRuntimeMetaVersion version, InputStream stream) throws IOException {
    final short type = ReadWriteIOUtils.readShort(stream);
    switch (type) {
      case 1:
        return PipeRuntimeNonCriticalException.deserializeFrom(version, stream);
      case 2:
        return PipeRuntimeCriticalException.deserializeFrom(version, stream);
      case 3:
        return PipeRuntimeConnectorCriticalException.deserializeFrom(version, stream);
      case 4:
        return PipeRuntimeOutOfMemoryCriticalException.deserializeFrom(version, stream);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported PipeRuntimeException type %s.", type));
    }
  }
}
