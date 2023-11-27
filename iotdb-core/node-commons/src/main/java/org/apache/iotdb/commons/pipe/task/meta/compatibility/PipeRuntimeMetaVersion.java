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

package org.apache.iotdb.commons.pipe.task.meta.compatibility;

import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta.PipeRuntimeMetaV1;
import org.apache.iotdb.commons.pipe.task.meta.compatibility.runtimemeta.PipeRuntimeMetaV2;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public enum PipeRuntimeMetaVersion {

  // For compatibility use
  VERSION_1(PipeStatus.RUNNING.getType()),

  VERSION_2(Byte.MAX_VALUE),
  VERSION_3((byte) (Byte.MAX_VALUE - 1)),
  ;

  private static final Map<Byte, PipeRuntimeMetaVersion> VERSION_MAP = new HashMap<>();

  static {
    // For compatibility use
    for (final PipeStatus status : PipeStatus.values()) {
      VERSION_MAP.put(status.getType(), VERSION_1);
    }

    for (final PipeRuntimeMetaVersion version : PipeRuntimeMetaVersion.values()) {
      VERSION_MAP.put(version.getVersion(), version);
    }
  }

  private final byte version;

  PipeRuntimeMetaVersion(byte version) {
    this.version = version;
  }

  public byte getVersion() {
    return version;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(version, outputStream);
  }

  public static PipeRuntimeMetaVersion deserialize(InputStream inputStream) throws IOException {
    return deserialize(ReadWriteIOUtils.readByte(inputStream));
  }

  public static PipeRuntimeMetaVersion deserialize(ByteBuffer byteBuffer) {
    return deserialize(ReadWriteIOUtils.readByte(byteBuffer));
  }

  public static PipeRuntimeMetaVersion deserialize(byte version) {
    return VERSION_MAP.get(version);
  }

  /////////////////////////////// RuntimeMeta deserializer ///////////////////////////////

  public static PipeRuntimeMeta deserializeRuntimeMeta(InputStream inputStream) throws IOException {
    PipeRuntimeMetaVersion pipeRuntimeMetaVersion =
        PipeRuntimeMetaVersion.deserialize(ReadWriteIOUtils.readByte(inputStream));
    switch (pipeRuntimeMetaVersion) {
      case VERSION_1:
        PipeStatus status = PipeStatus.getPipeStatus(pipeRuntimeMetaVersion.getVersion());
        return PipeRuntimeMetaV1.deserialize(inputStream, status).toCurrentPipeRuntimeMetaVersion();
      case VERSION_2:
        return PipeRuntimeMetaV2.deserialize(inputStream).toCurrentPipeRuntimeMetaVersion();
      case VERSION_3:
        return PipeRuntimeMeta.deserialize(inputStream);
      default:
        throw new UnsupportedOperationException(
            "Unknown pipe runtime meta version: " + pipeRuntimeMetaVersion.getVersion());
    }
  }

  public static PipeRuntimeMeta deserializeRuntimeMeta(ByteBuffer byteBuffer) {
    PipeRuntimeMetaVersion pipeRuntimeMetaVersion =
        PipeRuntimeMetaVersion.deserialize(ReadWriteIOUtils.readByte(byteBuffer));
    switch (pipeRuntimeMetaVersion) {
      case VERSION_1:
        PipeStatus status = PipeStatus.getPipeStatus(pipeRuntimeMetaVersion.getVersion());
        return PipeRuntimeMetaV1.deserialize(byteBuffer, status).toCurrentPipeRuntimeMetaVersion();
      case VERSION_2:
        return PipeRuntimeMetaV2.deserialize(byteBuffer).toCurrentPipeRuntimeMetaVersion();
      case VERSION_3:
        return PipeRuntimeMeta.deserialize(byteBuffer);
      default:
        throw new UnsupportedOperationException(
            "Unknown pipe runtime meta version: " + pipeRuntimeMetaVersion.getVersion());
    }
  }
}
