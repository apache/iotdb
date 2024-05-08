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
 *
 */
package org.apache.iotdb.commons.sync.pipesink;

import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public interface PipeSink {

  void setAttribute(Map<String, String> params) throws PipeSinkException;

  String getPipeSinkName();

  PipeSinkType getType();

  String showAllAttributes();

  TPipeSinkInfo getTPipeSinkInfo();

  void serialize(OutputStream outputStream) throws IOException;

  void deserialize(InputStream inputStream) throws IOException;

  void deserialize(ByteBuffer buffer);

  static PipeSink deserializePipeSink(InputStream inputStream) throws IOException {
    PipeSinkType pipeSinkType = PipeSinkType.values()[ReadWriteIOUtils.readByte(inputStream)];
    PipeSink pipeSink;
    switch (pipeSinkType) {
      case IoTDB:
        pipeSink = new IoTDBPipeSink();
        pipeSink.deserialize(inputStream);
        break;
      case ExternalPipe:
        // TODO(ext-pipe): deserialize external pipesink here
      default:
        throw new UnsupportedOperationException(
            String.format("Can not recognize PipeSinkType %s.", pipeSinkType.name()));
    }
    return pipeSink;
  }

  static PipeSink deserializePipeSink(ByteBuffer buffer) {
    PipeSinkType pipeSinkType = PipeSinkType.values()[ReadWriteIOUtils.readByte(buffer)];
    PipeSink pipeSink;
    switch (pipeSinkType) {
      case IoTDB:
        pipeSink = new IoTDBPipeSink();
        pipeSink.deserialize(buffer);
        break;
      case ExternalPipe:
        // TODO(ext-pipe): deserialize external pipesink here
      default:
        throw new UnsupportedOperationException(
            String.format("Can not recognize PipeSinkType %s.", pipeSinkType.name()));
    }
    return pipeSink;
  }

  enum PipeSinkType {
    IoTDB,
    ExternalPipe
  }
}
