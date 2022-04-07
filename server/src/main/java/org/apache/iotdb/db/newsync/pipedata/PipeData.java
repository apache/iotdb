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
package org.apache.iotdb.db.newsync.pipedata;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.newsync.receiver.load.ILoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class PipeData {
  private static final Logger logger = LoggerFactory.getLogger(PipeData.class);

  protected long serialNumber;

  public PipeData(long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public long getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public abstract PipeDataType getType();

  public long serialize(DataOutputStream stream) throws IOException {
    long serializeSize = 0;
    stream.writeByte((byte) getType().ordinal());
    serializeSize += Byte.BYTES;
    stream.writeLong(serialNumber);
    serializeSize += Long.BYTES;
    return serializeSize;
  }

  public byte[] serialize() throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    serialize(new DataOutputStream(byteStream));
    return byteStream.toByteArray();
  }

  public static PipeData deserialize(DataInputStream stream)
      throws IOException, IllegalPathException {
    PipeDataType type = PipeDataType.values()[stream.readByte()];
    switch (type) {
      case TSFILE:
        return TsFilePipeData.deserialize(stream);
      case DELETION:
        return DeletionPipeData.deserialize(stream);
      case SCHEMA:
        return SchemaPipeData.deserialize(stream);
      default:
        logger.error("Deserialize PipeData error because Unknown type {}.", type);
        throw new UnsupportedOperationException(
            "Deserialize PipeData error because Unknown type " + type);
    }
  }

  public static PipeData deserialize(byte[] bytes) throws IllegalPathException, IOException {
    return deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public abstract ILoader createLoader();

  public enum PipeDataType {
    TSFILE,
    DELETION,
    SCHEMA
  }
}
