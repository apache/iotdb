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
package org.apache.iotdb.db.sync.pipedata;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.sync.pipedata.load.ILoader;

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

  protected PipeData() {}

  protected PipeData(long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public long getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(long serialNumber) {
    this.serialNumber = serialNumber;
  }

  public abstract PipeDataType getPipeDataType();

  public long serialize(DataOutputStream stream) throws IOException {
    long serializeSize = 0;
    stream.writeByte(getPipeDataType().getType());
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

  public void deserialize(DataInputStream stream) throws IOException, IllegalPathException {
    serialNumber = stream.readLong();
  }

  public static PipeData createPipeData(DataInputStream stream)
      throws IOException, IllegalPathException {
    PipeData pipeData;
    PipeDataType type = PipeDataType.getPipeDataType(stream.readByte());
    switch (type) {
      case TSFILE:
        pipeData = new TsFilePipeData();
        break;
      case DELETION:
        pipeData = new DeletionPipeData();
        break;
      default:
        logger.error("Deserialize PipeData error because Unknown type {}.", type);
        throw new UnsupportedOperationException(
            "Deserialize PipeData error because Unknown type " + type);
    }
    pipeData.deserialize(stream);
    return pipeData;
  }

  public static PipeData createPipeData(byte[] bytes) throws IllegalPathException, IOException {
    return createPipeData(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public abstract ILoader createLoader();

  public enum PipeDataType {
    TSFILE((byte) 0),
    DELETION((byte) 1);

    private final byte type;

    PipeDataType(byte type) {
      this.type = type;
    }

    public byte getType() {
      return type;
    }

    public static PipeDataType getPipeDataType(byte type) {
      switch (type) {
        case 0:
          return PipeDataType.TSFILE;
        case 1:
          return PipeDataType.DELETION;
        default:
          throw new IllegalArgumentException("Invalid input: " + type);
      }
    }
  }
}
