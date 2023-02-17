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
package org.apache.iotdb.commons.sync.pipe;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeMessage {
  private final String message;
  private final PipeMessageType type;

  public PipeMessage(PipeMessageType type, String message) {
    this.type = type;
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public PipeMessageType getType() {
    return type;
  }

  public ByteBuffer serializeToByteBuffer() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(dataOutputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public void serialize(DataOutputStream dataOutputStream) throws IOException {
    ReadWriteIOUtils.write(message, dataOutputStream);
    ReadWriteIOUtils.write(type.getType(), dataOutputStream);
  }

  public static PipeMessage deserialize(ByteBuffer buffer) {
    String message = ReadWriteIOUtils.readString(buffer);
    PipeMessageType type = PipeMessageType.getPipeStatus(ReadWriteIOUtils.readByte(buffer));
    return new PipeMessage(type, message);
  }

  @Override
  public String toString() {
    return "PipeMessage{" + "message='" + message + '\'' + ", type=" + type + '}';
  }

  public enum PipeMessageType {
    NORMAL((byte) 1),
    WARN((byte) 2),
    ERROR((byte) 3);

    private byte type;

    PipeMessageType(byte type) {
      this.type = type;
    }

    public byte getType() {
      return type;
    }

    public static PipeMessageType getPipeStatus(byte type) {
      switch (type) {
        case 1:
          return PipeMessageType.NORMAL;
        case 2:
          return PipeMessageType.WARN;
        case 3:
          return PipeMessageType.ERROR;
        default:
          throw new IllegalArgumentException("Invalid input: " + type);
      }
    }
  }
}
