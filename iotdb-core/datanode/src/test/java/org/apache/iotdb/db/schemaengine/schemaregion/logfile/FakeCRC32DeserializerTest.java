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
package org.apache.iotdb.db.schemaengine.schemaregion.logfile;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class FakeCRC32DeserializerTest {

  @Test
  public void deserializeReadsCompletePayloadAfterShortRead() throws IOException {
    byte[] payload = new byte[] {1, 2, 3, 4};

    byte[] deserialized =
        new FakeCRC32Deserializer<>(new ByteBufferDeserializer())
            .deserialize(new OneByteAtATimeInputStream(serialize(payload, true)));

    Assert.assertArrayEquals(payload, deserialized);
  }

  @Test
  public void deserializeThrowsWhenPayloadIsTruncated() throws IOException {
    byte[] bytes = serialize(new byte[] {1, 2}, false, false);

    Assert.assertThrows(
        EOFException.class,
        () ->
            new FakeCRC32Deserializer<>(new ByteBufferDeserializer())
                .deserialize(new OneByteAtATimeInputStream(bytes)));
  }

  private static byte[] serialize(byte[] payload, boolean complete) throws IOException {
    return serialize(payload, complete, true);
  }

  private static byte[] serialize(byte[] payload, boolean complete, boolean writeValidationCode)
      throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeInt(complete ? payload.length : payload.length + 1);
      dataOutputStream.write(payload);
      if (writeValidationCode) {
        dataOutputStream.writeLong(0L);
      }
    }
    return outputStream.toByteArray();
  }

  private static class ByteBufferDeserializer implements IDeserializer<byte[]> {

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
  }

  private static class OneByteAtATimeInputStream extends InputStream {

    private final byte[] bytes;
    private int index;

    private OneByteAtATimeInputStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int read() {
      return index < bytes.length ? bytes[index++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) {
        return 0;
      }
      if (index >= bytes.length) {
        return -1;
      }
      b[off] = bytes[index++];
      return 1;
    }
  }
}
