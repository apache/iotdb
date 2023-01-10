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

package org.apache.iotdb.db.metadata.logfile;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This classed is used for keep mlog compatible with that of 0.14 snapshot versions. The element T
 * will be deserialized from InputStream as format: content data length (4B) + content data (var
 * length) + validation code (long). The validation code will be filled by a meaningless long value,
 * and it will be read for no usage.
 */
@NotThreadSafe
public class FakeCRC32Deserializer<T> implements IDeserializer<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FakeCRC32Deserializer.class);

  private static final InputStream EMPTY_INPUT_STREAM_PLACE_HOLDER =
      new ByteArrayInputStream(new byte[0]);

  private final IDeserializer<T> deserializer;

  private final ConfigurableDataInputStream dataInputStream =
      new ConfigurableDataInputStream(EMPTY_INPUT_STREAM_PLACE_HOLDER);

  public FakeCRC32Deserializer(IDeserializer<T> deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public T deserialize(InputStream inputStream) throws IOException {
    dataInputStream.changeInputStream(inputStream);
    int logLength = dataInputStream.readInt();
    if (logLength <= 0) {
      LOGGER.error("Read log length {} is negative.", logLength);
      throw new IOException(
          new IllegalArgumentException(
              String.format("Read log length %s is negative.", logLength)));
    }

    byte[] logBuffer = new byte[logLength];
    if (logLength < inputStream.read(logBuffer, 0, logLength)) {
      throw new EOFException();
    }

    T result = deserializer.deserialize(ByteBuffer.wrap(logBuffer));

    // read a long to keep compatible with old version (CRC32 code)
    dataInputStream.readLong();
    return result;
  }

  private static class ConfigurableDataInputStream extends DataInputStream {

    private ConfigurableDataInputStream(@NotNull InputStream in) {
      super(in);
    }

    private void changeInputStream(InputStream in) {
      this.in = in;
    }
  }
}
