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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This classed is used for keep mlog compatible with that of 0.14 snapshot versions. The element T
 * will be serialized to OutputStream as format: content data length (4B) + content data (var
 * length) + validation code (long). The validation code will be filled by a meaningless long value.
 */
@NotThreadSafe
public class FakeCRC32Serializer<T> implements ISerializer<T> {

  // bytes data of a long for compatibility with old version (CRC32 code)
  private static final byte[] PLACE_HOLDER = new byte[Long.BYTES];

  private static final int INITIALIZED_BUFFER_SIZE = 8192;

  private final ByteArrayOutputStream logBufferStream =
      new ByteArrayOutputStream(INITIALIZED_BUFFER_SIZE);
  private final ByteBuffer logLengthBuffer = ByteBuffer.allocate(Integer.BYTES);

  private final ISerializer<T> serializer;

  public FakeCRC32Serializer(ISerializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public void serialize(T t, OutputStream outputStream) throws IOException {
    serializer.serialize(t, logBufferStream);
    // write the length of plan data
    logLengthBuffer.putInt(logBufferStream.size());
    outputStream.write(logLengthBuffer.array());
    // write a long to keep compatible with old version (CRC32 code)
    logBufferStream.write(PLACE_HOLDER);
    logBufferStream.writeTo(outputStream);
    // clear buffer
    logLengthBuffer.clear();
    logBufferStream.reset();
  }
}
