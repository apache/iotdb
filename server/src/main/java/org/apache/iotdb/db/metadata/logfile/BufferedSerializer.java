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

/**
 * This class provides the ability to buffer the data serialized by nested serializer.
 *
 * @param <T>
 */
@NotThreadSafe
public class BufferedSerializer<T> implements ISerializer<T> {

  private static final int INITIALIZED_BUFFER_SIZE = 8192;

  private final ByteArrayOutputStream logBufferStream =
      new ByteArrayOutputStream(INITIALIZED_BUFFER_SIZE);

  private final ISerializer<T> serializer;

  public BufferedSerializer(ISerializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public void serialize(T t, OutputStream outputStream) throws IOException {
    serializer.serialize(t, logBufferStream);
    logBufferStream.writeTo(outputStream);
    // clear buffer
    logBufferStream.reset();
  }
}
