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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * This interface defines the behaviour of a Serializer of T. An instance of this interface provides
 * the ability to serialize an instance of T to InputStream or Bytebuffer.
 *
 * @param <T>
 */
public interface ISerializer<T> {

  default void serialize(T t, OutputStream outputStream) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default void serialize(T t, ByteBuffer buffer) {
    throw new UnsupportedOperationException();
  }
}
