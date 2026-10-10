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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

final class PipeTransferPayloadReader {

  private PipeTransferPayloadReader() {}

  static String readString(final ByteBuffer buffer) {
    checkLength(buffer);
    return ReadWriteIOUtils.readString(buffer);
  }

  static byte[] readBinary(final ByteBuffer buffer) {
    checkLength(buffer);
    return ReadWriteIOUtils.readBinary(buffer).getValues();
  }

  private static void checkLength(final ByteBuffer buffer) {
    // Check the buffer limit before ReadWriteIOUtils allocates from the length prefix.
    if (buffer.remaining() < Integer.BYTES
        || buffer.getInt(buffer.position()) > buffer.remaining() - Integer.BYTES) {
      throw new BufferUnderflowException();
    }
  }
}
