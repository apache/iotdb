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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.nio.ByteBuffer;

class TRaftRequestParser {

  private TRaftRequestParser() {}

  static long extractTimestamp(IConsensusRequest request, long fallbackTimestamp) {
    if (request.hasTime()) {
      return request.getTime();
    }
    ByteBuffer buffer = request.serializeToByteBuffer().duplicate();
    if (buffer.remaining() < Long.BYTES) {
      return fallbackTimestamp;
    }
    return buffer.getLong(buffer.position());
  }

  static byte[] extractRawRequest(IConsensusRequest request) {
    ByteBuffer buffer = request.serializeToByteBuffer().duplicate();
    byte[] result = new byte[buffer.remaining()];
    buffer.get(result);
    return result;
  }

  static IConsensusRequest buildRequest(byte[] rawRequest) {
    return new ByteBufferConsensusRequest(ByteBuffer.wrap(rawRequest));
  }
}
