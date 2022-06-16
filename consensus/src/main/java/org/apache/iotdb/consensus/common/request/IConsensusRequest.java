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

package org.apache.iotdb.consensus.common.request;

import java.nio.ByteBuffer;

public interface IConsensusRequest {
  /**
   * Serialize all the data to a ByteBuffer.
   *
   * <p>In a specific implementation, ByteBuf or PublicBAOS can be used to reduce the number of
   * memory copies.
   *
   * <p>To improve efficiency, a specific implementation could return a DirectByteBuffer to reduce
   * the memory copy required to send an RPC
   *
   * <p>Note: The implementation needs to ensure that the data in the returned Bytebuffer cannot be
   * changed or an error may occur
   */
  ByteBuffer serializeToByteBuffer();
}
