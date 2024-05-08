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

/*
In general, for the requests from the leader, we can directly strong-cast the class to reduce
the cost of deserialization during the execution of the leader state machine. For the requests
received by the followers, the responsibility of deserialization can generally be transferred
to the state machine layer
*/
public class ByteBufferConsensusRequest implements IConsensusRequest {

  private final ByteBuffer byteBuffer;

  public ByteBufferConsensusRequest(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    return byteBuffer;
  }
}
