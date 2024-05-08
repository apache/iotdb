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

/**
 * This class is used to represent the sync log request from IoTConsensus. That we use this class
 * rather than ByteBufferConsensusRequest is because the serialization method is different between
 * these two classes. And we need to separate them in DataRegionStateMachine when deserialize the
 * PlanNode from ByteBuffer
 */
public class IoTConsensusRequest implements IConsensusRequest {

  private final ByteBuffer byteBuffer;

  public IoTConsensusRequest(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    return byteBuffer;
  }
}
