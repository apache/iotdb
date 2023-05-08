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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;

class RequestMessage implements Message {

  private final IConsensusRequest actualRequest;
  private volatile ByteString serializedContent;

  RequestMessage(IConsensusRequest request) {
    this.actualRequest = request;
    serializedContent = null;
  }

  IConsensusRequest getActualRequest() {
    return actualRequest;
  }

  @Override
  public ByteString getContent() {
    if (serializedContent == null) {
      synchronized (this) {
        if (serializedContent == null) {
          serializedContent =
              UnsafeByteOperations.unsafeWrap(actualRequest.serializeToByteBuffer());
        }
      }
    }
    return serializedContent;
  }
}
