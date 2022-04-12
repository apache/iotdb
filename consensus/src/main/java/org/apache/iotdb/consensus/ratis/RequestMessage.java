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

import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class RequestMessage implements Message {

  private final Logger logger = LoggerFactory.getLogger(RequestMessage.class);

  private final IConsensusRequest actualRequest;
  private volatile ByteString serializedContent;
  private final int DEFAULT_BUFFER_SIZE = 1024 * 10;

  public RequestMessage(IConsensusRequest request) {
    this.actualRequest = request;
    serializedContent = null;
  }

  public IConsensusRequest getActualRequest() {
    return actualRequest;
  }

  @Override
  public ByteString getContent() {
    if (serializedContent == null) {
      synchronized (this) {
        if (serializedContent == null) {
          ByteBufferConsensusRequest req;
          if (actualRequest instanceof ByteBufferConsensusRequest) {
            req = (ByteBufferConsensusRequest) actualRequest;
            serializedContent = ByteString.copyFrom(req.getContent());
            req.getContent().flip();
          } else {
            // TODO Pooling
            ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            actualRequest.serializeRequest(byteBuffer);
            byteBuffer.flip();
            serializedContent = ByteString.copyFrom(byteBuffer);
            byteBuffer.flip();
          }
        }
      }
    }
    return serializedContent;
  }
}
