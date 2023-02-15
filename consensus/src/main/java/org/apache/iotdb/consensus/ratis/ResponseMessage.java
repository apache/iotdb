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

import org.apache.iotdb.common.rpc.thrift.TSStatus;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseMessage implements Message {

  /**
   * This content holder may hold 1. TSStatus, which may be serialized when called getContent() 2.
   * DataSet, which never need to be serialized
   */
  private final Object contentHolder;

  private volatile ByteString serializedData;
  private final Logger logger = LoggerFactory.getLogger(ResponseMessage.class);

  ResponseMessage(Object content) {
    this.contentHolder = content;
    this.serializedData = null;
  }

  Object getContentHolder() {
    return contentHolder;
  }

  @Override
  public ByteString getContent() {
    if (serializedData == null) {
      synchronized (this) {
        if (serializedData == null) {
          assert contentHolder instanceof TSStatus;
          TSStatus status = (TSStatus) contentHolder;
          try {
            serializedData = ByteString.copyFrom(Utils.serializeTSStatus(status));
          } catch (TException e) {
            logger.warn("serialize TSStatus failed {}", status);
          }
        }
      }
    }
    return serializedData;
  }
}
