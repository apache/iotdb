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

package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.mpp.rpc.thrift.TPushMultiPipeMetaReq;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

final class PushMultiPipeMetaHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(PushMultiPipeMetaHelper.class);

  private PushMultiPipeMetaHelper() {
    // Utility class
  }

  interface Handler {

    TPushPipeMetaRespExceptionMessage handleDropPipe(String pipeName) throws Exception;

    TPushPipeMetaRespExceptionMessage handleSinglePipeMeta(ByteBuffer pipeMeta) throws Exception;
  }

  static TPushPipeMetaResp pushMultiPipeMeta(
      final TPushMultiPipeMetaReq req, final Handler handler) {
    final List<TPushPipeMetaRespExceptionMessage> exceptionMessages = new ArrayList<>();
    try {
      if (req.isSetPipeNamesToDrop()) {
        for (final String pipeNameToDrop : req.getPipeNamesToDrop()) {
          final TPushPipeMetaRespExceptionMessage message = handler.handleDropPipe(pipeNameToDrop);
          if (message != null) {
            exceptionMessages.add(message);
          }
        }
      } else if (req.isSetPipeMetas()) {
        for (final ByteBuffer pipeMeta : req.getPipeMetas()) {
          final TPushPipeMetaRespExceptionMessage message = handler.handleSinglePipeMeta(pipeMeta);
          if (message != null) {
            exceptionMessages.add(message);
          }
        }
      } else {
        throw new Exception("Invalid TPushMultiPipeMetaReq");
      }

      return exceptionMessages.isEmpty()
          ? new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          : new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
              .setExceptionMessages(exceptionMessages);
    } catch (final Exception e) {
      LOGGER.warn("Error occurred when pushing multi pipe meta", e);
      return new TPushPipeMetaResp()
          .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_ERROR.getStatusCode()))
          .setExceptionMessages(exceptionMessages);
    }
  }
}
