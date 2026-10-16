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

    /**
     * Handles all pipe metadata in one agent invocation. Alter pipe sends the old dropped metadata
     * and the new metadata together, so they must be visible to the agent at the same time when it
     * decides whether the old task's local progress can be reused.
     *
     * <p>The default implementation preserves the per-metadata behavior for handlers that do not
     * need batch processing.
     *
     * @param pipeMetas serialized pipe metadata to process as one batch
     * @param exceptionMessages destination for per-pipe failures
     * @return {@code true} after the batch was processed, or {@code false} only when the task agent
     *     could not acquire its write lock before the metadata handling timeout
     */
    default boolean handlePipeMetaChanges(
        final List<ByteBuffer> pipeMetas,
        final List<TPushPipeMetaRespExceptionMessage> exceptionMessages)
        throws Exception {
      for (final ByteBuffer pipeMeta : pipeMetas) {
        final TPushPipeMetaRespExceptionMessage message = handleSinglePipeMeta(pipeMeta);
        if (message != null) {
          exceptionMessages.add(message);
        }
      }
      return true;
    }
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
        // A false result is reserved for the task-agent write-lock timeout. Per-pipe processing
        // failures are returned through exceptionMessages and use PIPE_PUSH_META_ERROR below.
        if (!handler.handlePipeMetaChanges(req.getPipeMetas(), exceptionMessages)) {
          return new TPushPipeMetaResp()
              .setStatus(new TSStatus(TSStatusCode.PIPE_PUSH_META_TIMEOUT.getStatusCode()));
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
