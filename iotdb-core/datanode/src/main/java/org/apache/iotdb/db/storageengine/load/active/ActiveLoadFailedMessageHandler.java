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

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ActiveLoadFailedMessageHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ActiveLoadFailedMessageHandler.class);

  private static final Map<String, ExceptionMessageHandler> EXCEPTION_MESSAGE_HANDLER_MAP =
      Collections.unmodifiableMap(
          new HashMap<String, ExceptionMessageHandler>() {
            {
              // system is memory constrains
              put(
                  "memory",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_MEMORY_9A60DF29,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
              // system is read only
              put(
                  "read only",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_16FA5F18,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
              // Timed out to wait for procedure return. The procedure is still running.
              put(
                  "procedure return",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_TIME_E18630DE,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
              // DataNode is not enough, please register more.
              put(
                  "not enough",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_THE_5F811A8B,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
              // Fail to connect to any config node. Please check status of ConfigNodes or logs of
              // connected DataNode.
              put(
                  "any config node",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_FAIL_F59307B8,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
              // Current query is time out, query start time is 1729653161797, ddl is
              // -3046040214706, current time is 1729653184210, please check your statement or
              // modify timeout parameter
              put(
                  "query is time out",
                  filePair ->
                      LOGGER.info(
                          StorageEngineMessages
                              .STORAGE_LOG_REJECTING_AUTO_LOAD_TSFILE_ISGENERATEDBYPIPE_DUE_TO_CURRENT_264E12EE,
                          filePair.getFile(),
                          filePair.isGeneratedByPipe()));
            }
          });

  @FunctionalInterface
  private interface ExceptionMessageHandler {
    void handle(final ActiveLoadPendingQueue.ActiveLoadEntry entry);
  }

  public static boolean isStatusShouldRetry(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final TSStatus status) {
    if (status != null
        && status.getCode() == TSStatusCode.LOAD_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()) {
      LOGGER.info(
          StorageEngineMessages.ACTIVE_LOAD_TEMPORARILY_UNAVAILABLE,
          entry.getFile(),
          entry.isGeneratedByPipe(),
          status);
      return true;
    }
    return isExceptionMessageShouldRetry(entry, status == null ? null : status.getMessage());
  }

  public static boolean isExceptionMessageShouldRetry(
      final ActiveLoadPendingQueue.ActiveLoadEntry entry, final String message) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      EXCEPTION_MESSAGE_HANDLER_MAP.get("read only").handle(entry);
      return true;
    }

    for (String key : EXCEPTION_MESSAGE_HANDLER_MAP.keySet()) {
      if (message != null && message.contains(key)) {
        EXCEPTION_MESSAGE_HANDLER_MAP.get(key).handle(entry);
        return true;
      }
    }

    return false;
  }
}
