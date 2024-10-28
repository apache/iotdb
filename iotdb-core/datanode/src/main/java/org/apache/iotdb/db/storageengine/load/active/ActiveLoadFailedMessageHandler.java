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

import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.tsfile.utils.Pair;
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
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to memory constraints, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
              // system is read only
              put(
                  "read only",
                  filePair ->
                      LOGGER.info(
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the system is read only, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
              // Timed out to wait for procedure return. The procedure is still running.
              put(
                  "procedure return",
                  filePair ->
                      LOGGER.info(
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to time out to wait for procedure return, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
              // DataNode is not enough, please register more.
              put(
                  "not enough",
                  filePair ->
                      LOGGER.info(
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to the datanode is not enough, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
              // Fail to connect to any config node. Please check status of ConfigNodes or logs of
              // connected DataNode.
              put(
                  "any config node",
                  filePair ->
                      LOGGER.info(
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to fail to connect to any config node, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
              // Current query is time out, query start time is 1729653161797, ddl is
              // -3046040214706, current time is 1729653184210, please check your statement or
              // modify timeout parameter
              put(
                  "query is time out",
                  filePair ->
                      LOGGER.info(
                          "Rejecting auto load tsfile {} (isGeneratedByPipe = {}) due to current query is time out, will retry later.",
                          filePair.getLeft(),
                          filePair.getRight()));
            }
          });

  @FunctionalInterface
  private interface ExceptionMessageHandler {
    void handle(final Pair<String, Boolean> filePair);
  }

  public static boolean isExceptionMessageShouldRetry(
      final Pair<String, Boolean> filePair, final String message) {
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      EXCEPTION_MESSAGE_HANDLER_MAP.get("read only").handle(filePair);
      return true;
    }

    for (String key : EXCEPTION_MESSAGE_HANDLER_MAP.keySet()) {
      if (message != null && message.contains(key)) {
        EXCEPTION_MESSAGE_HANDLER_MAP.get(key).handle(filePair);
        return true;
      }
    }

    return false;
  }
}
