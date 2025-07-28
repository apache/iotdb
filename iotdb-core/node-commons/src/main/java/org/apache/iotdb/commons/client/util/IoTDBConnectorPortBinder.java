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

package org.apache.iotdb.commons.client.util;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.utils.function.Consumer;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IoTDBConnectorPortBinder {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnectorPortBinder.class);

  // ===========================bind================================

  public static void bindPort(
      final String customSendPortStrategy,
      final int minSendPortRange,
      final int maxSendPortRange,
      final List<Integer> candidatePorts,
      final Consumer<Integer, Exception> consumer) {
    final boolean isRange =
        PipeSinkConstant.CONNECTOR_IOTDB_SEND_PORT_RESTRICTION_RANGE_STRATEGY.equals(
            customSendPortStrategy);
    boolean portFound = false;
    int index = 0;
    boolean searching = isRange || !candidatePorts.isEmpty();
    while (searching) {
      int port = isRange ? minSendPortRange + index : candidatePorts.get(index);
      try {
        consumer.accept(port);
        portFound = true;
        break;
      } catch (Exception ignored) {
      }
      index++;
      searching = isRange ? port <= maxSendPortRange : candidatePorts.size() > index;
    }
    if (!portFound) {
      String exceptionMessage =
          isRange
              ? String.format(
                  "Failed to find an available send port within the range %d to %d.",
                  minSendPortRange, maxSendPortRange)
              : String.format(
                  "Failed to find an available send port in the candidate list [%s].",
                  candidatePorts);
      LOGGER.warn(exceptionMessage);
      throw new PipeConnectionException(exceptionMessage);
    }
  }
}
