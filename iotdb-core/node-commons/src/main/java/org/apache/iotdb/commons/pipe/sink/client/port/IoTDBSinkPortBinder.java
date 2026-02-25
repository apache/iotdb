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

package org.apache.iotdb.commons.pipe.sink.client.port;

import org.apache.iotdb.commons.pipe.datastructure.interval.IntervalManager;
import org.apache.iotdb.commons.pipe.datastructure.interval.PlainInterval;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.utils.function.Consumer;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class IoTDBSinkPortBinder {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSinkPortBinder.class);

  // ===========================bind================================

  /**
   * Find an available port in the configured ports.
   *
   * @param candidatePorts the ports
   * @param consumer the binding process, must be called when successfully find a port
   */
  public static void bindPort(
      final IntervalManager<PlainInterval> candidatePorts,
      final Consumer<Integer, Exception> consumer) {
    // Bind random port when the ports are not configured
    if (candidatePorts.isEmpty()) {
      try {
        consumer.accept(0);
      } catch (final Exception e) {
        throw new PipeConnectionException(e.getMessage(), e);
      }
      return;
    }
    final Iterator<Long> iterator = candidatePorts.iterator();
    boolean portFound = false;
    while (iterator.hasNext()) {
      final int port = Math.toIntExact(iterator.next());
      try {
        consumer.accept(port);
        portFound = true;
        break;
      } catch (final Exception ignore) {
      }
    }
    if (!portFound) {
      final String exceptionMessage =
          String.format("Failed to find an available sending ports in %s.", candidatePorts);
      PipeLogger.log(LOGGER::warn, exceptionMessage);
      throw new PipeConnectionException(exceptionMessage);
    }
  }
}
