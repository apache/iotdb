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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner;

import org.apache.iotdb.db.pipe.source.dataregion.realtime.disruptor.ExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorQueueExceptionHandler implements ExceptionHandler<Object> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DisruptorQueueExceptionHandler.class);

  @Override
  public void handleEventException(final Throwable ex, final long sequence, final Object event) {
    LOGGER.error("Exception processing: {} {}", sequence, event, ex);
  }

  @Override
  public void handleOnStartException(final Throwable ex) {
    LOGGER.warn("Exception during onStart()", ex);
  }

  @Override
  public void handleOnShutdownException(final Throwable ex) {
    LOGGER.warn("Exception during onShutdown()", ex);
  }
}
