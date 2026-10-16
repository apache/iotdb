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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.disruptor;

/**
 * Exception handler for event processing errors
 *
 * <p>This interface is based on LMAX Disruptor (https://github.com/LMAX-Exchange/disruptor) and
 * adapted for IoTDB's Pipe module.
 *
 * @param <T> event type
 */
public interface ExceptionHandler<T> {
  /**
   * Handle exception during event processing
   *
   * @param ex exception
   * @param sequence sequence number
   * @param event the event
   */
  void handleEventException(Throwable ex, long sequence, T event);

  /** Handle exception during processor start */
  void handleOnStartException(Throwable ex);

  /** Handle exception during processor shutdown */
  void handleOnShutdownException(Throwable ex);
}
