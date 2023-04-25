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

package org.apache.iotdb.db.pipe.core.collector;

import org.apache.iotdb.pipe.api.event.Event;

// TODO: move to pipe-api
public interface PipeCollector extends AutoCloseable {

  /** Start the collector. */
  // TODO: what's the difference between start() and supply()?
  void start();

  /**
   * Check if the collector has been started.
   *
   * @return true if the collector has been started, false otherwise.
   */
  // TODO: check if this method is necessary. maybe close() is enough.
  boolean hasBeenStarted();

  /**
   * Supply the event to the collector.
   *
   * @return the event to be supplied.
   */
  // TODO: check() may be a better name.
  Event supply();
}
