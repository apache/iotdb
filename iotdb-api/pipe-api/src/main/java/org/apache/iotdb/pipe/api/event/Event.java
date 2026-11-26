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

package org.apache.iotdb.pipe.api.event;

/** This interface is used to abstract events in collaboration tasks. */
public interface Event {

  /** Whether this event carries internal control flow rather than user data. */
  default boolean isControlEvent() {
    return false;
  }

  /** Whether the processor must forward this event unchanged to preserve internal ordering. */
  default boolean shouldSkipProcessing() {
    return false;
  }

  /** Binds the event to its source instance. Events without source metadata may ignore this. */
  default void bindSource(final long sourceId) {
    // Do nothing by default.
  }
}
