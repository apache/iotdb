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

package org.apache.iotdb.db.pipe.collector.realtime.matcher;

import org.apache.iotdb.db.pipe.collector.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeCollectEvent;

import java.util.Set;

public interface PipeDataRegionMatcher {

  /**
   * Register a collector. If the collector's pattern matches the event's schema info, the event
   * will be assigned to the collector.
   */
  void register(PipeRealtimeDataRegionExtractor collector);

  /** Deregister a collector. */
  void deregister(PipeRealtimeDataRegionExtractor collector);

  /** Get the number of registered collectors in this matcher. */
  int getRegisterCount();

  /**
   * Match the event's schema info with the registered collectors' patterns. If the event's schema
   * info matches the pattern of a collector, the collector will be returned.
   *
   * @param event the event to be matched
   * @return the matched collectors
   */
  Set<PipeRealtimeDataRegionExtractor> match(PipeRealtimeCollectEvent event);

  /** Clear all the registered collectors and internal data structures. */
  void clear();
}
