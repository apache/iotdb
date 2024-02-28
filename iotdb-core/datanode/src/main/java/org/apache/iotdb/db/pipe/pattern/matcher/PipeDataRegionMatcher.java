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

package org.apache.iotdb.db.pipe.pattern.matcher;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;

import java.util.Set;

public interface PipeDataRegionMatcher {

  /**
   * Register a extractor. If the extractor's pattern matches the event's schema info, the event
   * will be assigned to the extractor.
   */
  void register(PipeRealtimeDataRegionExtractor extractor);

  /** Deregister a extractor. */
  void deregister(PipeRealtimeDataRegionExtractor extractor);

  /** Get the number of registered extractors in this matcher. */
  int getRegisterCount();

  /**
   * Match the event's schema info with the registered extractors' patterns. If the event's schema
   * info matches the pattern of a extractor, the extractor will be returned.
   *
   * @param event the event to be matched
   * @return the matched extractors
   */
  Set<PipeRealtimeDataRegionExtractor> match(PipeRealtimeEvent event);

  /** Check if a pattern is legal. Different pattern format may have different rules. */
  boolean patternIsLegal(String pattern);

  /** Check if a pattern matches all time-series under a database. */
  boolean patternCoverDb(String pattern, String device);

  /** Check if a device's all measurements are covered by the pattern. */
  boolean patternCoverDevice(String pattern, String device);

  /**
   * Check if a device may have some measurements matched by the pattern.
   *
   * <p>NOTE: this is just a loose check and may have false positives. To further check if a
   * measurement matches the pattern, please use {@link
   * PipeDataRegionMatcher#patternMatchMeasurement} after this.
   */
  boolean patternMayOverlapWithDevice(String pattern, String device);

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link PipeDataRegionMatcher#patternMayOverlapWithDevice} is
   * true.
   */
  boolean patternMatchMeasurement(String pattern, String device, String measurement);

  /** Clear all the registered extractors and internal data structures. */
  void clear();
}
