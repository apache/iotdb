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
import org.apache.iotdb.db.pipe.pattern.PipePatternFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PipePatternMatcherManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipePatternMatcherManager.class);
  private final Map<PipePatternFormat, PipeDataRegionMatcher> format2MatcherMap = new HashMap<>();

  public void register(PipeRealtimeDataRegionExtractor extractor) {
    getMatcherByFormat(extractor.getPatternFormat()).register(extractor);
  }

  public void deregister(PipeRealtimeDataRegionExtractor extractor) {
    getMatcherByFormat(extractor.getPatternFormat()).deregister(extractor);
  }

  public Set<PipeRealtimeDataRegionExtractor> match(PipeRealtimeEvent event) {
    final Set<PipeRealtimeDataRegionExtractor> matchedExtractors = new HashSet<>();
    // match extractors of all formats.
    for (PipeDataRegionMatcher matcher : format2MatcherMap.values()) {
      matchedExtractors.addAll(matcher.match(event));
    }
    return matchedExtractors;
  }

  /** Check if a pattern is legal. Different pattern format may have different rules. */
  public boolean patternIsLegal(PipePatternFormat format, String pattern) {
    return getMatcherByFormat(format).patternIsLegal(pattern);
  }

  /** Check if a pattern matches all time-series under a database. */
  public boolean patternCoversDb(PipePatternFormat format, String pattern, String db) {
    return getMatcherByFormat(format).patternCoverDb(pattern, db);
  }

  /** Check if a device's all measurements are covered by the pattern. */
  public boolean patternCoverDevice(PipePatternFormat format, String pattern, String device) {
    return getMatcherByFormat(format).patternCoverDevice(pattern, device);
  }

  /**
   * Check if a device may have some measurements matched by the pattern.
   *
   * <p>NOTE: this is just a loose check and may have false positives. To further check if a
   * measurement matches the pattern, please use {@link
   * PipePatternMatcherManager#patternMatchMeasurement} after this.
   */
  public boolean patternMayOverlapWithDevice(
      PipePatternFormat format, String pattern, String device) {
    return getMatcherByFormat(format).patternMayOverlapWithDevice(pattern, device);
  }

  /**
   * Check if a full path with device and measurement can be matched by pattern under specified
   * format.
   *
   * <p>NOTE: this is only called when {@link PipePatternMatcherManager#patternMayOverlapWithDevice}
   * is true.
   */
  public boolean patternMatchMeasurement(
      PipePatternFormat format, String pattern, String device, String measurement) {
    return getMatcherByFormat(format).patternMatchMeasurement(pattern, device, measurement);
  }

  public int getRegisterCount() {
    return format2MatcherMap.values().stream()
        .mapToInt(PipeDataRegionMatcher::getRegisterCount)
        .sum();
  }

  public void clear() {
    format2MatcherMap.values().forEach(PipeDataRegionMatcher::clear);
  }

  private PipeDataRegionMatcher getMatcherByFormat(PipePatternFormat format) {
    if (format2MatcherMap.containsKey(format)) {
      return format2MatcherMap.get(format);
    } else {
      LOGGER.warn(
          "Failed to find pattern matcher with format {}, use default: {}",
          format,
          PipePatternFormat.getDefaultFormat());
      return format2MatcherMap.get(PipePatternFormat.getDefaultFormat());
    }
  }

  private PipePatternMatcherManager() {
    format2MatcherMap.put(PipePatternFormat.PREFIX, new PrefixPatternMatcher());
    format2MatcherMap.put(PipePatternFormat.IOTDB, new IotdbPatternMatcher());
  }

  private static class PipePatternMatcherManagerHolder {
    private static final PipePatternMatcherManager INSTANCE = new PipePatternMatcherManager();
  }

  public static PipePatternMatcherManager getInstance() {
    return PipePatternMatcherManagerHolder.INSTANCE;
  }
}
