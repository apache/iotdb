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
    return getMatcherByFormat(event.getPatternFormat()).match(event);
  }

  public boolean patternIsLegal(PipePatternFormat format, String pattern) {
    return getMatcherByFormat(format).patternIsLegal(pattern);
  }

  public boolean patternCoverDevice(PipePatternFormat format, String pattern, String device) {
    return getMatcherByFormat(format).patternCoverDevice(pattern, device);
  }

  public boolean patternOverlapWithDevice(PipePatternFormat format, String pattern, String device) {
    return getMatcherByFormat(format).patternOverlapWithDevice(pattern, device);
  }

  /**
   * NOTE: this is only called when {@link
   * PipePatternMatcherManager#patternOverlapWithDevice(PipePatternFormat, String, String)} is true.
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
