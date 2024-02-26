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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;

import java.util.HashSet;
import java.util.Set;

public class IotdbPatternMatcher extends CachedSchemaPatternMatcher {

  @Override
  public boolean patternCoverDevice(String pattern, String device) {
    try {
      PartialPath devicePath = new PartialPath(device);
      PartialPath patternPath = new PartialPath(pattern);
      // To cover the device, device should be a prefix of pattern and the last level of pattern
      // should be * or **.
      // For example, pattern is root.**.d1.** and device is root.db1.d1, then pattern covers
      // device.
      return patternPath.matchPrefixPath(devicePath) && patternPath.endWithWildcard();
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  @Override
  public boolean patternOverlapWithDevice(String pattern, String device) {
    try {
      PartialPath devicePath = new PartialPath(device);
      PartialPath patternPath = new PartialPath(pattern);
      // To overlap with pattern, device should be a prefix of pattern.
      // For example, pattern is root.**.s1 and device is root.db1.d1, then they overlap.
      return patternPath.matchPrefixPath(devicePath);
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  @Override
  public boolean patternMatchMeasurement(String pattern, String device, String measurement) {
    try {
      PartialPath measurementPath = new PartialPath(device, measurement);
      PartialPath patternPath = new PartialPath(pattern);
      return patternPath.matchFullPath(measurementPath);
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  @Override
  protected Set<PipeRealtimeDataRegionExtractor> filterExtractorsByDevice(String device) {
    final Set<PipeRealtimeDataRegionExtractor> filteredExtractors = new HashSet<>();

    for (PipeRealtimeDataRegionExtractor extractor : extractors) {
      String pattern = extractor.getPattern();
      try {
        PartialPath devicePath = new PartialPath(device);
        PartialPath extractorPath = new PartialPath(pattern);
        if (
        // To match the extractor, the device path should be a prefix of the extractor path.
        extractorPath.matchPrefixPath(devicePath)) {
          filteredExtractors.add(extractor);
        }
      } catch (IllegalPathException e) {
        LOGGER.warn("Illegal path exception: ", e);
      }
    }

    return filteredExtractors;
  }
}
