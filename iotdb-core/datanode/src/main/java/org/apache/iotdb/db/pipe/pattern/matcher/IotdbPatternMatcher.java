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
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IotdbPatternMatcher extends CachedSchemaPatternMatcher {

  @Override
  public Set<PipeRealtimeDataRegionExtractor> match(PipeRealtimeEvent event) {
    final Set<PipeRealtimeDataRegionExtractor> matchedExtractors = new HashSet<>();

    lock.readLock().lock();
    try {
      if (extractors.isEmpty()) {
        return matchedExtractors;
      }

      // HeartbeatEvent will be assigned to all extractors
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        return extractors;
      }

      for (final Map.Entry<String, String[]> entry : event.getSchemaInfo().entrySet()) {
        final String device = entry.getKey();
        final String[] measurements = entry.getValue();

        // 1. try to get matched extractors from cache, if not success, match them by device
        final Set<PipeRealtimeDataRegionExtractor> extractorsFilteredByDevice =
            deviceToExtractorsCache.get(device, this::filterExtractorsByDevice);
        // this would not happen
        if (extractorsFilteredByDevice == null) {
          LOGGER.warn("Match result NPE when handle device {}", device);
          continue;
        }

        // 2. filter matched candidate extractors by measurements
        if (measurements.length == 0) {
          // `measurements` is empty (only in case of tsfile event). match all extractors.
          matchedExtractors.addAll(extractorsFilteredByDevice);
        } else {
          // `measurements` is not empty (only in case of tablet event). match extractors by
          // measurements.
          extractorsFilteredByDevice.forEach(
              extractor -> {
                final String pattern = extractor.getPattern();
                if (patternCoverDevice(pattern, device)) {
                  // Ends with wildcard means the pattern matches all measurements of a device.
                  matchedExtractors.add(extractor);
                } else {
                  for (final String measurement : measurements) {
                    // Ignore null measurement for partial insert
                    if (measurement == null) {
                      continue;
                    }

                    if (patternMatchMeasurement(pattern, device, measurement)) {
                      matchedExtractors.add(extractor);
                      // There would be no more matched extractors because the measurements are
                      // unique
                      break;
                    }
                  }
                }
              });
        }

        if (matchedExtractors.size() == extractors.size()) {
          break;
        }
      }
    } finally {
      lock.readLock().unlock();
    }

    return matchedExtractors;
  }

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
