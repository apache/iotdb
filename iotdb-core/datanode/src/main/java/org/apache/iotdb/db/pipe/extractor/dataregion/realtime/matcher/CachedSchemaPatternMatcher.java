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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.matcher;

import org.apache.iotdb.commons.pipe.datastructure.pattern.PipePattern;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class CachedSchemaPatternMatcher {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CachedSchemaPatternMatcher.class);

  public static boolean match(
      final PipeRealtimeEvent event, final PipeRealtimeDataRegionExtractor extractor) {
    // HeartbeatEvent will be assigned to all extractors
    if (event.getEvent() instanceof PipeHeartbeatEvent) {
      return true;
    }

    // Deletion event will be assigned to extractors listened to it
    if (event.getEvent() instanceof PipeSchemaRegionWritePlanEvent) {
      return extractor.shouldExtractDeletion();
    }

    for (final Map.Entry<String, String[]> entry : event.getSchemaInfo().entrySet()) {
      final String device = entry.getKey();
      final String[] measurements = entry.getValue();

      if (!filterByDevice(device, extractor)) {
        return false;
      }

      // 2. filter matched candidate extractors by measurements
      if (measurements.length == 0) {
        // `measurements` is empty (only in case of tsfile event). match all extractors.
        //
        // case 1: the pattern can match all measurements of the device.
        // in this case, the extractor can be matched without checking the measurements.
        //
        // case 2: the pattern may match some measurements of the device.
        // in this case, we can't get all measurements efficiently here,
        // so we just ASSUME the extractor matches and do more checks later.
        return true;
      } else {
        final PipePattern pattern = extractor.getPipePattern();
        if (Objects.isNull(pattern) || pattern.isRoot() || pattern.coversDevice(device)) {
          // The pattern can match all measurements of the device.
          return true;
        } else {
          for (final String measurement : measurements) {
            // Ignore null measurement for partial insert
            if (measurement == null) {
              continue;
            }

            if (pattern.matchesMeasurement(device, measurement)) {
              return true;
            }
          }
        }
      }
    }

    return false;
  }

  private static boolean filterByDevice(
      final String device, final PipeRealtimeDataRegionExtractor extractor) {
    return extractor.shouldExtractInsertion()
        && (Objects.isNull(extractor.getPipePattern())
            || extractor.getPipePattern().mayOverlapWithDevice(device));
  }

  private CachedSchemaPatternMatcher() {
    // Utility class
  }
}
