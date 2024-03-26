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

package org.apache.iotdb.commons.pipe.pattern;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.Objects;

public class IoTDBPipePattern extends PipePattern {

  private final PartialPath patternPartialPath;

  public IoTDBPipePattern(String pattern) {
    super(pattern);

    try {
      patternPartialPath = new PartialPath(getPattern());
    } catch (IllegalPathException e) {
      throw new PipeException("Illegal IoTDBPipePattern: " + getPattern(), e);
    }
  }

  @Override
  public String getDefaultPattern() {
    return PipeExtractorConstant.EXTRACTOR_PATTERN_IOTDB_DEFAULT_VALUE;
  }

  @Override
  public boolean isLegal() {
    if (!pattern.startsWith("root")) {
      return false;
    }

    try {
      PathUtils.isLegalPath(pattern);
      return true;
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean coversDb(String db) {
    try {
      return patternPartialPath.include(
          new PartialPath(db, IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean coversDevice(String device) {
    try {
      return patternPartialPath.include(
          new PartialPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean mayOverlapWithDevice(String device) {
    try {
      // Another way is to use patternPath.overlapWith("device.*"),
      // there will be no false positives but time cost may be higher.
      return patternPartialPath.matchPrefixPath(new PartialPath(device));
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public boolean matchesMeasurement(String device, String measurement) {
    // For aligned timeseries, empty measurement is an alias of the time column.
    if (Objects.isNull(measurement) || measurement.isEmpty()) {
      return false;
    }

    try {
      return patternPartialPath.matchFullPath(new PartialPath(device, measurement));
    } catch (IllegalPathException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return "IoTDBPipePattern" + super.toString();
  }
}
