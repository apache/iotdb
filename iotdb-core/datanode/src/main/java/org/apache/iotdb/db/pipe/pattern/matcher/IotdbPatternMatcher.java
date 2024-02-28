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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.PathUtils;

public class IotdbPatternMatcher extends CachedSchemaPatternMatcher {

  @Override
  public boolean patternIsLegal(String pattern) {
    if (!pattern.startsWith("root")) {
      return false;
    }

    try {
      PathUtils.isLegalPath(pattern);
    } catch (IllegalPathException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean patternCoverDb(String pattern, String db) {
    try {
      PartialPath patternPath = new PartialPath(pattern);
      return patternPath.include(new PartialPath(db, IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  @Override
  public boolean patternCoverDevice(String pattern, String device) {
    try {
      PartialPath patternPath = new PartialPath(pattern);
      return patternPath.include(new PartialPath(device, IoTDBConstant.ONE_LEVEL_PATH_WILDCARD));
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  @Override
  public boolean patternMayOverlapWithDevice(String pattern, String device) {
    try {
      PartialPath devicePath = new PartialPath(device);
      PartialPath patternPath = new PartialPath(pattern);
      // Another way is to use patternPath.overlapWith("device.*"),
      // there will be no false positives but time cost may be higher.
      return patternPath.matchPrefixPath(devicePath);
    } catch (IllegalPathException e) {
      LOGGER.warn("Illegal path exception: ", e);
      return false;
    }
  }

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link
   * IotdbPatternMatcher#patternMayOverlapWithDevice(String, String)} is true.
   */
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
}
