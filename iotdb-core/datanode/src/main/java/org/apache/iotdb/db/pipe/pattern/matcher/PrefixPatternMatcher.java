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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class PrefixPatternMatcher extends CachedSchemaPatternMatcher {

  @Override
  public boolean patternIsLegal(String pattern) {
    if (!pattern.startsWith("root")) {
      return false;
    }

    try {
      PathUtils.isLegalPath(pattern);
    } catch (IllegalPathException e) {
      try {
        if ("root".equals(pattern) || "root.".equals(pattern)) {
          return true;
        }

        // Split the pattern to nodes.
        String[] pathNodes = StringUtils.splitPreserveAllTokens(pattern, "\\.");

        // Check whether the pattern without last node is legal.
        PathUtils.splitPathToDetachedNodes(
            String.join(".", Arrays.copyOfRange(pathNodes, 0, pathNodes.length - 1)));
        String lastNode = pathNodes[pathNodes.length - 1];

        // Check whether the last node is legal.
        if (!"".equals(lastNode)) {
          Double.parseDouble(lastNode);
        }
      } catch (Exception ignored) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean patternCoverDb(String pattern, String db) {
    return pattern.length() <= db.length() && db.startsWith(pattern);
  }

  @Override
  public boolean patternCoverDevice(String pattern, String device) {
    // for example, pattern is root.a.b and device is root.a.b.c
    // in this case, the extractor can be matched without checking the measurements
    return pattern.length() <= device.length() && device.startsWith(pattern);
  }

  @Override
  public boolean patternMayOverlapWithDevice(String pattern, String device) {
    return (
        // for example, pattern is root.a.b and device is root.a.b.c
        // in this case, the extractor can be matched without checking the measurements
        pattern.length() <= device.length() && device.startsWith(pattern))
        // for example, pattern is root.a.b.c and device is root.a.b
        // in this case, the extractor can be selected as candidate, but the measurements should
        // be checked further
        || (pattern.length() > device.length() && pattern.startsWith(device));
  }

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link
   * PrefixPatternMatcher#patternMayOverlapWithDevice(String, String)} is true.
   */
  @Override
  public boolean patternMatchMeasurement(String pattern, String device, String measurement) {
    // We assume that the device is already matched.
    if (pattern.length() <= device.length()) {
      return true;
    }

    // For example, pattern is "root.a.b.c", device is "root.a.b", then measurements "c" and "cc"
    // can be matched,
    // measurements "d" or "dc" can't be matched.
    String dotAndMeasurement = TsFileConstant.PATH_SEPARATOR + measurement;
    return
    // low cost check comes first
    pattern.length() <= device.length() + dotAndMeasurement.length()
        // high cost check comes later
        && dotAndMeasurement.startsWith(pattern.substring(device.length()));
  }
}
