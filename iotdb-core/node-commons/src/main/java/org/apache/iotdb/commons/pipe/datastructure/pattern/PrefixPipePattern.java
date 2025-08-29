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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.utils.PathUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.common.constant.TsFileConstant;

import java.util.Arrays;

public class PrefixPipePattern extends PipePattern {

  public PrefixPipePattern(final String pattern) {
    super(pattern);
  }

  @Override
  public String getDefaultPattern() {
    return PipeSourceConstant.EXTRACTOR_PATTERN_PREFIX_DEFAULT_VALUE;
  }

  @Override
  public boolean isLegal() {
    if (!pattern.startsWith("root")) {
      return false;
    }

    try {
      PathUtils.isLegalPath(pattern);
    } catch (final IllegalPathException e) {
      try {
        if ("root".equals(pattern) || "root.".equals(pattern)) {
          return true;
        }

        // Split the pattern to nodes.
        final String[] pathNodes = StringUtils.splitPreserveAllTokens(pattern, "\\.");

        // Check whether the pattern without last node is legal.
        PathUtils.splitPathToDetachedNodes(
            String.join(".", Arrays.copyOfRange(pathNodes, 0, pathNodes.length - 1)));
        final String lastNode = pathNodes[pathNodes.length - 1];

        // Check whether the last node is legal.
        if (!"".equals(lastNode)) {
          Double.parseDouble(lastNode);
        }
      } catch (final Exception ignored) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean coversDb(final String db) {
    return pattern.length() <= db.length() && db.startsWith(pattern);
  }

  @Override
  public boolean coversDevice(final String device) {
    // for example, pattern is root.a.b and device is root.a.b.c
    // in this case, the extractor can be matched without checking the measurements
    return pattern.length() <= device.length() && device.startsWith(pattern);
  }

  @Override
  public boolean mayOverlapWithDevice(final String device) {
    return (
        // for example, pattern is root.a.b and device is root.a.b.c
        // in this case, the extractor can be matched without checking the measurements
        pattern.length() <= device.length() && device.startsWith(pattern))
        // for example, pattern is root.a.b.c and device is root.a.b
        // in this case, the extractor can be selected as candidate, but the measurements should
        // be checked further
        || (pattern.length() > device.length() && pattern.startsWith(device));
  }

  @Override
  public boolean mayOverlapWithDb(final String db) {
    return
    // for example, pattern is root.a.b and db is root.a.b.c
    (pattern.length() <= db.length() && db.startsWith(pattern))
        // for example, pattern is root.a.b.c and db is root.a.b
        || (pattern.length() > db.length() && pattern.startsWith(db));
  }

  @Override
  public boolean matchesMeasurement(final String device, final String measurement) {
    // We assume that the device is already matched.
    if (pattern.length() <= device.length()) {
      return true;
    }

    // For example, pattern is "root.a.b.c", device is "root.a.b",
    // then measurements "c" and "cc" can be matched,
    // measurements "d" or "dc" can't be matched.
    final String dotAndMeasurement = TsFileConstant.PATH_SEPARATOR + measurement;
    return
    // low cost check comes first
    pattern.length() <= device.length() + dotAndMeasurement.length()
        // high cost check comes later
        && dotAndMeasurement.startsWith(pattern.substring(device.length()));
  }

  @Override
  public String toString() {
    return "PrefixPipePattern" + super.toString();
  }
}
