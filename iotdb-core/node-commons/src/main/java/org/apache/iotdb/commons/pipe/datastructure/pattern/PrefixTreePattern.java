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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.utils.PathUtils;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PrefixTreePattern extends TreePattern {

  private final String pattern;

  public PrefixTreePattern(final boolean isTreeModelDataAllowedToBeCaptured, final String pattern) {
    super(isTreeModelDataAllowedToBeCaptured);
    this.pattern = pattern != null ? pattern : getDefaultPattern();
  }

  public PrefixTreePattern(final String pattern) {
    this(true, pattern);
  }

  private String getDefaultPattern() {
    return PipeSourceConstant.EXTRACTOR_PATTERN_PREFIX_DEFAULT_VALUE;
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public boolean isRoot() {
    return Objects.isNull(pattern) || this.pattern.equals(this.getDefaultPattern());
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
  public boolean coversDevice(final IDeviceID device) {
    final String deviceStr = device.toString();
    // for example, pattern is root.a.b and device is root.a.b.c
    // in this case, the extractor can be matched without checking the measurements
    return pattern.length() <= deviceStr.length() && deviceStr.startsWith(pattern);
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
  public boolean mayOverlapWithDevice(final IDeviceID device) {
    final String deviceStr = device.toString();
    return
    // for example, pattern is root.a.b and device is root.a.b.c
    // in this case, the extractor can be matched without checking the measurements
    (pattern.length() <= deviceStr.length() && deviceStr.startsWith(pattern))
        // for example, pattern is root.a.b.c and device is root.a.b
        // in this case, the extractor can be selected as candidate, but the measurements should
        // be checked further
        || (pattern.length() > deviceStr.length() && pattern.startsWith(deviceStr));
  }

  @Override
  public boolean matchesMeasurement(final IDeviceID device, String measurement) {
    final String deviceStr = device.toString();
    // We assume that the device is already matched.
    if (pattern.length() <= deviceStr.length()) {
      return true;
    }

    // For example, pattern is "root.a.b.c", device is "root.a.b",
    // then measurements "c" and "cc" can be matched,
    // measurements "d" or "dc" can't be matched.
    final String dotAndMeasurement = TsFileConstant.PATH_SEPARATOR + measurement;
    return
    // low cost check comes first
    pattern.length() <= deviceStr.length() + dotAndMeasurement.length()
        // high cost check comes later
        && dotAndMeasurement.startsWith(pattern.substring(deviceStr.length()));
  }

  @Override
  public List<PartialPath> getBaseInclusionPaths() {
    if (isRoot()) {
      return Collections.singletonList(new PartialPath(new String[] {"root", "**"}));
    }

    final List<PartialPath> paths = new ArrayList<>();
    try {
      // 1. "root.d1"
      paths.add(new PartialPath(pattern));
    } catch (final IllegalPathException ignored) {
    }
    try {
      // 2. "root.d1*"
      paths.add(new PartialPath(pattern + "*"));
    } catch (final IllegalPathException ignored) {
    }
    try {
      // 3. "root.d1.**"
      paths.add(
          new PartialPath(
              pattern + TsFileConstant.PATH_SEPARATOR + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException ignored) {
    }
    try {
      // 4. "root.d1*.**"
      paths.add(
          new PartialPath(
              pattern
                  + "*"
                  + TsFileConstant.PATH_SEPARATOR
                  + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD));
    } catch (final IllegalPathException ignored) {
    }

    return paths;
  }

  @Override
  public String toString() {
    return "PrefixTreePattern{pattern='"
        + pattern
        + "', isTreeModelDataAllowedToBeCaptured="
        + isTreeModelDataAllowedToBeCaptured
        + '}';
  }
}
