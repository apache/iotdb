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

package org.apache.iotdb.db.pipe.pattern;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_VERSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_VERSION_V2_VALUE;

public abstract class PipePattern {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipePattern.class);

  protected final String pattern;

  protected PipePattern(String pattern) {
    this.pattern = pattern != null ? pattern : getDefaultPattern();
  }

  public String getPattern() {
    return pattern;
  }

  public boolean isRoot() {
    return Objects.isNull(pattern) || this.pattern.equals(this.getDefaultPattern());
  }

  public static PipePattern getPipePatternFromSourceParameters(PipeParameters sourceParameters) {
    String pattern = sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY);
    if (sourceParameters.hasAttribute(SOURCE_VERSION_KEY)
        && sourceParameters.getString(SOURCE_VERSION_KEY).equals(SOURCE_VERSION_V2_VALUE)) {
      // Pipes with "source.version"="2" can specify the pattern format.
      String format =
          sourceParameters.getStringByKeys(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY);
      if (Objects.isNull(format)) {
        LOGGER.info("Pattern format not specified, use iotdb format by default.");
        return new IotdbPipePattern(pattern);
      }

      switch (format.toLowerCase()) {
        case EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE:
          return new IotdbPipePattern(pattern);
        case EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE:
          return new PrefixPipePattern(pattern);
        default:
          LOGGER.info("Unknown pattern format: {}, use iotdb format by default.", format);
          return new IotdbPipePattern(pattern);
      }
    } else {
      // Pipes without "source.version" attribute always use the PREFIX format.
      return new PrefixPipePattern(pattern);
    }
  }

  public abstract String getDefaultPattern();

  /** Check if this pattern is legal. Different pattern type may have different rules. */
  public abstract boolean isLegal();

  /** Check if this pattern matches all time-series under a database. */
  public abstract boolean coversDb(String db);

  /** Check if a device's all measurements are covered by this pattern. */
  public abstract boolean coversDevice(String device);

  /**
   * Check if a device may have some measurements matched by the pattern.
   *
   * <p>NOTE: this is just a loose check and may have false positives. To further check if a
   * measurement matches the pattern, please use {@link PipePattern#matchesMeasurement} after this.
   */
  public abstract boolean mayOverlapWithDevice(String device);

  /**
   * Check if a full path with device and measurement can be matched by pattern.
   *
   * <p>NOTE: this is only called when {@link PipePattern#mayOverlapWithDevice} is true.
   */
  public abstract boolean matchesMeasurement(String device, String measurement);

  @Override
  public String toString() {
    return "{pattern='" + pattern + "'}";
  }
}
