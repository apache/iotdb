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

import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_VERSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_VERSION_V2_VALUE;

public enum PipePatternFormat {
  PREFIX,
  IOTDB;

  public static PipePatternFormat getDefaultFormat() {
    return IOTDB;
  }

  public static PipePatternFormat getFormatFromSourceParameters(PipeParameters sourceParameters) {
    if (sourceParameters.hasAttribute(SOURCE_VERSION_KEY)
        && sourceParameters.getString(SOURCE_VERSION_KEY).equals(SOURCE_VERSION_V2_VALUE)) {
      // Pipes with "source.version"="2" can specify the pattern format.
      return valueOf(
          sourceParameters
              .getStringOrDefault(
                  Arrays.asList(EXTRACTOR_PATTERN_FORMAT_KEY, SOURCE_PATTERN_FORMAT_KEY),
                  getDefaultFormat().toString())
              .toUpperCase());
    } else {
      // Pipes without "source.version" attribute always use the PREFIX format.
      return PREFIX;
    }
  }

  /** Get the default pattern of this {@link PipePatternFormat}. */
  public String getDefaultPattern() {
    if (this.equals(PREFIX)) {
      return PipeExtractorConstant.EXTRACTOR_PATTERN_PREFIX_DEFAULT_VALUE;
    } else if (this.equals(IOTDB)) {
      return PipeExtractorConstant.EXTRACTOR_PATTERN_IOTDB_DEFAULT_VALUE;
    } else {
      throw new UnsupportedOperationException("Unsupported pipe pattern type.");
    }
  }
}
