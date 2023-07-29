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

package org.apache.iotdb.db.pipe.config.constant;

public class PipeExtractorConstant {

  public static final String EXTRACTOR_KEY = "extractor";

  public static final String EXTRACTOR_PATTERN_KEY = "extractor.pattern";
  public static final String EXTRACTOR_PATTERN_DEFAULT_VALUE = "root";

  public static final String EXTRACTOR_HISTORY_ENABLE_KEY = "extractor.history.enable";
  public static final String EXTRACTOR_HISTORY_START_TIME = "extractor.history.start-time";
  public static final String EXTRACTOR_HISTORY_END_TIME = "extractor.history.end-time";

  public static final String EXTRACTOR_REALTIME_ENABLE = "extractor.realtime.enable";
  public static final String EXTRACTOR_REALTIME_MODE = "extractor.realtime.mode";
  public static final String EXTRACTOR_REALTIME_MODE_HYBRID = "hybrid";
  public static final String EXTRACTOR_REALTIME_MODE_FILE = "file";
  public static final String EXTRACTOR_REALTIME_MODE_LOG = "log";
  public static final String EXTRACTOR_REALTIME_MODE_FORCED_LOG = "forced-log";

  private PipeExtractorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
