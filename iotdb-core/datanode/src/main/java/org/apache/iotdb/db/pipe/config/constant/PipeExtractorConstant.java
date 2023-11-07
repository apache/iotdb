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

import static org.apache.iotdb.commons.conf.IoTDBConstant.GB;

public class PipeExtractorConstant {

  public static final String EXTRACTOR_KEY = "extractor";
  public static final String SOURCE_KEY = "source";

  public static final String EXTRACTOR_PATTERN_KEY = "extractor.pattern";
  public static final String SOURCE_PATTERN_KEY = "source.pattern";
  public static final String EXTRACTOR_PATTERN_DEFAULT_VALUE = "root";

  public static final String EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY =
      "extractor.forwarding-pipe-requests";
  public static final String SOURCE_FORWARDING_PIPE_REQUESTS_KEY =
      "source.forwarding-pipe-requests";
  public static final boolean EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE = true;

  public static final String EXTRACTOR_HISTORY_ENABLE_KEY = "extractor.history.enable";
  public static final String SOURCE_HISTORY_ENABLE_KEY = "source.history.enable";
  public static final boolean EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_HISTORY_START_TIME_KEY = "extractor.history.start-time";
  public static final String SOURCE_HISTORY_START_TIME_KEY = "source.history.start-time";
  public static final String EXTRACTOR_HISTORY_END_TIME_KEY = "extractor.history.end-time";
  public static final String SOURCE_HISTORY_END_TIME_KEY = "source.history.end-time";
  public static final String EXTRACTOR_HISTORY_LOOSE_RANGE_KEY = "extractor.history.loose-range";
  public static final String SOURCE_HISTORY_LOOSE_RANGE_KEY = "source.history.loose-range";

  public static final String EXTRACTOR_REALTIME_ENABLE_KEY = "extractor.realtime.enable";
  public static final String SOURCE_REALTIME_ENABLE_KEY = "source.realtime.enable";
  public static final boolean EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE = true;
  public static final String EXTRACTOR_REALTIME_MODE_KEY = "extractor.realtime.mode";
  public static final String SOURCE_REALTIME_MODE_KEY = "source.realtime.mode";
  public static final String EXTRACTOR_REALTIME_MODE_HYBRID_VALUE = "hybrid";
  public static final String EXTRACTOR_REALTIME_MODE_FILE_VALUE = "file";
  public static final String EXTRACTOR_REALTIME_MODE_LOG_VALUE = "log";
  public static final String EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE = "forced-log";
  public static final String EXTRACTOR_LOCAL_SPLIT_ENABLE_KEY = "extractor.local-split.enable";
  public static final String EXTRACTOR_SPLIT_MAX_CONCURRENT_FILE_KEY =
      "extractor.split.max-concurrent-file";
  public static final int EXTRACTOR_SPLIT_MAX_CONCURRENT_FILE_DEFAULT_VALUE = 16;
  public static final String EXTRACTOR_SPLIT_MAX_FILE_BATCH_SIZE_KEY =
      "extractor.split.max-file-batch-size";
  public static final long EXTRACTOR_SPLIT_MAX_FILE_BATCH_SIZE_DEFAULT_VALUE = 4 * GB;

  private PipeExtractorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
