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

package org.apache.iotdb.commons.pipe.config.constant;

public class PipeExtractorConstant {

  public static final String EXTRACTOR_KEY = "extractor";
  public static final String SOURCE_KEY = "source";

  public static final String EXTRACTOR_INCLUSION_KEY = "extractor.inclusion";
  public static final String SOURCE_INCLUSION_KEY = "source.inclusion";
  public static final String EXTRACTOR_INCLUSION_DATA_VALUE = "data";
  public static final String EXTRACTOR_INCLUSION_SCHEMA_VALUE = "schema";
  public static final String EXTRACTOR_INCLUSION_TTL_VALUE = "ttl";
  public static final String EXTRACTOR_INCLUSION_FUNCTION_VALUE = "function";
  public static final String EXTRACTOR_INCLUSION_TRIGGER_VALUE = "trigger";
  public static final String EXTRACTOR_INCLUSION_MODEL_VALUE = "model";
  public static final String EXTRACTOR_INCLUSION_AUTHORITY_VALUE = "authority";
  public static final String EXTRACTOR_INCLUSION_DEFAULT_VALUE = "data, schema";

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
  public static final String EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE = "stream";
  public static final String EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE = "batch";

  private PipeExtractorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
