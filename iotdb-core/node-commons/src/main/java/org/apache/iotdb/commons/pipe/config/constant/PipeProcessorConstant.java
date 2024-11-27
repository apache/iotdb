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

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class PipeProcessorConstant {

  public static final String PROCESSOR_KEY = "processor";

  public static final String PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY =
      "processor.down-sampling.split-file";
  public static final boolean PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE = false;
  public static final String PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY =
      "processor.down-sampling.memory-limit-in-bytes";
  public static final long PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE = 16 * MB;

  public static final String PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY =
      "processor.output.max-delay-seconds";
  public static final long PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE = -1;
  public static final String PROCESSOR_OUTPUT_DATABASE_KEY = "processor.output.database";
  public static final String PROCESSOR_OUTPUT_DATABASE_DEFAULT_VALUE = "";
  public static final String PROCESSOR_OUTPUT_MEASUREMENTS_KEY = "processor.output.measurements";
  public static final String PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE = "";
  public static final String PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_KEY =
      "processor.output.min-report-interval-seconds";
  public static final long PROCESSOR_OUTPUT_MIN_REPORT_INTERVAL_SECONDS_DEFAULT_VALUE = 30;
  public static final String PROCESSOR_OPERATORS_KEY = "processor.operators";
  public static final String PROCESSOR_OPERATORS_DEFAULT_VALUE = "";
  public static final String PROCESSOR_WINDOWING_STRATEGY_KEY = "processor.windowing-strategy";
  public static final String PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE = "tumbling";
  public static final String PROCESSOR_SLIDING_BOUNDARY_TIME_KEY =
      "processor.sliding.boundary-time";
  public static final long PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE = 0;
  public static final String PROCESSOR_SLIDING_SECONDS_KEY = "processor.sliding.seconds";
  public static final long PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE = 120;

  public static final String PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_KEY =
      "processor.tumbling-time.interval-seconds";
  public static final long PROCESSOR_TUMBLING_TIME_INTERVAL_SECONDS_DEFAULT_VALUE = 60;

  public static final String PROCESSOR_SDT_COMPRESSION_DEVIATION_KEY =
      "processor.sdt.compression-deviation";
  public static final double PROCESSOR_SDT_COMPRESSION_DEVIATION_DEFAULT_VALUE = 0;
  public static final String PROCESSOR_SDT_MIN_TIME_INTERVAL_KEY =
      "processor.sdt.min-time-interval";
  public static final long PROCESSOR_SDT_MIN_TIME_INTERVAL_DEFAULT_VALUE = 0;
  public static final String PROCESSOR_SDT_MAX_TIME_INTERVAL_KEY =
      "processor.sdt.max-time-interval";
  public static final long PROCESSOR_SDT_MAX_TIME_INTERVAL_DEFAULT_VALUE = Long.MAX_VALUE;

  public static final String PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION =
      "processor.changing-value.compression-deviation";
  public static final double PROCESSOR_CHANGING_VALUE_COMPRESSION_DEVIATION_DEFAULT_VALUE = 0;
  public static final String PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_KEY =
      "processor.changing-value.min-time-interval";
  public static final long PROCESSOR_CHANGING_VALUE_MIN_TIME_INTERVAL_DEFAULT_VALUE = 0;
  public static final String PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_KEY =
      "processor.changing-value.max-time-interval";
  public static final long PROCESSOR_CHANGING_VALUE_MAX_TIME_INTERVAL_DEFAULT_VALUE =
      Long.MAX_VALUE;

  public static final String _PROCESSOR_OUTPUT_SERIES_KEY = "processor.output-series";
  public static final String PROCESSOR_OUTPUT_SERIES_KEY = "processor.output.series";

  public static final String PROCESSOR_RENAME_DATABASE_NEW_DB_NAME = "processor.new-db-name";

  private PipeProcessorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
