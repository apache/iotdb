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

public class PipeCollectorConstant {

  public static final String COLLECTOR_KEY = "collector";

  public static final String COLLECTOR_PATTERN_KEY = "collector.pattern";
  public static final String COLLECTOR_PATTERN_DEFAULT_VALUE = "root";

  public static final String COLLECTOR_HISTORY_ENABLE_KEY = "collector.history.enable";
  public static final String COLLECTOR_HISTORY_START_TIME = "collector.history.start-time";
  public static final String COLLECTOR_HISTORY_END_TIME = "collector.history.end-time";

  public static final String COLLECTOR_REALTIME_ENABLE = "collector.realtime.enable";
  public static final String COLLECTOR_REALTIME_MODE = "collector.realtime.mode";
  public static final String COLLECTOR_REALTIME_MODE_HYBRID = "hybrid";
  public static final String COLLECTOR_REALTIME_MODE_FILE = "file";
  public static final String COLLECTOR_REALTIME_MODE_LOG = "log";

  private PipeCollectorConstant() {
    throw new IllegalStateException("Utility class");
  }
}
