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

package org.apache.iotdb.rpc.subscription.config;

public class TopicConstant {

  public static final String PATH_KEY = "path";
  public static final String PATH_DEFAULT_VALUE = "root.**";
  public static final String PATTERN_KEY = "pattern";
  public static final String PATTERN_DEFAULT_VALUE = "root";

  public static final String DATABASE_NAME_KEY = "database-name";
  public static final String TABLE_NAME_KEY = "table-name";
  public static final String DATABASE_NAME_DEFAULT_VALUE = ".*";
  public static final String TABLE_NAME_DEFAULT_VALUE = ".*";

  public static final String START_TIME_KEY = "start-time";
  public static final String END_TIME_KEY = "end-time";
  public static final String NOW_TIME_VALUE = "now";

  public static final String MODE_KEY = "mode";
  public static final String MODE_LIVE_VALUE = "live";
  public static final String MODE_SNAPSHOT_VALUE = "snapshot";
  public static final String MODE_DEFAULT_VALUE = MODE_LIVE_VALUE;

  public static final String FORMAT_KEY = "format";
  public static final String FORMAT_SESSION_DATA_SETS_HANDLER_VALUE = "SessionDataSetsHandler";
  public static final String FORMAT_TS_FILE_HANDLER_VALUE = "TsFileHandler";
  public static final String FORMAT_DEFAULT_VALUE = FORMAT_SESSION_DATA_SETS_HANDLER_VALUE;

  public static final String LOOSE_RANGE_KEY = "loose-range";
  public static final String LOOSE_RANGE_TIME_VALUE = "time";
  public static final String LOOSE_RANGE_PATH_VALUE = "path";
  public static final String LOOSE_RANGE_ALL_VALUE = "all";
  public static final String LOOSE_RANGE_DEFAULT_VALUE = "";

  public static final String STRICT_KEY = "strict";
  public static final String STRICT_DEFAULT_VALUE = "true";

  private TopicConstant() {
    throw new IllegalStateException("Utility class");
  }
}
