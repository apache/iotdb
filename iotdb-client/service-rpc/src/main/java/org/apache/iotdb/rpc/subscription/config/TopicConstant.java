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

  public static final String START_TIME_KEY = "start-time";
  public static final String END_TIME_KEY = "end-time";
  public static final String NOW_TIME_VALUE = "now";

  public static final String FORMAT_KEY = "format";
  public static final String FORMAT_SESSION_DATA_SETS_HANDLER_VALUE = "SessionDataSetsHandler";
  public static final String FORMAT_TS_FILE_HANDLER_VALUE = "TsFileHandler";
  public static final String FORMAT_DEFAULT_VALUE = FORMAT_SESSION_DATA_SETS_HANDLER_VALUE;

  private TopicConstant() {
    throw new IllegalStateException("Utility class");
  }
}
