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

package org.apache.iotdb.db.query.udf.example.relational.iotdblocal;

import org.apache.iotdb.udf.api.IoTDBLocal;

/** Shared IoTDBLocal log markers for integration tests. */
public final class IoTDBLocalLogHelper {

  public static final String CAUSE_MESSAGE = "iotdb-local-it-log-cause";

  public static final String SCALAR_BEFORE_START = "IOTDB_LOCAL_SCALAR_BEFORE_START";
  public static final String SCALAR_EVALUATE = "IOTDB_LOCAL_SCALAR_EVALUATE";
  public static final String SCALAR_BEFORE_DESTROY = "IOTDB_LOCAL_SCALAR_BEFORE_DESTROY";

  public static final String UDAF_BEFORE_START = "IOTDB_LOCAL_UDAF_BEFORE_START";
  public static final String UDAF_ADD_INPUT = "IOTDB_LOCAL_UDAF_ADD_INPUT";
  public static final String UDAF_COMBINE_STATE = "IOTDB_LOCAL_UDAF_COMBINE_STATE";
  public static final String UDAF_OUTPUT_FINAL = "IOTDB_LOCAL_UDAF_OUTPUT_FINAL";
  public static final String UDAF_BEFORE_DESTROY = "IOTDB_LOCAL_UDAF_BEFORE_DESTROY";

  public static final String TVF_BEFORE_START = "IOTDB_LOCAL_TVF_BEFORE_START";
  public static final String TVF_PROCESS = "IOTDB_LOCAL_TVF_PROCESS";
  public static final String TVF_BEFORE_DESTROY = "IOTDB_LOCAL_TVF_BEFORE_DESTROY";

  private IoTDBLocalLogHelper() {}

  public static void logAllApis(IoTDBLocal local, String phase) {
    RuntimeException cause = new RuntimeException(CAUSE_MESSAGE);
    local.info(phase + "_INFO_PLAIN");
    local.info(phase + "_INFO_FORMAT loaded {} rows", 3);
    local.info(phase + "_INFO_CAUSE", cause);
    local.warn(phase + "_WARN_PLAIN");
    local.warn(phase + "_WARN_FORMAT warn {} {}", "a", "b");
    local.warn(phase + "_WARN_CAUSE", cause);
    local.error(phase + "_ERROR_PLAIN");
    local.error(phase + "_ERROR_FORMAT error code={}", 500);
    local.error(phase + "_ERROR_CAUSE", cause);
  }

  public static String infoPlain(String phase) {
    return phase + "_INFO_PLAIN";
  }

  public static String infoFormat(String phase) {
    return phase + "_INFO_FORMAT loaded 3 rows";
  }

  public static String infoCause(String phase) {
    return phase + "_INFO_CAUSE";
  }

  public static String warnPlain(String phase) {
    return phase + "_WARN_PLAIN";
  }

  public static String warnFormat(String phase) {
    return phase + "_WARN_FORMAT warn a b";
  }

  public static String warnCause(String phase) {
    return phase + "_WARN_CAUSE";
  }

  public static String errorPlain(String phase) {
    return phase + "_ERROR_PLAIN";
  }

  public static String errorFormat(String phase) {
    return phase + "_ERROR_FORMAT error code=500";
  }

  public static String errorCause(String phase) {
    return phase + "_ERROR_CAUSE";
  }
}
