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

package org.apache.iotdb.db.constant;

import org.apache.iotdb.commons.path.PartialPath;

/** this class contains several constants used in SQL. */
public class SqlConstant {

  protected SqlConstant() {
    // forbidding instantiation
  }

  private static final String[] SINGLE_ROOT_ARRAY = {"root", "**"};
  private static final String[] SINGLE_TIME_ARRAY = {"time"};
  public static final PartialPath TIME_PATH = new PartialPath(SINGLE_TIME_ARRAY);
  public static final String RESERVED_TIME = "time";
  public static final String NOW_FUNC = "now()";

  public static final String ROOT = "root";
  public static final String QUOTE = "'";
  public static final String DQUOTE = "\"";
  public static final String BOOLEAN_TRUE = "true";
  public static final String BOOLEAN_FALSE = "false";
  public static final String BOOLEAN_TRUE_NUM = "1";
  public static final String BOOLEAN_FALSE_NUM = "0";

  // names of aggregations
  public static final String MIN_TIME = "min_time";
  public static final String MAX_TIME = "max_time";
  public static final String MAX_VALUE = "max_value";
  public static final String MIN_VALUE = "min_value";
  public static final String EXTREME = "extreme";
  public static final String FIRST_VALUE = "first_value";
  public static final String LAST_VALUE = "last_value";
  public static final String COUNT = "count";
  public static final String AVG = "avg";
  public static final String SUM = "sum";
  public static final String COUNT_IF = "count_if";

  // names of scalar functions
  public static final String DIFF = "diff";

  public static final String LAST = "last";
  public static final String TIME_DURATION = "time_duration";

  public static final String CAST_FUNCTION = "CAST";
  public static final String CAST_TYPE = "type";

  public static String[] getSingleRootArray() {
    return SINGLE_ROOT_ARRAY;
  }

  public static boolean isReservedPath(PartialPath pathStr) {
    return pathStr.equals(TIME_PATH);
  }
}
