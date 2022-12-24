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

package org.apache.iotdb.db.protocol.influxdb.constant;

import org.apache.iotdb.db.constant.SqlConstant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** this class contains several constants used in SQL. */
public class InfluxSqlConstant extends SqlConstant {

  private InfluxSqlConstant() {
    // forbidding instantiation
    super();
  }

  public static final String STAR = "*";

  public static final String MAX = "max";
  public static final String MIN = "min";
  public static final String FIRST = "first";
  public static final String MEAN = "mean";
  public static final String MEDIAN = "median";
  public static final String MODE = "mode";
  public static final String SPREAD = "spread";
  public static final String STDDEV = "stddev";
  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.asList(MIN, MAX, FIRST, LAST, MEAN, COUNT, MEDIAN, MODE, SPREAD, STDDEV, SUM));
  private static final Set<String> NATIVE_SELECTOR_FUNCTION_NAMES =
      new HashSet<>(Arrays.asList(MIN, MAX, FIRST, LAST));
  private static final Set<String> ONLY_TRAVERSE_FUNCTION_NAMES =
      new HashSet<>(Arrays.asList(MEDIAN, MODE, SPREAD, STDDEV));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }

  public static Set<String> getNativeSelectorFunctionNames() {
    return NATIVE_SELECTOR_FUNCTION_NAMES;
  }

  public static Set<String> getOnlyTraverseFunctionNames() {
    return ONLY_TRAVERSE_FUNCTION_NAMES;
  }
}
