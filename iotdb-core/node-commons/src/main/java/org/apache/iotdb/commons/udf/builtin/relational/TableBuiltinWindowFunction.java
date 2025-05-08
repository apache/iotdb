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

package org.apache.iotdb.commons.udf.builtin.relational;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableBuiltinWindowFunction {
  RANK("rank"),
  DENSE_RANK("dense_rank"),
  ROW_NUMBER("row_number"),
  PERCENT_RANK("percent_rank"),
  CUME_DIST("cume_dist"),
  NTILE("ntile"),
  FIRST_VALUE("first_value"),
  LAST_VALUE("last_value"),
  NTH_VALUE("nth_value"),
  LEAD("lead"),
  LAG("lag"),
  ;

  private final String functionName;

  TableBuiltinWindowFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> BUILT_IN_WINDOW_FUNCTION_NAME =
      new HashSet<>(
          Arrays.stream(TableBuiltinWindowFunction.values())
              .map(TableBuiltinWindowFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getBuiltInWindowFunctionName() {
    return BUILT_IN_WINDOW_FUNCTION_NAME;
  }
}
