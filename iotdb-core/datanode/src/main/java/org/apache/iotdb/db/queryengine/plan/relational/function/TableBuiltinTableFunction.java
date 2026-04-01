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

package org.apache.iotdb.db.queryengine.plan.relational.function;

import org.apache.iotdb.commons.udf.builtin.relational.tvf.CapacityTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.CumulateTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.HOPTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.SessionTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.TumbleTableFunction;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.VariationTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.ClassifyTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.ForecastTableFunction;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.PatternMatchTableFunction;
import org.apache.iotdb.udf.api.relational.TableFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableBuiltinTableFunction {
  TUMBLE("tumble"),
  HOP("hop"),
  CUMULATE("cumulate"),
  SESSION("session"),
  VARIATION("variation"),
  CAPACITY("capacity"),
  FORECAST("forecast"),
  PATTERN_MATCH("pattern_match"),
  CLASSIFY("classify");

  private final String functionName;

  TableBuiltinTableFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> BUILT_IN_TABLE_FUNCTION_NAME =
      new HashSet<>(
          Arrays.stream(TableBuiltinTableFunction.values())
              .map(TableBuiltinTableFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getBuiltInTableFunctionName() {
    return BUILT_IN_TABLE_FUNCTION_NAME;
  }

  public static boolean isBuiltInTableFunction(String functionName) {
    return BUILT_IN_TABLE_FUNCTION_NAME.contains(functionName.toLowerCase());
  }

  public static TableFunction getBuiltinTableFunction(String functionName) {
    switch (functionName.toLowerCase()) {
      case "tumble":
        return new TumbleTableFunction();
      case "hop":
        return new HOPTableFunction();
      case "cumulate":
        return new CumulateTableFunction();
      case "session":
        return new SessionTableFunction();
      case "variation":
        return new VariationTableFunction();
      case "pattern_match":
        return new PatternMatchTableFunction();
      case "capacity":
        return new CapacityTableFunction();
      case "forecast":
        return new ForecastTableFunction();
      case "classify":
        return new ClassifyTableFunction();
      default:
        throw new UnsupportedOperationException("Unsupported table function: " + functionName);
    }
  }
}
