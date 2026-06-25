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
import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

/** Exercises all IoTDBLocal log APIs with distinctive markers for integration tests. */
public class IoTDBLocalLogFunction implements ScalarFunction {

  public static final String INFO_PLAIN = "IOTDB_LOCAL_IT_INFO_PLAIN";
  public static final String INFO_FORMAT = "IOTDB_LOCAL_IT_INFO_FORMAT loaded 3 rows";
  public static final String INFO_CAUSE = "IOTDB_LOCAL_IT_INFO_CAUSE";
  public static final String WARN_PLAIN = "IOTDB_LOCAL_IT_WARN_PLAIN";
  public static final String WARN_FORMAT = "IOTDB_LOCAL_IT_WARN_FORMAT warn ab";
  public static final String WARN_CAUSE = "IOTDB_LOCAL_IT_WARN_CAUSE";
  public static final String ERROR_PLAIN = "IOTDB_LOCAL_IT_ERROR_PLAIN";
  public static final String ERROR_FORMAT = "IOTDB_LOCAL_IT_ERROR_FORMAT error code=500";
  public static final String ERROR_CAUSE = "IOTDB_LOCAL_IT_ERROR_CAUSE";

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.STRING).build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments, IoTDBLocal local) {
    RuntimeException cause = new RuntimeException("iotdb-local-it-log-cause");
    local.info(INFO_PLAIN);
    local.info("IOTDB_LOCAL_IT_INFO_FORMAT loaded {} rows", 3);
    local.info(INFO_CAUSE, cause);
    local.warn(WARN_PLAIN);
    local.warn("IOTDB_LOCAL_IT_WARN_FORMAT warn {} {}", "a", "b");
    local.warn(WARN_CAUSE, cause);
    local.error(ERROR_PLAIN);
    local.error("IOTDB_LOCAL_IT_ERROR_FORMAT error code={}", 500);
    local.error(ERROR_CAUSE, cause);
  }

  @Override
  public Object evaluate(Record input) {
    return "ok";
  }
}
