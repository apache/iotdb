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

package org.apache.iotdb.udf;

import org.apache.iotdb.udf.api.customizer.config.ScalarFunctionConfig;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFParameterNotValidException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

/** This is an internal example of the ScalarFunction implementation. */
public class ScalarFunctionExample implements ScalarFunction {
  /**
   * CREATE DATABASE test;
   *
   * <p>USE test;
   *
   * <p>CREATE TABLE t1(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT);
   *
   * <p>INSERT INTO t1(time, device_id, s1, s2) VALUES (1, 'd1', 'a', 1), (2, 'd1', null, 2), (3,
   * 'd1', 'c', null);
   *
   * <p>CREATE FUNCTION contain_null AS 'org.apache.iotdb.udf.ScalarFunctionExample';
   *
   * <p>SHOW FUNCTIONS;
   *
   * <p>SELECT time, device_id, s1, s2, contain_null(s1, s2) as contain_null, contain_null(s1) as
   * s1_isnull, contain_null(s2) as s2_isnull FROM t1;
   */
  @Override
  public void validate(FunctionParameters parameters) throws UDFException {
    if (parameters.getChildExpressionsSize() < 1) {
      throw new UDFParameterNotValidException("At least one parameter is required.");
    }
  }

  @Override
  public void beforeStart(FunctionParameters parameters, ScalarFunctionConfig configurations) {
    configurations.setOutputDataType(Type.BOOLEAN);
  }

  @Override
  public Object evaluate(Record input) throws UDFException {
    for (int i = 0; i < input.size(); i++) {
      if (input.isNull(i)) {
        return true;
      }
    }
    return false;
  }
}
