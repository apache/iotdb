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

import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

public class ScalarFunctionExample implements ScalarFunction {
  @Override
  public void validate(FunctionParameters parameters) throws Exception {
    if (parameters.getChildExpressionsSize() > 1) {
      throw new Exception("Only one child expression is allowed");
    }
  }

  @Override
  public Type inferOutputType(FunctionParameters parameters) {
    return parameters.getChildExpressionDataTypes().get(0);
  }

  @Override
  public Object evaluate(Record input) throws Exception {
    return input.getObject(0);
  }
}
