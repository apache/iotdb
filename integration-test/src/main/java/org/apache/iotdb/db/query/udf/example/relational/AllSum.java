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

package org.apache.iotdb.db.query.udf.example.relational;

import org.apache.iotdb.udf.api.customizer.analysis.ScalarFunctionAnalysis;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFArgumentNotValidException;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import java.util.HashSet;
import java.util.Set;

/** Calculate the sum of all arguments. Only support inputs of INT32,INT64,DOUBLE,FLOAT type. */
public class AllSum implements ScalarFunction {

  private Type outputDataType;

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() < 1) {
      throw new UDFArgumentNotValidException("At least one parameter is required.");
    }
    for (int i = 0; i < arguments.getArgumentsSize(); i++) {
      if (arguments.getDataType(i) != Type.INT32
          && arguments.getDataType(i) != Type.INT64
          && arguments.getDataType(i) != Type.FLOAT
          && arguments.getDataType(i) != Type.DOUBLE) {
        throw new UDFArgumentNotValidException(
            "Only support inputs of INT32,INT64,DOUBLE,FLOAT type.");
      }
    }
    return new ScalarFunctionAnalysis.Builder()
        .outputDataType(inferOutputDataType(arguments))
        .build();
  }

  @Override
  public void beforeStart(FunctionArguments arguments) throws UDFException {
    this.outputDataType = inferOutputDataType(arguments);
  }

  private Type inferOutputDataType(FunctionArguments arguments) {
    Set<Type> inputTypeSet = new HashSet<>();
    for (int i = 0; i < arguments.getArgumentsSize(); i++) {
      inputTypeSet.add(arguments.getDataType(i));
    }
    if (inputTypeSet.contains(Type.DOUBLE)) {
      return Type.DOUBLE;
    } else if (inputTypeSet.contains(Type.FLOAT)) {
      return Type.FLOAT;
    } else if (inputTypeSet.contains(Type.INT64)) {
      return Type.INT64;
    } else {
      return Type.INT32;
    }
  }

  @Override
  public Object evaluate(Record input) {
    double res = 0;
    for (int i = 0; i < input.size(); i++) {
      if (!input.isNull(i)) {
        switch (input.getDataType(i)) {
          case INT32:
            res += input.getInt(i);
            break;
          case INT64:
            res += input.getLong(i);
            break;
          case FLOAT:
            res += input.getFloat(i);
            break;
          case DOUBLE:
            res += input.getDouble(i);
            break;
        }
      }
    }
    switch (outputDataType) {
      case INT32:
        return (int) res;
      case INT64:
        return (long) res;
      case FLOAT:
        return (float) res;
      case DOUBLE:
        return res;
      default:
        throw new RuntimeException("Unexpected output type.");
    }
  }
}
