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
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import java.time.LocalDate;

public class DatePlus implements ScalarFunction {

  @Override
  public ScalarFunctionAnalysis analyze(FunctionArguments arguments)
      throws UDFArgumentNotValidException {
    if (arguments.getArgumentsSize() != 2) {
      throw new UDFArgumentNotValidException("Only two parameter is required.");
    }
    if (arguments.getDataType(0) != Type.DATE) {
      throw new UDFArgumentNotValidException("The first parameter should be DATE type.");
    }
    if (arguments.getDataType(1) != Type.INT32 && arguments.getDataType(1) != Type.INT64) {
      throw new UDFArgumentNotValidException("The second parameter should be INT type.");
    }
    return new ScalarFunctionAnalysis.Builder().outputDataType(Type.DATE).build();
  }

  @Override
  public Object evaluate(Record input) {
    LocalDate date = input.getLocalDate(0);
    int days = input.getInt(1);
    return date.plusDays(days);
  }
}
