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

package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.common.rpc.thrift.FunctionType;
import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.TableFunction;

public class TableUDFUtils {

  private TableUDFUtils() {
    // private constructor
  }

  public static boolean isScalarFunction(String functionName) {
    UDFInformation information =
        UDFManagementService.getInstance().getUDFInformation(Model.TABLE, functionName);
    return FunctionType.SCALAR.equals(information.getUdfType().getType());
  }

  public static boolean isAggregateFunction(String functionName) {
    UDFInformation information =
        UDFManagementService.getInstance().getUDFInformation(Model.TABLE, functionName);
    return FunctionType.AGGREGATE.equals(information.getUdfType().getType());
  }

  public static boolean isTableFunction(String functionName) {
    UDFInformation information =
        UDFManagementService.getInstance().getUDFInformation(Model.TABLE, functionName);
    return FunctionType.TABLE.equals(information.getUdfType().getType());
  }

  public static ScalarFunction getScalarFunction(String functionName) throws UDFException {
    return UDFManagementService.getInstance().reflect(functionName, ScalarFunction.class);
  }

  public static AggregateFunction getAggregateFunction(String functionName) throws UDFException {
    return UDFManagementService.getInstance().reflect(functionName, AggregateFunction.class);
  }

  public static TableFunction getTableFunction(String functionName) throws UDFException {
    return UDFManagementService.getInstance().reflect(functionName, TableFunction.class);
  }
}
