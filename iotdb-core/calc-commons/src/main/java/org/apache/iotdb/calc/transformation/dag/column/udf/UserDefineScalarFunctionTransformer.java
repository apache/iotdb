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

package org.apache.iotdb.calc.transformation.dag.column.udf;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.RecordIterator;
import org.apache.iotdb.calc.execution.relational.ColumnTransformerBuilder;
import org.apache.iotdb.calc.i18n.CalcMessages;
import org.apache.iotdb.calc.plan.planner.TableOperatorGenerator.IoTDBLocalFactory;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.multi.MultiColumnTransformer;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionArguments;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.ScalarFunction;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.rpc.TSStatusCode.EXECUTE_UDF_ERROR;

public class UserDefineScalarFunctionTransformer extends MultiColumnTransformer {

  private final ScalarFunction scalarFunction;
  private final FunctionArguments parameters;
  private final List<Type> inputTypes;
  private final IoTDBLocal ioTDBLocal;
  private boolean init = false;

  public UserDefineScalarFunctionTransformer(
      Type returnType,
      ScalarFunction scalarFunction,
      List<ColumnTransformer> childrenTransformers,
      FunctionArguments parameters,
      ColumnTransformerBuilder.Context context) {
    super(returnType, childrenTransformers);
    this.scalarFunction = scalarFunction;
    this.parameters = parameters;
    this.ioTDBLocal =
        IoTDBLocalFactory.createIoTDBLocal(
            context.getIoTDBLocalFactory(),
            context.getSessionInfo(),
            context.getFragmentInstanceId(),
            context.getOuterGlobalQueryId(),
            context.getOuterQueryDeadlineMs());
    this.inputTypes =
        childrenTransformers.stream().map(ColumnTransformer::getType).collect(Collectors.toList());
  }

  private void initIfNeeded() {
    if (init) {
      return;
    }
    init = true;
    try {
      scalarFunction.beforeStart(parameters, ioTDBLocal);
    } catch (UDFException e) {
      throw new IoTDBRuntimeException(e, EXECUTE_UDF_ERROR.getStatusCode());
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    initIfNeeded();
    RecordIterator iterator = new RecordIterator(childrenColumns, inputTypes, positionCount);
    while (iterator.hasNext()) {
      try {
        Object result = scalarFunction.evaluate(iterator.next(), ioTDBLocal);
        if (result == null) {
          builder.appendNull();
        } else {
          builder.writeObject(result);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            CalcMessages.EXCEPTION_ERROR_OCCURS_EVALUATING_USER_DEFINED_SCALAR_FUNCTION_05903C18
                + scalarFunction.getClass().getName(),
            e);
      }
    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    initIfNeeded();
    RecordIterator iterator = new RecordIterator(childrenColumns, inputTypes, positionCount);
    int i = 0;
    while (iterator.hasNext()) {
      try {
        Record input = iterator.next();
        if (selection[i++]) {
          builder.appendNull();
          continue;
        }
        Object result = scalarFunction.evaluate(input, ioTDBLocal);
        if (result == null) {
          builder.appendNull();
        } else {
          builder.writeObject(result);
        }
      } catch (Throwable e) {
        throw new RuntimeException(
            CalcMessages.EXCEPTION_ERROR_OCCURS_EVALUATING_USER_DEFINED_SCALAR_FUNCTION_05903C18
                + scalarFunction.getClass().getName(),
            e);
      }
    }
  }

  @Override
  public void close() {
    // ensure beforeStart was called
    initIfNeeded();
    super.close();
    scalarFunction.beforeDestroy(ioTDBLocal);
    ioTDBLocal.close();
  }

  @Override
  protected void checkType() {
    // do nothing
  }
}
