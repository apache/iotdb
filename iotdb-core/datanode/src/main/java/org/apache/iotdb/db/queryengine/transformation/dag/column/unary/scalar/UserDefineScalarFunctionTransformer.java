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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.MultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDFParametersFactory;
import org.apache.iotdb.udf.api.access.ColumnToRowIterator;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.relational.ScalarFunction;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

// TODO(UDSF): encapsulate refect and validate logic
public class UserDefineScalarFunctionTransformer extends MultiColumnTransformer {

  private final ScalarFunction scalarFunction;
  private final List<TSDataType> childrenTypes;

  public UserDefineScalarFunctionTransformer(
      Type returnType,
      String functionName,
      List<Expression> children,
      List<ColumnTransformer> childrenTransformers) {
    super(returnType, childrenTransformers);
    ScalarFunction scalarFunction =
        UDFManagementService.getInstance().reflect(functionName, ScalarFunction.class);
    this.childrenTypes =
        childrenTransformers.stream()
            .map(ColumnTransformer::getType)
            .map(UDFDataTypeTransformer::transformReadTypeToTSDataType)
            .collect(Collectors.toList());
    // TODO: 1、Table UDF 里不应该再用 String Expression 了
    // TODO：2、想办法弄到 attributes
    UDFParameters udfParameters =
        UDFParametersFactory.buildUdfParameters(
            children.stream().map(Expression::toString).collect(Collectors.toList()),
            childrenTypes,
            Collections.emptyMap());
    try {
      //      scalarFunction.validate(new UDFParameterValidator(udfParameters));
      //      scalarFunction.beforeStart(udfParameters, new ScalarFunctionConfig());
    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }

    this.scalarFunction = scalarFunction;
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    ColumnToRowIterator iterator =
        new ColumnToRowIterator(childrenTypes, childrenColumns, positionCount);
    //    while (iterator.hasNextRow()) {
    //      try {
    //        Row row = iterator.next();
    //        Object result = scalarFunction.evaluate(row);
    //        if (result == null) {
    //          builder.appendNull();
    //        } else {
    //          builder.writeObject(result);
    //        }
    //      } catch (Exception e) {
    //        throw new RuntimeException(
    //            "Error occurs when evaluating UDF " + scalarFunction.getClass().getName(), e);
    //      }
    //    }
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount, boolean[] selection) {
    ColumnToRowIterator iterator =
        new ColumnToRowIterator(childrenTypes, childrenColumns, positionCount);
    int i = 0;
    //    while (iterator.hasNextRow()) {
    //      try {
    //        Row row = iterator.next();
    //        Object result = scalarFunction.evaluate(row);
    //        if (selection[i++] || result == null) {
    //          builder.appendNull();
    //        } else {
    //          builder.writeObject(result);
    //        }
    //      } catch (Exception e) {
    //        throw new RuntimeException(
    //            "Error occurs when evaluating UDF " + scalarFunction.getClass().getName(), e);
    //      }
    //    }
  }

  @Override
  protected void checkType() {
    // TODO: implement this method
  }
}
