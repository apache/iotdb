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

package org.apache.iotdb.db.mpp.plan.expression.other;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.multi.builtin.BuiltInScalarFunctionHelper;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.Transformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class CaseWhenThenExpressionHelper implements BuiltInScalarFunctionHelper {
  @Override
  public void checkBuiltInScalarFunctionInputDataType(TSDataType tsDataType)
      throws SemanticException {}

  @Override
  public TSDataType getBuiltInScalarFunctionReturnType(FunctionExpression functionExpression) {
    return null;
  }

  @Override
  public ColumnTransformer getBuiltInScalarFunctionColumnTransformer(
      FunctionExpression expression, ColumnTransformer columnTransformer) {
    return null;
  }

  @Override
  public Transformer getBuiltInScalarFunctionTransformer(
      FunctionExpression expression, LayerPointReader layerPointReader) {
    return null;
  }
}
