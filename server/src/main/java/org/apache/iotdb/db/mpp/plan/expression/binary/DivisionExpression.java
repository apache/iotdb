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

package org.apache.iotdb.db.mpp.plan.expression.binary;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.ArithmeticDivisionColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.binary.ArithmeticBinaryTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.binary.ArithmeticDivisionTransformer;
import org.apache.iotdb.tsfile.read.common.type.Type;

import java.nio.ByteBuffer;

public class DivisionExpression extends ArithmeticBinaryExpression {

  public DivisionExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  public DivisionExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
  }

  @Override
  protected ColumnTransformer getConcreteBinaryTransformer(
      ColumnTransformer leftColumnTransformer,
      ColumnTransformer rightColumnTransformer,
      Type type) {
    return new ArithmeticDivisionColumnTransformer(
        this, type, leftColumnTransformer, rightColumnTransformer);
  }

  @Override
  protected ArithmeticBinaryTransformer constructTransformer(
      LayerPointReader leftParentLayerPointReader, LayerPointReader rightParentLayerPointReader) {
    return new ArithmeticDivisionTransformer(
        leftParentLayerPointReader, rightParentLayerPointReader);
  }

  @Override
  protected String operator() {
    return "/";
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.DIVISION;
  }
}
