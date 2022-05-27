/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.db.mpp.plan.expression.ternary;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary.BetweenTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.transformer.ternary.TernaryTransformer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BetweenExpression extends TernaryExpression {
  private final boolean isNotBetween;

  public boolean isNotBetween() {
    return isNotBetween;
  }

  public BetweenExpression(
      Expression firstExpression,
      Expression secondExpression,
      Expression thirdExpression,
      boolean isNotBetween) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = isNotBetween;
  }

  public BetweenExpression(
      Expression firstExpression, Expression secondExpression, Expression thirdExpression) {
    super(firstExpression, secondExpression, thirdExpression);
    this.isNotBetween = false;
  }

  public BetweenExpression(ByteBuffer byteBuffer) {
    super(byteBuffer);
    this.isNotBetween = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public final void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> firstExpressions = new ArrayList<>();
    firstExpression.concat(prefixPaths, firstExpressions);

    List<Expression> secondExpressions = new ArrayList<>();
    secondExpression.concat(prefixPaths, secondExpressions);

    List<Expression> thirdExpressions = new ArrayList<>();
    secondExpression.concat(prefixPaths, thirdExpressions);

    reconstruct(firstExpressions, secondExpressions, thirdExpressions, resultExpressions);
  }

  @Override
  public final void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> firstExpressions = new ArrayList<>();
    firstExpression.removeWildcards(wildcardsRemover, firstExpressions);

    List<Expression> secondExpressions = new ArrayList<>();
    secondExpression.removeWildcards(wildcardsRemover, secondExpressions);

    List<Expression> thirdExpressions = new ArrayList<>();
    thirdExpression.removeWildcards(wildcardsRemover, secondExpressions);
    reconstruct(firstExpressions, secondExpressions, thirdExpressions, resultExpressions);
  }

  private void reconstruct(
      List<Expression> firstExpressions,
      List<Expression> secondExpressions,
      List<Expression> thirdExpressions,
      List<Expression> resultExpressions) {
    for (Expression fe : firstExpressions) {
      for (Expression se : secondExpressions)
        for (Expression te : thirdExpressions) {
          switch (operator()) {
            case "between":
              resultExpressions.add(new BetweenExpression(fe, se, te, isNotBetween));
              break;
            default:
              throw new UnsupportedOperationException();
          }
        }
    }
  }

  @Override
  protected TernaryTransformer constructTransformer(
      LayerPointReader firstParentLayerPointReader,
      LayerPointReader secondParentLayerPointReader,
      LayerPointReader thirdParentLayerPointReader) {
    return new BetweenTransformer(
        firstParentLayerPointReader,
        secondParentLayerPointReader,
        thirdParentLayerPointReader,
        isNotBetween);
  }

  @Override
  protected String operator() {
    return "between";
  }

  @Override
  public TSDataType inferTypes(TypeProvider typeProvider) {
    return TSDataType.BOOLEAN;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.BETWEEN;
  }

  protected void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(isNotBetween, byteBuffer);
  }

  public Expression getExpression() {
    return this;
  }
}
