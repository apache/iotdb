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

package org.apache.iotdb.db.queryengine.transformation.dag.column.ternary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.read.common.type.Type;

import static org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression.getEscapeCharacter;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

public class Like3ColumnTransformer extends TernaryColumnTransformer {
  public Like3ColumnTransformer(
      Type retuenType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(retuenType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  @Override
  protected void checkType() {
    if (!isCharType(firstColumnTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + firstColumnTransformer.getType().getTypeEnum());
    }
    if (!isCharType(secondColumnTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + secondColumnTransformer.getType().getTypeEnum());
    }
    if (!isCharType(thirdColumnTransformer.getType())) {
      throw new UnsupportedOperationException(
          "Unsupported Type: " + thirdColumnTransformer.getType().getTypeEnum());
    }
  }

  @Override
  protected void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      transform(firstColumn, secondColumn, thirdColumn, builder, i);
    }
  }

  @Override
  protected void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]) {
        transform(firstColumn, secondColumn, thirdColumn, builder, i);
      } else {
        builder.appendNull();
      }
    }
  }

  private void transform(
      Column firstColumn, Column secondColumn, Column thirdColumn, ColumnBuilder builder, int i) {
    if (!firstColumn.isNull(i) && !secondColumn.isNull(i) && !thirdColumn.isNull(i)) {
      LikePattern pattern =
          LikePattern.compile(
              secondColumn.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET),
              getEscapeCharacter(
                  thirdColumn.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET)),
              false);
      builder.writeBoolean(
          pattern
              .getMatcher()
              .match(
                  firstColumn.getBinary(i).getValues(), 0, firstColumn.getBinary(i).getLength()));
    } else {
      builder.appendNull();
    }
  }
}
