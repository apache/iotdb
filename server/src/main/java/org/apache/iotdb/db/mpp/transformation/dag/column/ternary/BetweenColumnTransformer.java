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

package org.apache.iotdb.db.mpp.transformation.dag.column.ternary;

import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.util.TransformUtils;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.Type;

public class BetweenColumnTransformer extends CompareTernaryColumnTransformer {
  private final boolean isNotBetween;

  public BetweenColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer,
      boolean isNotBetween) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
    this.isNotBetween = isNotBetween;
  }

  @Override
  protected void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!firstColumn.isNull(i) && !secondColumn.isNull(i) && !thirdColumn.isNull(i)) {
        boolean flag;
        if (firstColumnTransformer.getType() instanceof BinaryType) {
          flag =
              ((TransformUtils.compare(
                              firstColumnTransformer
                                  .getType()
                                  .getBinary(firstColumn, i)
                                  .getStringValue(),
                              secondColumnTransformer
                                  .getType()
                                  .getBinary(secondColumn, i)
                                  .getStringValue())
                          >= 0)
                      && (TransformUtils.compare(
                              firstColumnTransformer
                                  .getType()
                                  .getBinary(firstColumn, i)
                                  .getStringValue(),
                              thirdColumnTransformer
                                  .getType()
                                  .getBinary(thirdColumn, i)
                                  .getStringValue())
                          <= 0))
                  ^ isNotBetween;
        } else {
          flag =
              ((Double.compare(
                              firstColumnTransformer.getType().getDouble(firstColumn, i),
                              secondColumnTransformer.getType().getDouble(secondColumn, i))
                          >= 0)
                      && (Double.compare(
                              firstColumnTransformer.getType().getDouble(firstColumn, i),
                              thirdColumnTransformer.getType().getDouble(thirdColumn, i))
                          <= 0))
                  ^ isNotBetween;
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
  }
}
