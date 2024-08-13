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

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.TernaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BytesUtils;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SubStringFunctionColumnTransformer.EMPTY_STRING;

public class SubString3ColumnTransformer extends TernaryColumnTransformer {
  public SubString3ColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  protected void doTransform(
      Column firstColumn,
      Column secondColumn,
      Column thirdColumn,
      ColumnBuilder builder,
      int positionCount) {
    Type firstType = firstColumnTransformer.getType();
    Type secondType = secondColumnTransformer.getType();
    Type thirdType = thirdColumnTransformer.getType();
    for (int i = 0; i < positionCount; i++) {
      if (!firstColumn.isNull(i) && !secondColumn.isNull(i) && !thirdColumn.isNull(i)) {
        String currentValue =
            firstType.getBinary(firstColumn, i).getStringValue(TSFileConfig.STRING_CHARSET);
        int beginPosition = secondType.getInt(secondColumn, i);
        int endPosition = thirdType.getInt(thirdColumn, i);
        if (beginPosition >= currentValue.length() || endPosition < 0) {
          currentValue = EMPTY_STRING;
        } else {
          if (endPosition >= currentValue.length()) {
            currentValue = currentValue.substring(beginPosition);
          } else {
            currentValue = currentValue.substring(beginPosition, endPosition);
          }
        }
        builder.writeBinary(BytesUtils.valueOf(currentValue));
      } else {
        builder.appendNull();
      }
    }
  }
}
