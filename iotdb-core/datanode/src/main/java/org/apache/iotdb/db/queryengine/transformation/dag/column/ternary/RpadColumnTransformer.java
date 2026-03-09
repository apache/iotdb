/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.utils.BytePaddingUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

public class RpadColumnTransformer extends TernaryColumnTransformer {

  public RpadColumnTransformer(
      Type returnType,
      ColumnTransformer firstColumnTransformer,
      ColumnTransformer secondColumnTransformer,
      ColumnTransformer thirdColumnTransformer) {
    super(returnType, firstColumnTransformer, secondColumnTransformer, thirdColumnTransformer);
  }

  @Override
  protected void doTransform(
      Column inputData,
      Column targetLength,
      Column paddingData,
      ColumnBuilder builder,
      int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!inputData.isNull(i) && !targetLength.isNull(i) && !paddingData.isNull(i)) {
        byte[] bytes =
            BytePaddingUtils.padBytes(
                inputData.getBinary(i).getValues(),
                targetLength.getLong(i),
                paddingData.getBinary(i).getValues(),
                inputData.getBinary(i).getValues().length,
                "Rpad");
        builder.writeBinary(new Binary(bytes));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column inputData,
      Column targetLength,
      Column paddingData,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {
    for (int i = 0; i < positionCount; i++) {
      if (selection[i]
          && !inputData.isNull(i)
          && !targetLength.isNull(i)
          && !paddingData.isNull(i)) {
        byte[] bytes =
            BytePaddingUtils.padBytes(
                inputData.getBinary(i).getValues(),
                targetLength.getLong(i),
                paddingData.getBinary(i).getValues(),
                inputData.getBinary(i).getValues().length,
                "Rpad");
        builder.writeBinary(new Binary(bytes));
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {}
}
