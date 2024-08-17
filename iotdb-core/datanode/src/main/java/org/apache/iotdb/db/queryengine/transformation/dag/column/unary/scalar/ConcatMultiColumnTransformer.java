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
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.MultiColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.util.List;

public class ConcatMultiColumnTransformer extends MultiColumnTransformer {

  public ConcatMultiColumnTransformer(
      Type returnType, List<ColumnTransformer> columnTransformerList) {
    super(returnType, columnTransformerList);
  }

  @Override
  protected void checkType() {
    // do nothing because the type is checked in tableMetaDataImpl
  }

  @Override
  protected void doTransform(
      List<Column> childrenColumns, ColumnBuilder builder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      boolean isNull = true;
      int length = 0;
      // for loop to check whether all is null, and calc the real length
      for (Column childrenColumn : childrenColumns) {
        if (!childrenColumn.isNull(i)) {
          isNull = false;
          length += childrenColumn.getBinary(i).getValues().length;
        }
      }
      if (isNull) {
        builder.appendNull();
      } else {
        byte[] result = new byte[length];
        // for loop to append the values which is not null
        int index = 0;
        for (Column childrenColumn : childrenColumns) {
          if (!childrenColumn.isNull(i)) {
            byte[] value = childrenColumn.getBinary(i).getValues();
            System.arraycopy(value, 0, result, index, value.length);
            index += value.length;
          }
        }
        builder.writeBinary(new Binary(result));
      }
    }
  }
}
