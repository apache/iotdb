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

package org.apache.iotdb.db.queryengine.transformation.dag.column.binary;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TransformUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isBlobType;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isBool;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl.isCharType;

public abstract class CompareBinaryColumnTransformer extends BinaryColumnTransformer {

  protected CompareBinaryColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder builder, int positionCount) {
    // if either column is all null, append nullCount. For now, a RunLengthEncodeColumn with
    // mayHaveNull == true is all null
    if ((leftColumn.mayHaveNull() && leftColumn instanceof RunLengthEncodedColumn)
        || (rightColumn.mayHaveNull() && rightColumn instanceof RunLengthEncodedColumn)) {
      builder.appendNull(positionCount);
      return;
    }
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        boolean flag = false;
        // compare binary type
        if (isCharType(leftTransformer.getType()) || isBlobType(leftTransformer.getType())) {
          flag =
              transform(
                  TransformUtils.compare(
                      leftTransformer.getType().getBinary(leftColumn, i),
                      rightTransformer.getType().getBinary(rightColumn, i)));
        } else if (isBool(leftTransformer.getType())) {
          flag =
              transform(
                  Boolean.compare(
                      leftTransformer.getType().getBoolean(leftColumn, i),
                      rightTransformer.getType().getBoolean(rightColumn, i)));
        } else {
          final double left = leftTransformer.getType().getDouble(leftColumn, i);
          final double right = rightTransformer.getType().getDouble(rightColumn, i);
          if (!Double.isNaN(left) && !Double.isNaN(right)) {
            flag = transform(Double.compare(left, right));
          }
        }
        returnType.writeBoolean(builder, flag);
      } else {
        builder.appendNull();
      }
    }
  }

  @Override
  protected void checkType() {
    // Boolean type can only be compared by == or !=
    if (leftTransformer.typeNotEquals(TypeEnum.BOOLEAN)
        && rightTransformer.typeNotEquals(TypeEnum.BOOLEAN)) {
      return;
    }
    throw new UnsupportedOperationException("Unsupported Type");
  }

  /**
   * Transform int value of flag to corresponding boolean value.
   *
   * @param flag input int value
   * @return result boolean value
   */
  protected abstract boolean transform(int flag);
}
