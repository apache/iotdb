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
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.BinaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import static org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.ConcatColumnTransformer.concat;

public class Concat2ColumnTransformer extends BinaryColumnTransformer {

  public Concat2ColumnTransformer(
      Type returnType, ColumnTransformer leftTransformer, ColumnTransformer rightTransformer) {
    super(returnType, leftTransformer, rightTransformer);
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  @Override
  protected void doTransform(
      Column leftColumn, Column rightColumn, ColumnBuilder columnBuilder, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!leftColumn.isNull(i) && !rightColumn.isNull(i)) {
        columnBuilder.writeBinary(
            new Binary(
                concat(leftColumn.getBinary(i).getValues(), rightColumn.getBinary(i).getValues())));
      } else if (!leftColumn.isNull(i)) {
        columnBuilder.writeBinary(leftColumn.getBinary(i));
      } else if (!rightColumn.isNull(i)) {
        columnBuilder.writeBinary(rightColumn.getBinary(i));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(
      Column leftColumn,
      Column rightColumn,
      ColumnBuilder builder,
      int positionCount,
      boolean[] selection) {}
}
