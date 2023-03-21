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

package org.apache.iotdb.db.mpp.transformation.dag.column;

import org.apache.iotdb.db.mpp.transformation.dag.column.binary.WhenThenColumnTransformer;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.BinaryType;
import org.apache.iotdb.tsfile.read.common.type.BooleanType;
import org.apache.iotdb.tsfile.read.common.type.DoubleType;
import org.apache.iotdb.tsfile.read.common.type.FloatType;
import org.apache.iotdb.tsfile.read.common.type.IntType;
import org.apache.iotdb.tsfile.read.common.type.LongType;
import org.apache.iotdb.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;

public class CaseWhenThenColumnTransformer extends ColumnTransformer {

  List<WhenThenColumnTransformer> whenThenTransformers;

  ColumnTransformer elseTransformer;

  public CaseWhenThenColumnTransformer(
      Type returnType,
      List<WhenThenColumnTransformer> whenThenTransformers,
      ColumnTransformer elseTransformer) {
    super(returnType);
    this.whenThenTransformers = whenThenTransformers;
    this.elseTransformer = elseTransformer;
  }

  public List<WhenThenColumnTransformer> getWhenThenColumnTransformers() {
    return whenThenTransformers;
  }

  public ColumnTransformer getElseTransformer() {
    return elseTransformer;
  }

  private void writeToColumnBuilder(
      ColumnTransformer childTransformer, Column column, int index, ColumnBuilder builder) {
    if (returnType instanceof BooleanType) {
      builder.writeBoolean(childTransformer.getType().getBoolean(column, index));
    } else if (returnType instanceof IntType) {
      builder.writeInt(childTransformer.getType().getInt(column, index));
    } else if (returnType instanceof LongType) {
      builder.writeLong(childTransformer.getType().getLong(column, index));
    } else if (returnType instanceof FloatType) {
      builder.writeFloat(childTransformer.getType().getFloat(column, index));
    } else if (returnType instanceof DoubleType) {
      builder.writeDouble(childTransformer.getType().getDouble(column, index));
    } else if (returnType instanceof BinaryType) {
      builder.writeBinary(childTransformer.getType().getBinary(column, index));
    } else {
      throw new UnsupportedOperationException("Unsupported Type");
    }
  }

  /*
  whenThen[0] -> column[0] -> {xx, null, yy, ...}
  whenThen[1] -> column[1] -> {null, zz, cc, ...}
  ...

   */
  @Override
  protected void evaluate() {
    whenThenTransformers.forEach(ColumnTransformer::tryEvaluate);
    elseTransformer.tryEvaluate();
    int positionCount = whenThenTransformers.get(0).getColumnCachePositionCount();
    ColumnBuilder builder = returnType.createColumnBuilder(positionCount);
    //        List<Object> resultList = new ArrayList<>(positionCount);
    //        for
    List<Column> columnList = new ArrayList<>();
    for (WhenThenColumnTransformer whenThenTransformer : whenThenTransformers) {
      columnList.add(whenThenTransformer.getColumn());
    }
    Column elseColumn = elseTransformer.getColumn();
    for (int i = 0; i < positionCount; i++) {
      boolean hasValue = false;
      for (int j = 0; j < whenThenTransformers.size(); j++) {
        Column whenThenColumn = columnList.get(j);
        if (!whenThenColumn.isNull(i)) {
          writeToColumnBuilder(whenThenTransformers.get(j), whenThenColumn, i, builder);
          hasValue = true;
          break;
        }
      }
      if (!hasValue) {
        if (!elseColumn.isNull(i)) {
          writeToColumnBuilder(elseTransformer, elseColumn, i, builder);
        } else {
          builder.appendNull();
        }
      }
    }

    initializeColumnCache(builder.build());
  }

  @Override
  protected void checkType() {}
}
