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

package org.apache.iotdb.db.mpp.transformation.dag.column.multi;

import org.apache.iotdb.db.mpp.transformation.dag.adapter.ElasticSerializableRowRecordListBackedMultiColumnRow;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.type.Type;

public class MappableUDFColumnTransformer extends ColumnTransformer {

  private final ColumnTransformer[] inputColumnTransformers;

  private final UDTFExecutor executor;

  private final TSDataType[] inputDataTypes;

  public MappableUDFColumnTransformer(
      Type returnType,
      ColumnTransformer[] inputColumnTransformers,
      TSDataType[] inputDataTypes,
      UDTFExecutor executor) {
    super(returnType);
    this.inputColumnTransformers = inputColumnTransformers;
    this.executor = executor;
    this.inputDataTypes = inputDataTypes;
  }

  @Override
  public void evaluate() {
    for (ColumnTransformer inputColumnTransformer : inputColumnTransformers) {
      inputColumnTransformer.tryEvaluate();
    }
    int size = inputColumnTransformers.length;
    Column[] columns = new Column[size];
    // attention: get positionCount before calling getColumn
    int positionCount = inputColumnTransformers[0].getColumnCachePositionCount();
    for (int i = 0; i < size; i++) {
      columns[i] = inputColumnTransformers[i].getColumn();
    }
    ColumnBuilder columnBuilder = returnType.createColumnBuilder(positionCount);
    for (int i = 0; i < positionCount; i++) {

      Object[] values = new Object[size];
      for (int j = 0; j < size; j++) {
        if (columns[j].isNull(i)) {
          values[j] = null;
        } else {
          values[j] = columns[j].getObject(i);
        }
      }
      // construct input row for executor
      ElasticSerializableRowRecordListBackedMultiColumnRow row =
          new ElasticSerializableRowRecordListBackedMultiColumnRow(inputDataTypes);
      row.setRowRecord(values);
      executor.execute(row);
      Object res = executor.getCurrentValue();
      if (res != null) {
        returnType.writeObject(columnBuilder, res);
      } else {
        columnBuilder.appendNull();
      }
    }
    initializeColumnCache(columnBuilder.build());
  }

  @Override
  protected void checkType() {
    // do nothing
  }

  public ColumnTransformer[] getInputColumnTransformers() {
    return inputColumnTransformers;
  }

  @Override
  public void close() {
    // finalize executor
    executor.beforeDestroy();
  }
}
