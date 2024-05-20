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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;

public abstract class UnaryTransformer extends Transformer {

  protected final LayerReader layerReader;
  protected final TSDataType layerReaderDataType;
  protected final boolean isLayerReaderConstant;

  protected UnaryTransformer(LayerReader layerReader) {
    this.layerReader = layerReader;
    layerReaderDataType = layerReader.getDataTypes()[0];
    isLayerReaderConstant = layerReader.isConstantPointReader();
  }

  @Override
  public final boolean isConstantPointReader() {
    return isLayerReaderConstant;
  }

  @Override
  public YieldableState yieldValue() throws Exception {
    final YieldableState state = layerReader.yield();
    if (!YieldableState.YIELDABLE.equals(state)) {
      return state;
    }

    Column[] columns = layerReader.current();
    cachedColumns = transform(columns);
    layerReader.consumedAll();

    return YieldableState.YIELDABLE;
  }

  protected Column[] transform(Column[] columns) throws QueryProcessException, IOException {
    // Prepare ColumnBuilder with given type
    int count = columns[0].getPositionCount();
    TSDataType dataType = getDataTypes()[0];
    ColumnBuilder builder = TypeUtils.initColumnBuilder(dataType, count);

    // Transform data
    transform(columns, builder);

    // Build cache
    Column valueColumn = builder.build();
    Column timeColumn = columns[1];
    return new Column[] {valueColumn, timeColumn};
  }

  protected void transform(Column[] columns, ColumnBuilder builder)
      throws QueryProcessException, IOException {}
}
