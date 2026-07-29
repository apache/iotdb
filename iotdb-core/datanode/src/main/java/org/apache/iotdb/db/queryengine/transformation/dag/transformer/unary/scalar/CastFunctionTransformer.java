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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.scalar;

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;
import org.apache.iotdb.db.utils.TypeServices.Transformation.CastColumnStrategy;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

import java.io.IOException;

import static org.apache.iotdb.db.utils.TypeServices.Transformation.CAST_COLUMN_SERVICE;

public class CastFunctionTransformer extends UnaryTransformer {
  private final TSDataType targetDataType;

  public CastFunctionTransformer(LayerReader layerReader, TSDataType targetDataType) {
    super(layerReader);
    this.targetDataType = targetDataType;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {targetDataType};
  }

  @Override
  protected Column[] transform(Column[] columns) throws QueryProcessException, IOException {
    final Type sourceType;
    try {
      sourceType = Type.fromTsDataType(layerReaderDataType);
    } catch (final UnsupportedOperationException ignored) {
      throw new UnsupportedOperationException(
          String.format("Unsupported source dataType: %s", layerReaderDataType));
    }

    final Type targetType;
    try {
      targetType = Type.fromTsDataType(targetDataType);
    } catch (final UnsupportedOperationException ignored) {
      throw new UnsupportedOperationException(
          String.format("Unsupported target dataType: %s", layerReaderDataType));
    }

    final CastColumnStrategy strategy = CAST_COLUMN_SERVICE.call(sourceType).call(targetType);
    strategy.validate();
    if (layerReaderDataType == targetDataType) {
      return columns;
    }

    final Column valueColumn = columns[0];
    final int positionCount = valueColumn.getPositionCount();
    final boolean[] isNulls = valueColumn.isNull();
    final ColumnBuilder builder = strategy.createBuilder(positionCount);
    for (int i = 0; i < positionCount; i++) {
      if (isNulls[i]) {
        builder.appendNull();
      } else {
        strategy.cast(valueColumn, i, builder);
      }
    }
    return new Column[] {builder.build(), columns[1]};
  }
}
