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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.unary.UnaryTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;

public class DiffFunctionTransformer extends UnaryTransformer {
  private final boolean ignoreNull;

  // cache the last non-null value
  private double lastValue;

  // indicate whether lastValue is null
  private boolean lastValueIsNull = true;

  public DiffFunctionTransformer(LayerReader layerReader, boolean ignoreNull) {
    super(layerReader);
    this.ignoreNull = ignoreNull;
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.DOUBLE};
  }

  @Override
  public void transform(Column[] columns, ColumnBuilder builder) throws QueryProcessException {
    switch (layerReaderDataType) {
      case INT32:
        transformInt(columns, builder);
        return;
      case INT64:
        transformLong(columns, builder);
        return;
      case FLOAT:
        transformFloat(columns, builder);
        return;
      case DOUBLE:
        transformDouble(columns, builder);
        return;
      case BLOB:
      case TEXT:
      case DATE:
      case STRING:
      case TIMESTAMP:
      case BOOLEAN:
      default:
        throw new QueryProcessException("Unsupported data type: " + layerReaderDataType);
    }
  }

  private void transformInt(Column[] columns, ColumnBuilder builder) {
    // Position count at first iteration is count
    // Then it become count - 1 at latter iteration
    int count = columns[0].getPositionCount();

    for (int i = 0; i < count; i++) {
      if (columns[0].isNull(i)) {
        lastValueIsNull |= !ignoreNull;
        builder.appendNull();
      } else {
        double currentValue = columns[0].getInt(i);

        if (lastValueIsNull) {
          builder.appendNull();
        } else {
          builder.writeDouble(currentValue - lastValue);
        }

        lastValue = currentValue;
        lastValueIsNull = false;
      }
    }
  }

  private void transformLong(Column[] columns, ColumnBuilder builder) {
    // Position count at first iteration is count
    // Then it become count - 1 at latter iteration
    int count = columns[0].getPositionCount();

    for (int i = 0; i < count; i++) {
      if (columns[0].isNull(i)) {
        lastValueIsNull |= !ignoreNull;
        builder.appendNull();
      } else {
        double currentValue = columns[0].getLong(i);

        if (lastValueIsNull) {
          builder.appendNull();
        } else {
          builder.writeDouble(currentValue - lastValue);
        }

        lastValue = currentValue;
        lastValueIsNull = false;
      }
    }
  }

  private void transformFloat(Column[] columns, ColumnBuilder builder) {
    // Position count at first iteration is count
    // Then it become count - 1 at latter iteration
    int count = columns[0].getPositionCount();

    for (int i = 0; i < count; i++) {
      if (columns[0].isNull(i)) {
        lastValueIsNull |= !ignoreNull;
        builder.appendNull();
      } else {
        double currentValue = columns[0].getFloat(i);

        if (lastValueIsNull) {
          builder.appendNull();
        } else {
          builder.writeDouble(currentValue - lastValue);
        }

        lastValue = currentValue;
        lastValueIsNull = false;
      }
    }
  }

  private void transformDouble(Column[] columns, ColumnBuilder builder) {
    // Position count at first iteration is count
    // Then it become count - 1 at latter iteration
    int count = columns[0].getPositionCount();

    for (int i = 0; i < count; i++) {
      if (columns[0].isNull(i)) {
        lastValueIsNull |= !ignoreNull;
        builder.appendNull();
      } else {
        double currentValue = columns[0].getDouble(i);

        if (lastValueIsNull) {
          builder.appendNull();
        } else {
          builder.writeDouble(currentValue - lastValue);
        }

        lastValue = currentValue;
        lastValueIsNull = false;
      }
    }
  }
}
