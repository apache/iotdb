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

package org.apache.iotdb.db.queryengine.transformation.dag.util;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;

public class TypeUtils {
  public static ColumnBuilder initColumnBuilder(TSDataType type, int count) {
    switch (type) {
      case INT32:
        return new IntColumnBuilder(null, count);
      case INT64:
        return new LongColumnBuilder(null, count);
      case FLOAT:
        return new FloatColumnBuilder(null, count);
      case DOUBLE:
        return new DoubleColumnBuilder(null, count);
      case BOOLEAN:
        return new BooleanColumnBuilder(null, count);
      case TEXT:
        return new BinaryColumnBuilder(null, count);
      default:
        throw new UnSupportedDataTypeException(
            "Do not support create ColumnBuilder with data type" + type);
    }
  }

  public static double castValueToDouble(Column column, TSDataType type, int index)
      throws QueryProcessException {
    switch (type) {
      case INT32:
        return column.getInt(index);
      case INT64:
        return column.getLong(index);
      case FLOAT:
        return column.getFloat(index);
      case DOUBLE:
        return column.getDouble(index);
      case BOOLEAN:
        return column.getBoolean(index) ? 1 : 0;
      default:
        throw new QueryProcessException("Unsupported data type: " + type);
    }
  }
}
