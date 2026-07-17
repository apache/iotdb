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

import org.apache.iotdb.calc.exception.QueryProcessException;
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;

public class TypeUtils {
  public static ColumnBuilder initColumnBuilder(TSDataType type, int count) {
    return TypeServices.TRANSFORMATION_COLUMN_BUILDER_SERVICE
        .call(Type.fromTsDataType(type))
        .apply(count);
  }

  public static double castValueToDouble(Column column, TSDataType type, int index)
      throws QueryProcessException {
    return TypeServices.TRANSFORMATION_VALUE_TO_DOUBLE_SERVICE
        .call(Type.fromTsDataType(type))
        .convert(column, index);
  }
}
