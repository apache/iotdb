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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression;

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericDataType;

import java.util.List;

public class CastComputation extends Computation {
  private final Computation inner;
  private final DataType targetType;

  public CastComputation(Computation inner, DataType targetType) {
    this.inner = inner;
    this.targetType = targetType;
  }

  @Override
  public Object evaluate(List<Object> values) {
    Object value = inner.evaluate(values);
    if (value == null) {
      return null;
    }

    if (targetType instanceof GenericDataType) {
      String typeName = ((GenericDataType) targetType).getName().getValue().toUpperCase();

      switch (typeName) {
        case "INT32":
          if (value instanceof Number) {
            return ((Number) value).intValue();
          }
          return Integer.parseInt(value.toString());
        case "INT64":
          if (value instanceof Number) {
            return ((Number) value).longValue();
          }
          return Long.parseLong(value.toString());
        case "FLOAT":
          if (value instanceof Number) {
            return ((Number) value).floatValue();
          }
          return Float.parseFloat(value.toString());
        case "DOUBLE":
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.parseDouble(value.toString());
        case "STRING":
          return value.toString();
        case "BOOLEAN":
          if (value instanceof Boolean) {
            return value;
          }
          return Boolean.parseBoolean(value.toString());
        default:
          throw new UnsupportedOperationException("Unsupported cast to type: " + typeName);
      }
    } else {
      throw new UnsupportedOperationException(
          "Target type is not a GenericDataType: " + targetType);
    }
  }
}
