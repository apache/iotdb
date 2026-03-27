/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes equality and hash based on specified column channels of a TsBlock. Used for peer group
 * detection in RANK window functions.
 */
public class SimpleTsBlockWithPositionEqualsAndHash implements TsBlockWithPositionEqualsAndHash {
  private final List<Integer> channels;
  private final List<TSDataType> types;

  public SimpleTsBlockWithPositionEqualsAndHash(List<TSDataType> allTypes, List<Integer> channels) {
    this.channels = channels;
    this.types = new ArrayList<>(channels.size());
    for (int channel : channels) {
      types.add(allTypes.get(channel));
    }
  }

  @Override
  public boolean equals(TsBlock left, int leftPosition, TsBlock right, int rightPosition) {
    for (int i = 0; i < channels.size(); i++) {
      int channel = channels.get(i);
      Column leftColumn = left.getColumn(channel);
      Column rightColumn = right.getColumn(channel);

      boolean leftNull = leftColumn.isNull(leftPosition);
      boolean rightNull = rightColumn.isNull(rightPosition);
      if (leftNull != rightNull) {
        return false;
      }
      if (leftNull) {
        continue;
      }

      if (!valueEquals(leftColumn, leftPosition, rightColumn, rightPosition, types.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long hashCode(TsBlock block, int position) {
    long hash = 0;
    for (int i = 0; i < channels.size(); i++) {
      Column column = block.getColumn(channels.get(i));
      hash = hash * 31 + valueHash(column, position, types.get(i));
    }
    return hash;
  }

  private static boolean valueEquals(
      Column left, int leftPos, Column right, int rightPos, TSDataType type) {
    switch (type) {
      case INT32:
      case DATE:
        return left.getInt(leftPos) == right.getInt(rightPos);
      case INT64:
      case TIMESTAMP:
        return left.getLong(leftPos) == right.getLong(rightPos);
      case FLOAT:
        return Float.compare(left.getFloat(leftPos), right.getFloat(rightPos)) == 0;
      case DOUBLE:
        return Double.compare(left.getDouble(leftPos), right.getDouble(rightPos)) == 0;
      case BOOLEAN:
        return left.getBoolean(leftPos) == right.getBoolean(rightPos);
      case TEXT:
      case BLOB:
      case STRING:
        return left.getBinary(leftPos).equals(right.getBinary(rightPos));
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private static long valueHash(Column column, int position, TSDataType type) {
    if (column.isNull(position)) {
      return 0;
    }
    switch (type) {
      case INT32:
      case DATE:
        return column.getInt(position);
      case INT64:
      case TIMESTAMP:
        long v = column.getLong(position);
        return v ^ (v >>> 32);
      case FLOAT:
        return Float.floatToIntBits(column.getFloat(position));
      case DOUBLE:
        long dv = Double.doubleToLongBits(column.getDouble(position));
        return dv ^ (dv >>> 32);
      case BOOLEAN:
        return column.getBoolean(position) ? 1231 : 1237;
      case TEXT:
      case BLOB:
      case STRING:
        return column.getBinary(position).hashCode();
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
