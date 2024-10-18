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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil.getFlatVariableWidthSize;
import static org.apache.iotdb.db.queryengine.plan.relational.utils.TypeUtil.notDistinctFrom;

public class HashStrategy implements FlatHashStrategy {

  private final boolean isAnyVariableWidth;

  private final int totalFlatFixedLength;

  private final List<KeyField> keyFields;

  public HashStrategy(List<Type> types) {
    this.isAnyVariableWidth = types.stream().anyMatch(TypeUtil::isFlatVariableWidth);

    int fixedOffset = 0;
    ImmutableList.Builder<KeyField> builder = ImmutableList.builder();
    for (int i = 0; i < types.size(); i++) {
      Type type = types.get(i);
      builder.add(new KeyField(i, type, fixedOffset));
      fixedOffset += 1 + TypeUtil.getFlatFixedSize(type);
    }
    keyFields = builder.build();

    totalFlatFixedLength = fixedOffset;
  }

  @Override
  public boolean isAnyVariableWidth() {
    return isAnyVariableWidth;
  }

  @Override
  public int getTotalFlatFixedLength() {
    return totalFlatFixedLength;
  }

  @Override
  public int getTotalVariableWidth(Column[] columns, int position) {
    int result = 0;
    for (int i = 0; i < columns.length; i++) {
      result += getFlatVariableWidthSize(keyFields.get(i).type, columns[i], position);
    }
    return result;
  }

  @Override
  public void readFlat(
      byte[] fixedChunk, int fixedOffset, byte[] variableChunk, ColumnBuilder[] columnBuilders) {
    for (int i = 0; i < columnBuilders.length; i++) {
      KeyField keyField = keyFields.get(i);
      if (fixedChunk[fixedOffset + keyField.fieldIsNullOffset] != 0) {
        columnBuilders[i].appendNull();
      } else {
        TypeUtil.readFlat(
            keyField.type,
            fixedChunk,
            fixedOffset + keyField.fieldFixedOffset,
            variableChunk,
            columnBuilders[i]);
      }
    }
  }

  @Override
  public void writeFlat(
      Column[] columns,
      int position,
      byte[] fixedChunk,
      int fixedOffset,
      byte[] variableChunk,
      int variableOffset) {
    for (int i = 0; i < columns.length; i++) {
      KeyField keyField = keyFields.get(i);
      if (columns[i].isNull(position)) {
        fixedChunk[fixedOffset + keyField.fieldIsNullOffset] = (byte) 1;
      } else {
        TypeUtil.writeFlat(
            keyField.getType(),
            columns[i],
            position,
            fixedChunk,
            fixedOffset + keyField.fieldFixedOffset,
            variableChunk,
            variableOffset);
        variableOffset += TypeUtil.getFlatVariableWidthSize(keyField.type, columns[i], position);
      }
    }
  }

  @Override
  public boolean valueNotDistinctFrom(
      byte[] leftFixedChunk,
      int leftFixedOffset,
      byte[] leftVariableChunk,
      Column[] rightColumns,
      int rightPosition) {
    for (int i = 0; i < rightColumns.length; i++) {
      KeyField keyField = keyFields.get(i);
      boolean leftIsNull = leftFixedChunk[leftFixedOffset + keyField.fieldIsNullOffset] != 0;
      boolean rightIsNull = rightColumns[i].isNull(rightPosition);

      if (leftIsNull != rightIsNull) {
        return false;
      }

      if (leftIsNull) {
        continue;
      }

      if (!notDistinctFrom(
          keyField.type,
          leftFixedChunk,
          leftFixedOffset + keyField.fieldFixedOffset,
          leftVariableChunk,
          rightColumns[i],
          rightPosition)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long hash(Column[] columns, int position) {
    long result = 0L;
    long hash;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].isNull(position)) {
        hash = 0L;
      } else {
        hash = TypeUtil.hash(keyFields.get(i).type, columns[i], position);
      }
      result = CombineHashFunction.getHash(result, hash);
    }

    return result;
  }

  @Override
  public long hash(byte[] fixedChunk, int fixedOffset, byte[] variableChunk) {
    long result = 0L;
    long hash;
    for (int i = 0; i < keyFields.size(); i++) {
      if (fixedChunk[fixedOffset + keyFields.get(i).fieldIsNullOffset] != 0) {
        hash = 0L;
      } else {
        hash =
            TypeUtil.hash(
                keyFields.get(i).getType(),
                fixedChunk,
                fixedOffset + keyFields.get(i).fieldFixedOffset,
                variableChunk);
      }
      result = CombineHashFunction.getHash(result, hash);
    }

    return result;
  }

  @Override
  public void hashBatched(Column[] columns, long[] hashes, int offset, int length) {
    for (int i = 0; i < length; i++) {
      hashes[i] = hash(columns, i);
    }
  }

  private static class KeyField {
    private final int index;
    private final Type type;
    private final int fieldIsNullOffset;
    private final int fieldFixedOffset;

    KeyField(int index, Type type, int fieldIsNullOffset) {
      this.index = index;
      this.type = type;
      this.fieldIsNullOffset = fieldIsNullOffset;
      fieldFixedOffset = fieldIsNullOffset + 1;
    }

    public int getIndex() {
      return index;
    }

    public Type getType() {
      return type;
    }

    public int getFieldIsNullOffset() {
      return fieldIsNullOffset;
    }

    public int getFieldFixedOffset() {
      return fieldFixedOffset;
    }
  }
}
