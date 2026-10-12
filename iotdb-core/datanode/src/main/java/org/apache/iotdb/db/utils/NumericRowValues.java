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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/**
 * Primitive storage for a numeric row. FLOAT and DOUBLE use raw IEEE 754 bits; BOOLEAN uses 0/1.
 * TAGs live in a separate String[] and occupy the first logical columns; FIELDs follow them in
 * {@code long[]}. Nulls need separate bits because every long bit pattern is a valid numeric value.
 */
public final class NumericRowValues {
  private long[] values;
  private final BitSet nulls;
  private String[] tagValues;
  private final int size;
  private int tagCount;

  public NumericRowValues(int tagLength, int valueLength) {
    this.values = new long[valueLength];
    this.nulls = new BitSet(tagLength + valueLength);
    this.tagValues = tagLength == 0 ? null : new String[tagLength];
    this.size = tagLength + valueLength;
    this.tagCount = tagLength;
  }

  public int size() {
    return size;
  }

  public long[] getValues() {
    return values;
  }

  public String getTag(int index) {
    return index < 0 || index >= size || isNull(index) || tagValues == null || index >= tagCount
        ? null
        : tagValues[index];
  }

  public void setTag(int index, String value) {
    if (index < 0 || index >= tagCount) {
      throw new IndexOutOfBoundsException(index);
    }
    if (value == null) {
      tagValues[index] = null;
      nulls.set(index);
      return;
    }
    tagValues[index] = value;
    nulls.clear(index);
  }

  public boolean isNull(int index) {
    return nulls.get(index);
  }

  public void setNull(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index);
    }
    // Null slots have no payload on the wire; keep their in-memory representation canonical too.
    if (index >= tagCount) {
      values[index - tagCount] = 0;
    }
    if (tagValues != null && index < tagCount) {
      tagValues[index] = null;
    }
    nulls.set(index);
  }

  public long get(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index);
    }
    return index < tagCount ? 0 : values[index - tagCount];
  }

  public void setRawValue(int index, long value) {
    if (index < tagCount) {
      throw new IllegalArgumentException();
    }
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException(index);
    }
    values[index - tagCount] = value;
    nulls.clear(index);
  }

  public String[] getTagValues() {
    return tagValues;
  }

  public void setColumnCategories(ColumnCategory[] categories) {
    if (categories == null || categories.length != size) {
      throw new IllegalArgumentException();
    }
    int newTagCount = 0;
    while (newTagCount < size && categories[newTagCount] == ColumnCategory.TAG) {
      newTagCount++;
    }
    for (int i = newTagCount; i < size; i++) {
      if (categories[i] == ColumnCategory.TAG) {
        throw new IllegalArgumentException();
      }
    }
    if (newTagCount != tagCount) {
      throw new IllegalArgumentException();
    }
  }

  public static boolean supports(TSDataType type) {
    return type != null
        && switch (type) {
          case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP -> true;
          case TEXT, STRING, BLOB, OBJECT, VECTOR, UNKNOWN -> false;
        };
  }

  /** Validates the optional FIELD-only RPC bitmap without changing its cursor. */
  public static void validateNullBitmap(ByteBuffer bitmap, int fieldCount) {
    if (bitmap == null) {
      return;
    }
    int length = (fieldCount + 7) / 8;
    if (bitmap.remaining() != length) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_NUMERIC_ROW_NULL_BITMAP_LENGTH_MISMATCH_EXPECTED_ARG_BYTES_FOR_ARG_FIELDS_BUT_GOT_ARG_31F1DB42,
              length,
              fieldCount,
              bitmap.remaining()));
    }
    if (fieldCount % 8 != 0
        && (bitmap.get(bitmap.limit() - 1) & ~((1 << (fieldCount % 8)) - 1)) != 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_NUMERIC_ROW_NULL_BITMAP_HAS_SET_BITS_OUTSIDE_ARG_FIELDS_AD6066D1,
              fieldCount));
    }
  }

  public static boolean isNullField(ByteBuffer bitmap, int fieldIndex) {
    return bitmap != null
        && (bitmap.get(bitmap.position() + fieldIndex / 8) & (1 << (fieldIndex % 8))) != 0;
  }

  public Object getObject(int index, TSDataType type) {
    if (isNull(index)) {
      return null;
    }
    return switch (type) {
      case BOOLEAN -> get(index) != 0;
      case INT32, DATE -> (int) get(index);
      case INT64, TIMESTAMP -> get(index);
      case FLOAT -> Float.intBitsToFloat((int) get(index));
      case DOUBLE -> Double.longBitsToDouble(get(index));
      // Binary is required only at existing per-cell APIs (schema, Pipe and subscription).
      case STRING -> {
        if (index >= tagCount) {
          throw new IllegalArgumentException();
        }
        yield new Binary(getTag(index), TSFileConfig.STRING_CHARSET);
      }
      default -> throw unsupported(type);
    };
  }

  // Used only at APIs that consume individual objects (last cache, diagnostics and type coercion).
  public void setObject(int index, TSDataType type, Object value) {
    if (value == null) {
      setNull(index);
      return;
    }
    if (type == TSDataType.STRING) {
      setTag(
          index,
          value instanceof Binary binary
              ? binary.getStringValue(TSFileConfig.STRING_CHARSET)
              : (String) value);
      return;
    }
    if (index < tagCount || index < 0 || index >= size) {
      throw new IllegalArgumentException();
    }
    values[index - tagCount] =
        switch (type) {
          case BOOLEAN -> (boolean) value ? 1 : 0;
          case INT32, DATE -> (int) value;
          case INT64, TIMESTAMP -> (long) value;
          case FLOAT -> Float.floatToRawIntBits((float) value);
          case DOUBLE -> Double.doubleToRawLongBits((double) value);
          default -> throw unsupported(type);
        };
    nulls.clear(index);
  }

  public boolean cast(int index, TSDataType source, TSDataType target) {
    if (isNull(index)) {
      return true;
    }
    if (source == TSDataType.STRING && target == TSDataType.STRING) {
      return true;
    }
    if (!supports(target) || source == null || !target.isCompatible(source)) {
      return false;
    }
    if (source == target) {
      return true;
    }
    setObject(index, target, target.castFromSingleValue(source, getObject(index, source)));
    return true;
  }

  /** Returns a new primitive row; -1 in the mapping represents an inserted null column. */
  public NumericRowValues remap(int[] newToOld, int newTagCount) {
    NumericRowValues result = new NumericRowValues(newTagCount, newToOld.length - newTagCount);
    for (int i = 0; i < newToOld.length; i++) {
      int old = newToOld[i];
      if (old < 0 || old >= size() || isNull(old)) {
        result.setNull(i);
      } else if (i < newTagCount) {
        if (tagValues != null && old < this.tagCount && tagValues[old] != null) {
          result.setTag(i, tagValues[old]);
        }
      } else if (old >= this.tagCount) {
        result.setRawValue(i, get(old));
      } else {
        result.setNull(i);
      }
    }
    return result;
  }

  public void read(int index, TSDataType type, ByteBuffer buffer) {
    if (type == TSDataType.STRING) {
      setTag(index, ReadWriteIOUtils.readString(buffer));
      return;
    }
    if (index < tagCount || index < 0 || index >= size) {
      throw new IllegalArgumentException();
    }
    values[index - tagCount] =
        switch (type) {
          case BOOLEAN -> buffer.get() == 0 ? 0 : 1;
          case INT32, DATE, FLOAT -> buffer.getInt();
          case INT64, TIMESTAMP, DOUBLE -> buffer.getLong();
          default -> throw unsupported(type);
        };
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(NumericRowValues.class)
        + RamUsageEstimator.sizeOf(values)
        + (tagValues == null ? 0 : RamUsageEstimator.sizeOf(tagValues))
        + RamUsageEstimator.shallowSizeOfInstance(BitSet.class)
        + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
        + (long) nulls.size() / Byte.SIZE;
  }

  private static UnSupportedDataTypeException unsupported(TSDataType type) {
    return new UnSupportedDataTypeException(DataNodeQueryMessages.UNSUPPORTED_DATA_TYPE + type);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof NumericRowValues row)
        || size != row.size
        || tagCount != row.tagCount
        || !Arrays.equals(values, row.values)
        || !nulls.equals(row.nulls)) {
      return false;
    }
    // An allocated array cleared by a partial failure equals an absent all-null TAG array.
    for (int i = 0; i < size(); i++) {
      if (!Objects.equals(getTag(i), row.getTag(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 31 * (31 * size + tagCount) + Arrays.hashCode(values) + nulls.hashCode();
    for (int i = 0; i < size(); i++) {
      hash = 31 * hash + Objects.hashCode(getTag(i));
    }
    return hash;
  }
}
