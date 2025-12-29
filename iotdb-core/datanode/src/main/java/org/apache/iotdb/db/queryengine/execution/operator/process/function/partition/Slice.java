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

package org.apache.iotdb.db.queryengine.execution.operator.process.function.partition;

import org.apache.iotdb.db.utils.ObjectTypeUtils;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.RecordIterator.OBJECT_ERR_MSG;
import static org.apache.iotdb.udf.api.type.Type.OBJECT;

/** Parts of partition. */
public class Slice {
  private final Column[] requiredColumns;
  private final Column[] passThroughColumns;
  private final List<Type> requiredDataTypes;
  private final long size;
  private final long estimatedSize;

  public Slice(
      int startIndex,
      int endIndex,
      Column[] columns,
      List<Integer> requiredChannels,
      List<Integer> passThroughChannels,
      List<Type> dataTypes) {
    this.size = endIndex - startIndex;
    List<Column> partitionColumns =
        Arrays.stream(columns)
            .map(i -> i.getRegion(startIndex, (int) size))
            .collect(Collectors.toList());
    this.requiredColumns =
        requiredChannels.stream().map(partitionColumns::get).toArray(Column[]::new);
    this.requiredDataTypes =
        requiredChannels.stream().map(dataTypes::get).collect(Collectors.toList());
    this.passThroughColumns =
        passThroughChannels.stream().map(partitionColumns::get).toArray(Column[]::new);

    Set<Integer> channels = new HashSet<>();
    channels.addAll(requiredChannels);
    channels.addAll(passThroughChannels);
    this.estimatedSize =
        channels.stream().map(i -> columns[i].getRetainedSizeInBytes()).reduce(0L, Long::sum);
  }

  public long getSize() {
    return size;
  }

  public Column[] getPassThroughResult(int[] indexes) {
    return Arrays.stream(passThroughColumns)
        .map(i -> i.getPositions(indexes, 0, indexes.length))
        .toArray(Column[]::new);
  }

  public Column[] getRequiredColumns() {
    return requiredColumns;
  }

  public Iterator<Record> getRequiredRecordIterator(boolean requireSnapshot) {
    if (!requireSnapshot) {
      return new Iterator<Record>() {
        private final RecordImpl record = new RecordImpl(-1, requiredColumns, requiredDataTypes);

        @Override
        public boolean hasNext() {
          record.offset++;
          return record.offset < size;
        }

        @Override
        public Record next() {
          return record;
        }
      };
    } else {
      return new Iterator<Record>() {
        private int curIndex = 0;

        @Override
        public boolean hasNext() {
          return curIndex < size;
        }

        @Override
        public Record next() {
          if (!hasNext()) {
            throw new java.util.NoSuchElementException();
          }
          final int idx = curIndex++;
          return getRecord(idx, requiredColumns, requiredDataTypes);
        }
      };
    }
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  private Record getRecord(int offset, Column[] originalColumns, List<Type> dataTypes) {
    return new RecordImpl(offset, originalColumns, dataTypes);
  }

  private static class RecordImpl implements Record {
    private int offset;
    private final Column[] originalColumns;
    private final List<Type> dataTypes;

    private RecordImpl(int offset, Column[] originalColumns, List<Type> dataTypes) {
      this.offset = offset;
      this.originalColumns = originalColumns;
      this.dataTypes = dataTypes;
    }

    @Override
    public int getInt(int columnIndex) {
      return originalColumns[columnIndex].getInt(offset);
    }

    @Override
    public long getLong(int columnIndex) {
      return originalColumns[columnIndex].getLong(offset);
    }

    @Override
    public float getFloat(int columnIndex) {
      return originalColumns[columnIndex].getFloat(offset);
    }

    @Override
    public double getDouble(int columnIndex) {
      return originalColumns[columnIndex].getDouble(offset);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      return originalColumns[columnIndex].getBoolean(offset);
    }

    @Override
    public Binary getBinary(int columnIndex) {
      Type type = dataTypes.get(columnIndex);
      if (type == OBJECT) {
        throw new UnsupportedOperationException(OBJECT_ERR_MSG);
      }
      return getBinarySafely(columnIndex);
    }

    public Binary getBinarySafely(int columnIndex) {
      return originalColumns[columnIndex].getBinary(offset);
    }

    @Override
    public String getString(int columnIndex) {
      Binary binary = originalColumns[columnIndex].getBinary(offset);
      Type type = dataTypes.get(columnIndex);
      if (type == OBJECT) {
        return BytesUtils.parseObjectByteArrayToString(binary.getValues());
      } else if (type == Type.BLOB) {
        return BytesUtils.parseBlobByteArrayToString(binary.getValues());
      } else {
        return binary.getStringValue(TSFileConfig.STRING_CHARSET);
      }
    }

    @Override
    public LocalDate getLocalDate(int columnIndex) {
      return DateUtils.parseIntToLocalDate(originalColumns[columnIndex].getInt(offset));
    }

    @Override
    public Object getObject(int columnIndex) {
      Type type = dataTypes.get(columnIndex);
      if (type == OBJECT) {
        throw new UnsupportedOperationException(OBJECT_ERR_MSG);
      }
      return originalColumns[columnIndex].getObject(offset);
    }

    @Override
    public Optional<File> getObjectFile(int columnIndex) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      return ObjectTypeUtils.getObjectPathFromBinary(getBinarySafely(columnIndex));
    }

    @Override
    public long objectLength(int columnIndex) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      Binary binary = getBinarySafely(columnIndex);
      return ObjectTypeUtils.getObjectLength(binary);
    }

    @Override
    public Binary readObject(int columnIndex, long offset, int length) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      Binary binary = getBinarySafely(columnIndex);
      return new Binary(ObjectTypeUtils.readObjectContent(binary, offset, length, true).array());
    }

    @Override
    public Binary readObject(int columnIndex) {
      return readObject(columnIndex, 0L, -1);
    }

    @Override
    public Type getDataType(int columnIndex) {
      return dataTypes.get(columnIndex);
    }

    @Override
    public boolean isNull(int columnIndex) {
      return originalColumns[columnIndex].isNull(offset);
    }

    @Override
    public int size() {
      return originalColumns.length;
    }
  }
}
