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

import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/** Parts of partition. */
public class Slice {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(Slice.class);
  private final Column[] requiredColumns;
  private final Column[] passThroughColumns;
  private final List<Type> dataTypes;
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
    this.passThroughColumns =
        passThroughChannels.stream().map(partitionColumns::get).toArray(Column[]::new);
    this.dataTypes = dataTypes;

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

  public Iterator<Record> getRequiredRecordIterator() {
    return new Iterator<Record>() {
      private int curIndex = 0;

      @Override
      public boolean hasNext() {
        return curIndex < size;
      }

      @Override
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int idx = curIndex++;
        return getRecord(idx, requiredColumns);
      }
    };
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  private Record getRecord(int offset, Column[] originalColumns) {
    return new RecordImpl(offset, originalColumns, dataTypes);
  }

  private static class RecordImpl implements Record {
    private final int offset;
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
      return originalColumns[columnIndex].getBinary(offset);
    }

    @Override
    public String getString(int columnIndex) {
      return originalColumns[columnIndex]
          .getBinary(offset)
          .getStringValue(TSFileConfig.STRING_CHARSET);
    }

    @Override
    public LocalDate getLocalDate(int columnIndex) {
      return DateUtils.parseIntToLocalDate(originalColumns[columnIndex].getInt(offset));
    }

    @Override
    public Object getObject(int columnIndex) {
      return originalColumns[columnIndex].getObject(offset);
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
