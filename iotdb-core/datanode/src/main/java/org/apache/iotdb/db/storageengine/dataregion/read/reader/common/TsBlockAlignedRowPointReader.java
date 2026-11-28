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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.common;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.tsfile.utils.TsPrimitiveType.TsVector;

/**
 * Aligned row {@link AlignedPointReader} over a {@link TsBlock} that records row completeness while
 * constructing each {@link TsVector}.
 */
public class TsBlockAlignedRowPointReader implements AlignedPointReader {

  private final TsBlock tsBlock;
  private final Column timeColumn;
  private final Column[] valueColumns;
  private final int positionCount;
  private int rowIndex = 0;
  private boolean lastRowComplete = false;

  public TsBlockAlignedRowPointReader(TsBlock tsBlock) {
    this.tsBlock = tsBlock;
    this.timeColumn = tsBlock.getTimeColumn();
    this.valueColumns = tsBlock.getValueColumns();
    this.positionCount = tsBlock.getPositionCount();
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return rowIndex < positionCount;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    long time = timeColumn.getLong(rowIndex);
    TsPrimitiveType[] vector = new TsPrimitiveType[valueColumns.length];
    boolean complete = true;
    for (int i = 0; i < valueColumns.length; i++) {
      Column col = valueColumns[i];
      if (col.isNull(rowIndex)) {
        complete = false;
      } else {
        TsPrimitiveType val = col.getTsPrimitiveType(rowIndex);
        vector[i] = val;
        if (complete && (val == null || (val instanceof TsBinary && val.getBinary() == null))) {
          complete = false;
        }
      }
    }
    rowIndex++;
    lastRowComplete = complete;
    return new TimeValuePair(time, new TsVector(vector));
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    long time = timeColumn.getLong(rowIndex);
    TsPrimitiveType[] vector = new TsPrimitiveType[valueColumns.length];
    for (int i = 0; i < valueColumns.length; i++) {
      Column col = valueColumns[i];
      if (!col.isNull(rowIndex)) {
        vector[i] = col.getTsPrimitiveType(rowIndex);
      }
    }
    return new TimeValuePair(time, new TsVector(vector));
  }

  @Override
  public boolean isLastRowComplete() {
    return lastRowComplete;
  }

  @Override
  public long getUsedMemorySize() {
    return tsBlock.getRetainedSizeInBytes();
  }

  @Override
  public void close() {
    // Nothing to close for TsBlock-backed iterator.
  }
}
