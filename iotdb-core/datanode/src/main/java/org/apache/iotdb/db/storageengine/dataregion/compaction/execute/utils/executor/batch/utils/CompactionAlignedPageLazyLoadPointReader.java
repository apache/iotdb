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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

public class CompactionAlignedPageLazyLoadPointReader implements IPointReader {
  private final TimePageReader timeReader;
  private final List<ValuePageReader> valueReaders;
  private final boolean ignoreAllNullRows;

  private boolean hasNextRow = false;

  private int timeIndex;
  private long currentTime;
  private TsPrimitiveType currentRow;

  public CompactionAlignedPageLazyLoadPointReader(
      TimePageReader timeReader, List<ValuePageReader> valueReaders, boolean ignoreAllNullRows)
      throws IOException {
    this.timeIndex = -1;
    this.timeReader = timeReader;
    this.valueReaders = valueReaders;
    this.ignoreAllNullRows = ignoreAllNullRows;
    prepareNextRow();
  }

  private void prepareNextRow() throws IOException {
    while (timeReader.hasNextTime()) {
      currentTime = timeReader.nextTime();
      ++this.timeIndex;
      boolean someValueNotNull = false;
      TsPrimitiveType[] valuesInThisRow = new TsPrimitiveType[this.valueReaders.size()];

      for (int i = 0; i < valueReaders.size(); ++i) {
        TsPrimitiveType value =
            valueReaders.get(i) == null
                ? null
                : this.valueReaders.get(i).nextValue(currentTime, timeIndex);
        someValueNotNull = someValueNotNull || value != null;
        valuesInThisRow[i] = value;
      }
      if (timeReader.isDeleted(currentTime)) {
        continue;
      }

      if (!ignoreAllNullRows || someValueNotNull) {
        currentRow = new TsPrimitiveType.TsVector(valuesInThisRow);
        hasNextRow = true;
        return;
      }
    }

    hasNextRow = false;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return hasNextRow;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    TimeValuePair ret = currentTimeValuePair();
    prepareNextRow();
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return new TimeValuePair(currentTime, currentRow);
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
