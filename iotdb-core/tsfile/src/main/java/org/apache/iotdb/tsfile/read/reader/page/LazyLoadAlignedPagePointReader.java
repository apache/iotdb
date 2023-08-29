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

package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

/**
 * This class is used to read data of aligned-series row by row. It won't deserialize all data point
 * of one page in memory. In contrast, it constructs row one by one as needed
 */
public class LazyLoadAlignedPagePointReader implements IPointReader {

  private TimePageReader timeReader;
  private List<ValuePageReader> valueReaders;

  private boolean hasNextRow = false;

  private int timeIndex;
  private long currentTime;
  private TsPrimitiveType currentRow;

  public LazyLoadAlignedPagePointReader(
      TimePageReader timeReader, List<ValuePageReader> valueReaders) throws IOException {
    this.timeIndex = -1;
    this.timeReader = timeReader;
    this.valueReaders = valueReaders;
    prepareNextRow();
  }

  private void prepareNextRow() throws IOException {
    while (true) {
      if (!timeReader.hasNextTime()) {
        hasNextRow = false;
        return;
      }
      currentTime = timeReader.nextTime();
      timeIndex++;
      boolean someValueNotNull = false;
      TsPrimitiveType[] valuesInThisRow = new TsPrimitiveType[valueReaders.size()];
      for (int i = 0; i < valueReaders.size(); i++) {
        TsPrimitiveType value =
            valueReaders.get(i) == null
                ? null
                : valueReaders.get(i).nextValue(currentTime, timeIndex);
        someValueNotNull = someValueNotNull || (value != null);
        valuesInThisRow[i] = value;
      }
      if (someValueNotNull) {
        currentRow = new TsPrimitiveType.TsVector(valuesInThisRow);
        hasNextRow = true;
        break;
      }
    }
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
  public void close() throws IOException {}
}
