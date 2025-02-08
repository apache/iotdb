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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.TsPrimitiveType;

public class DescFakedSeriesReader extends AscFakedSeriesReader {

  public DescFakedSeriesReader(long[] timestamps, long value) {
    super(timestamps, value);
    index = size - 1;
  }

  public DescFakedSeriesReader(long startTime, int size, int interval, int modValue) {
    super(startTime, size, interval, modValue);
    index = size - 1;
    startTime = startTime + interval * size;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return index >= 0;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (initWithTimeList) {
      return new TimeValuePair(timestamps[index--], TsPrimitiveType.getByType(DATA_TYPE, value));
    } else {
      long time = startTime;
      startTime -= interval;
      index--;
      return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue));
    }
  }
}
