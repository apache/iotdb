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
package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;

public class FakedSeriesReader implements IPointReader {

  private int index;
  private int size;
  private boolean initWithTimeList;
  private static final TSDataType DATA_TYPE = TSDataType.INT64;

  // init with time list and value
  private long[] timestamps;
  private long value;

  // init with startTime, size, interval and modValue
  private long startTime;
  private int interval;
  private int modValue;

  public FakedSeriesReader(long[] timestamps, long value) {
    this.initWithTimeList = true;
    this.index = 0;
    this.size = timestamps.length;
    this.timestamps = timestamps;
    this.value = value;
  }

  public FakedSeriesReader(long startTime, int size, int interval, int modValue) {
    this.initWithTimeList = false;
    this.index = 0;
    this.size = size;
    this.startTime = startTime;
    this.interval = interval;
    this.modValue = modValue;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    return index < size;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (initWithTimeList) {
      return new TimeValuePair(timestamps[index++], TsPrimitiveType.getByType(DATA_TYPE, value));
    } else {
      long time = startTime;
      startTime += interval;
      index++;
      return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.INT64, time % modValue));
    }
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    throw new IOException("current() in FakedPrioritySeriesReader is an empty method.");
  }

  @Override
  public void close() {}
}
