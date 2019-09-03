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

package org.apache.iotdb.db.tools.logvisual;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesDataItem;

/**
 * TimeSeriesStatistics store the the count, mean, max, min of both time and measurements.
 */
public class TimeSeriesStatistics {

  public static final String[] HEADER = new String[] {
      "name", "count", "meanInterval", "maxInterval", "minInterval", "meanVal", "maxVal", "minVal", "valSum"
  };

  private String name;
  private int size = 0;
  private double meanInterval = 0.0;
  private long maxInterval = Long.MIN_VALUE;
  private long minInterval = Long.MAX_VALUE;
  private double meanVal = 0.0;
  private double maxVal = Double.MIN_VALUE;
  private double minVal = Double.MAX_VALUE;
  private double valSum = 0.0;
  
  private Object[] statisticArray;

  TimeSeriesStatistics(TimeSeries timeSeries) {
    Date lastDate = null;
    name = (String) timeSeries.getKey();
    for (int i = 0; i < timeSeries.getItemCount(); i++) {
      TimeSeriesDataItem dataItem = timeSeries.getDataItem(i);
      Date currDate = dataItem.getPeriod().getStart();
      double value = dataItem.getValue().doubleValue();
      if (lastDate == null) {
        lastDate = currDate;
      } else {
        long interval = currDate.getTime() - lastDate.getTime();
        lastDate = currDate;
        meanInterval = (meanInterval * size + interval) / (size + 1);
        maxInterval = maxInterval < interval ? interval : maxInterval;
        minInterval = minInterval < interval ? minInterval : interval;
      }
      meanVal = (meanVal * size + value) / (size + 1);
      maxVal = maxVal < value ? value : maxVal;
      minVal = minVal < value ? minVal : value;
      valSum += value;
      size ++;
    }
  }

  public String getName() {
    return name;
  }

  public int getSize() {
    return size;
  }

  public Object[] toArray() {
    if (statisticArray != null) {
      return statisticArray;
    }
    statisticArray = new Object[HEADER.length];
    int i = 0;
    statisticArray[i++] = name;
    statisticArray[i++] = size;
    statisticArray[i++] = meanInterval;
    statisticArray[i++] = maxInterval;
    statisticArray[i++] = minInterval;
    statisticArray[i++] = meanVal;
    statisticArray[i++] = maxVal;
    statisticArray[i++] = minVal;
    statisticArray[i] = valSum;
    return statisticArray;
  }

  public static void serializeHeader(Writer writer) throws IOException {
    writer.write(String.join(",", HEADER) + "\n");
  }
  
  public void serialize(Writer writer) throws IOException {
    Object[] statArray = toArray();
    StringBuilder builder = new StringBuilder(statArray[0].toString());
    for (int i = 1; i < statArray.length; i++) {
      builder.append(",").append(statArray[i]);
    }
    builder.append("\n");
    writer.write(builder.toString());
  }
}