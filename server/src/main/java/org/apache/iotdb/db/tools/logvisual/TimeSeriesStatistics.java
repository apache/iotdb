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

import java.util.Date;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesDataItem;

/**
 * TimeSeriesStatistics store the the count, mean, max, min of both time and measurements.
 */
public class TimeSeriesStatistics {

  public static final String[] HEADER = new String[] {
      "name", "count", "meanInterval", "maxInterval", "minInterval", "meanVal", "maxVal", "minVal"
  };

  private String name;
  private int size = 0;
  private double meanInterval = 0.0;
  private long maxInterval = Long.MIN_VALUE;
  private long minInterval = Long.MAX_VALUE;
  private double meanVal = 0.0;
  private double maxVal = Double.MIN_VALUE;
  private double minVal = Double.MAX_VALUE;

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
    Object[] ret = new Object[HEADER.length];
    int i = 0;
    ret[i++] = name;
    ret[i++] = size;
    ret[i++] = meanInterval;
    ret[i++] = maxInterval;
    ret[i++] = minInterval;
    ret[i++] = meanVal;
    ret[i++] = maxVal;
    ret[i] = minVal;
    return ret;
  }
}