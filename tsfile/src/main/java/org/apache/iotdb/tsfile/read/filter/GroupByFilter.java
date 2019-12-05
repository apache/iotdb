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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

import java.io.Serializable;

public class GroupByFilter implements Filter, Serializable {

  private static final long serialVersionUID = -1211805021419281440L;
  private final long unit;
  private final long slidingStep;
  private final long startTime;
  private final long endTime;
  private final FilterType filterType;

  public GroupByFilter(long unit, long slidingStep, long startTime, long endTime, FilterType filterType) {
    this.unit = unit;
    this.slidingStep = slidingStep;
    this.startTime = startTime;
    this.endTime = endTime;
    this.filterType = filterType;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return satisfyStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  @Override
  public boolean satisfy(long time, Object value) {
    if (time < startTime || time > endTime)
      return false;
    else
      return (time - startTime) % slidingStep <= unit;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    if (endTime < this.startTime)
      return false;
    else if (startTime <= this.startTime)
      return true;
    else if (startTime > this.endTime)
      return false;
    else {
      long minTime = startTime - this.startTime;
      long count = minTime / slidingStep;
      if (minTime <= unit + count * slidingStep)
        return true;
      else {
        if (this.endTime <= (count + 1) * slidingStep + this.startTime) {
          return false;
        }
        else {
          return endTime >= (count + 1) * slidingStep + this.startTime;
        }
      }
    }
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    if (startTime >= this.startTime && endTime <= this.endTime) {
      long minTime = startTime - this.startTime;
      long maxTime = endTime - this.startTime;
      long count = minTime / slidingStep;
      return minTime <= unit + count * slidingStep && maxTime <= unit + count * slidingStep;
    }
    return false;
  }

  @Override
  public Filter clone() {
    return new GroupByFilter(unit, slidingStep, startTime, endTime, filterType);
  }

  @Override
  public String toString() {
    return "GroupByFilter{}";
  }
}
