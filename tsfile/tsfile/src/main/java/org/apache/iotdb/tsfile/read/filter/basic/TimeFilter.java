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

package org.apache.iotdb.tsfile.read.filter.basic;

import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.Serializable;

public abstract class TimeFilter extends Filter {

  protected TimeFilter() {
    // do nothing
  }

  @Override
  public boolean satisfy(long time, Object value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      satisfyInfo[i] = timeSatisfy(tsBlock.getTimeByIndex(i));
    }
    return satisfyInfo;
  }

  protected abstract boolean timeSatisfy(long time);

  @Override
  public boolean canSkip(IMetadata metadata) {
    Statistics<? extends Serializable> timeStatistics = metadata.getTimeStatistics();
    return !satisfyStartEndTime(timeStatistics.getStartTime(), timeStatistics.getEndTime());
  }

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    Statistics<? extends Serializable> timeStatistics = metadata.getTimeStatistics();
    return containStartEndTime(timeStatistics.getStartTime(), timeStatistics.getEndTime());
  }
}
