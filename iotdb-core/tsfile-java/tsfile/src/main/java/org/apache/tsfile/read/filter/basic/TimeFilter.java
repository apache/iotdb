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

package org.apache.tsfile.read.filter.basic;

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;

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
  public boolean satisfyBoolean(long time, boolean value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyInteger(long time, int value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyLong(long time, long value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyFloat(long time, float value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyDouble(long time, double value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyBinary(long time, Binary value) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyBooleanRow(long time, boolean[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyIntegerRow(long time, int[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyLongRow(long time, long[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyFloatRow(long time, float[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyDoubleRow(long time, double[] values) {
    // only use time to filter
    return timeSatisfy(time);
  }

  @Override
  public boolean satisfyBinaryRow(long time, Binary[] values) {
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

  @Override
  public boolean[] satisfyTsBlock(boolean[] selection, TsBlock tsBlock) {
    boolean[] satisfyInfo = new boolean[selection.length];
    System.arraycopy(selection, 0, satisfyInfo, 0, selection.length);
    for (int i = 0; i < selection.length; i++) {
      if (selection[i]) {
        satisfyInfo[i] = timeSatisfy(tsBlock.getTimeByIndex(i));
      }
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
