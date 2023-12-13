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
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class ValueFilter extends Filter {

  protected int measurementIndex;

  protected ValueFilter(int measurementIndex) {
    this.measurementIndex = measurementIndex;
  }

  protected ValueFilter(ByteBuffer buffer) {
    this.measurementIndex = ReadWriteIOUtils.readInt(buffer);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    if (value == null) {
      // null not satisfy any filter, except IS NULL
      return false;
    }
    return valueSatisfy(value);
  }

  @Override
  public boolean satisfyRow(long time, Object[] values) {
    return satisfy(time, values[measurementIndex]);
  }

  @Override
  public boolean[] satisfyTsBlock(TsBlock tsBlock) {
    Column valueColumn = tsBlock.getValueColumns()[measurementIndex];
    boolean[] satisfyInfo = new boolean[tsBlock.getPositionCount()];
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      if (valueColumn.isNull(i)) {
        // null not satisfy any filter, except IS NULL
        satisfyInfo[i] = false;
      } else {
        satisfyInfo[i] = valueSatisfy(valueColumn.getObject(i));
      }
    }
    return satisfyInfo;
  }

  protected abstract boolean valueSatisfy(Object value);

  @Override
  public boolean canSkip(IMetadata metadata) {
    Optional<Statistics<? extends Serializable>> statistics =
        metadata.getMeasurementStatistics(measurementIndex);
    return statistics.map(this::canSkip).orElse(true);
  }

  protected abstract boolean canSkip(Statistics<? extends Serializable> statistics);

  @Override
  public boolean allSatisfy(IMetadata metadata) {
    Optional<Statistics<? extends Serializable>> statistics =
        metadata.getMeasurementStatistics(measurementIndex);
    return statistics.map(this::allSatisfy).orElse(false);
  }

  protected abstract boolean allSatisfy(Statistics<? extends Serializable> statistics);

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return true;
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return false;
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    throw new UnsupportedOperationException("Value filter does not support getTimeRanges()");
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    ReadWriteIOUtils.write(measurementIndex, outputStream);

    // serialize more fields in subclasses
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValueFilter that = (ValueFilter) o;
    return measurementIndex == that.measurementIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(measurementIndex);
  }
}
