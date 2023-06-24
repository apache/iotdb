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

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NotFilter implements Filter, Serializable {

  private static final long serialVersionUID = 584860326604020881L;
  private Filter that;

  public static final String CONTAIN_NOT_ERR_MSG =
      "This predicate contains a not! Did you forget to run this predicate through PredicateRemoveNotRewriter? ";

  public NotFilter() {}

  public NotFilter(Filter that) {
    this.that = that;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    throw new UnsupportedOperationException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean allSatisfy(Statistics statistics) {
    throw new UnsupportedOperationException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return !that.satisfy(time, value);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    throw new UnsupportedOperationException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    throw new UnsupportedOperationException(CONTAIN_NOT_ERR_MSG + this);
  }

  @Override
  public Filter copy() {
    return new NotFilter(that.copy());
  }

  public Filter getFilter() {
    return this.that;
  }

  @Override
  public String toString() {
    return "not (" + that + ")";
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      that.serialize(outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    that = FilterFactory.deserialize(buffer);
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.NOT;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NotFilter)) {
      return false;
    }
    NotFilter other = ((NotFilter) obj);
    return this.that.equals(other.that);
  }

  @Override
  public int hashCode() {
    return Objects.hash(that);
  }

  @Override
  public List<TimeRange> getTimeRanges() {
    List<TimeRange> list = that.getTimeRanges();
    if (list.isEmpty()) {
      return list;
    }
    List<TimeRange> res = new ArrayList<>();
    if (list.get(0).getMin() != Long.MIN_VALUE) {
      res.add(new TimeRange(Long.MIN_VALUE, list.get(0).getMin() - 1));
    }
    for (int i = 1, size = list.size(); i < size; i++) {
      long left = list.get(i - 1).getMax() + 1;
      long right = list.get(i).getMin() - 1;
      if (left <= right) {
        res.add(new TimeRange(left, right));
      }
    }

    if (list.get(list.size() - 1).getMax() != Long.MAX_VALUE) {
      res.add(new TimeRange(list.get(list.size() - 1).getMax() + 1, Long.MAX_VALUE));
    }
    return res;
  }

  @Override
  public Filter reverse() {
    return that;
  }
}
