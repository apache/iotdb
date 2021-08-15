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
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * in clause.
 *
 * @param <T> comparable data type
 */
public class In<T extends Comparable<T>> implements Filter {

  private static final long serialVersionUID = 8572705136773595399L;

  protected Set<T> values;

  protected boolean not;

  protected FilterType filterType;

  public In() {}

  public In(Set<T> values, FilterType filterType, boolean not) {
    this.values = values;
    this.filterType = filterType;
    this.not = not;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return true;
  }

  @Override
  public boolean satisfy(long time, Object value) {
    Object v = filterType == FilterType.TIME_FILTER ? time : value;
    return this.values.contains(v) != not;
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return true;
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return true;
  }

  @Override
  public Filter copy() {
    return new In(new HashSet(values), filterType, not);
  }

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.write(not, outputStream);
      outputStream.write(values.size());
      for (T value : values) {
        ReadWriteIOUtils.writeObject(value, outputStream);
      }
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    not = ReadWriteIOUtils.readBool(buffer);
    values = new HashSet<>();
    for (int i = 0; i < buffer.get(); i++) {
      values.add((T) ReadWriteIOUtils.readObject(buffer));
    }
  }

  @Override
  public String toString() {
    List<T> valueList = new ArrayList<>(values);
    Collections.sort(valueList);
    return filterType + " < " + "reverse: " + not + ", " + valueList;
  }

  @Override
  public FilterSerializeId getSerializeId() {
    return FilterSerializeId.IN;
  }

  public Set<T> getValues() {
    return values;
  }
}
