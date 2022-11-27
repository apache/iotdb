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

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/** Filter is a top level filter abstraction. */
public interface Filter {

  /**
   * To examine whether the statistics is satisfied with the filter.
   *
   * @param statistics statistics with min time, max time, min value, max value.
   */
  boolean satisfy(Statistics statistics);

  /**
   * To examine whether the single point(with time and value) is satisfied with the filter.
   *
   * @param time single point time
   * @param value single point value
   */
  boolean satisfy(long time, Object value);

  /**
   * To examine whether the min time and max time are satisfied with the filter.
   *
   * @param startTime start time of a page, series or device
   * @param endTime end time of a page, series or device
   */
  boolean satisfyStartEndTime(long startTime, long endTime);

  /**
   * To examine whether the partition [startTime, endTime] is subsets of filter.
   *
   * @param startTime start time of a partition
   * @param endTime end time of a partition
   */
  boolean containStartEndTime(long startTime, long endTime);

  Filter copy();

  void serialize(DataOutputStream outputStream);

  default void serialize(ByteBuffer buffer) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    this.serialize(dataOutputStream);
    buffer.put(byteArrayOutputStream.toByteArray());
  }

  void deserialize(ByteBuffer buffer);

  FilterSerializeId getSerializeId();

  default List<TimeRange> getTimeRanges() {
    return Collections.emptyList();
  }
}
