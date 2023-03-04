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
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/** NotFilter necessary. Use InvertExpressionVisitor */
public class NotFilter implements Filter, Serializable {

  private static final long serialVersionUID = 584860326604020881L;
  private Filter that;

  public NotFilter() {}

  public NotFilter(Filter that) {
    this.that = that;
  }

  @Override
  public boolean satisfy(Statistics statistics) {
    return !that.satisfy(statistics);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return !that.satisfy(time, value);
  }

  /**
   * Notice that, if the not filter only contains value filter, this method may return false, this
   * may cause misunderstanding.
   */
  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return !that.satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public boolean containStartEndTime(long startTime, long endTime) {
    return !that.satisfyStartEndTime(startTime, endTime);
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
    return "NotFilter: " + that;
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
}
