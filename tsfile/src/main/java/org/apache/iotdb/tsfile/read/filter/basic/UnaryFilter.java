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

import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Definition for unary filter operations.
 *
 * @param <T> comparable data type
 */
public abstract class UnaryFilter<T extends Comparable<T>> implements Filter, Serializable {

  private static final long serialVersionUID = 1431606024929453556L;
  protected T value;

  protected FilterType filterType;

  public UnaryFilter() {}

  protected UnaryFilter(T value, FilterType filterType) {
    this.value = value;
    this.filterType = filterType;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public FilterType getFilterType() {
    return filterType;
  }

  @Override
  public abstract String toString();

  @Override
  public abstract Filter copy();

  @Override
  public void serialize(DataOutputStream outputStream) {
    try {
      outputStream.write(getSerializeId().ordinal());
      outputStream.write(filterType.ordinal());
      ReadWriteIOUtils.writeObject(value, outputStream);
    } catch (IOException ignored) {
      // ignored
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    filterType = FilterType.values()[buffer.get()];
    value = (T) ReadWriteIOUtils.readObject(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UnaryFilter)) {
      return false;
    }
    UnaryFilter other = ((UnaryFilter) obj);
    return this.value.equals(other.value)
        && this.filterType.equals(other.filterType)
        && this.getSerializeId().equals(other.getSerializeId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, filterType, getSerializeId());
  }
}
