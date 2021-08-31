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
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.Serializable;

public class TimeValuePair implements Serializable, Comparable<TimeValuePair> {

  private long timestamp;
  private TsPrimitiveType value;

  public TimeValuePair(long timestamp, TsPrimitiveType value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public TsPrimitiveType getValue() {
    return value;
  }

  public void setValue(TsPrimitiveType value) {
    this.value = value;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(timestamp).append(" : ").append(getValue());
    return stringBuilder.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof TimeValuePair) {
      return ((TimeValuePair) object).getTimestamp() == timestamp
          && ((TimeValuePair) object).getValue() != null
          && ((TimeValuePair) object).getValue().equals(value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return ((Long) timestamp).hashCode() + value.hashCode();
  }

  public int getSize() {
    return 8 + 8 + value.getSize();
  }

  @Override
  public int compareTo(TimeValuePair o) {
    return Long.compare(this.getTimestamp(), o.getTimestamp());
  }
}
