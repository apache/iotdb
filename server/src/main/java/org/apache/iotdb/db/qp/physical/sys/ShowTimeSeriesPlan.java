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
package org.apache.iotdb.db.qp.physical.sys;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

public class ShowTimeSeriesPlan extends ShowPlan {

  // path can be root, root.*  root.*.*.a etc.. if the wildcard is not at the tail, then each
  // * wildcard can only match one level, otherwise it can match to the tail.
  private PartialPath path;
  private boolean isContains;
  private String key;
  private String value;
  private int limit = 0;
  private int offset = 0;
  // if is true, the result will be sorted according to the inserting frequency of the timeseries
  private boolean orderByHeat;

  private boolean hasLimit;

  public ShowTimeSeriesPlan(PartialPath path) {
    super(ShowContentType.TIMESERIES);
    this.path = path;
  }

  public ShowTimeSeriesPlan(PartialPath path, boolean isContains, String key, String value, int limit,
      int offset, boolean orderByHeat) {
    super(ShowContentType.TIMESERIES);
    this.path = path;
    this.isContains = isContains;
    this.key = key;
    this.value = value;
    this.limit = limit;
    this.offset = offset;
    this.orderByHeat = orderByHeat;
  }

  public ShowTimeSeriesPlan() {
    super(ShowContentType.TIMESERIES);
  }

  public PartialPath getPath() {
    return this.path;
  }

  public boolean isContains() {
    return isContains;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isOrderByHeat() {
    return orderByHeat;
  }

  public void setOrderByHeat(boolean orderByHeat) {
    this.orderByHeat = orderByHeat;
  }

  public boolean hasLimit() {
    return hasLimit;
  }

  public void setHasLimit(boolean hasLimit) {
    this.hasLimit = hasLimit;
  }

  @Override
  public void serialize(DataOutputStream outputStream) throws IOException {
    outputStream.write(PhysicalPlanType.SHOW_TIMESERIES.ordinal());

    putString(outputStream, path.getFullPath());
    outputStream.writeBoolean(isContains);
    putString(outputStream, key);
    putString(outputStream, value);

    outputStream.writeInt(limit);
    outputStream.writeInt(offset);
    outputStream.writeBoolean(orderByHeat);

    outputStream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    isContains = buffer.get() == 1;
    key = readString(buffer);
    value = readString(buffer);

    limit = buffer.getInt();
    limit = buffer.getInt();
    orderByHeat = buffer.get() == 1;

    this.index = buffer.getLong();
  }
}