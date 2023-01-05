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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter;

import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OrderByParameter {

  // items closer to the list head have higher priority
  private final List<SortItem> sortItemList;

  public OrderByParameter(List<SortItem> sortItemList) {
    this.sortItemList = sortItemList;
  }

  public OrderByParameter() {
    this.sortItemList = new ArrayList<>();
  }

  public List<SortItem> getSortItemList() {
    return sortItemList;
  }

  public boolean isEmpty() {
    return sortItemList.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sortInfo = new StringBuilder("OrderBy:");
    for (SortItem sortItem : sortItemList) {
      sortInfo.append(" ");
      sortInfo.append(sortItem.toSQLString());
    }
    return sortInfo.toString();
  }

  public void serializeAttributes(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(sortItemList.size(), byteBuffer);
    for (SortItem sortItem : sortItemList) {
      sortItem.serialize(byteBuffer);
    }
  }

  public void serializeAttributes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sortItemList.size(), stream);
    for (SortItem sortItem : sortItemList) {
      sortItem.serialize(stream);
    }
  }

  public static OrderByParameter deserialize(ByteBuffer byteBuffer) {
    int sortItemSize = ReadWriteIOUtils.readInt(byteBuffer);
    List<SortItem> sortItemList = new ArrayList<>(sortItemSize);
    while (sortItemSize > 0) {
      sortItemList.add(SortItem.deserialize(byteBuffer));
      sortItemSize--;
    }
    return new OrderByParameter(sortItemList);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrderByParameter that = (OrderByParameter) o;
    return sortItemList.equals(that.sortItemList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortItemList);
  }
}
