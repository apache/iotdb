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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrderBy extends Node {

  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(OrderBy.class);

  private final List<SortItem> sortItems;

  public OrderBy(List<SortItem> sortItems) {
    super(null);
    requireNonNull(sortItems, "sortItems is null");
    checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
    this.sortItems = ImmutableList.copyOf(sortItems);
  }

  public OrderBy(NodeLocation location, List<SortItem> sortItems) {
    super(requireNonNull(location, "location is null"));
    requireNonNull(sortItems, "sortItems is null");
    checkArgument(!sortItems.isEmpty(), "sortItems should not be empty");
    this.sortItems = ImmutableList.copyOf(sortItems);
  }

  public List<SortItem> getSortItems() {
    return sortItems;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitOrderBy(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return sortItems;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("sortItems", sortItems).toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    OrderBy o = (OrderBy) obj;
    return Objects.equals(sortItems, o.sortItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortItems);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(sortItems.size(), buffer);
    for (SortItem sortItem : sortItems) {
      sortItem.serialize(buffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(sortItems.size(), stream);
    for (SortItem sortItem : sortItems) {
      sortItem.serialize(stream);
    }
  }

  public OrderBy(ByteBuffer byteBuffer) {
    super(null);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    sortItems = new ArrayList<>(size);

    for (int i = 0; i < size; i++) {
      sortItems.add(new SortItem(byteBuffer));
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(sortItems);
    return size;
  }
}
