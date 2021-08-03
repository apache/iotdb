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

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StorageGroupMNodePlan extends MNodePlan {
  private long dataTTL;

  public StorageGroupMNodePlan() {
    super(false, Operator.OperatorType.STORAGE_GROUP_MNODE);
  }

  public StorageGroupMNodePlan(String name, long dataTTL, int childSize) {
    super(false, Operator.OperatorType.STORAGE_GROUP_MNODE);
    this.name = name;
    this.dataTTL = dataTTL;
    this.childSize = childSize;
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>();
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.STORAGE_GROUP_MNODE.ordinal());
    putString(buffer, name);
    buffer.putLong(dataTTL);
    buffer.putInt(childSize);

    buffer.putLong(index);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.STORAGE_GROUP_MNODE.ordinal());
    putString(stream, name);
    stream.writeLong(dataTTL);
    stream.writeInt(childSize);

    stream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = readString(buffer);
    dataTTL = buffer.getLong();
    childSize = buffer.getInt();
    index = buffer.getLong();
  }

  @Override
  public String toString() {
    return "StorageGroupMNode{" + name + "," + dataTTL + "," + childSize + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StorageGroupMNodePlan that = (StorageGroupMNodePlan) o;
    return Objects.equals(name, that.name)
        && Objects.equals(dataTTL, that.dataTTL)
        && Objects.equals(childSize, that.childSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataTTL, childSize);
  }
}
