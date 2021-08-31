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
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MNodePlan extends PhysicalPlan {
  protected String name;
  protected int childSize;

  public MNodePlan() {
    super(false, Operator.OperatorType.MNODE);
  }

  public MNodePlan(String name, int childSize) {
    super(false, Operator.OperatorType.MNODE);
    this.name = name;
    this.childSize = childSize;
  }

  public MNodePlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }

  @Override
  public List<PartialPath> getPaths() {
    return new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getChildSize() {
    return childSize;
  }

  public void setChildSize(int childSize) {
    this.childSize = childSize;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.MNODE.ordinal());
    putString(buffer, name);
    buffer.putInt(childSize);
    buffer.putLong(index);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.MNODE.ordinal());
    putString(stream, name);
    stream.writeInt(childSize);
    stream.writeLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = readString(buffer);
    childSize = buffer.getInt();
    index = buffer.getLong();
  }

  @Override
  public String toString() {
    return "MNode{" + name + "," + childSize + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MNodePlan that = (MNodePlan) o;
    return Objects.equals(name, that.name) && Objects.equals(childSize, that.childSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, childSize);
  }
}
