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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class SetStorageGroupPlan extends PhysicalPlan {
  private PartialPath path;

  public SetStorageGroupPlan() {
    super(false, Operator.OperatorType.SET_STORAGE_GROUP);
  }

  public SetStorageGroupPlan(PartialPath path) {
    super(false, Operator.OperatorType.SET_STORAGE_GROUP);
    this.path = path;
  }
  
  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }
  
  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();
    if (path != null) {
      ret.add(path);
    }
    return ret;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.SET_STORAGE_GROUP.ordinal());
    byte[] fullPathBytes = path.getFullPath().getBytes();
    stream.writeInt(fullPathBytes.length);
    stream.write(fullPathBytes);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int length = buffer.getInt();
    byte[] fullPathBytes = new byte[length];
    buffer.get(fullPathBytes);
    path = new PartialPath(new String(fullPathBytes));
  }

  @Override
  public String toString() {
    return "SetStorageGroup{" + path + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SetStorageGroupPlan that = (SetStorageGroupPlan) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
