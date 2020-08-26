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
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class FlushPlan extends PhysicalPlan {

  private List<Path> storageGroups;

  private Boolean isSeq;

  private boolean isSync;

  /**
   * only for deserialize
   */
  public FlushPlan() {
    super(false, OperatorType.FLUSH);
  }

  public FlushPlan(Boolean isSeq, List<Path> storageGroups) {
    super(false, OperatorType.FLUSH);
    this.storageGroups = storageGroups;
    this.isSeq = isSeq;
    this.isSync = false;
  }

  public FlushPlan(Boolean isSeq, boolean isSync, List<Path> storageGroups) {
    super(false, OperatorType.FLUSH);
    this.storageGroups = storageGroups;
    this.isSeq = isSeq;
    this.isSync = isSync;
  }

  public Boolean isSeq() {
    return isSeq;
  }

  public boolean isSync() {
    return isSync;
  }

  @Override
  public List<Path> getPaths() {
    return storageGroups;
  }

  @Override
  public List<String> getPathsStrings() {
    List<String> ret = new ArrayList<>();
    for (Path path : storageGroups) {
      ret.add(path.getFullPath());
    }
    return ret;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.FLUSH.ordinal());
    stream.writeByte((isSeq == null || !isSeq) ? 0 : 1);
    stream.writeByte(isSync ? 1 : 0);
    stream.writeInt(storageGroups.size());
    for (Path storageGroup : storageGroups) {
      ReadWriteIOUtils.write(storageGroup.getFullPath(), stream);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.FLUSH.ordinal();
    buffer.put((byte) type);
    buffer.put((byte) ((isSeq == null || !isSeq) ? 0 : 1));
    buffer.put((byte) (isSync ? 1 : 0));
    buffer.putInt(storageGroups.size());
    for (Path storageGroup : storageGroups) {
      ReadWriteIOUtils.write(storageGroup.getFullPath(), buffer);
    }

  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    this.isSeq = (buffer.get() == 1) ? true : null;
    this.isSync = buffer.get() == 1;
    int storageGroupsSize = buffer.getInt();
    this.storageGroups = new ArrayList<>(storageGroupsSize);
    for (int i = 0; i < storageGroupsSize; i++) {
      storageGroups.add(new Path(ReadWriteIOUtils.readString(buffer)));
    }
  }

  @Override
  public String toString() {
    return "FlushPlan{"
        + " storageGroups=" + storageGroups
        + ", isSeq=" + isSeq
        + ", isSync=" + isSync
        + "}";
  }
}
