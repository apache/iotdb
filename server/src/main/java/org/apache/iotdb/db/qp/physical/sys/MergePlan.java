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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MergePlan extends PhysicalPlan {

  public MergePlan(OperatorType operatorType) {
    super(false, operatorType);
  }

  public MergePlan() {
    super(false, OperatorType.MERGE);
  }

  public MergePlan(OperatorType operatorType, List<PartialPath> storageGroups) {
    super(false, operatorType);
    if (storageGroups == null) {
      this.storageGroupPartitionIds = null;
    } else {
      this.storageGroupPartitionIds = new HashMap<>();
      for (PartialPath path : storageGroups) {
        this.storageGroupPartitionIds.put(path, null);
      }
    }
  }

  public MergePlan(List<PartialPath> storageGroups) {
    super(false, OperatorType.MERGE);
    if (storageGroups == null) {
      this.storageGroupPartitionIds = null;
    } else {
      this.storageGroupPartitionIds = new HashMap<>();
      for (PartialPath path : storageGroups) {
        this.storageGroupPartitionIds.put(path, null);
      }
    }
  }

  private Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupPartitionIds;

  @Override
  public List<PartialPath> getPaths() {
    if (storageGroupPartitionIds == null) {
      return Collections.emptyList();
    }
    List<PartialPath> ret = new ArrayList<>();
    for (Map.Entry<PartialPath, List<Pair<Long, Boolean>>> entry :
        storageGroupPartitionIds.entrySet()) {
      ret.add(entry.getKey());
    }
    return ret;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.MERGE.ordinal());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.MERGE.ordinal());
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {}
}
