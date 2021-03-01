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

package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DeletePartitionPlan extends PhysicalPlan {

  private PartialPath storageGroupName;
  private Set<Long> partitionId;

  public DeletePartitionPlan(PartialPath storageGroupName, Set<Long> partitionId) {
    super(false, OperatorType.DELETE_PARTITION);
    this.storageGroupName = storageGroupName;
    this.partitionId = partitionId;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public PartialPath getStorageGroupName() {
    return storageGroupName;
  }

  public Set<Long> getPartitionId() {
    return partitionId;
  }
}
