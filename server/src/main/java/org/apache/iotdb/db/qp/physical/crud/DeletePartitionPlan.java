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

import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class DeletePartitionPlan extends PhysicalPlan {

  private String storageGroupName;
  private Set<Long> partitionId;

  public DeletePartitionPlan(String storageGroupName, Set<Long> partitionId) {
    super(false, OperatorType.DELETE_PARTITION);
    this.storageGroupName = storageGroupName;
    this.partitionId = partitionId;
  }

  @Override
  public List<Path> getPaths() {
    return null;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public Set<Long> getPartitionId() {
    return partitionId;
  }
}
