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

package org.apache.iotdb.db.mpp.plan.statement.internal;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSetInfo;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class InvalidateSchemaCacheStatement extends Statement {

  private final List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList;

  private final DataPartition dataPartition;

  public InvalidateSchemaCacheStatement(
      List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList,
      DataPartition dataPartition) {
    super();
    this.regionRequestList = regionRequestList;
    this.dataPartition = dataPartition;
    setType(StatementType.INVALIDATE_SCHEMA_CACHE);
  }

  public List<Pair<RegionReplicaSetInfo, List<PartialPath>>> getRegionRequestList() {
    return regionRequestList;
  }

  public DataPartition getDataPartition() {
    return dataPartition;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return regionRequestList.stream()
        .map(Pair::getRight)
        .flatMap(Collection::parallelStream)
        .collect(Collectors.toList());
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInvalidateSchemaCache(this, context);
  }
}
