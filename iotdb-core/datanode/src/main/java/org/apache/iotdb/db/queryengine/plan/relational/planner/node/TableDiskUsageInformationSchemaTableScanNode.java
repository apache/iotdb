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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableDiskUsageInformationSchemaTableScanNode extends InformationSchemaTableScanNode {

  private List<Integer> regions;

  public TableDiskUsageInformationSchemaTableScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      TRegionReplicaSet regionReplicaSet,
      List<Integer> regions) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet);
    this.regions = regions;
  }

  private TableDiskUsageInformationSchemaTableScanNode() {
    super();
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DISK_USAGE_INFORMATION_SCHEMA_TABLE_SCAN_NODE;
  }

  public void setRegions(List<Integer> regions) {
    this.regions = regions;
  }

  public List<Integer> getRegions() {
    return regions;
  }

  @Override
  public PlanNode clone() {
    return new TableDiskUsageInformationSchemaTableScanNode(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        regionReplicaSet,
        regions);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    super.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(regions.size(), byteBuffer);
    for (Integer region : regions) {
      ReadWriteIOUtils.write(region, byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    super.serializeAttributes(stream);
    ReadWriteIOUtils.write(regions.size(), stream);
    for (Integer region : regions) {
      ReadWriteIOUtils.write(region, stream);
    }
  }

  public static InformationSchemaTableScanNode deserialize(ByteBuffer byteBuffer) {
    TableDiskUsageInformationSchemaTableScanNode node =
        new TableDiskUsageInformationSchemaTableScanNode();
    TableScanNode.deserializeMemberVariables(byteBuffer, node, true);
    int length = ReadWriteIOUtils.readInt(byteBuffer);
    List<Integer> regions = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      regions.add(ReadWriteIOUtils.readInt(byteBuffer));
    }

    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    node.regions = regions;
    return node;
  }
}
