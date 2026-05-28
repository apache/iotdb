/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.IPlanVisitor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.TableScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExternalTsFileScanNode extends TableScanNode {
  private List<String> tsFilePaths;

  protected ExternalTsFileScanNode() {}

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      List<String> tsFilePaths) {
    super(id, qualifiedObjectName, outputSymbols, assignments);
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
  }

  public ExternalTsFileScanNode(
      PlanNodeId id,
      QualifiedObjectName qualifiedObjectName,
      List<Symbol> outputSymbols,
      Map<Symbol, ColumnSchema> assignments,
      Expression pushDownPredicate,
      long pushDownLimit,
      long pushDownOffset,
      List<String> tsFilePaths) {
    super(
        id,
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset);
    this.tsFilePaths = Collections.unmodifiableList(new ArrayList<>(tsFilePaths));
  }

  @Override
  public <R, C> R accept(IPlanVisitor<R, C> visitor, C context) {
    return ((PlanVisitor<R, C>) visitor).visitExternalTsFileScan(this, context);
  }

  @Override
  public ExternalTsFileScanNode clone() {
    return new ExternalTsFileScanNode(
        getPlanNodeId(),
        qualifiedObjectName,
        outputSymbols,
        assignments,
        pushDownPredicate,
        pushDownLimit,
        pushDownOffset,
        tsFilePaths);
  }

  public List<String> getTsFilePaths() {
    return tsFilePaths;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.EXTERNAL_TSFILE_SCAN_NODE.serialize(byteBuffer);
    TableScanNode.serializeMemberVariables(this, byteBuffer, true);
    serializeTsFilePaths(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.EXTERNAL_TSFILE_SCAN_NODE.serialize(stream);
    TableScanNode.serializeMemberVariables(this, stream, true);
    serializeTsFilePaths(stream);
  }

  private void serializeTsFilePaths(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(tsFilePaths.size(), byteBuffer);
    for (String tsFilePath : tsFilePaths) {
      ReadWriteIOUtils.write(tsFilePath, byteBuffer);
    }
  }

  private void serializeTsFilePaths(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tsFilePaths.size(), stream);
    for (String tsFilePath : tsFilePaths) {
      ReadWriteIOUtils.write(tsFilePath, stream);
    }
  }

  public static ExternalTsFileScanNode deserialize(ByteBuffer byteBuffer) {
    ExternalTsFileScanNode node = new ExternalTsFileScanNode();
    TableScanNode.deserializeMemberVariables(byteBuffer, node, true);

    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<String> tsFilePaths = new ArrayList<>(size);
    while (size-- > 0) {
      tsFilePaths.add(ReadWriteIOUtils.readString(byteBuffer));
    }
    node.tsFilePaths = Collections.unmodifiableList(tsFilePaths);

    node.setPlanNodeId(PlanNodeId.deserialize(byteBuffer));
    return node;
  }

  @Override
  public String toString() {
    return "ExternalTsFileScanNode-" + this.getPlanNodeId();
  }
}
