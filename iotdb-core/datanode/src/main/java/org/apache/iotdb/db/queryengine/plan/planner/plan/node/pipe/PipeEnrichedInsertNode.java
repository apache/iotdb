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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.consensus.statemachine.dataregion.DataExecutionVisitor;
import org.apache.iotdb.db.exception.DataTypeInconsistentException;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AbstractMemTable;
import org.apache.iotdb.db.trigger.executor.TriggerFireVisitor;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class aims to mark the {@link InsertNode} to prevent forwarding pipe insertions. The
 * handling logic is defined in:
 *
 * <p>1.{@link RegionWriteExecutor}, to serialize and reach the target data region.
 *
 * <p>2.{@link TriggerFireVisitor}, to fire the trigger before writing to data region.
 *
 * <p>3.{@link DataExecutionVisitor}, to actually write data on data region and mark it as received
 * from pipe.
 */
public class PipeEnrichedInsertNode extends InsertNode {

  private final InsertNode insertNode;

  public PipeEnrichedInsertNode(final InsertNode insertNode) {
    super(insertNode.getPlanNodeId());
    this.insertNode = insertNode;
  }

  public InsertNode getInsertNode() {
    return insertNode;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return insertNode.isGeneratedByPipe();
  }

  @Override
  public void markAsGeneratedByPipe() {
    insertNode.markAsGeneratedByPipe();
  }

  @Override
  public PlanNodeId getPlanNodeId() {
    return insertNode.getPlanNodeId();
  }

  @Override
  public void setPlanNodeId(final PlanNodeId id) {
    insertNode.setPlanNodeId(id);
  }

  @Override
  public List<PlanNode> getChildren() {
    return insertNode.getChildren();
  }

  @Override
  public void addChild(final PlanNode child) {
    insertNode.addChild(child);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.PIPE_ENRICHED_INSERT_DATA;
  }

  @Override
  public PlanNode clone() {
    return new PipeEnrichedInsertNode((InsertNode) insertNode.clone());
  }

  @Override
  public PlanNode createSubNode(final int subNodeId, final int startIndex, final int endIndex) {
    return new PipeEnrichedInsertNode(
        (InsertNode) insertNode.createSubNode(subNodeId, startIndex, endIndex));
  }

  @Override
  public PlanNode cloneWithChildren(final List<PlanNode> children) {
    return new PipeEnrichedInsertNode((InsertNode) insertNode.cloneWithChildren(children));
  }

  @Override
  public int allowedChildCount() {
    return insertNode.allowedChildCount();
  }

  @Override
  public List<String> getOutputColumnNames() {
    return insertNode.getOutputColumnNames();
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitPipeEnrichedInsertNode(this, context);
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    return insertNode.splitByPartition(analysis).stream()
        .map(
            plan ->
                plan instanceof PipeEnrichedInsertNode
                    ? plan
                    : new PipeEnrichedInsertNode((InsertNode) plan))
        .collect(Collectors.toList());
  }

  @Override
  public InsertNode mergeInsertNode(List<InsertNode> insertNodes) {
    return insertNode.mergeInsertNode(insertNodes);
  }

  @Override
  public TRegionReplicaSet getDataRegionReplicaSet() {
    return insertNode.getDataRegionReplicaSet();
  }

  @Override
  public void setDataRegionReplicaSet(final TRegionReplicaSet dataRegionReplicaSet) {
    insertNode.setDataRegionReplicaSet(dataRegionReplicaSet);
  }

  @Override
  public PartialPath getTargetPath() {
    return insertNode.getTargetPath();
  }

  @Override
  public void setTargetPath(final PartialPath targetPath) {
    insertNode.setTargetPath(targetPath);
  }

  @Override
  public boolean isAligned() {
    return insertNode.isAligned();
  }

  @Override
  public void setAligned(final boolean aligned) {
    insertNode.setAligned(aligned);
  }

  @Override
  public MeasurementSchema[] getMeasurementSchemas() {
    return insertNode.getMeasurementSchemas();
  }

  @Override
  public void setMeasurementSchemas(final MeasurementSchema[] measurementSchemas) {
    insertNode.setMeasurementSchemas(measurementSchemas);
  }

  @Override
  public String[] getMeasurements() {
    return insertNode.getMeasurements();
  }

  @Override
  public TSDataType[] getDataTypes() {
    return insertNode.getDataTypes();
  }

  @Override
  public TSDataType getDataType(final int index) {
    return insertNode.getDataType(index);
  }

  @Override
  public void setDataTypes(final TSDataType[] dataTypes) {
    insertNode.setDataTypes(dataTypes);
  }

  @Override
  public IDeviceID getDeviceID() {
    return insertNode.getDeviceID();
  }

  @Override
  public void setDeviceID(final IDeviceID deviceID) {
    insertNode.setDeviceID(deviceID);
  }

  @Override
  public long getSearchIndex() {
    return insertNode.getSearchIndex();
  }

  @Override
  public SearchNode setSearchIndex(final long searchIndex) {
    insertNode.setSearchIndex(searchIndex);
    return this;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.PIPE_ENRICHED_INSERT_DATA.serialize(byteBuffer);
    insertNode.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.PIPE_ENRICHED_INSERT_DATA.serialize(stream);
    insertNode.serialize(stream);
  }

  public static PipeEnrichedInsertNode deserialize(final ByteBuffer buffer) {
    return new PipeEnrichedInsertNode((InsertNode) PlanNodeType.deserialize(buffer));
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return insertNode.getRegionReplicaSet();
  }

  @Override
  public long getMinTime() {
    return insertNode.getMinTime();
  }

  @Override
  public void markFailedMeasurement(final int index) {
    insertNode.markFailedMeasurement(index);
  }

  @Override
  public boolean hasValidMeasurements() {
    return insertNode.hasValidMeasurements();
  }

  @Override
  public void setFailedMeasurementNumber(final int failedMeasurementNumber) {
    insertNode.setFailedMeasurementNumber(failedMeasurementNumber);
  }

  @Override
  public int getFailedMeasurementNumber() {
    return insertNode.getFailedMeasurementNumber();
  }

  @Override
  public void setProgressIndex(final ProgressIndex progressIndex) {
    insertNode.setProgressIndex(progressIndex);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return insertNode.getProgressIndex();
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PipeEnrichedInsertNode
        && insertNode.equals(((PipeEnrichedInsertNode) o).insertNode);
  }

  @Override
  public int hashCode() {
    return insertNode.hashCode();
  }

  @Override
  public void checkDataType(AbstractMemTable memTable) throws DataTypeInconsistentException {
    insertNode.checkDataType(memTable);
  }
}
