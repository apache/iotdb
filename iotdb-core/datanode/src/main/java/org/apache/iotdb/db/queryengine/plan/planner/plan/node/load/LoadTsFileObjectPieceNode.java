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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.load.splitter.LoadTsFileObjectFileBatch;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;

import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.exception.write.PageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LoadTsFileObjectPieceNode extends WritePlanNode {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileObjectPieceNode.class);

  private final LoadTsFileObjectFileBatch objectBatch;

  public LoadTsFileObjectPieceNode(
      final PlanNodeId id, final LoadTsFileObjectFileBatch objectBatch) {
    super(id);
    this.objectBatch = Objects.requireNonNull(objectBatch, "objectBatch");
  }

  public LoadTsFileObjectFileBatch getObjectBatch() {
    return objectBatch;
  }

  public long getDataSize() {
    return objectBatch.getDataSize();
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(final PlanNode child) {
    // no-op
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.LOAD_TSFILE_OBJECT_PIECE;
  }

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of LoadTsFileObjectPieceNode is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    try {
      final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
      final DataOutputStream stream = new DataOutputStream(byteOutputStream);
      serializeAttributes(stream);
      byteBuffer.put(byteOutputStream.toByteArray());
    } catch (final IOException e) {
      LOGGER.error("Serialize to ByteBuffer error.", e);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.LOAD_TSFILE_OBJECT_PIECE.serialize(stream);
    objectBatch.serialize(stream);
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final LoadTsFilePieceNode.ByteBufferInputStream stream =
        new LoadTsFilePieceNode.ByteBufferInputStream(buffer);
    try {
      final TsFileData data = TsFileData.deserialize(stream);
      if (!(data instanceof LoadTsFileObjectFileBatch)) {
        LOGGER.error(
            "Expected LoadTsFileObjectFileBatch in LoadTsFileObjectPieceNode, got {}",
            data.getType());
        return null;
      }
      return new LoadTsFileObjectPieceNode(
          PlanNodeId.deserialize(stream), (LoadTsFileObjectFileBatch) data);
    } catch (IOException | PageException | IllegalPathException e) {
      LOGGER.error("Deserialize {} error.", LoadTsFileObjectPieceNode.class.getName(), e);
      return null;
    }
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    throw new NotImplementedException("split LoadTsFileObjectPieceNode is not implemented");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LoadTsFileObjectPieceNode that = (LoadTsFileObjectPieceNode) o;
    return Objects.equals(objectBatch, that.objectBatch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectBatch);
  }

  @Override
  public String toString() {
    return "LoadTsFileObjectPieceNode{dataSize=" + getDataSize() + '}';
  }
}
