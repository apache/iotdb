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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * For IoTConsensus sync. See <a href="https://github.com/apache/iotdb/pull/12955">github pull
 * request</a> for details.
 */
public class ContinuousSameSearchIndexSeparatorNode extends SearchNode implements WALEntryValue {

  public ContinuousSameSearchIndexSeparatorNode() {
    super(new PlanNodeId(""));
  }

  public ContinuousSameSearchIndexSeparatorNode(PlanNodeId id) {
    super(id);
    this.searchIndex = -1;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(PlanNodeType.CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR.getNodeType());
    // search index is always -1
    buffer.putLong(-1);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES + Long.BYTES;
  }

  public static ContinuousSameSearchIndexSeparatorNode deserializeFromWAL(DataInputStream stream)
      throws IOException {
    long ignored = stream.readLong();
    return new ContinuousSameSearchIndexSeparatorNode(new PlanNodeId(""));
  }

  public static ContinuousSameSearchIndexSeparatorNode deserializeFromWAL(ByteBuffer buffer) {
    long ignored = buffer.getLong();
    return new ContinuousSameSearchIndexSeparatorNode(new PlanNodeId(""));
  }

  // region all operations below are unsupported

  private static final String UNSUPPORTED_MESSAGE =
      "ContinuousSameSearchIndexSeparatorNode not support this operation";

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public PlanNode clone() {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return null;
  }
}
