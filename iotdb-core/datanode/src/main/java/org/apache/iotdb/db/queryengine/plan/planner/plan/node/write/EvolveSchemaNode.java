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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution.SchemaEvolution;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.utils.io.IOUtils;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EvolveSchemaNode extends SearchNode implements WALEntryValue {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EvolveSchemaNode.class);

  protected TRegionReplicaSet regionReplicaSet;
  protected ProgressIndex progressIndex;
  private final List<SchemaEvolution> schemaEvolutions;

  public EvolveSchemaNode(PlanNodeId id,
      List<SchemaEvolution> schemaEvolutions) {
    super(id);
    this.schemaEvolutions = schemaEvolutions;
  }

  public static PlanNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    int size = ReadWriteForEncodingUtils.readVarInt(stream);
    List<SchemaEvolution> evolutions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      evolutions.add(SchemaEvolution.createFrom(stream));
    }

    EvolveSchemaNode evolveSchemaNode = new EvolveSchemaNode(new PlanNodeId(""), evolutions);
    evolveSchemaNode.setSearchIndex(searchIndex);

    return evolveSchemaNode;
  }

  public static PlanNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    int size = ReadWriteForEncodingUtils.readVarInt(buffer);
    List<SchemaEvolution> evolutions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      evolutions.add(SchemaEvolution.createFrom(buffer));
    }

    EvolveSchemaNode evolveSchemaNode = new EvolveSchemaNode(new PlanNodeId(""), evolutions);
    evolveSchemaNode.setSearchIndex(searchIndex);

    return evolveSchemaNode;
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    int size = ReadWriteForEncodingUtils.readVarInt(buffer);
    List<SchemaEvolution> evolutions = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      evolutions.add(SchemaEvolution.createFrom(buffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);

    // EvolveSchemaNode has no child
    int ignoredChildrenSize = ReadWriteIOUtils.readInt(buffer);
    return new EvolveSchemaNode(planNodeId, evolutions);
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    return this;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return Collections.singletonList(this);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlanNode clone() {
    return new EvolveSchemaNode(id, schemaEvolutions);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.emptyList();
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.EVOLVE_SCHEMA.serialize(byteBuffer);
    IOUtils.writeList(schemaEvolutions, byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.EVOLVE_SCHEMA.serialize(stream);
    IOUtils.writeList(schemaEvolutions, stream);
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(PlanNodeType.EVOLVE_SCHEMA.getNodeType());
    buffer.putLong(searchIndex);
    try {
      IOUtils.writeList(schemaEvolutions, buffer);
    } catch (IOException e) {
      LOGGER.warn("Error writing schema evolutions to WAL", e);
    }
  }

  @Override
  public int serializedSize() {
    return 0;
  }
}
