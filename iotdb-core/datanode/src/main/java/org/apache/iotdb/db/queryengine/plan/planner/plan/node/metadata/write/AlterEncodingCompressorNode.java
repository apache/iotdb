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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write;

import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.SerializeUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AlterEncodingCompressorNode extends PlanNode implements ISchemaRegionPlan {

  public static final AlterEncodingCompressorNode MOCK_INSTANCE =
      new AlterEncodingCompressorNode(new PlanNodeId(""), null, false, null, null);

  private final PathPatternTree patternTree;
  private final boolean ifExists;
  private final TSEncoding encoding;
  private final CompressionType compressionType;

  public AlterEncodingCompressorNode(
      final PlanNodeId id,
      final PathPatternTree patternTree,
      final boolean ifExists,
      final TSEncoding encoding,
      final CompressionType compressionType) {
    super(id);
    this.patternTree = patternTree;
    this.ifExists = ifExists;
    this.encoding = encoding;
    this.compressionType = compressionType;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(final PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterEncodingCompressor(this, context);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.ALTER_ENCODING_COMPRESSOR.serialize(byteBuffer);
    patternTree.serialize(byteBuffer);
    ReadWriteIOUtils.write(ifExists, byteBuffer);
    ReadWriteIOUtils.write(SerializeUtils.serializeNullable(encoding), byteBuffer);
    ReadWriteIOUtils.write(SerializeUtils.serializeNullable(compressionType), byteBuffer);
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.ALTER_ENCODING_COMPRESSOR.serialize(stream);
    patternTree.serialize(stream);
    ReadWriteIOUtils.write(ifExists, stream);
    ReadWriteIOUtils.write(SerializeUtils.serializeNullable(encoding), stream);
    ReadWriteIOUtils.write(SerializeUtils.serializeNullable(compressionType), stream);
  }

  public static AlterEncodingCompressorNode deserialize(final ByteBuffer byteBuffer) {
    final PathPatternTree patternTree = PathPatternTree.deserialize(byteBuffer);
    final boolean ifExists = ReadWriteIOUtils.readBoolean(byteBuffer);
    final TSEncoding encoding =
        SerializeUtils.deserializeEncodingNullable(ReadWriteIOUtils.readByte(byteBuffer));
    final CompressionType compressor =
        SerializeUtils.deserializeCompressorNullable(ReadWriteIOUtils.readByte(byteBuffer));
    final PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new AlterEncodingCompressorNode(planNodeId, patternTree, ifExists, encoding, compressor);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.ALTER_ENCODING_COMPRESSOR;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterEncodingCompressor(this, context);
  }
}
