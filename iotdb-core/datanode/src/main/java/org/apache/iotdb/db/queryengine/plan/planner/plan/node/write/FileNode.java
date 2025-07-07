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
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

// TODO:[OBJECT] WAL serde
public class FileNode extends SearchNode implements WALEntryValue {

  private final boolean isEOF;

  private final long offset;

  private byte[] content;

  private String filePath;

  public FileNode(boolean isEOF, long offset, byte[] content) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.content = content;
  }

  public FileNode(boolean isEOF, long offset, String filePath) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.filePath = filePath;
  }

  public boolean isEOF() {
    return isEOF;
  }

  public byte[] getContent() {
    return content;
  }

  public long getOffset() {
    return offset;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(searchIndex);
    buffer.put((byte) (isEOF ? 1 : 0));
    buffer.putLong(offset);
    WALWriteUtils.write(filePath, buffer);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES
        + Long.BYTES
        + Byte.BYTES
        + Long.BYTES
        + ReadWriteIOUtils.sizeToWrite(filePath);
  }

  public static FileNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    boolean isEOF = stream.readByte() == 1;
    long offset = stream.readLong();
    String filePath = ReadWriteIOUtils.readString(stream);

    FileNode fileNode = new FileNode(isEOF, offset, filePath);
    fileNode.setSearchIndex(searchIndex);
    return fileNode;
  }

  public static FileNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    boolean isEOF = buffer.get() == 1;
    long offset = buffer.getLong();
    String filePath = ReadWriteIOUtils.readString(buffer);

    FileNode fileNode = new FileNode(isEOF, offset, filePath);
    fileNode.setSearchIndex(searchIndex);
    return fileNode;
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    return null;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return null;
  }

  @Override
  public void setProgressIndex(ProgressIndex progressIndex) {}

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return null;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    return null;
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.OBJECT_FILE_NODE;
  }

  @Override
  public long getMemorySize() {
    return super.getMemorySize();
  }
}
