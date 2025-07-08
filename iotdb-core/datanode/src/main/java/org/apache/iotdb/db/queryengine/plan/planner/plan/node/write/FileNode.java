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
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

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

  public FileNode(boolean isEOF, long offset, byte[] content, String filePath) {
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
    // TODO haonan only need relativePath, offset, length, eof
    buffer.putShort(getType().getNodeType());
    buffer.putLong(searchIndex);
    buffer.put((byte) (isEOF ? 1 : 0));
    buffer.putLong(offset);
    buffer.putInt(content.length);
    WALWriteUtils.write(filePath, buffer);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES
        + Long.BYTES
        + Byte.BYTES
        + Long.BYTES
        + Integer.BYTES
        + ReadWriteIOUtils.sizeToWrite(filePath);
  }

  public static FileNode deserializeFromWAL(DataInputStream stream) throws IOException {
    // TODO haonan only be called in recovery, should only deserialize relativePath, offset, eof,
    // length
    long searchIndex = stream.readLong();
    boolean isEOF = stream.readByte() == 1;
    long offset = stream.readLong();
    int contentLength = stream.readInt();
    String filePath = ReadWriteIOUtils.readString(stream);
    Optional<File> objectFile = TierManager.getInstance().getAbsoluteObjectFilePath(filePath);
    byte[] contents = new byte[contentLength];
    if (objectFile.isPresent()) {
      try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
        raf.seek(offset);
        raf.read(contents);
      }
    } else {
      throw new ObjectFileNotExist(filePath);
    }

    FileNode fileNode = new FileNode(isEOF, offset, contents, filePath);
    fileNode.setSearchIndex(searchIndex);

    return fileNode;
  }

  public static FileNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    boolean isEOF = buffer.get() == 1;
    long offset = buffer.getLong();
    int contentLength = buffer.getInt();
    String filePath = ReadWriteIOUtils.readString(buffer);
    Optional<File> objectFile = TierManager.getInstance().getAbsoluteObjectFilePath(filePath);
    byte[] contents = new byte[contentLength];
    if (objectFile.isPresent()) {
      try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
        raf.seek(offset);
        raf.read(contents);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new ObjectFileNotExist(filePath);
    }

    FileNode fileNode = new FileNode(isEOF, offset, contents, filePath);
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
    return content.length;
  }
}
