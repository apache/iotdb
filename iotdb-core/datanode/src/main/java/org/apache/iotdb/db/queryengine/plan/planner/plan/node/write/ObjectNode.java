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
import org.apache.iotdb.commons.exception.runtime.SerializationRunTimeException;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.storageengine.dataregion.IObjectPath;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class ObjectNode extends SearchNode implements WALEntryValue {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectNode.class);

  private final boolean isEOF;

  private final long offset;

  private byte[] content;

  private IObjectPath filePath;

  private final int contentLength;

  private TRegionReplicaSet dataRegionReplicaSet;

  private boolean isGeneratedByRemoteConsensusLeader;

  public ObjectNode(boolean isEOF, long offset, byte[] content, IObjectPath filePath) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.filePath = filePath;
    this.content = content;
    this.contentLength = content.length;
  }

  public ObjectNode(boolean isEOF, long offset, int contentLength, IObjectPath filePath) {
    super(new PlanNodeId(""));
    this.isEOF = isEOF;
    this.offset = offset;
    this.filePath = filePath;
    this.contentLength = contentLength;
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

  public void setFilePath(IObjectPath filePath) {
    this.filePath = filePath;
  }

  public String getFilePathString() {
    return filePath.toString();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(getType().getNodeType());
    buffer.putLong(searchIndex);
    buffer.put((byte) (isEOF ? 1 : 0));
    buffer.putLong(offset);
    try {
      filePath.serialize(buffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    buffer.putInt(content.length);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES
        + Long.BYTES
        + Byte.BYTES
        + Long.BYTES
        + Integer.BYTES
        + filePath.getSerializedSize();
  }

  public static ObjectNode deserializeFromWAL(DataInputStream stream) throws IOException {
    long searchIndex = stream.readLong();
    boolean isEOF = stream.readByte() == 1;
    long offset = stream.readLong();
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(stream);
    int contentLength = stream.readInt();
    ObjectNode objectNode = new ObjectNode(isEOF, offset, contentLength, filePath);
    objectNode.setSearchIndex(searchIndex);
    return objectNode;
  }

  public static ObjectNode deserializeFromWAL(ByteBuffer buffer) {
    long searchIndex = buffer.getLong();
    boolean isEOF = buffer.get() == 1;
    long offset = buffer.getLong();
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(buffer);
    Optional<File> objectFile =
        TierManager.getInstance().getAbsoluteObjectFilePath(filePath.toString());
    int contentLength = buffer.getInt();
    byte[] contents = new byte[contentLength];
    if (objectFile.isPresent()) {
      try (RandomAccessFile raf = new RandomAccessFile(objectFile.get(), "r")) {
        raf.seek(offset);
        raf.read(contents);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new ObjectFileNotExist(filePath.toString());
    }

    ObjectNode objectNode = new ObjectNode(isEOF, offset, contents, filePath);
    objectNode.setSearchIndex(searchIndex);
    return objectNode;
  }

  public static ObjectNode deserialize(ByteBuffer byteBuffer) {
    boolean isEoF = ReadWriteIOUtils.readBool(byteBuffer);
    long offset = ReadWriteIOUtils.readLong(byteBuffer);
    IObjectPath filePath = IObjectPath.getDeserializer().deserializeFrom(byteBuffer);
    int contentLength = ReadWriteIOUtils.readInt(byteBuffer);
    byte[] content = ReadWriteIOUtils.readBytes(byteBuffer, contentLength);
    return new ObjectNode(isEoF, offset, content, filePath);
  }

  @Override
  public SearchNode merge(List<SearchNode> searchNodes) {
    if (searchNodes.size() == 1) {
      return searchNodes.get(0);
    }
    throw new UnsupportedOperationException("Merge is not supported");
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
    return dataRegionReplicaSet;
  }

  public void setDataRegionReplicaSet(TRegionReplicaSet dataRegionReplicaSet) {
    this.dataRegionReplicaSet = dataRegionReplicaSet;
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(isEOF, byteBuffer);
    ReadWriteIOUtils.write(offset, byteBuffer);
    filePath.serialize(byteBuffer);
    ReadWriteIOUtils.write(contentLength, byteBuffer);
    byteBuffer.put(content);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(isEOF, stream);
    ReadWriteIOUtils.write(offset, stream);
    filePath.serialize(stream);
    ReadWriteIOUtils.write(contentLength, stream);
    stream.write(content);
  }

  public ByteBuffer serialize() {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(WALEntryType.OBJECT_FILE_NODE.getCode(), stream);
      ReadWriteIOUtils.write((long) TsFileProcessor.MEMTABLE_NOT_EXIST, stream);
      ReadWriteIOUtils.write(getType().getNodeType(), stream);
      byte[] contents = new byte[contentLength];
      boolean readSuccess = false;
      IOException ioException = null;
      for (int i = 0; i < 2; i++) {
        Optional<File> objectFile =
            TierManager.getInstance().getAbsoluteObjectFilePath(filePath.toString());
        if (objectFile.isPresent()) {
          try {
            readContentFromFile(objectFile.get(), contents);
            readSuccess = true;
          } catch (IOException e) {
            ioException = e;
          }
          if (readSuccess) {
            break;
          }
        }
        Optional<File> objectTmpFile =
            TierManager.getInstance().getAbsoluteObjectFilePath(filePath + ".tmp");
        if (objectTmpFile.isPresent()) {
          try {
            readContentFromFile(objectTmpFile.get(), contents);
            readSuccess = true;
          } catch (IOException e) {
            ioException = e;
          }
          if (readSuccess) {
            break;
          }
        }
      }
      if (!readSuccess && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Error when read object file {}.", filePath.toString(), ioException);
      }
      ReadWriteIOUtils.write(readSuccess && isEOF, stream);
      ReadWriteIOUtils.write(offset, stream);
      filePath.serialize(stream);
      ReadWriteIOUtils.write(contentLength, stream);
      stream.write(contents);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (IOException e) {
      throw new SerializationRunTimeException(e);
    }
  }

  private void readContentFromFile(File file, byte[] contents) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(offset);
      raf.read(contents);
    }
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.OBJECT_FILE_NODE;
  }

  @Override
  public long getMemorySize() {
    return contentLength;
  }

  @Override
  public void markAsGeneratedByRemoteConsensusLeader() {
    isGeneratedByRemoteConsensusLeader = true;
  }

  public boolean isGeneratedByRemoteConsensusLeader() {
    return isGeneratedByRemoteConsensusLeader;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitWriteObjectFile(this, context);
  }
}
