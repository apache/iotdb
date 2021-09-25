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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.*;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MTreeFile {

  private static final int HEADER_LENGTH = 64;
  private static final int NODE_LENGTH =
      IoTDBDescriptor.getInstance().getConfig().getMetaFileNodeLength();
  private static final int NODE_HEADER_LENGTH = 17; // bitmap+pre_position+extend_position
  private static final int NODE_DATA_LENGTH = NODE_LENGTH - NODE_HEADER_LENGTH;

  private static final byte IS_USED_MASK = (byte) 0x80;

  private static final byte NODE_TYPE_MASK = (byte) 0xF0;
  private static final byte INTERNAL_NODE = (byte) 0x80;
  private static final byte ROOT_NODE = (byte) 0x90;
  private static final byte STORAGE_GROUP_NODE = (byte) 0xA0;
  private static final byte MEASUREMENT_NODE = (byte) 0xB0;
  private static final byte EXTENSION_NODE = (byte) 0xC0;

  private final IMNodeSerializer mNodeSerializer = new MNodePersistenceSerializer();
  private final ISlottedFileAccess fileAccess;

  private int headerLength;
  private short nodeLength;
  private long rootPosition;
  private long firstStorageGroupPosition;
  private long firstTimeseriesPosition;
  private volatile long firstFreePosition;
  private int mNodeCount;
  private int storageGroupCount;
  private int timeseriesCount;

  private final List<Long> freePosition = new LinkedList<>();

  public MTreeFile(String filepath) throws IOException {
    File metaFile = new File(filepath);
    boolean isNew = !metaFile.exists();
    fileAccess = new SlottedFile(filepath, HEADER_LENGTH, NODE_LENGTH);

    if (isNew) {
      initMetaFileHeader();
    } else {
      readMetaFileHeader();
      fileCheck();
    }
  }

  private void initMetaFileHeader() throws IOException {
    headerLength = HEADER_LENGTH;
    nodeLength = (short) NODE_LENGTH;
    rootPosition = HEADER_LENGTH;
    firstStorageGroupPosition = 0;
    firstTimeseriesPosition = 0;
    firstFreePosition = rootPosition + nodeLength;
    mNodeCount = 0;
    storageGroupCount = 0;
    timeseriesCount = 0;
    writeMetaFileHeader();
  }

  private void readMetaFileHeader() throws IOException {
    ByteBuffer buffer = fileAccess.readHeader();
    headerLength = buffer.get();
    nodeLength = buffer.getShort();
    rootPosition = buffer.getLong();
    firstStorageGroupPosition = buffer.getLong();
    firstTimeseriesPosition = buffer.getLong();
    firstFreePosition = buffer.getLong();
    mNodeCount = buffer.getInt();
    storageGroupCount = buffer.getInt();
    timeseriesCount = buffer.getInt();
  }

  private void writeMetaFileHeader() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(headerLength);
    buffer.put((byte) headerLength);
    buffer.putShort(nodeLength);
    buffer.putLong(rootPosition);
    buffer.putLong(firstStorageGroupPosition);
    buffer.putLong(firstTimeseriesPosition);
    buffer.putLong(firstFreePosition);
    buffer.putInt(mNodeCount);
    buffer.putInt(storageGroupCount);
    buffer.putInt(timeseriesCount);
    buffer.position(0);
    fileAccess.writeHeader(buffer);
  }

  private void fileCheck() throws IOException {
    IPersistenceInfo persistenceInfo = IPersistenceInfo.createPersistenceInfo(rootPosition);
    IMNode mNode = read(persistenceInfo);
  }

  public IMNode read(String path) throws IOException {
    String[] nodes = path.split("\\.");
    if (!nodes[0].equals("root")) {
      return null;
    }
    IMNode mNode = read(IPersistenceInfo.createPersistenceInfo(rootPosition));
    for (int i = 1; i < nodes.length; i++) {
      if (!mNode.hasChild(nodes[i])) {
        return null;
      }
      mNode = read(mNode.getChild(nodes[i]).getPersistenceInfo());
    }
    return mNode;
  }

  public IMNode readRecursively(IPersistenceInfo persistenceInfo) throws IOException {
    IMNode mNode = read(persistenceInfo);
    Map<String, IMNode> children = mNode.getChildren();
    IMNode child;
    for (Map.Entry<String, IMNode> entry : children.entrySet()) {
      child = entry.getValue();
      child = readRecursively(child.getPersistenceInfo());
      entry.setValue(child);
      child.setParent(mNode);
    }
    return mNode;
  }

  public IMNode read(IPersistenceInfo persistenceInfo) throws IOException {
    long position = persistenceInfo.getPosition();
    Pair<Byte, ByteBuffer> mNodeData = readBytesFromFile(position);
    byte nodeType = mNodeData.left;
    ByteBuffer dataBuffer = mNodeData.right;
    IMNode mNode;
    switch (nodeType) {
      case INTERNAL_NODE: // normal InternalMNode
        mNode = mNodeSerializer.deserializeInternalMNode(dataBuffer);
        break;
      case ROOT_NODE: // root
        mNode = mNodeSerializer.deserializeInternalMNode(dataBuffer);
        if (!mNode.getName().equals(IoTDBConstant.PATH_ROOT)) {
          throw new IOException("file corrupted");
        }
        break;
      case STORAGE_GROUP_NODE: // StorageGroupMNode
        mNode = mNodeSerializer.deserializeStorageGroupMNode(dataBuffer);
        break;
      case MEASUREMENT_NODE: // MeasurementMNode
        mNode = mNodeSerializer.deserializeMeasurementMNode(dataBuffer);
        break;
      default:
        throw new IOException("file corrupted");
    }
    mNode.setPersistenceInfo(persistenceInfo);

    return mNode;
  }

  private Pair<Byte, ByteBuffer> readBytesFromFile(long position) throws IOException {
    if (position < headerLength) {
      throw new IOException("wrong node position");
    }

    ByteBuffer buffer = ByteBuffer.allocate(nodeLength);
    fileAccess.readBytes(position, buffer);

    byte bitmap = buffer.get();
    if ((bitmap & IS_USED_MASK) == 0) {
      throw new IOException("file corrupted");
    }
    byte nodeType = (byte) (bitmap & NODE_TYPE_MASK);
    if (nodeType > MEASUREMENT_NODE) {
      throw new IOException("file corrupted");
    }

    long parentPosition = buffer.getLong();
    long prePosition;
    long extendPosition = buffer.getLong();

    int mNodeLength = buffer.getInt() + 4; // length + data
    int num = (mNodeLength / NODE_DATA_LENGTH) + ((mNodeLength % NODE_DATA_LENGTH) == 0 ? 0 : 1);
    ByteBuffer dataBuffer = ByteBuffer.allocate(mNodeLength - 4);
    buffer.limit(Math.min(buffer.position() + dataBuffer.remaining(), buffer.limit()));
    dataBuffer.put(buffer);
    for (int i = 1; i < num; i++) {
      buffer.clear();
      fileAccess.readBytes(extendPosition, buffer);
      bitmap = buffer.get();
      if ((bitmap & IS_USED_MASK) == 0) {
        throw new IOException("file corrupted");
      }
      if (((byte) (bitmap & NODE_TYPE_MASK)) != EXTENSION_NODE) {
        // 地址空间对应非扩展节点
        throw new IOException("File corrupted");
      }
      prePosition = buffer.getLong();
      extendPosition = buffer.getLong();
      buffer.limit(Math.min(buffer.position() + dataBuffer.remaining(), buffer.limit()));
      dataBuffer.put(buffer);
    }
    dataBuffer.flip();

    return new Pair<>(nodeType, dataBuffer);
  }

  public void write(IMNode mNode) throws IOException {
    if (mNode == null) {
      throw new IOException("MNode is null and cannot be persist.");
    }
    if (!mNode.isLoaded()) {
      return;
    }

    if (mNode.getPersistenceInfo() == null) {
      if (mNode.getName().equals("root")) {
        mNode.setPersistenceInfo(IPersistenceInfo.createPersistenceInfo(rootPosition));
      } else {
        mNode.setPersistenceInfo(IPersistenceInfo.createPersistenceInfo(getFreePos()));
      }
    }

    byte bitmap;
    ByteBuffer dataBuffer;
    if (mNode.isStorageGroup()) {
      dataBuffer = mNodeSerializer.serializeStorageGroupMNode((StorageGroupMNode) mNode);
      bitmap = STORAGE_GROUP_NODE;
    } else if (mNode.isMeasurement()) {
      dataBuffer = mNodeSerializer.serializeMeasurementMNode((MeasurementMNode) mNode);
      bitmap = MEASUREMENT_NODE;
    } else {
      dataBuffer = mNodeSerializer.serializeInternalMNode((InternalMNode) mNode);
      if (mNode.getName().equals("root")) {
        bitmap = ROOT_NODE;
      } else {
        bitmap = INTERNAL_NODE;
      }
    }

    long position = mNode.getPersistenceInfo().getPosition();
    IMNode parent = mNode.getParent();
    long parentPosition =
        (parent == null || !parent.isPersisted()) ? 0 : parent.getPersistenceInfo().getPosition();
    writeBytesToFile(dataBuffer, bitmap, position, parentPosition);
  }

  public void writeRecursively(IMNode mNode) throws IOException {
    for (IMNode child : mNode.getChildren().values()) {
      writeRecursively(child);
    }
    write(mNode);
  }

  private void writeBytesToFile(
      ByteBuffer dataBuffer, byte bitmap, long position, long parentPosition) throws IOException {
    int mNodeDataLength = dataBuffer.remaining();
    int mNodeLength = mNodeDataLength + 4; // length + data
    int bufferNum =
        (mNodeLength / NODE_DATA_LENGTH) + ((mNodeLength % NODE_DATA_LENGTH) == 0 ? 0 : 1);
    int dataBufferEnd = dataBuffer.limit();

    ByteBuffer buffer = ByteBuffer.allocate(nodeLength);
    long currentPos = position;
    long prePos = parentPosition;
    long extensionPos = (bufferNum == 1) ? 0 : getFreePos();

    buffer.put(bitmap);
    buffer.putLong(prePos);
    buffer.putLong(extensionPos);
    buffer.putInt(mNodeDataLength); // put mNode data length
    dataBuffer.limit(
        Math.min(
            dataBuffer.position() + NODE_DATA_LENGTH - 4,
            dataBufferEnd)); // current buffer already put an int, 4B
    buffer.put(dataBuffer);
    buffer.flip();
    if (currentPos < headerLength) {
      throw new IOException("illegal position for node");
    }
    fileAccess.writeBytes(currentPos, buffer);

    bitmap = EXTENSION_NODE;
    for (int i = 1; i < bufferNum; i++) {
      buffer.clear();
      prePos = currentPos;
      currentPos = extensionPos;
      buffer.put(bitmap);
      buffer.putLong(prePos);
      extensionPos = (i == bufferNum - 1) ? 0 : getFreePos();
      buffer.putLong(extensionPos);
      dataBuffer.limit(Math.min(dataBuffer.position() + NODE_DATA_LENGTH, dataBufferEnd));
      buffer.put(dataBuffer);
      buffer.flip();

      if (currentPos < headerLength) {
        throw new IOException("illegal position for node");
      }
      fileAccess.writeBytes(currentPos, buffer);
    }
  }

  public synchronized long getFreePos() throws IOException {
    if (freePosition.size() != 0) {
      return freePosition.remove(0);
    }
    firstFreePosition += nodeLength;
    return firstFreePosition - nodeLength;
  }

  public long getRootPosition() {
    return rootPosition;
  }

  public void remove(IPersistenceInfo persistenceInfo) throws IOException {
    long position = persistenceInfo.getPosition();
    ByteBuffer buffer = ByteBuffer.allocate(17);
    while (position != 0) {
      fileAccess.readBytes(position, buffer);
      byte bitmap = buffer.get();
      long pre = buffer.getLong();
      long extension = buffer.getLong();

      buffer.clear();
      buffer.put((byte) (0));
      if (freePosition.size() == 0) {
        buffer.putLong(fileAccess.getFileLength());
      } else {
        buffer.putLong(freePosition.get(0));
      }
      buffer.flip();
      fileAccess.writeBytes(position, buffer);
      freePosition.add(0, position);

      position = extension;
    }
  }

  public void clear() throws IOException {}

  public void sync() throws IOException {
    writeMetaFileHeader();
    fileAccess.sync();
  }

  public void close() throws IOException {
    fileAccess.close();
  }
}
