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

package org.apache.tsfile.read;

import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TsFileDeviceIterator implements Iterator<Pair<IDeviceID, Boolean>> {

  private final TsFileSequenceReader reader;
  private final DeserializeConfig deserializeConfig;
  private final Iterator<MetadataIndexNode> tableMetadataIndexNodeIterator;
  private final Queue<Pair<IDeviceID, long[]>> queue = new LinkedList<>();
  private final List<long[]> leafDeviceNodeOffsetList = new LinkedList<>();
  private Pair<IDeviceID, Boolean> currentDevice = null;
  private MetadataIndexNode measurementNode;

  private static final Logger logger = LoggerFactory.getLogger(TsFileDeviceIterator.class);

  public TsFileDeviceIterator(TsFileSequenceReader reader) throws IOException {
    this.reader = reader;
    this.deserializeConfig = reader.getDeserializeContext();
    this.tableMetadataIndexNodeIterator =
        reader.readFileMetadata().getTableMetadataIndexNodeMap().values().iterator();
  }

  public Pair<IDeviceID, Boolean> current() {
    return currentDevice;
  }

  @Override
  public boolean hasNext() {
    try {
      prepareNextTable();
      if (!queue.isEmpty()) {
        return true;
      } else if (leafDeviceNodeOffsetList.isEmpty()) {
        // device queue is empty and all device leaf node has been read
        return false;
      } else {
        // queue is empty but there are still some devices on leaf node not being read yet
        long[] nextDeviceLeafNodeOffset = leafDeviceNodeOffsetList.remove(0);
        getDevicesAndEntriesOfOneLeafNode(
            nextDeviceLeafNodeOffset[0], nextDeviceLeafNodeOffset[1], queue);
        return true;
      }
    } catch (IOException e) {
      throw new TsFileRuntimeException(e);
    }
  }

  private void prepareNextTable() throws IOException {
    if (!queue.isEmpty() || !leafDeviceNodeOffsetList.isEmpty()) {
      return;
    }
    if (!tableMetadataIndexNodeIterator.hasNext()) {
      return;
    }
    MetadataIndexNode nextTableMetadataIndexNode = tableMetadataIndexNodeIterator.next();

    if (nextTableMetadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      getDevicesOfLeafNode(nextTableMetadataIndexNode, queue);
    } else {
      getAllDeviceLeafNodeOffset(nextTableMetadataIndexNode, leafDeviceNodeOffsetList);
    }
  }

  @Override
  public Pair<IDeviceID, Boolean> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Pair<IDeviceID, long[]> startEndPair = queue.remove();
    try {
      // get the first measurement node of this device, to know if the device is aligned
      this.measurementNode =
          reader.readMetadataIndexNode(startEndPair.right[0], startEndPair.right[1], false);
      boolean isAligned = reader.isAlignedDevice(measurementNode);
      currentDevice = new Pair<>(startEndPair.left, isAligned);
      return currentDevice;
    } catch (IOException e) {
      throw new TsFileRuntimeException(
          "Error occurred while reading a time series metadata block.");
    }
  }

  public MetadataIndexNode getFirstMeasurementNodeOfCurrentDevice() {
    return measurementNode;
  }

  /**
   * Get devices and first measurement node offset.
   *
   * @param startOffset start offset of device leaf node
   * @param endOffset end offset of device leaf node
   * @param measurementNodeOffsetQueue device -> first measurement node offset
   */
  public void getDevicesAndEntriesOfOneLeafNode(
      Long startOffset, Long endOffset, Queue<Pair<IDeviceID, long[]>> measurementNodeOffsetQueue)
      throws IOException {
    try {
      ByteBuffer nextBuffer = reader.readData(startOffset, endOffset);
      MetadataIndexNode deviceLeafNode =
          deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
              nextBuffer, deserializeConfig);
      getDevicesOfLeafNode(deviceLeafNode, measurementNodeOffsetQueue);
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error(
          "Something error happened while getting all devices of file {}", reader.getFileName());
      throw e;
    }
  }

  /**
   * Get all devices and its corresponding entries on the specific device leaf node.
   *
   * @param deviceLeafNode this node must be device leaf node
   */
  private void getDevicesOfLeafNode(
      MetadataIndexNode deviceLeafNode, Queue<Pair<IDeviceID, long[]>> measurementNodeOffsetQueue) {
    if (!deviceLeafNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      throw new IllegalStateException("the first param should be device leaf node.");
    }
    List<IMetadataIndexEntry> childrenEntries = deviceLeafNode.getChildren();
    for (int i = 0; i < childrenEntries.size(); i++) {
      IMetadataIndexEntry deviceEntry = childrenEntries.get(i);
      long childStartOffset = deviceEntry.getOffset();
      long childEndOffset =
          i == childrenEntries.size() - 1
              ? deviceLeafNode.getEndOffset()
              : childrenEntries.get(i + 1).getOffset();
      long[] offset = {childStartOffset, childEndOffset};
      measurementNodeOffsetQueue.add(
          new Pair<>(((DeviceMetadataIndexEntry) deviceEntry).getDeviceID(), offset));
    }
  }

  /**
   * Get the device leaf node offset under the specific device internal node.
   *
   * @param deviceInternalNode this node must be device internal node
   */
  private void getAllDeviceLeafNodeOffset(
      MetadataIndexNode deviceInternalNode, List<long[]> leafDeviceNodeOffsets) throws IOException {
    if (!deviceInternalNode.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE)) {
      throw new IllegalStateException("the first param should be device internal node.");
    }
    try {
      int metadataIndexListSize = deviceInternalNode.getChildren().size();
      boolean isCurrentLayerLeafNode = false;
      for (int i = 0; i < metadataIndexListSize; i++) {
        IMetadataIndexEntry entry = deviceInternalNode.getChildren().get(i);
        long startOffset = entry.getOffset();
        long endOffset = deviceInternalNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = deviceInternalNode.getChildren().get(i + 1).getOffset();
        }
        if (i == 0) {
          // check is current layer device leaf node or device internal node. Just need to check the
          // first entry, because the rest are the same
          MetadataIndexNodeType nodeType =
              MetadataIndexNodeType.deserialize(
                  ReadWriteIOUtils.readByte(reader.readData(endOffset - 1, endOffset)));
          isCurrentLayerLeafNode = nodeType.equals(MetadataIndexNodeType.LEAF_DEVICE);
        }
        if (isCurrentLayerLeafNode) {
          // is device leaf node
          long[] offset = {startOffset, endOffset};
          leafDeviceNodeOffsets.add(offset);
          continue;
        }
        ByteBuffer nextBuffer = reader.readData(startOffset, endOffset);
        getAllDeviceLeafNodeOffset(
            deserializeConfig.deviceMetadataIndexNodeBufferDeserializer.deserialize(
                nextBuffer, deserializeConfig),
            leafDeviceNodeOffsets);
      }
    } catch (StopReadTsFileByInterruptException e) {
      throw e;
    } catch (Exception e) {
      logger.error(
          "Something error happened while getting all devices of file {}", reader.getFileName());
      throw e;
    }
  }
}
