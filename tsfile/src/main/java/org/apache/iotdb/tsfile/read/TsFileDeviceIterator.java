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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TsFileDeviceIterator implements Iterator<Pair<String, Boolean>> {
  private final TsFileSequenceReader reader;

  // device -> firstMeasurmentNode offset
  private final Queue<Pair<String, long[]>> queue;
  private Pair<String, Boolean> currentDevice = null;
  private MetadataIndexNode measurementNode;

  // <startOffset, endOffset>, device leaf node offset in this file
  private final List<long[]> leafDeviceNodeOffsetList;

  public TsFileDeviceIterator(
      TsFileSequenceReader reader,
      List<long[]> leafDeviceNodeOffsetList,
      Queue<Pair<String, long[]>> queue) {
    this.reader = reader;
    this.queue = queue;
    this.leafDeviceNodeOffsetList = leafDeviceNodeOffsetList;
  }

  public Pair<String, Boolean> current() {
    return currentDevice;
  }

  @Override
  public boolean hasNext() {
    if (!queue.isEmpty()) {
      return true;
    } else if (leafDeviceNodeOffsetList.size() == 0) {
      // device queue is empty and all device leaf node has been read
      return false;
    } else {
      // queue is empty but there are still some devices on leaf node not being read yet
      long[] nextDeviceLeafNodeOffset = leafDeviceNodeOffsetList.remove(0);
      try {
        reader.getDevicesAndEntriesOfOneLeafNode(
            nextDeviceLeafNodeOffset[0], nextDeviceLeafNodeOffset[1], queue);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
  }

  @Override
  public Pair<String, Boolean> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Pair<String, long[]> startEndPair = queue.remove();
    try {
      // get the first measurement node of this device, to know if the device is aligned
      this.measurementNode =
          MetadataIndexNode.deserializeFrom(
              reader.readData(startEndPair.right[0], startEndPair.right[1]));
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
}
