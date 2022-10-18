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
  private final Queue<Pair<String, Pair<Long, Long>>> queue;
  private Pair<String, Boolean> currentDevice = null;
  private MetadataIndexNode measurementNode;

  // <startOffset, endOffset>, device leaf node offset in this file
  private final List<Pair<Long, Long>> leafDeviceNodeOffsetList;

  public TsFileDeviceIterator(
      TsFileSequenceReader reader,
      List<Pair<Long, Long>> leafDeviceNodeOffsetList,
      Queue<Pair<String, Pair<Long, Long>>> queue) {
    this.reader = reader;
    this.queue = queue;
    this.leafDeviceNodeOffsetList = leafDeviceNodeOffsetList;
  }

  public Pair<String, Boolean> current() {
    return currentDevice;
  }

  @Override
  public boolean hasNext() {
    // return !queue.isEmpty();
    if (!queue.isEmpty()) {
      return true;
    } else if (leafDeviceNodeOffsetList.size() == 0) {
      return false;
    } else {
      Pair<Long, Long> nextDeviceLeafNodeOffset = leafDeviceNodeOffsetList.remove(0);
      try {
        reader.getDevicesOfOneNodeWithIsAligned(
            nextDeviceLeafNodeOffset.left, nextDeviceLeafNodeOffset.right, queue);
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
    Pair<String, Pair<Long, Long>> startEndPair = queue.remove();
    try {
      // get the first measurment node of this device, to know if the device is alignd
      this.measurementNode =
          MetadataIndexNode.deserializeFrom(
              reader.readData(startEndPair.right.left, startEndPair.right.right));
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
