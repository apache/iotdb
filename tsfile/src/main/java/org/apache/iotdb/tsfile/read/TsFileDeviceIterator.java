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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class TsFileDeviceIterator implements Iterator<Pair<String, Boolean>> {
  private final TsFileSequenceReader reader;
  private final Queue<Pair<String, Pair<Long, Long>>> queue;
  private Pair<String, Boolean> currentDevice = null;

  public TsFileDeviceIterator(
      TsFileSequenceReader reader, Queue<Pair<String, Pair<Long, Long>>> queue) {
    this.reader = reader;
    this.queue = queue;
  }

  public Pair<String, Boolean> current() {
    return currentDevice;
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty();
  }

  @Override
  public Pair<String, Boolean> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Pair<String, Pair<Long, Long>> startEndPair = queue.remove();
    List<Pair<String, Boolean>> devices = new ArrayList<>();
    try {
      MetadataIndexNode measurementNode =
          MetadataIndexNode.deserializeFrom(
              reader.readData(startEndPair.right.left, startEndPair.right.right));
      // if tryToGetFirstTimeseriesMetadata(node) returns null, the device is not aligned
      boolean isAligned = reader.tryToGetFirstTimeseriesMetadata(measurementNode) != null;
      currentDevice = new Pair<>(startEndPair.left, isAligned);
      return currentDevice;
    } catch (IOException e) {
      throw new TsFileRuntimeException(
          "Error occurred while reading a time series metadata block.");
    }
  }
}
