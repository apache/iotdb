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

package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

public class MetadataIndexConstructor {

  private static final int MAX_DEGREE_OF_INDEX_NODE = TSFileDescriptor.getInstance().getConfig()
      .getMaxDegreeOfIndexNode();

  public MetadataIndexConstructor() {
  }

  public static MetadataIndexNode constructMetadataIndex(Map<String, List<TimeseriesMetadata>>
      deviceTimeseriesMetadataMap, TsFileOutput out) throws IOException {
    Map<String, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();

    // for timeseriesMetadata of each device
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap
        .entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
      TimeseriesMetadata timeseriesMetadata;
      MetadataIndexNode currentIndexNode = new MetadataIndexNode();
      for (int i = 0; i < entry.getValue().size(); i++) {
        timeseriesMetadata = entry.getValue().get(i);
        if (i % MAX_DEGREE_OF_INDEX_NODE == 0) {
          if (currentIndexNode.isFull()) {
            currentIndexNode = addCurrentIndexNodeToQueueAndReset(currentIndexNode,
                measurementMetadataIndexQueue, out);
          }
          currentIndexNode.addEntry(new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(),
              out.getPosition(), MetadataIndexNodeType.LEAF_MEASUREMENT));
        }
        timeseriesMetadata.serializeTo(out.wrapAsStream());
      }
      addCurrentIndexNodeToQueueAndReset(currentIndexNode, measurementMetadataIndexQueue, out);
      deviceMetadataIndexMap.put(entry.getKey(),
          pollFinalMetadataIndexQueue(measurementMetadataIndexQueue,
              out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }

    MetadataIndexNode metadataIndexNode = new MetadataIndexNode();
    // if not exceed the max child nodes num, ignore the device index and directly point to the measurement
    if (deviceMetadataIndexMap.size() <= MAX_DEGREE_OF_INDEX_NODE) {
      for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition(),
            MetadataIndexNodeType.INTERNAL_MEASUREMENT));
        entry.getValue().serializeTo(out.wrapAsStream());
      }
      metadataIndexNode.setEndOffset(out.getPosition());
      return metadataIndexNode;
    }

    // else, build level index for devices
    Queue<MetadataIndexNode> deviceMetadaIndexQueue = new ArrayDeque<>();
    MetadataIndexNode currentIndexNode = new MetadataIndexNode();

    int deviceIndex = 0;
    for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
      if (deviceIndex % MAX_DEGREE_OF_INDEX_NODE == 0) {
        if (currentIndexNode.isFull()) {
          currentIndexNode = addCurrentIndexNodeToQueueAndReset(currentIndexNode,
              deviceMetadaIndexQueue, out);
        }
        currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(),
            out.getPosition(), MetadataIndexNodeType.LEAF_DEVICE));
      }
      entry.getValue().serializeTo(out.wrapAsStream());
      deviceIndex++;
    }
    addCurrentIndexNodeToQueueAndReset(currentIndexNode, deviceMetadaIndexQueue, out);
    MetadataIndexNode deviceMetadataIndexNode = pollFinalMetadataIndexQueue(deviceMetadaIndexQueue,
        out, MetadataIndexNodeType.INTERNAL_DEVICE);
    deviceMetadataIndexNode.setEndOffset(out.getPosition());
    return deviceMetadataIndexNode;
  }

  private static MetadataIndexNode pollFinalMetadataIndexQueue(
      Queue<MetadataIndexNode> metadataIndexNodeQueue, TsFileOutput out, MetadataIndexNodeType type)
      throws IOException {
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNode metadataIndexNode;
    MetadataIndexNode currentIndexNode = new MetadataIndexNode();
    while (queueSize >= MAX_DEGREE_OF_INDEX_NODE) {
      for (int i = 0; i < queueSize; i++) {
        metadataIndexNode = metadataIndexNodeQueue.poll();
        if (i % MAX_DEGREE_OF_INDEX_NODE == 0) {
          if (currentIndexNode.isFull()) {
            currentIndexNode.setEndOffset(out.getPosition());
            metadataIndexNodeQueue.add(currentIndexNode);
            currentIndexNode = new MetadataIndexNode();
          }
        }
        currentIndexNode.addEntry(new MetadataIndexEntry(metadataIndexNode.peek().getName(),
            out.getPosition(), type));
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      currentIndexNode.setEndOffset(out.getPosition());
      metadataIndexNodeQueue.add(currentIndexNode);
      currentIndexNode = new MetadataIndexNode();
      queueSize = metadataIndexNodeQueue.size();
    }
    return metadataIndexNodeQueue.poll();
  }

  private static MetadataIndexNode addCurrentIndexNodeToQueueAndReset(
      MetadataIndexNode currentIndexNode,
      Queue<MetadataIndexNode> metadataIndexNodeQueue, TsFileOutput out) throws IOException {
    currentIndexNode.setEndOffset(out.getPosition());
    metadataIndexNodeQueue.add(currentIndexNode);
    return new MetadataIndexNode();
  }
}
