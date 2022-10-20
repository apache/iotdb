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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

public class MetadataIndexConstructor {

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  private MetadataIndexConstructor() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Construct metadata index tree
   *
   * @param deviceTimeseriesMetadataMap device => TimeseriesMetadata list
   * @param out tsfile output
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static MetadataIndexNode constructMetadataIndex(
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap, TsFileOutput out)
      throws IOException {

    Map<String, MetadataIndexNode> deviceMetadataIndexMap = new TreeMap<>();

    // for timeseriesMetadata of each device
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
      TimeseriesMetadata timeseriesMetadata;
      MetadataIndexNode currentIndexNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
      for (int i = 0; i < entry.getValue().size(); i++) {
        timeseriesMetadata = entry.getValue().get(i);
        if (i % config.getMaxDegreeOfIndexNode() == 0) {
          if (currentIndexNode.isFull()) {
            addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
          }
          currentIndexNode.addEntry(
              new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(), out.getPosition()));
        }
        timeseriesMetadata.serializeTo(out.wrapAsStream());
      }
      addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
      deviceMetadataIndexMap.put(
          entry.getKey(),
          generateRootNode(
              measurementMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }

    return checkAndBuildLevelIndex(deviceMetadataIndexMap, out);
  }

  public static MetadataIndexNode checkAndBuildLevelIndex(
      Map<String, MetadataIndexNode> deviceMetadataIndexMap, TsFileOutput out) throws IOException {
    // if not exceed the max child nodes num, ignore the device index and directly point to the
    // measurement
    if (deviceMetadataIndexMap.size() <= config.getMaxDegreeOfIndexNode()) {
      MetadataIndexNode metadataIndexNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        entry.getValue().serializeTo(out.wrapAsStream());
      }
      metadataIndexNode.setEndOffset(out.getPosition());
      return metadataIndexNode;
    }

    // else, build level index for devices
    Queue<MetadataIndexNode> deviceMetadataIndexQueue = new ArrayDeque<>();
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);

    for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
      // when constructing from internal node, each node is related to an entry
      if (currentIndexNode.isFull()) {
        addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
        currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      }
      currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
      entry.getValue().serializeTo(out.wrapAsStream());
    }
    addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadataIndexQueue, out);
    MetadataIndexNode deviceMetadataIndexNode =
        generateRootNode(deviceMetadataIndexQueue, out, MetadataIndexNodeType.INTERNAL_DEVICE);
    deviceMetadataIndexNode.setEndOffset(out.getPosition());
    return deviceMetadataIndexNode;
  }

  /**
   * Generate root node, using the nodes in the queue as leaf nodes. The final metadata tree has two
   * levels: measurement leaf nodes will generate to measurement root node; device leaf nodes will
   * generate to device root node
   *
   * @param metadataIndexNodeQueue queue of metadataIndexNode
   * @param out tsfile output
   * @param type MetadataIndexNode type
   */
  public static MetadataIndexNode generateRootNode(
      Queue<MetadataIndexNode> metadataIndexNodeQueue, TsFileOutput out, MetadataIndexNodeType type)
      throws IOException {
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNode metadataIndexNode;
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(type);
    while (queueSize != 1) {
      for (int i = 0; i < queueSize; i++) {
        metadataIndexNode = metadataIndexNodeQueue.poll();
        // when constructing from internal node, each node is related to an entry
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
          currentIndexNode = new MetadataIndexNode(type);
        }
        currentIndexNode.addEntry(
            new MetadataIndexEntry(metadataIndexNode.peek().getName(), out.getPosition()));
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
      currentIndexNode = new MetadataIndexNode(type);
      queueSize = metadataIndexNodeQueue.size();
    }
    return metadataIndexNodeQueue.poll();
  }

  public static void addCurrentIndexNodeToQueue(
      MetadataIndexNode currentIndexNode,
      Queue<MetadataIndexNode> metadataIndexNodeQueue,
      TsFileOutput out)
      throws IOException {
    currentIndexNode.setEndOffset(out.getPosition());
    metadataIndexNodeQueue.add(currentIndexNode);
  }
}
