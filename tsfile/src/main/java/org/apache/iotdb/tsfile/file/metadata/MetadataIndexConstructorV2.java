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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

public class MetadataIndexConstructorV2 {

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  private MetadataIndexConstructorV2() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Construct metadata index tree
   *
   * @param deviceTimeseriesMetadataMap device => TimeseriesMetadata list
   * @param tsFileOutput tsfile output
   * @param metadataIndexOutput metadataIndex output
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static MetadataIndexNodeV2 constructMetadataIndex(
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap,
      TsFileOutput tsFileOutput,
      TsFileOutput metadataIndexOutput)
      throws IOException {

    Queue<MetadataIndexNodeV2> metadataIndexQueue = new ArrayDeque<>();
    MetadataIndexNodeV2 currentIndexNode = new MetadataIndexNodeV2();
    currentIndexNode.setLeaf(true);
    int serializedTimeseriesMetadataNum = 0;
    boolean isNewDevice;
    // for timeseriesMetadata of each device
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      isNewDevice = true;
      TimeseriesMetadata timeseriesMetadata;
      for (int i = 0; i < entry.getValue().size(); i++) {
        timeseriesMetadata = entry.getValue().get(i);
        if (serializedTimeseriesMetadataNum == 0
            || serializedTimeseriesMetadataNum >= config.getMaxDegreeOfIndexNode()
            || isNewDevice) {
          if (currentIndexNode.isFull()) {
            addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexQueue, tsFileOutput);
            currentIndexNode = new MetadataIndexNodeV2();
            currentIndexNode.setLeaf(true);
          }
          currentIndexNode.addEntry(
              new MetadataIndexEntry(
                  entry.getKey()
                      + TsFileConstant.PATH_SEPARATOR
                      + timeseriesMetadata.getMeasurementId(),
                  tsFileOutput.getPosition()));
          serializedTimeseriesMetadataNum = 0;
          isNewDevice = false;
        }
        timeseriesMetadata.serializeTo(tsFileOutput.wrapAsStream());
        serializedTimeseriesMetadataNum++;
      }
    }
    addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexQueue, tsFileOutput); // ?
    return generateRootNode(metadataIndexQueue, metadataIndexOutput);
  }

  /**
   * Generate root node, using the nodes in the queue as leaf nodes. The final metadata tree has two
   * levels: measurement leaf nodes will generate to measurement root node; device leaf nodes will
   * generate to device root node
   *
   * @param metadataIndexNodeQueue queue of metadataIndexNode
   * @param out tsfile output
   */
  private static MetadataIndexNodeV2 generateRootNode(
      Queue<MetadataIndexNodeV2> metadataIndexNodeQueue, TsFileOutput out) throws IOException {
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNodeV2 metadataIndexNode;
    MetadataIndexNodeV2 currentIndexNode = new MetadataIndexNodeV2();
    while (queueSize != 1) {
      for (int i = 0; i < queueSize; i++) {
        metadataIndexNode = metadataIndexNodeQueue.poll();
        // when constructing from internal node, each node is related to an entry
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
          currentIndexNode = new MetadataIndexNodeV2();
        }
        currentIndexNode.addEntry(
            new MetadataIndexEntry(metadataIndexNode.peek().getName(), out.getPosition()));
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
      currentIndexNode = new MetadataIndexNodeV2();
      queueSize = metadataIndexNodeQueue.size();
    }
    return metadataIndexNodeQueue.poll();
  }

  private static void addCurrentIndexNodeToQueue(
      MetadataIndexNodeV2 currentIndexNode,
      Queue<MetadataIndexNodeV2> metadataIndexNodeQueue,
      TsFileOutput out)
      throws IOException {
    currentIndexNode.setEndOffset(out.getPosition());
    metadataIndexNodeQueue.add(currentIndexNode);
  }
}
