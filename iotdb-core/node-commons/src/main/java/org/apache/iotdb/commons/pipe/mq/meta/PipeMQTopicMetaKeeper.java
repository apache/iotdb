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

package org.apache.iotdb.commons.pipe.mq.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class PipeMQTopicMetaKeeper {
  private Map<String, PipeMQTopicMeta> topicNameToPipeMQTopicMetaMap;

  public PipeMQTopicMetaKeeper() {
    topicNameToPipeMQTopicMetaMap = new ConcurrentHashMap<>();
  }

  /////////////////////////////////  PipeMeta  /////////////////////////////////

  public void addPipeMQTopicMeta(String pipeName, PipeMQTopicMeta pipeMQTopicMeta) {
    topicNameToPipeMQTopicMetaMap.put(pipeName, pipeMQTopicMeta);
  }

  public PipeMQTopicMeta getPipeMQTopicMeta(String pipeName) {
    return topicNameToPipeMQTopicMetaMap.get(pipeName);
  }

  public void removePipeMQTopicMeta(String pipeName) {
    topicNameToPipeMQTopicMetaMap.remove(pipeName);
  }

  public boolean containsPipeMQTopicMeta(String pipeName) {
    return topicNameToPipeMQTopicMetaMap.containsKey(pipeName);
  }

  public Iterable<PipeMQTopicMeta> getPipeMQTopicMetaList() {
    return topicNameToPipeMQTopicMetaMap.values();
  }

  public int getPipeMQTopicMetaCount() {
    return topicNameToPipeMQTopicMetaMap.size();
  }

  public PipeMQTopicMeta getPipeMQTopicMetaByPipeName(String pipeName) {
    return topicNameToPipeMQTopicMetaMap.get(pipeName);
  }

  public void clear() {
    this.topicNameToPipeMQTopicMetaMap.clear();
  }

  public boolean isEmpty() {
    return topicNameToPipeMQTopicMetaMap.isEmpty();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(topicNameToPipeMQTopicMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, PipeMQTopicMeta> entry : topicNameToPipeMQTopicMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      topicNameToPipeMQTopicMetaMap.put(topicName, PipeMQTopicMeta.deserialize(fileInputStream));
    }
  }

  /////////////////////////////////  Override  /////////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeMQTopicMetaKeeper that = (PipeMQTopicMetaKeeper) o;
    return Objects.equals(topicNameToPipeMQTopicMetaMap, that.topicNameToPipeMQTopicMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicNameToPipeMQTopicMetaMap);
  }

  @Override
  public String toString() {
    return "PipeMQTopicMetaKeeper{"
        + "topicNameToPipeMQTopicMetaMap="
        + topicNameToPipeMQTopicMetaMap
        + '}';
  }
}
