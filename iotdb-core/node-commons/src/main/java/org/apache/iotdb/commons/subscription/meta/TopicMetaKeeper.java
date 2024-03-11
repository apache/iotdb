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

package org.apache.iotdb.commons.subscription.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TopicMetaKeeper {
  private Map<String, TopicMeta> topicNameToTopicMetaMap;

  public TopicMetaKeeper() {
    topicNameToTopicMetaMap = new ConcurrentHashMap<>();
  }

  /////////////////////////////////  TopicMeta  /////////////////////////////////

  public void addTopicMeta(String topicName, TopicMeta topicMeta) {
    topicNameToTopicMetaMap.put(topicName, topicMeta);
  }

  public TopicMeta getTopicMeta(String topicName) {
    return topicNameToTopicMetaMap.get(topicName);
  }

  public Iterable<TopicMeta> getAllTopicMeta() {
    return topicNameToTopicMetaMap.values();
  }

  public void removeTopicMeta(String topicName) {
    topicNameToTopicMetaMap.remove(topicName);
  }

  public boolean containsTopicMeta(String topicName) {
    return topicNameToTopicMetaMap.containsKey(topicName);
  }

  public void clear() {
    this.topicNameToTopicMetaMap.clear();
  }

  public boolean isEmpty() {
    return topicNameToTopicMetaMap.isEmpty();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(topicNameToTopicMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, TopicMeta> entry : topicNameToTopicMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      topicNameToTopicMetaMap.put(topicName, TopicMeta.deserialize(fileInputStream));
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
    TopicMetaKeeper that = (TopicMetaKeeper) o;
    return Objects.equals(topicNameToTopicMetaMap, that.topicNameToTopicMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicNameToTopicMetaMap);
  }

  @Override
  public String toString() {
    return "TopicMetaKeeper{" + "topicNameToTopicMetaMap=" + topicNameToTopicMetaMap + '}';
  }
}
