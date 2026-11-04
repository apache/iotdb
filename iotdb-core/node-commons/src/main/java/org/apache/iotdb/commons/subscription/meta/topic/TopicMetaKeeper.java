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

package org.apache.iotdb.commons.subscription.meta.topic;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TopicMetaKeeper {

  private final Map<String, TopicMeta> topicNameToTopicMetaMap;

  private final ReentrantReadWriteLock topicMetaKeeperLock;

  public TopicMetaKeeper() {
    topicNameToTopicMetaMap = new ConcurrentHashMap<>();
    topicMetaKeeperLock = new ReentrantReadWriteLock(true);
  }

  /////////////////////////////////  Lock  /////////////////////////////////

  public void acquireReadLock() {
    topicMetaKeeperLock.readLock().lock();
  }

  public void releaseReadLock() {
    topicMetaKeeperLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    topicMetaKeeperLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    topicMetaKeeperLock.writeLock().unlock();
  }

  /////////////////////////////////  TopicMeta  /////////////////////////////////

  public void addTopicMeta(String topicName, TopicMeta topicMeta) {
    topicNameToTopicMetaMap.put(generateTopicMetaKeeperKey(topicMeta), topicMeta);
  }

  public TopicMeta getTopicMeta(String topicName) {
    final TopicMeta treeTopicMeta = getTopicMeta(topicName, false);
    return Objects.nonNull(treeTopicMeta) ? treeTopicMeta : getTopicMeta(topicName, true);
  }

  public TopicMeta getTopicMeta(String topicName, boolean isTableModel) {
    return topicNameToTopicMetaMap.get(generateTopicMetaKeeperKey(topicName, isTableModel));
  }

  public Iterable<TopicMeta> getAllTopicMeta() {
    return topicNameToTopicMetaMap.values();
  }

  public void removeTopicMeta(String topicName) {
    final String treeModelKey = generateTopicMetaKeeperKey(topicName, false);
    if (topicNameToTopicMetaMap.remove(treeModelKey) == null) {
      topicNameToTopicMetaMap.remove(generateTopicMetaKeeperKey(topicName, true));
    }
  }

  public void removeTopicMeta(String topicName, boolean isTableModel) {
    topicNameToTopicMetaMap.remove(generateTopicMetaKeeperKey(topicName, isTableModel));
  }

  public boolean containsTopicMeta(String topicName) {
    return containsTopicMeta(topicName, false) || containsTopicMeta(topicName, true);
  }

  public boolean containsTopicMeta(String topicName, boolean isTableModel) {
    return topicNameToTopicMetaMap.containsKey(generateTopicMetaKeeperKey(topicName, isTableModel));
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
      ReadWriteIOUtils.write(entry.getValue().getTopicName(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String topicName = ReadWriteIOUtils.readString(fileInputStream);
      final TopicMeta topicMeta = TopicMeta.deserialize(fileInputStream);
      topicNameToTopicMetaMap.put(generateTopicMetaKeeperKey(topicMeta), topicMeta);
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

  /////////////////////////////////  Tree & Table Isolation  /////////////////////////////////

  private static String generateTopicMetaKeeperKey(final TopicMeta topicMeta) {
    return generateTopicMetaKeeperKey(topicMeta.getTopicName(), topicMeta.visibleUnderTableModel());
  }

  private static String generateTopicMetaKeeperKey(
      final String topicName, final boolean isTableModel) {
    return (isTableModel ? "table:" : "tree:") + topicName;
  }
}
