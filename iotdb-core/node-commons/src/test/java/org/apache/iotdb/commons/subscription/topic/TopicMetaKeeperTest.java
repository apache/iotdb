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

package org.apache.iotdb.commons.subscription.topic;

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMetaKeeper;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class TopicMetaKeeperTest {

  @Test
  public void testSameNameTreeAndTableTopicsCanCoexist() {
    final String topicName = "topic";
    final TopicMeta treeTopicMeta =
        new TopicMeta(
            topicName,
            1L,
            Collections.singletonMap(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE));
    final TopicMeta tableTopicMeta =
        new TopicMeta(
            topicName,
            2L,
            Collections.singletonMap(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE));
    final TopicMetaKeeper keeper = new TopicMetaKeeper();

    keeper.addTopicMeta(topicName, treeTopicMeta);
    keeper.addTopicMeta(topicName, tableTopicMeta);

    Assert.assertSame(treeTopicMeta, keeper.getTopicMeta(topicName, false));
    Assert.assertSame(tableTopicMeta, keeper.getTopicMeta(topicName, true));
    Assert.assertSame(treeTopicMeta, keeper.getTopicMeta(topicName));
    Assert.assertEquals(2, sizeOf(keeper.getAllTopicMeta()));

    keeper.removeTopicMeta(topicName, false);
    Assert.assertFalse(keeper.containsTopicMeta(topicName, false));
    Assert.assertTrue(keeper.containsTopicMeta(topicName, true));
    Assert.assertSame(tableTopicMeta, keeper.getTopicMeta(topicName));
  }

  @Test
  public void testSnapshotPreservesSameNameTreeAndTableTopics() throws Exception {
    final String topicName = "topic";
    final TopicMetaKeeper keeper = new TopicMetaKeeper();
    keeper.addTopicMeta(
        topicName,
        new TopicMeta(
            topicName,
            1L,
            Collections.singletonMap(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)));
    keeper.addTopicMeta(
        topicName,
        new TopicMeta(
            topicName,
            2L,
            Collections.singletonMap(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TABLE_VALUE)));

    final Path snapshotFile = Files.createTempFile("topic-meta-keeper", ".snapshot");
    try {
      try (final FileOutputStream outputStream = new FileOutputStream(snapshotFile.toFile())) {
        keeper.processTakeSnapshot(outputStream);
      }

      try (final FileInputStream inputStream = new FileInputStream(snapshotFile.toFile())) {
        Assert.assertEquals(2, ReadWriteIOUtils.readInt(inputStream));
        for (int i = 0; i < 2; i++) {
          Assert.assertEquals(topicName, ReadWriteIOUtils.readString(inputStream));
          TopicMeta.deserialize(inputStream);
        }
      }

      final TopicMetaKeeper restoredKeeper = new TopicMetaKeeper();
      try (final FileInputStream inputStream = new FileInputStream(snapshotFile.toFile())) {
        restoredKeeper.processLoadSnapshot(inputStream);
      }
      Assert.assertNotNull(restoredKeeper.getTopicMeta(topicName, false));
      Assert.assertNotNull(restoredKeeper.getTopicMeta(topicName, true));
    } finally {
      Files.deleteIfExists(snapshotFile);
    }
  }

  private static int sizeOf(final Iterable<TopicMeta> topicMetas) {
    int size = 0;
    for (final TopicMeta ignored : topicMetas) {
      size++;
    }
    return size;
  }
}
