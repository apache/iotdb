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
package org.apache.iotdb.db.schemaengine.schemaregion.tag;

import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TagManagerTest {

  private File tempDir;
  private MemSchemaRegionStatistics regionStatistics;
  private TagManager tagManager;

  @After
  public void tearDown() throws Exception {
    if (tagManager != null) {
      tagManager.clear();
    }
    if (regionStatistics != null) {
      regionStatistics.clear();
    }
    if (tempDir != null) {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void removeIndexIgnoresMissingEntriesAndReleasesOnlyExistingMemory() throws Exception {
    initTagManager();
    final IMeasurementMNode<?> node = newMeasurementMNode("s0");

    tagManager.removeIndex("missingKey", "missingValue", node);
    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());

    tagManager.addIndex("key", "value", node);
    final long expectedMemory = indexMemory("key", "value", 1);
    Assert.assertEquals(expectedMemory, regionStatistics.getRegionMemoryUsage());

    tagManager.removeIndex("key", "missingValue", node);
    Assert.assertEquals(expectedMemory, regionStatistics.getRegionMemoryUsage());

    tagManager.removeIndex("key", "value", newMeasurementMNode("other"));
    Assert.assertEquals(expectedMemory, regionStatistics.getRegionMemoryUsage());

    tagManager.removeIndex("key", "value", node);
    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());

    tagManager.removeIndex("key", "value", node);
    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());
  }

  @Test
  public void concurrentAddIndexRequestsMemoryForActualInsertionsOnly() throws Exception {
    initTagManager();
    final String tagKey = "key";
    final String tagValue = "value";
    final int measurementCount = 128;
    final List<IMeasurementMNode<?>> nodes = new ArrayList<>();
    for (int i = 0; i < measurementCount; i++) {
      nodes.add(newMeasurementMNode("s" + i));
    }

    final int workerCount = 16;
    final ExecutorService executorService = Executors.newFixedThreadPool(workerCount);
    final CountDownLatch readyLatch = new CountDownLatch(workerCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>();
    for (final IMeasurementMNode<?> node : nodes) {
      futures.add(
          executorService.submit(
              () -> {
                readyLatch.countDown();
                startLatch.await();
                tagManager.addIndex(tagKey, tagValue, node);
                return null;
              }));
    }

    try {
      Assert.assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
      startLatch.countDown();
      for (final Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executorService.shutdownNow();
    }
    Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

    final long expectedMemory = indexMemory(tagKey, tagValue, measurementCount);
    Assert.assertEquals(expectedMemory, regionStatistics.getRegionMemoryUsage());

    for (final IMeasurementMNode<?> node : nodes) {
      tagManager.addIndex(tagKey, tagValue, node);
    }
    Assert.assertEquals(expectedMemory, regionStatistics.getRegionMemoryUsage());

    for (final IMeasurementMNode<?> node : nodes) {
      tagManager.removeIndex(tagKey, tagValue, node);
    }
    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());
  }

  @Test
  public void concurrentAddAndRemoveIndexEventuallyReleasesAllMemory() throws Exception {
    initTagManager();
    final String tagKey = "key";
    final String tagValue = "value";
    final IMeasurementMNode<?> node = newMeasurementMNode("s0");

    final int workerCount = 16;
    final int roundCount = 1000;
    final ExecutorService executorService = Executors.newFixedThreadPool(workerCount);
    final CountDownLatch readyLatch = new CountDownLatch(workerCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < workerCount; i++) {
      futures.add(
          executorService.submit(
              () -> {
                readyLatch.countDown();
                startLatch.await();
                for (int round = 0; round < roundCount; round++) {
                  tagManager.addIndex(tagKey, tagValue, node);
                  tagManager.removeIndex(tagKey, tagValue, node);
                }
                return null;
              }));
    }

    try {
      Assert.assertTrue(readyLatch.await(10, TimeUnit.SECONDS));
      startLatch.countDown();
      for (final Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executorService.shutdownNow();
    }
    Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS));

    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());

    tagManager.addIndex(tagKey, tagValue, node);
    Assert.assertEquals(indexMemory(tagKey, tagValue, 1), regionStatistics.getRegionMemoryUsage());

    tagManager.removeIndex(tagKey, tagValue, node);
    Assert.assertEquals(0, regionStatistics.getRegionMemoryUsage());
  }

  private void initTagManager() throws Exception {
    tempDir = Files.createTempDirectory("tag-manager").toFile();
    regionStatistics = new MemSchemaRegionStatistics(0, new MemSchemaEngineStatistics());
    tagManager = new TagManager(tempDir.getAbsolutePath(), regionStatistics);
  }

  private static IMeasurementMNode<IMemMNode> newMeasurementMNode(final String measurement) {
    final IMNodeFactory<IMemMNode> nodeFactory =
        MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();
    return nodeFactory.createMeasurementMNode(
        null,
        measurement,
        new MeasurementSchema(
            measurement, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY),
        null);
  }

  private static long indexMemory(
      final String tagKey, final String tagValue, final int measurementCount) {
    return RamUsageEstimator.sizeOf(tagKey)
        + 4
        + RamUsageEstimator.sizeOf(tagValue)
        + 4
        + (RamUsageEstimator.NUM_BYTES_OBJECT_REF + 4) * measurementCount;
  }
}
