/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.datastructure.LinkedQueueVersion;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ConfigPlanListeningQueue implements SnapshotProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanListeningQueue.class);
  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

  private final ConcurrentIterableLinkedQueue<ConfigPhysicalPlan> queue =
      new ConcurrentIterableLinkedQueue<>();

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToPlan(ConfigPhysicalPlan plan) {
    if (queue.hasAnyIterators() && PipeConfigPlanFilter.shouldBeListenedByQueue(plan)) {
      queue.add(plan);
    }
  }

  public ConcurrentIterableLinkedQueue<ConfigPhysicalPlan>.DynamicIterator newIterator(int index) {
    return queue.iterateFrom(index);
  }

  public void returnIterator(
      ConcurrentIterableLinkedQueue<ConfigPhysicalPlan>.DynamicIterator itr) {
    itr.close();
    if (!queue.hasAnyIterators()) {
      queue.clear();
    }
  }

  /////////////////////////////// Snapshot ///////////////////////////////
  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      ReadWriteIOUtils.write(LinkedQueueVersion.VERSION_1.getVersion(), fileOutputStream);
      ReadWriteIOUtils.write(queue.getFirstIndex(), fileOutputStream);
      try (ConcurrentIterableLinkedQueue<ConfigPhysicalPlan>.DynamicIterator itr =
          queue.iterateFromEarliest()) {
        ConfigPhysicalPlan plan;
        while (true) {
          plan = itr.next(0);
          if (plan == null) {
            break;
          }
          ReadWriteIOUtils.write(plan.getSerializedSize(), fileOutputStream);
          ReadWriteIOUtils.write(plan.serializeToByteBuffer(), fileOutputStream);
        }
      }
      fileOutputStream.getFD().sync();
    }

    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot, snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      final LinkedQueueVersion version =
          LinkedQueueVersion.deserialize(ReadWriteIOUtils.readByte(fileInputStream));
      switch (version) {
        case VERSION_1:
          deserializeVersion1(fileInputStream);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown config plan listening queue version: " + version.getVersion());
      }
    }
  }

  private void deserializeVersion1(FileInputStream inputStream) throws IOException {
    clear();

    try (FileChannel channel = inputStream.getChannel()) {
      queue.setFirstIndex(ReadWriteIOUtils.readInt(inputStream));
      while (true) {
        int capacity = ReadWriteIOUtils.readInt(inputStream);
        if (capacity == -1) {
          // EOF
          return;
        }
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        channel.read(buffer);
        queue.add(ConfigPhysicalPlan.Factory.create(buffer));
      }
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  private void clear() {
    queue.clear();
  }

  /////////////////////////////// INSTANCE ///////////////////////////////

  public static ConfigPlanListeningQueue getInstance() {
    return ConfigPlanListeningQueueHolder.INSTANCE;
  }

  private static class ConfigPlanListeningQueueHolder {

    private static final ConfigPlanListeningQueue INSTANCE = new ConfigPlanListeningQueue();

    private ConfigPlanListeningQueueHolder() {
      // empty constructor
    }
  }
}
