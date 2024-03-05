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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.commons.pipe.mq.meta.PipeMQConsumerGroupMetaKeeper;
import org.apache.iotdb.commons.pipe.mq.meta.PipeMQTopicMetaKeeper;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTopicReq;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PipeMQInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeMQInfo.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_mq_info.bin";

  private final PipeMQTopicMetaKeeper pipeMQTopicMetaKeeper;
  private final PipeMQConsumerGroupMetaKeeper pipeMQConsumerGroupMetaKeeper;
  private final ReentrantReadWriteLock pipeMQInfoLock = new ReentrantReadWriteLock(true);

  public PipeMQInfo() {
    this.pipeMQTopicMetaKeeper = new PipeMQTopicMetaKeeper();
    this.pipeMQConsumerGroupMetaKeeper = new PipeMQConsumerGroupMetaKeeper();
  }

  /////////////////////////////// Lock ///////////////////////////////

  private void acquireReadLock() {
    pipeMQInfoLock.readLock().lock();
  }

  private void releaseReadLock() {
    pipeMQInfoLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    pipeMQInfoLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    pipeMQInfoLock.writeLock().unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////
  public void validateBeforeCreatingTopic(TCreateTopicReq createTopicReq) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeCreateTopicInternal(createTopicReq);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeCreateTopicInternal(TCreateTopicReq createTopicReq) throws PipeException {
    if (!isTopicExisted(createTopicReq.getTopicName())) {
      return;
    }

    final String exceptionMessage =
        String.format(
            "Failed to create pipe %s, the pipe with the same name has been created",
            createTopicReq.getTopicName());
    LOGGER.warn(exceptionMessage);
    throw new PipeException(exceptionMessage);
  }

  public void validateBeforeDroppingTopic(String topicName) throws PipeException {
    acquireReadLock();
    try {
      checkBeforeDropTopicInternal(topicName);
    } finally {
      releaseReadLock();
    }
  }

  private void checkBeforeDropTopicInternal(String topicName) throws PipeException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Check before dropping topic: {}, topic exists:{}", topicName, isTopicExisted(topicName));
    }

    // No matter whether the topic exists, we allow the drop operation executed on all nodes to
    // ensure the consistency.
    // DO NOTHING HERE!
  }

  public boolean isTopicExisted(String topicName) {
    acquireReadLock();
    try {
      return pipeMQTopicMetaKeeper.containsPipeMQTopicMeta(topicName);
    } finally {
      releaseReadLock();
    }
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    acquireReadLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (snapshotFile.exists() && snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to take snapshot, because snapshot file [{}] is already exist.",
            snapshotFile.getAbsolutePath());
        return false;
      }

      try (final FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
        pipeMQTopicMetaKeeper.processTakeSnapshot(fileOutputStream);
        // todo:
        // pipeMQConsumerGroupMetaKeeper.processTakeSnapshot(fileOutputStream);
        fileOutputStream.getFD().sync();
      }

      return true;
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    acquireWriteLock();
    try {
      final File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
      if (!snapshotFile.exists() || !snapshotFile.isFile()) {
        LOGGER.error(
            "Failed to load snapshot,snapshot file [{}] is not exist.",
            snapshotFile.getAbsolutePath());
        return;
      }

      try (final FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
        pipeMQTopicMetaKeeper.processLoadSnapshot(fileInputStream);
        // todo:
        // pipeMQConsumerGroupMetaKeeper.processTakeSnapshot(fileOutputStream);
      }
    } finally {
      releaseWriteLock();
    }
  }
}
