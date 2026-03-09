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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;

public class SimpleProgressIndexAssigner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProgressIndexAssigner.class);

  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final String PIPE_SYSTEM_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "pipe"
          + File.separator;
  private static final String REBOOT_TIMES_FILE_NAME = "reboot_times.txt";

  private boolean isSimpleConsensusEnable = false;

  private int rebootTimes = 0;
  private final AtomicLong insertionRequestId = new AtomicLong(1);

  public void start() {
    isSimpleConsensusEnable =
        IOTDB_CONFIG.getDataRegionConsensusProtocolClass().equals(SIMPLE_CONSENSUS);
    LOGGER.info("Starting SimpleProgressIndexAssigner ...");

    try {
      makeDirIfNecessary();
      parseRebootTimes();
      recordRebootTimes();
      LOGGER.info(
          "SimpleProgressIndexAssigner started successfully. isSimpleConsensusEnable: {}, rebootTimes: {}",
          isSimpleConsensusEnable,
          rebootTimes);
    } catch (Exception e) {
      LOGGER.error("Cannot start SimpleProgressIndexAssigner because of {}", e.getMessage(), e);
    }
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(PIPE_SYSTEM_DIR);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void parseRebootTimes() {
    File file = SystemFileFactory.INSTANCE.getFile(PIPE_SYSTEM_DIR + REBOOT_TIMES_FILE_NAME);
    if (!file.exists()) {
      rebootTimes = 0;
      return;
    }
    try {
      String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
      rebootTimes = Integer.parseInt(content);
    } catch (final Exception e) {
      rebootTimes = (int) (System.currentTimeMillis() / 1000);
      LOGGER.error(
          "Cannot parse reboot times from file {}, set the current time in seconds ({}) as the reboot times",
          file.getAbsolutePath(),
          rebootTimes);
    }
  }

  private void recordRebootTimes() {
    final File file = SystemFileFactory.INSTANCE.getFile(PIPE_SYSTEM_DIR + REBOOT_TIMES_FILE_NAME);
    try (final FileOutputStream fos = new FileOutputStream(file, false)) {
      fos.write(String.valueOf(rebootTimes + 1).getBytes(StandardCharsets.UTF_8));
      fos.flush();
      fos.getFD().sync();
    } catch (final Exception e) {
      LOGGER.error(
          "Cannot record reboot times {} to file {}, the reboot times will not be updated",
          rebootTimes,
          file.getAbsolutePath());
    }
  }

  public void assignIfNeeded(InsertNode insertNode) {
    if (!isSimpleConsensusEnable) {
      return;
    }

    insertNode.setProgressIndex(
        new SimpleProgressIndex(rebootTimes, insertionRequestId.getAndIncrement()));
  }

  public SimpleProgressIndex getSimpleProgressIndex() {
    return new SimpleProgressIndex(rebootTimes, insertionRequestId.getAndIncrement());
  }

  ////////////////////// Provided for Subscription Agent //////////////////////

  public int getRebootTimes() {
    return rebootTimes;
  }
}
