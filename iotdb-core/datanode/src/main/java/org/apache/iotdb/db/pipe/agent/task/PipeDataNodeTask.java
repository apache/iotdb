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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.stage.PipeTaskStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeDataNodeTask implements PipeTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTask.class);

  private final String pipeName;
  private final int regionId;

  private final PipeTaskStage extractorStage;
  private final PipeTaskStage processorStage;
  private final PipeTaskStage connectorStage;

  private volatile boolean isCompleted = false;

  public PipeDataNodeTask(
      final String pipeName,
      final int regionId,
      final PipeTaskStage extractorStage,
      final PipeTaskStage processorStage,
      final PipeTaskStage connectorStage) {
    this.pipeName = pipeName;
    this.regionId = regionId;

    this.extractorStage = extractorStage;
    this.processorStage = processorStage;
    this.connectorStage = connectorStage;
  }

  @Override
  public void create() {
    final long startTime = System.currentTimeMillis();
    extractorStage.create();
    processorStage.create();
    connectorStage.create();
    LOGGER.info(
        "Create pipe DN task {} successfully within {} ms",
        this,
        System.currentTimeMillis() - startTime);
  }

  @Override
  public void drop() {
    final long startTime = System.currentTimeMillis();
    extractorStage.drop();
    processorStage.drop();
    connectorStage.drop();
    LOGGER.info(
        "Drop pipe DN task {} successfully within {} ms",
        this,
        System.currentTimeMillis() - startTime);
  }

  @Override
  public void start() {
    final long startTime = System.currentTimeMillis();
    extractorStage.start();
    processorStage.start();
    connectorStage.start();
    LOGGER.info(
        "Start pipe DN task {} successfully within {} ms",
        this,
        System.currentTimeMillis() - startTime);
  }

  @Override
  public void stop() {
    final long startTime = System.currentTimeMillis();
    extractorStage.stop();
    processorStage.stop();
    connectorStage.stop();
    LOGGER.info(
        "Stop pipe DN task {} successfully within {} ms",
        this,
        System.currentTimeMillis() - startTime);
  }

  public int getRegionId() {
    return regionId;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public void markCompleted() {
    this.isCompleted = true;
  }

  @Override
  public String toString() {
    return pipeName + "@" + regionId;
  }
}
