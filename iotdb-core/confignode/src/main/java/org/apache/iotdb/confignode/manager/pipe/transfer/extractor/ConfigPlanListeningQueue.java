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

package org.apache.iotdb.confignode.manager.pipe.transfer.extractor;

import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.datastructure.LinkedQueueSerializerType;
import org.apache.iotdb.commons.pipe.event.SerializableEvent;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigSerializableEventType;
import org.apache.iotdb.confignode.manager.pipe.event.PipeWriteConfigPlanEvent;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ConfigPlanListeningQueue extends AbstractPipeListeningQueue
    implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanListeningQueue.class);

  private static final String SNAPSHOT_FILE_NAME = "pipe_listening_queue.bin";

  protected ConfigPlanListeningQueue() {
    super(LinkedQueueSerializerType.PLAIN);
  }

  /////////////////////////////// Function ///////////////////////////////

  public void tryListenToPlan(ConfigPhysicalPlan plan) {
    if (PipeConfigPlanFilter.shouldBeListenedByQueue(plan)) {
      PipeWriteConfigPlanEvent event = new PipeWriteConfigPlanEvent(plan);
      event.increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
      super.listenToElement(event);
    }
  }

  public void tryListenToSnapshot(String snapshotPath) {
    PipeConfigRegionSnapshotEvent event = new PipeConfigRegionSnapshotEvent(snapshotPath);
    event.increaseReferenceCount(ConfigPlanListeningQueue.class.getName());
    super.listenToElement(event);
  }

  /////////////////////////////// Element Ser / De Method ////////////////////////////////
  @Override
  protected ByteBuffer serializeToByteBuffer(Event event) {
    return ((SerializableEvent) event).serializeToByteBuffer();
  }

  @Override
  protected Event deserializeFromByteBuffer(ByteBuffer byteBuffer) {
    try {
      return PipeConfigSerializableEventType.deserialize(byteBuffer);
    } catch (IOException e) {
      LOGGER.error("Failed to load snapshot from byteBuffer {}.", byteBuffer);
    }
    return null;
  }

  /////////////////////////////// Snapshot ///////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return super.serializeToFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    super.deserializeFromFile(new File(snapshotDir + SNAPSHOT_FILE_NAME));
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
