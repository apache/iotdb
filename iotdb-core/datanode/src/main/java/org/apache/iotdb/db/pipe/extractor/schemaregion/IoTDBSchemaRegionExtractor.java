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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.pipe.datastructure.queue.listening.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.extractor.IoTDBNonDataRegionExtractor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeOperateSchemaQueueNode;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class IoTDBSchemaRegionExtractor extends IoTDBNonDataRegionExtractor {

  private SchemaRegionId schemaRegionId;

  private Set<PlanNodeType> listenedTypeSet = new HashSet<>();

  // If close() is called, hasBeenClosed will be set to true even if the extractor is started again.
  // If the extractor is closed, it should not be started again. This is to avoid the case that
  // the extractor is closed and then be reused by processor.
  private final AtomicBoolean hasBeenClosed = new AtomicBoolean(false);

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    schemaRegionId = new SchemaRegionId(regionId);
    listenedTypeSet = SchemaRegionListeningFilter.parseListeningPlanTypeSet(parameters);
  }

  @Override
  public void start() throws Exception {
    // Delay the start process to schema region leader ready
    if (!PipeAgent.runtime().isSchemaLeaderReady(schemaRegionId)
        || hasBeenStarted.get()
        || hasBeenClosed.get()
        || listenedTypeSet.isEmpty()) {
      return;
    }

    // Try open the queue if it is the first task
    if (PipeAgent.runtime().increaseAndGetSchemaListenerReferenceCount(schemaRegionId) == 1) {
      SchemaRegionConsensusImpl.getInstance()
          .write(schemaRegionId, new PipeOperateSchemaQueueNode(new PlanNodeId(""), true));
    }

    super.start();
  }

  // This method will return events only after schema region leader gets ready
  @Override
  public synchronized EnrichedEvent supply() throws Exception {
    if (!PipeAgent.runtime().isSchemaLeaderReady(schemaRegionId)
        || hasBeenClosed.get()
        || listenedTypeSet.isEmpty()) {
      return null;
    }

    // Delayed start
    if (!hasBeenStarted.get()) {
      start();
    }

    return super.supply();
  }

  @Override
  protected AbstractPipeListeningQueue getListeningQueue() {
    return PipeAgent.runtime().schemaListener(schemaRegionId);
  }

  @Override
  protected boolean isTypeListened(Event event) {
    return listenedTypeSet.contains(
        ((PipeSchemaRegionWritePlanEvent) event).getPlanNode().getType());
  }

  @Override
  public synchronized void close() throws Exception {
    if (hasBeenClosed.get()) {
      return;
    }
    hasBeenClosed.set(true);

    if (!hasBeenStarted.get()) {
      return;
    }
    super.close();

    if (!listenedTypeSet.isEmpty()) {
      // TODO: check me
      // The queue is not closed here, and is closed iff the PipeMetaKeeper
      // has no schema pipe after one successful sync
      PipeAgent.runtime().decreaseAndGetSchemaListenerReferenceCount(schemaRegionId);
    }
  }
}
