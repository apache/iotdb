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
import org.apache.iotdb.commons.pipe.datastructure.AbstractPipeListeningQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.extractor.IoTDBMetaExtractor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.event.common.schema.PipeWritePlanNodeEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.OperateSchemaQueueNode;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class IoTDBSchemaRegionExtractor extends IoTDBMetaExtractor {
  private Set<PlanNodeType> listenTypes = new HashSet<>();
  private static final AtomicInteger referenceCount = new AtomicInteger(0);

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    listenTypes = PipeSchemaNodeFilter.getPipeListenSet(parameters);
  }

  @Override
  public void start() throws Exception {
    // Delay the start process to schema region leader ready
    if (!SchemaNodeListeningQueue.getInstance(regionId).isLeaderReady()) {
      return;
    }
    // Typically if this is empty the PipeTask won't be created, this is just in case
    if (!listenTypes.isEmpty()) {
      referenceCount.getAndIncrement();
      if (referenceCount.get() == 1) {
        SchemaRegionConsensusImpl.getInstance()
            .write(
                new SchemaRegionId(regionId), new OperateSchemaQueueNode(new PlanNodeId(""), true));
      }
    }
    super.start();
  }

  // This method will return events only after schema region leader get ready
  @Override
  public EnrichedEvent supply() throws Exception {
    if (!SchemaNodeListeningQueue.getInstance(regionId).isLeaderReady()) {
      return null;
    }
    if (!hasBeenStarted.get()) {
      start();
    }
    return super.supply();
  }

  @Override
  protected AbstractPipeListeningQueue getListeningQueue() {
    return SchemaNodeListeningQueue.getInstance(regionId);
  }

  @Override
  protected boolean isListenType(Event event) {
    return listenTypes.contains(((PipeWritePlanNodeEvent) event).getPlanNode().getType());
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (!listenTypes.isEmpty()) {
      referenceCount.getAndDecrement();
      if (referenceCount.get() == 0) {
        SchemaRegionConsensusImpl.getInstance()
            .write(
                new SchemaRegionId(regionId),
                new OperateSchemaQueueNode(new PlanNodeId(""), false));
      }
    }
  }
}
