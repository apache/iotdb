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
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MetaProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.iotdb.IoTDBCommonExtractor;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeWriteSchemaPlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.OperateSchemaQueueReferenceNode;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class IoTDBSchemaRegionExtractor extends IoTDBCommonExtractor {
  private ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr;
  private Set<PlanNodeType> listenTypes = new HashSet<>();
  private List<PipeSnapshotEvent> historicalEvents = new ArrayList<>();

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    listenTypes =
        PipeSchemaNodeFilter.getPipeListenSet(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                EXTRACTOR_EXCLUSION_DEFAULT_VALUE));
  }

  @Override
  public void start() throws Exception {
    super.start();
    if (!listenTypes.isEmpty()) {
      SchemaRegionConsensusImpl.getInstance()
          .write(
              new SchemaRegionId(regionId),
              new OperateSchemaQueueReferenceNode(new PlanNodeId(""), true));
    }
    ProgressIndex progressIndex = pipeTaskMeta.getProgressIndex();
    long index;
    if (progressIndex instanceof MinimumProgressIndex) {
      // TODO: Trigger snapshot if not exists
      Pair<Long, List<PipeSnapshotEvent>> eventPair =
          SchemaNodeListeningQueue.getInstance(regionId).findAvailableSnapshots();
      index = !Objects.isNull(eventPair.getLeft()) ? eventPair.getLeft() + 1 : 0;
      historicalEvents = eventPair.getRight();
    } else {
      index = ((MetaProgressIndex) progressIndex).getIndex();
    }
    itr = SchemaNodeListeningQueue.getInstance(regionId).newIterator(index);
  }

  @Override
  public EnrichedEvent supply() throws Exception {
    if (!historicalEvents.isEmpty()) {
      if (historicalEvents.size() != 1) {
        // Do not report progress for non-final snapshot events since we re-transmit all snapshot
        // files currently
        return historicalEvents.remove(0);
      } else {
        PipeSchemaRegionSnapshotEvent event =
            (PipeSchemaRegionSnapshotEvent)
                historicalEvents
                    .remove(0)
                    .shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                        pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
        event.bindProgressIndex(new MetaProgressIndex(itr.getNextIndex() - 1));
        return event;
      }
    }

    EnrichedEvent event;
    do {
      // Return immediately
      event = (EnrichedEvent) itr.next(0);
    } while (event instanceof PipeWriteSchemaPlanEvent
        && (!listenTypes.contains((((PipeWriteSchemaPlanEvent) event).getPlanNode()).getType())
            || !isForwardingPipeRequests && event.isGeneratedByPipe()));

    if (Objects.isNull(event)) {
      return null;
    }

    EnrichedEvent targetEvent =
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName, pipeTaskMeta, null, Long.MIN_VALUE, Long.MAX_VALUE);
    targetEvent.bindProgressIndex(new MetaProgressIndex(itr.getNextIndex() - 1));
    targetEvent.increaseReferenceCount(IoTDBSchemaRegionExtractor.class.getName());
    return targetEvent;
  }

  @Override
  public void close() throws Exception {
    if (!hasBeenStarted.get()) {
      return;
    }
    SchemaNodeListeningQueue.getInstance(regionId).returnIterator(itr);
    if (!listenTypes.isEmpty()) {
      SchemaRegionConsensusImpl.getInstance()
          .write(
              new SchemaRegionId(regionId),
              new OperateSchemaQueueReferenceNode(new PlanNodeId(""), false));
    }
  }
}
