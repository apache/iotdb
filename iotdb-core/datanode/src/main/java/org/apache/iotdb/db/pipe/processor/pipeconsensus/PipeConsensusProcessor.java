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

package org.apache.iotdb.db.pipe.processor.pipeconsensus;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.ProgressIndexType;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeManager;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_CONSENSUS_PIPE_NAME_KEY;

public class PipeConsensusProcessor implements PipeProcessor {
  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private ConsensusPipeName consensusPipeName;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    consensusPipeName =
        new ConsensusPipeName(parameters.getString(PROCESSOR_CONSENSUS_PIPE_NAME_KEY));
  }

  private boolean isContainLocalData(EnrichedEvent enrichedEvent) {
    final ProgressIndex progressIndex = enrichedEvent.getProgressIndex();
    if (progressIndex instanceof RecoverProgressIndex) {
      return ((RecoverProgressIndex) progressIndex)
          .getDataNodeId2LocalIndex()
          .containsKey(DATA_NODE_ID);
    } else if (progressIndex instanceof HybridProgressIndex) {
      final Map<Short, ProgressIndex> type2Index =
          ((HybridProgressIndex) progressIndex).getType2Index();
      if (!type2Index.containsKey(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType())) {
        return false;
      }
      return ((RecoverProgressIndex)
              type2Index.get(ProgressIndexType.RECOVER_PROGRESS_INDEX.getType()))
          .getDataNodeId2LocalIndex()
          .containsKey(DATA_NODE_ID);
    }
    return false;
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (tsFileInsertionEvent instanceof EnrichedEvent) {
      if (!needFilter()) {
        eventCollector.collect(tsFileInsertionEvent);
        return;
      }
      // Only user-generated TsFileInsertionEvent can be replicated. Any tsFile synchronized from a
      // replica should not be replicated again
      final EnrichedEvent enrichedEvent = (EnrichedEvent) tsFileInsertionEvent;
      if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).isGeneratedByPipeConsensus()
          && isContainLocalData(enrichedEvent)) {
        eventCollector.collect(tsFileInsertionEvent);
      }
    }
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (tabletInsertionEvent instanceof EnrichedEvent) {
      if (!needFilter()) {
        eventCollector.collect(tabletInsertionEvent);
        return;
      }
      // Only user-generated TabletInsertionEvent can be replicated.
      final EnrichedEvent enrichedEvent = (EnrichedEvent) tabletInsertionEvent;
      if (isContainLocalData(enrichedEvent)) {
        eventCollector.collect(tabletInsertionEvent);
      }
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (event instanceof EnrichedEvent) {
      if (!needFilter()) {
        eventCollector.collect(event);
        return;
      }

      final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
      if (isContainLocalData(enrichedEvent)) {
        eventCollector.collect(event);
      }
    }
  }

  @Override
  public void close() throws Exception {}

  private boolean needFilter() {
    return ConsensusPipeManager.getProcessorFilterSwitch()
        .getOrDefault(this.consensusPipeName, true);
  }
}
