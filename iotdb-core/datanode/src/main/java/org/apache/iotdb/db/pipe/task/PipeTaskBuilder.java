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

package org.apache.iotdb.db.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskExtractorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskProcessorStage;

public class PipeTaskBuilder {

  private final PipeStaticMeta pipeStaticMeta;
  private final TConsensusGroupId dataRegionId;
  private final PipeTaskMeta pipeTaskMeta;

  public PipeTaskBuilder(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId dataRegionId, PipeTaskMeta pipeTaskMeta) {
    this.pipeStaticMeta = pipeStaticMeta;
    this.dataRegionId = dataRegionId;
    this.pipeTaskMeta = pipeTaskMeta;
  }

  public PipeTask build() {
    // event flow: extractor -> processor -> connector

    // we first build the extractor and connector, then build the processor.
    final PipeTaskExtractorStage extractorStage =
        new PipeTaskExtractorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            pipeStaticMeta.getExtractorParameters(),
            dataRegionId,
            pipeTaskMeta);

    final PipeTaskConnectorStage connectorStage =
        new PipeTaskConnectorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            pipeStaticMeta.getConnectorParameters());

    // the processor connects the extractor and connector.
    final PipeTaskProcessorStage processorStage =
        new PipeTaskProcessorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            pipeStaticMeta.getProcessorParameters(),
            dataRegionId,
            extractorStage.getEventSupplier(),
            connectorStage.getPipeConnectorPendingQueue());

    return new PipeTask(
        pipeStaticMeta.getPipeName(), dataRegionId, extractorStage, processorStage, connectorStage);
  }
}
