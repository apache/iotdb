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

import org.apache.iotdb.db.pipe.task.stage.PipeTaskCollectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskProcessorStage;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;

public class PipeTaskBuilder {

  private final String pipeName;
  private final String dataRegionId;
  private final PipeParameters pipeCollectorParameters;
  private final PipeParameters pipeProcessorParameters;
  private final PipeParameters pipeConnectorParameters;

  PipeTaskBuilder(
      String pipeName,
      String dataRegionId,
      PipeParameters pipeCollectorParameters,
      PipeParameters pipeProcessorParameters,
      PipeParameters pipeConnectorParameters) {
    this.pipeName = pipeName;
    this.dataRegionId = dataRegionId;
    this.pipeCollectorParameters = pipeCollectorParameters;
    this.pipeProcessorParameters = pipeProcessorParameters;
    this.pipeConnectorParameters = pipeConnectorParameters;
  }

  public PipeTask build() {
    // event flow: collector -> processor -> connector

    // we first build the collector and connector, then build the processor.
    final PipeTaskCollectorStage collectorStage =
        new PipeTaskCollectorStage(dataRegionId, pipeCollectorParameters);
    final PipeTaskConnectorStage connectorStage =
        new PipeTaskConnectorStage(pipeConnectorParameters);

    // the processor connects the collector and connector.
    final PipeTaskProcessorStage processorStage =
        new PipeTaskProcessorStage(
            pipeName,
            dataRegionId,
            collectorStage.getEventSupplier(),
            collectorStage.getCollectorPendingQueue(),
            pipeProcessorParameters,
            connectorStage.getPipeConnectorPendingQueue());

    return new PipeTask(pipeName, dataRegionId, collectorStage, processorStage, connectorStage);
  }
}
