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

import org.apache.iotdb.db.pipe.task.stage.PipeTaskStage;

public class PipeTask {

  private final String pipeName;
  private final String dataRegionId;

  private final PipeTaskStage collectorStage;
  private final PipeTaskStage processorStage;
  private final PipeTaskStage connectorStage;

  PipeTask(
      String pipeName,
      String dataRegionId,
      PipeTaskStage collectorStage,
      PipeTaskStage processorStage,
      PipeTaskStage connectorStage) {
    this.pipeName = pipeName;
    this.dataRegionId = dataRegionId;

    this.collectorStage = collectorStage;
    this.processorStage = processorStage;
    this.connectorStage = connectorStage;
  }

  public void create() {
    collectorStage.create();
    processorStage.create();
    connectorStage.create();
  }

  public void drop() {
    collectorStage.drop();
    processorStage.drop();
    connectorStage.drop();
  }

  public void start() {
    collectorStage.start();
    processorStage.start();
    connectorStage.start();
  }

  public void stop() {
    collectorStage.stop();
    processorStage.stop();
    connectorStage.stop();
  }

  public String getDataRegionId() {
    return dataRegionId;
  }

  public String getPipeName() {
    return pipeName;
  }
}
