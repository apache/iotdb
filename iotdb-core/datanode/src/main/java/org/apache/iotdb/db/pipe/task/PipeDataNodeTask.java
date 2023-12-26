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
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.stage.PipeTaskStage;

public class PipeDataNodeTask implements PipeTask {

  protected final String pipeName;
  protected final TConsensusGroupId regionId;

  private final PipeTaskStage extractorStage;
  private final PipeTaskStage processorStage;
  private final PipeTaskStage connectorStage;

  public PipeDataNodeTask(
      String pipeName,
      TConsensusGroupId regionId,
      PipeTaskStage extractorStage,
      PipeTaskStage processorStage,
      PipeTaskStage connectorStage) {
    this.pipeName = pipeName;
    this.regionId = regionId;

    this.extractorStage = extractorStage;
    this.processorStage = processorStage;
    this.connectorStage = connectorStage;
  }

  @Override
  public void create() {
    extractorStage.create();
    processorStage.create();
    connectorStage.create();
  }

  @Override
  public void drop() {
    extractorStage.drop();
    processorStage.drop();
    connectorStage.drop();
  }

  @Override
  public void start() {
    extractorStage.start();
    processorStage.start();
    connectorStage.start();
  }

  @Override
  public void stop() {
    extractorStage.stop();
    processorStage.stop();
    connectorStage.stop();
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public String getPipeName() {
    return pipeName;
  }
}
