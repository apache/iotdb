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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.confignode.manager.pipe.task.stage.PipeConfigNodeTaskStage;

public class PipeConfigNodeTask extends PipeTask {
  private final PipeConfigNodeTaskStage stage;

  public PipeConfigNodeTask(
      String pipeName, TConsensusGroupId regionId, PipeConfigNodeTaskStage configNodeStage) {
    super(pipeName, regionId, null, null, null);
    this.stage = configNodeStage;
  }

  @Override
  public void create() {
    stage.create();
  }

  @Override
  public void drop() {
    stage.drop();
  }

  @Override
  public void start() {
    stage.start();
  }

  @Override
  public void stop() {
    stage.stop();
  }
}
