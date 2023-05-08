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

package org.apache.iotdb.db.pipe.task.stage;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskCollectorStage implements PipeTaskStage {

  private final PipeParameters collectorParameters;

  private PipeCollector pipeCollector;

  PipeTaskCollectorStage(PipeParameters collectorParameters) {
    this.collectorParameters = collectorParameters;
  }

  @Override
  public void create() throws PipeException {
    this.pipeCollector = PipeAgent.plugin().reflectCollector(collectorParameters);
  }

  @Override
  public void start() throws PipeException {
    try {
      pipeCollector.start();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void stop() throws PipeException {
    // collector continuously collects data, so do nothing in stop
  }

  @Override
  public void drop() throws PipeException {
    try {
      pipeCollector.close();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public PipeSubtask getSubtask() {
    throw new UnsupportedOperationException("Collector stage does not have subtask.");
  }
}
