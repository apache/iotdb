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

package org.apache.iotdb.collector.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.pipe.api.PipeSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CollectorSourceTask extends CollectorTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(CollectorSourceTask.class);

  private final Map<String, String> sourceAttribute;
  private final PipeSource pipeSource;

  public CollectorSourceTask(
      final String taskId, final Map<String, String> sourceAttribute, final PipeSource pipeSource) {
    super(taskId);
    this.sourceAttribute = sourceAttribute;
    this.pipeSource = pipeSource;
  }

  @Override
  public void runMayThrow() throws Throwable {
    pipeSource.start();
  }

  public Map<String, String> getSourceAttribute() {
    return sourceAttribute;
  }

  public PipeSource getPipeSource() {
    return pipeSource;
  }

  public EventSupplier getEventSupplier() {
    return pipeSource::supply;
  }

  @Override
  public void stop() {
    try {
      pipeSource.close();
    } catch (final Exception e) {
      LOGGER.warn("failed to close pipe source {}", pipeSource, e);
    }
  }
}
