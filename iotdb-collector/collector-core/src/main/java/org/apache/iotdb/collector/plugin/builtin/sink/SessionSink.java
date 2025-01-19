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

package org.apache.iotdb.collector.plugin.builtin.sink;

import org.apache.iotdb.collector.plugin.builtin.source.event.SourceEvent;
import org.apache.iotdb.pipe.api.PipeSink;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeSinkRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionSink implements PipeSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionSink.class);

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeSinkRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void handshake() throws Exception {
    LOGGER.info("SessionSink handshake successfully");
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {}

  @Override
  public void transfer(Event event) throws Exception {
    final SourceEvent sourceEvent = (SourceEvent) event;
    LOGGER.info("SessionSink transfer successfully {}", sourceEvent);
  }

  @Override
  public void close() throws Exception {}
}
