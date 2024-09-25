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

package org.apache.iotdb.commons.pipe.agent.plugin.builtin.connector.donothing;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

public class DoNothingConnector implements PipeConnector {

  @Override
  public void validate(PipeParameterValidator validator) {
    // Do nothing
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration) {
    // Do nothing
  }

  @Override
  public void handshake() {
    // Do nothing
  }

  @Override
  public void heartbeat() {
    // Do nothing
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) {
    // Do nothing
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) {
    // Do nothing
  }

  @Override
  public void transfer(Event event) {
    // Do nothing
  }

  @Override
  public void close() {
    // Do nothing
  }
}
