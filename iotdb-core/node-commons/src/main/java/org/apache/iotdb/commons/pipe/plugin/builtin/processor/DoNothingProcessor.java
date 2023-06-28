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

package org.apache.iotdb.commons.pipe.plugin.builtin.processor;

import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.io.IOException;

/** This class is a placeholder and should not be used. */
public class DoNothingProcessor implements PipeProcessor {
  private static final String PLACEHOLDER_ERROR_MSG =
      "This class is a placeholder and should not be used.";

  @Override
  public void validate(PipeParameterValidator validator) {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws IOException {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws IOException {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws IOException {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(PLACEHOLDER_ERROR_MSG);
  }
}
