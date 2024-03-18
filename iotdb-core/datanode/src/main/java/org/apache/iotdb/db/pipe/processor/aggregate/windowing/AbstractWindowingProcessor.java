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

package org.apache.iotdb.db.pipe.processor.aggregate.windowing;

import org.apache.iotdb.db.pipe.processor.aggregate.AggregateProcessor;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

/**
 * {@link AbstractWindowingProcessor} is the processor defining the windows adoptable for {@link
 * AggregateProcessor}. This is only a formal {@link PipeProcessor} which acts as a {@link
 * PipePlugin} for {@link AggregateProcessor}, and thereby has no need to implement the {@link
 * Event} collection methods.However, the implementors can still use {@link
 * PipeProcessor#validate(PipeParameterValidator)}, {@link PipeProcessor#customize(PipeParameters,
 * PipeProcessorRuntimeConfiguration)} and {@link PipeProcessor#close()} as a normal processor to
 * configure its own logics, which will be called in the corresponding functions in {@link
 * AggregateProcessor}.
 */
public abstract class AbstractWindowingProcessor implements PipeProcessor {
  @Override
  public final void process(
      TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector) throws Exception {
    throw new UnsupportedOperationException(
        "The abstract windowing processor does not support process events");
  }

  @Override
  public final void process(
      TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector) throws Exception {
    throw new UnsupportedOperationException(
        "The abstract windowing processor does not support process events");
  }

  @Override
  public final void process(Event event, EventCollector eventCollector) throws Exception {
    throw new UnsupportedOperationException(
        "The abstract windowing processor does not support process events");
  }

  @Override
  public void close() throws Exception {}
}
