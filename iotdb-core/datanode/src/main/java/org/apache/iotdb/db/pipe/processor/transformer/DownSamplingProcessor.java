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

package org.apache.iotdb.db.pipe.processor.transformer;

import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY;

public class DownSamplingProcessor implements PipeProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DownSamplingProcessor.class);

  long intervalSeconds;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    // Do nothing
  }

  // TODO: Implement multiple down sampling methods, like "average"
  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    intervalSeconds =
        parameters.getLongOrDefault(
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_KEY,
            PROCESSOR_DOWN_SAMPLING_INTERVAL_SECONDS_DEFAULT_VALUE);
    LOGGER.info("DownSamplingProcessor intervalSeconds: {}", intervalSeconds);
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {}

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {}
}
