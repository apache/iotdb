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

package org.apache.iotdb.collector.plugin.builtin.source;

import org.apache.iotdb.collector.plugin.builtin.source.event.SourceEvent;
import org.apache.iotdb.pipe.api.PipeSource;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeSourceRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HttpSource implements PipeSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpSource.class);

  private static final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();
  private boolean isStarted = true;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void customize(PipeParameters parameters, PipeSourceRuntimeConfiguration configuration)
      throws Exception {}

  @Override
  public void start() {
    isStarted = true;
    while (isStarted) {
      Event event = new SourceEvent(String.valueOf(new Random().nextInt(1000)));
      try {
        queue.put(event);
        Thread.sleep(1000);
        LOGGER.info("event: {} created success", event);
      } catch (final InterruptedException e) {
        LOGGER.warn("failed to create event because {}", e.getMessage());
      }
    }
  }

  @Override
  public Event supply() throws Exception {
    return queue.take();
  }

  @Override
  public void close() throws Exception {
    isStarted = false;
  }
}
