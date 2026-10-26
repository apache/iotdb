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

package org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.throwing;

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@TreeModel
@TableModel
public class ThrowingExceptionProcessor implements PipeProcessor {

  private boolean throwInCustomize = false;
  private boolean throwInProcessTabletInsertionEvent = false;
  private boolean throwInProcessTsFileInsertionEvent = false;
  private boolean throwInProcessEvent = false;
  private boolean throwInClose = false;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final Set<String> throwingStages =
        Arrays.stream(
                validator.getParameters().getStringOrDefault("stages", "").toLowerCase().split(","))
            .collect(Collectors.toSet());

    final boolean throwInValidate = throwingStages.contains("validate");
    if (throwInValidate) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_VALIDATE);
    }

    throwInCustomize = throwingStages.contains("customize");
    throwInProcessTabletInsertionEvent = throwingStages.contains("process-tablet-insertion-event");
    throwInProcessTsFileInsertionEvent = throwingStages.contains("process-tsfile-insertion-event");
    throwInProcessEvent = throwingStages.contains("process-event");
    throwInClose = throwingStages.contains("close");
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    if (throwInCustomize) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_CUSTOMIZE);
    }
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (throwInProcessTabletInsertionEvent) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_PROCESS_TABLET);
    }
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (throwInProcessTsFileInsertionEvent) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_PROCESS_TSFILE);
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (throwInProcessEvent) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_PROCESS_EVENT);
    }
  }

  @Override
  public void close() throws Exception {
    if (throwInClose) {
      throw new Exception(PipeMessages.THROWING_EXCEPTION_IN_CLOSE);
    }
  }
}
