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

package org.apache.iotdb.db.pipe.processor.aggregate.window.processor;

import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_COUNT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_COUNT_KEY;

@TreeModel
public class CountWindowingProcessor extends AbstractSimpleTimeWindowingProcessor {

  private long count;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (long) args > 0,
        String.format("The parameter %s must be greater than 0", PROCESSOR_COUNT_KEY),
        parameters.getLongOrDefault(PROCESSOR_COUNT_KEY, PROCESSOR_COUNT_DEFAULT_VALUE));
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    count = parameters.getLongOrDefault(PROCESSOR_COUNT_KEY, PROCESSOR_COUNT_DEFAULT_VALUE);
  }

  @Override
  public Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp) {
    final TimeSeriesWindow result;
    if (windowList.isEmpty()) {
      result = new TimeSeriesWindow(this, 0L);
      result.setTimestamp(timeStamp);
      windowList.add(result);
      return Collections.singleton(result);
    }
    return Collections.emptySet();
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp) {
    if (timeStamp > window.getTimestamp()) {
      window.setTimestamp(timeStamp);
    }
    if ((long) window.getCustomizedRuntimeValue() >= count - 1) {
      return new Pair<>(
          WindowState.EMIT_AND_PURGE_WITH_COMPUTE,
          new WindowOutput()
              .setTimestamp(window.getTimestamp())
              .setProgressTime(window.getTimestamp()));
    }
    window.setCustomizedRuntimeValue((long) window.getCustomizedRuntimeValue() + 1);
    return new Pair<>(WindowState.COMPUTE, null);
  }

  @Override
  public WindowOutput forceOutput(final TimeSeriesWindow window) {
    return new WindowOutput()
        .setTimestamp(window.getTimestamp())
        .setProgressTime(window.getTimestamp());
  }
}
