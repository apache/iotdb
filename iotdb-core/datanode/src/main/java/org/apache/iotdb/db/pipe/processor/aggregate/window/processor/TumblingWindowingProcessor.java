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
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_KEY;

@TreeModel
public class TumblingWindowingProcessor extends AbstractSimpleTimeWindowingProcessor {
  private long slidingBoundaryTime;
  private long slidingInterval;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (long) args > 0,
        String.format("The parameter %s must be greater than 0", PROCESSOR_SLIDING_SECONDS_KEY),
        parameters.getLongOrDefault(
            PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE));
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    slidingBoundaryTime =
        parameters.hasAnyAttributes(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY)
            ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                parameters.getString(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY))
            : PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
    slidingInterval =
        TimestampPrecisionUtils.convertToCurrPrecision(
            parameters.getLongOrDefault(
                PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE),
            TimeUnit.SECONDS);
  }

  @Override
  public Set<TimeSeriesWindow> mayAddWindow(
      final List<TimeSeriesWindow> windowList, final long timeStamp) {
    final long lastTime =
        windowList.isEmpty()
            ? slidingBoundaryTime
            : windowList.get(windowList.size() - 1).getTimestamp();

    if (timeStamp >= (windowList.isEmpty() ? lastTime : lastTime + slidingInterval)) {
      final TimeSeriesWindow window = new TimeSeriesWindow(this, null);
      // Align to the last time + k * slidingInterval, k is a natural number
      window.setTimestamp(((timeStamp - lastTime) / slidingInterval) * slidingInterval + lastTime);
      windowList.add(window);
      return Collections.singleton(window);
    }
    return Collections.emptySet();
  }

  @Override
  public Pair<WindowState, WindowOutput> updateAndMaySetWindowState(
      final TimeSeriesWindow window, final long timeStamp) {
    if (timeStamp < window.getTimestamp()) {
      return new Pair<>(WindowState.IGNORE_VALUE, null);
    }
    if (timeStamp >= window.getTimestamp() + slidingInterval) {
      return new Pair<>(
          WindowState.EMIT_AND_PURGE_WITHOUT_COMPUTE,
          new WindowOutput()
              .setTimestamp(window.getTimestamp())
              .setProgressTime(window.getTimestamp() + slidingInterval));
    }
    return new Pair<>(WindowState.COMPUTE, null);
  }

  @Override
  public WindowOutput forceOutput(final TimeSeriesWindow window) {
    return new WindowOutput()
        .setTimestamp(window.getTimestamp())
        .setProgressTime(window.getTimestamp() + slidingInterval);
  }
}
