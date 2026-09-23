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

import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.TimeSeriesWindow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowState;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_KEY;

public class TumblingWindowingProcessorTest {

  @Test
  public void testMayAddWindowAlignsExtremeRangeWithoutOverflow() throws Exception {
    final TumblingWindowingProcessor processor = createProcessor(Long.MIN_VALUE, 1);
    final long interval = TimestampPrecisionUtils.convertToCurrPrecision(1, TimeUnit.SECONDS);
    final List<TimeSeriesWindow> windows = new ArrayList<>();

    Assert.assertEquals(1, processor.mayAddWindow(windows, Long.MAX_VALUE).size());
    Assert.assertEquals(1, windows.size());
    Assert.assertEquals(
        alignWindowStart(Long.MAX_VALUE, Long.MIN_VALUE, interval), windows.get(0).getTimestamp());
    Assert.assertTrue(windows.get(0).getTimestamp() > Long.MIN_VALUE);
  }

  @Test
  public void testWindowEndOverflowDoesNotEmitEarly() throws Exception {
    final TumblingWindowingProcessor processor = createProcessor(0, 1);
    final long interval = TimestampPrecisionUtils.convertToCurrPrecision(1, TimeUnit.SECONDS);
    final TimeSeriesWindow window = new TimeSeriesWindow(processor, null);
    window.setTimestamp(Long.MAX_VALUE - interval + 1);

    final Pair<WindowState, WindowOutput> result =
        processor.updateAndMaySetWindowState(window, Long.MAX_VALUE);

    Assert.assertEquals(WindowState.COMPUTE, result.getLeft());
    Assert.assertNull(result.getRight());
    Assert.assertEquals(Long.MAX_VALUE, processor.forceOutput(window).getProgressTime());

    final List<TimeSeriesWindow> windows = new ArrayList<>();
    windows.add(window);
    Assert.assertTrue(processor.mayAddWindow(windows, Long.MAX_VALUE).isEmpty());
  }

  private static TumblingWindowingProcessor createProcessor(
      final long slidingBoundaryTime, final long slidingSeconds) throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY, Long.toString(slidingBoundaryTime));
    attributes.put(PROCESSOR_SLIDING_SECONDS_KEY, Long.toString(slidingSeconds));
    final PipeParameters parameters = new PipeParameters(attributes);
    final TumblingWindowingProcessor processor = new TumblingWindowingProcessor();
    processor.validate(new PipeParameterValidator(parameters));
    processor.customize(parameters, () -> null);
    return processor;
  }

  private static long alignWindowStart(
      final long timestamp, final long baseTime, final long interval) {
    final BigInteger base = BigInteger.valueOf(baseTime);
    final BigInteger intervalValue = BigInteger.valueOf(interval);
    return base.add(
            BigInteger.valueOf(timestamp)
                .subtract(base)
                .divide(intervalValue)
                .multiply(intervalValue))
        .longValueExact();
  }
}
