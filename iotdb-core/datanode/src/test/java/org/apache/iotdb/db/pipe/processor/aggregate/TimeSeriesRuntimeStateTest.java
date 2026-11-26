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

package org.apache.iotdb.db.pipe.processor.aggregate;

import org.apache.iotdb.db.pipe.event.common.row.PipeRow;
import org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure.WindowOutput;
import org.apache.iotdb.pipe.api.access.Row;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimeSeriesRuntimeStateTest {
  @Test
  public void testRowUpdaterFollowsRecreatedSeriesType() throws Exception {
    List<Number> received = new ArrayList<>();
    // Record typed dispatch while retaining the real PipeRow primitive-array getters.
    TimeSeriesRuntimeState state =
        new TimeSeriesRuntimeState(null, null, null, null) {
          @Override
          public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
              long time, int value, long interval) {
            received.add(value);
            return null;
          }

          @Override
          public Pair<List<WindowOutput>, Pair<Long, ByteBuffer>> updateWindows(
              long time, double value, long interval) {
            received.add(value);
            return null;
          }
        };
    state.updateWindows(1L, row(TSDataType.INT32, new int[] {42}), 0, 0L);
    state.updateWindows(2L, row(TSDataType.DOUBLE, new double[] {2.5}), 0, 0L);
    state.updateWindows(3L, row(TSDataType.INT32, new int[] {43}), 0, 0L);
    assertEquals(Arrays.asList(42, 2.5, 43), received);
  }

  private Row row(TSDataType type, Object values) {
    return new PipeRow(
        0,
        "root.sg.d",
        false,
        null,
        new long[] {1},
        new TSDataType[] {type},
        new Object[] {values},
        null,
        new String[] {"s"});
  }
}
