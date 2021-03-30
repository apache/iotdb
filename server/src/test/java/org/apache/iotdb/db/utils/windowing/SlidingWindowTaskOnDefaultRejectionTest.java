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

package org.apache.iotdb.db.utils.windowing;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingTimeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.handler.SlidingTimeWindowEvaluationHandler;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Test;

import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SlidingWindowTaskOnDefaultRejectionTest {

  @Test
  public void testOnDefaultRejection() throws WindowingException {
    @SuppressWarnings("squid:S2925")
    SlidingTimeWindowEvaluationHandler handler =
        new SlidingTimeWindowEvaluationHandler(
            new SlidingTimeWindowConfiguration(TSDataType.INT32, 1, 1),
            window -> {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException ignored) {
                // ignored
              }
            });

    final int evaluationTasks =
        IoTDBDescriptor.getInstance().getConfig().getMaxPendingWindowEvaluationTasks()
            + IoTDBDescriptor.getInstance().getConfig().getConcurrentWindowEvaluationThread()
            + 1
            + 1;
    try {
      for (int i = 0; i < evaluationTasks; ++i) {
        handler.collect(i, i);
      }
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof RejectedExecutionException);
    }
  }
}
