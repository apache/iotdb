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

package org.apache.iotdb.db.utils.windowing.handler;

import org.apache.iotdb.db.utils.windowing.api.Evaluator;
import org.apache.iotdb.db.utils.windowing.configuration.SlidingTimeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.runtime.WindowEvaluationTask;
import org.apache.iotdb.db.utils.windowing.window.WindowImpl;

import java.util.LinkedList;
import java.util.Queue;

public class SlidingTimeWindowEvaluationHandler extends SlidingWindowEvaluationHandler {

  private final long timeInterval;
  private final long slidingStep;

  private final Queue<Integer> windowBeginIndexQueue;

  /** window: [begin, end) */
  private long currentWindowEndTime;

  /** window: [begin, end) */
  private long nextWindowBeginTime;

  public SlidingTimeWindowEvaluationHandler(
      SlidingTimeWindowConfiguration configuration, Evaluator evaluator) throws WindowingException {
    super(configuration, evaluator);

    timeInterval = configuration.getTimeInterval();
    slidingStep = configuration.getSlidingStep();

    windowBeginIndexQueue = new LinkedList<>();
  }

  @Override
  protected void createEvaluationTaskIfNecessary(long timestamp) {
    if (data.size() == 1) {
      windowBeginIndexQueue.add(0);
      currentWindowEndTime = timestamp + timeInterval;
      nextWindowBeginTime = timestamp + slidingStep;
      return;
    }

    while (nextWindowBeginTime <= timestamp) {
      windowBeginIndexQueue.add(data.size() - 1);
      nextWindowBeginTime += slidingStep;
    }

    while (currentWindowEndTime <= timestamp) {
      int windowBeginIndex = windowBeginIndexQueue.remove();
      TASK_POOL_MANAGER.submit(
          new WindowEvaluationTask(
              evaluator,
              new WindowImpl(data, windowBeginIndex, data.size() - 1 - windowBeginIndex)));
      data.setEvictionUpperBound(windowBeginIndex);
      currentWindowEndTime += slidingStep;
    }
  }
}
