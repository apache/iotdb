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
import org.apache.iotdb.db.utils.windowing.configuration.SlidingSizeWindowConfiguration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.runtime.WindowEvaluationTask;
import org.apache.iotdb.db.utils.windowing.window.WindowImpl;

public class SlidingSizeWindowEvaluationHandler extends SlidingWindowEvaluationHandler {

  private final int windowSize;
  private final int slidingStep;

  private int nextTriggerPoint;

  public SlidingSizeWindowEvaluationHandler(
      SlidingSizeWindowConfiguration configuration, Evaluator evaluator) throws WindowingException {
    super(configuration, evaluator);

    windowSize = configuration.getWindowSize();
    slidingStep = configuration.getSlidingStep();

    nextTriggerPoint = windowSize;
  }

  @Override
  protected void createEvaluationTaskIfNecessary(long timestamp) {
    if (data.size() != nextTriggerPoint) {
      return;
    }

    TASK_POOL_MANAGER.submit(
        new WindowEvaluationTask(
            evaluator, new WindowImpl(data, nextTriggerPoint - windowSize, windowSize)));
    data.setEvictionUpperBound(nextTriggerPoint - windowSize + 1);

    nextTriggerPoint += slidingStep;
  }
}
