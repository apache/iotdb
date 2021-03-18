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

import org.apache.iotdb.db.utils.windowing.runtime.WindowEvaluationTaskPoolManager;
import org.apache.iotdb.db.utils.windowing.configuration.WindowConfiguration;
import org.apache.iotdb.db.utils.windowing.api.Evaluator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class SlidingWindowEvaluationHandler {

  private static final WindowEvaluationTaskPoolManager TASK_POOL_MANAGER =
      WindowEvaluationTaskPoolManager.getInstance();

  private final TSDataType dataType;
  private final WindowConfiguration configuration;
  private final Evaluator evaluator;

  protected SlidingWindowEvaluationHandler(
      TSDataType dataType, WindowConfiguration configuration, Evaluator evaluator) {
    this.dataType = dataType;
    this.configuration = configuration;
    this.evaluator = evaluator;
  }

  public void add(long timestamp, int value) {}

  public void add(long timestamp, long value) {}

  public void add(long timestamp, float value) {}

  public void add(long timestamp, double value) {}

  public void add(long timestamp, boolean value) {}

  public void add(long timestamp, String value) {}

  public void add(long timestamp, Binary value) {}
}
