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
import org.apache.iotdb.db.utils.windowing.configuration.Configuration;
import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.db.utils.windowing.runtime.WindowEvaluationTaskPoolManager;
import org.apache.iotdb.db.utils.windowing.window.EvictableBatchList;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class SlidingWindowEvaluationHandler {

  protected static final WindowEvaluationTaskPoolManager TASK_POOL_MANAGER =
      WindowEvaluationTaskPoolManager.getInstance();

  protected final Configuration configuration;
  protected final Evaluator evaluator;

  protected final EvictableBatchList data;

  private long maxTime;

  protected SlidingWindowEvaluationHandler(Configuration configuration, Evaluator evaluator)
      throws WindowingException {
    this.configuration = configuration;
    this.evaluator = evaluator;

    configuration.check();

    data = new EvictableBatchList(configuration.getDataType());

    maxTime = Long.MIN_VALUE;
  }

  protected abstract void createEvaluationTaskIfNecessary(long timestamp);

  public final void collect(long timestamp, int value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putInt(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, long value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putLong(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, float value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putFloat(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, double value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putDouble(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, boolean value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putBoolean(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, String value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putBinary(timestamp, Binary.valueOf(value));
    createEvaluationTaskIfNecessary(timestamp);
  }

  public final void collect(long timestamp, Binary value) {
    if (timestamp <= maxTime) {
      return;
    }
    maxTime = timestamp;

    data.putBinary(timestamp, value);
    createEvaluationTaskIfNecessary(timestamp);
  }
}
