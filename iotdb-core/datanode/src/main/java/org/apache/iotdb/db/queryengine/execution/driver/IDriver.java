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

package org.apache.iotdb.db.queryengine.execution.driver;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

/**
 * {@link IDriver} encapsulates some methods which are necessary for FragmentInstanceTaskExecutor to
 * run a fragment instance.
 */
public interface IDriver {

  /**
   * Used to judge whether this {@link IDriver} should be scheduled for execution anymore.
   *
   * @return true if the {@link IDriver} is done or terminated due to failure, otherwise false.
   */
  boolean isFinished();

  /**
   * run the {@link IDriver} for {@param duration} time slice, the time of this run is likely not to
   * be equal to {@param duration}, the actual run time should be calculated by the caller.
   *
   * @param duration how long should this {@link IDriver} run
   * @return the returned ListenableFuture is used to represent status of this processing if
   *     isDone() return true, meaning that this {@link IDriver} is not blocked and is ready for
   *     next processing. Otherwise, meaning that this {@link IDriver} is blocked and not ready for
   *     next processing.
   */
  @SuppressWarnings("squid:S1452")
  ListenableFuture<?> processFor(Duration duration);

  /**
   * the id information about this {@link IDriver}.
   *
   * @return a {@link FragmentInstanceId} instance.
   */
  DriverTaskId getDriverTaskId();

  default long getEstimatedMemorySize() {
    return 0;
  }

  void setDriverTaskId(DriverTaskId driverTaskId);

  /** Clear resource used by this fragment instance. */
  void close();

  /**
   * Fail current {@link IDriver}.
   *
   * @param t reason cause this failure
   */
  void failed(Throwable t);

  /** return get Sink of current {@link IDriver}. */
  ISink getSink();

  DriverContext getDriverContext();
}
