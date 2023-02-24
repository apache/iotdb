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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISink;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

/**
 * IDriver encapsulates some methods which are necessary for FragmentInstanceTaskExecutor to run a
 * fragment instance
 */
public interface IDriver {

  /**
   * Used to judge whether this IDriver should be scheduled for execution anymore
   *
   * @return true if the IDriver is done or terminated due to failure, otherwise false.
   */
  boolean isFinished();

  /**
   * run the IDriver for {@param duration} time slice, the time of this run is likely not to be
   * equal to {@param duration}, the actual run time should be calculated by the caller
   *
   * @param duration how long should this IDriver run
   * @return the returned ListenableFuture<Void> is used to represent status of this processing if
   *     isDone() return true, meaning that this IDriver is not blocked and is ready for next
   *     processing. Otherwise, meaning that this IDriver is blocked and not ready for next
   *     processing.
   */
  ListenableFuture<?> processFor(Duration duration);

  /**
   * the id information about this IDriver.
   *
   * @return a {@link FragmentInstanceId} instance.
   */
  DriverTaskId getDriverTaskId();

  void setDriverTaskId(DriverTaskId driverTaskId);

  /** clear resource used by this fragment instance */
  void close();

  /**
   * fail current driver
   *
   * @param t reason cause this failure
   */
  void failed(Throwable t);

  /** @return get Sink of current IDriver */
  ISink getSink();

  int getDependencyDriverIndex();
}
