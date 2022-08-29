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
package org.apache.iotdb.commons.partition.executor;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/** All SeriesPartitionExecutors must be subclasses of SeriesPartitionExecutor */
public abstract class SeriesPartitionExecutor {

  // The params, executorName and seriesPartitionSlotNum, are global unique during system running.
  // Therefore, one executor instance is enough for usage.
  protected static SeriesPartitionExecutor EXECUTOR;

  protected final int seriesPartitionSlotNum;

  public SeriesPartitionExecutor(int seriesPartitionSlotNum) {
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  public abstract TSeriesPartitionSlot getSeriesPartitionSlot(String device);

  public static SeriesPartitionExecutor getSeriesPartitionExecutor(
      String executorName, int seriesPartitionSlotNum) {
    if (EXECUTOR == null) {
      initStaticSeriesPartitionExecutor(executorName, seriesPartitionSlotNum);
    }
    return EXECUTOR;
  }

  private static synchronized void initStaticSeriesPartitionExecutor(
      String executorName, int seriesPartitionSlotNum) {
    if (EXECUTOR == null) {
      try {
        Class<?> executor = Class.forName(executorName);
        Constructor<?> executorConstructor = executor.getConstructor(int.class);
        EXECUTOR =
            (SeriesPartitionExecutor) executorConstructor.newInstance(seriesPartitionSlotNum);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalArgumentException(
            String.format("Couldn't Constructor SeriesPartitionExecutor class: %s", executorName));
      }
    }
  }
}
