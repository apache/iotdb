/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.jvm;

import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.function.ToLongFunction;

public class JvmUtils {
  static double getUsageValue(
      MemoryPoolMXBean memoryPoolMxBean, ToLongFunction<MemoryUsage> getter) {
    MemoryUsage usage = getUsage(memoryPoolMxBean);
    if (usage == null) {
      return Double.NaN;
    }
    return getter.applyAsLong(usage);
  }

  private static MemoryUsage getUsage(MemoryPoolMXBean memoryPoolMxBean) {
    try {
      return memoryPoolMxBean.getUsage();
    } catch (InternalError e) {
      // Defensive for potential InternalError with some specific JVM options. Based on its Javadoc,
      // MemoryPoolMXBean.getUsage() should return null, not throwing InternalError, so it seems to
      // be a JVM bug.
      return null;
    }
  }
}
