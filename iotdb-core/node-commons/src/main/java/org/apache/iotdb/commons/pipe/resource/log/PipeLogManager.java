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

package org.apache.iotdb.commons.pipe.resource.log;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PipeLogManager {

  private final ConcurrentMap<Class<?>, PipeLogStatus> logClass2LogStatusMap =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<?>, ConcurrentMap<Object, PipeLogStatus>>
      logClass2Key2StatusMap = new ConcurrentHashMap<>();

  public Optional<Logger> schedule(
      final Class<?> logClass,
      final double maxAverageScale,
      final int maxLogInterval,
      final int scale) {
    return logClass2LogStatusMap
        .computeIfAbsent(
            logClass, k -> new PipeLogStatus(logClass, maxAverageScale, maxLogInterval))
        .schedule(scale);
  }

  public Optional<Logger> schedule(
      final Class<?> logClass,
      final Object key,
      final int maxAverageScale,
      final int maxLogInterval,
      final int scale) {
    return logClass2Key2StatusMap
        .computeIfAbsent(logClass, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(key, k -> new PipeLogStatus(logClass, maxAverageScale, maxLogInterval))
        .schedule(scale);
  }
}
