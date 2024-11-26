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

package org.apache.iotdb.commons.utils.BinaryAllocator;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.time.Duration;

public class AllocatorConfig {
  public int minAllocateSize = CommonDescriptor.getInstance().getConfig().getMinAllocateSize();

  public int maxAllocateSize = CommonDescriptor.getInstance().getConfig().getMaxAllocateSize();

  public int arenaNum = CommonDescriptor.getInstance().getConfig().getArenaNum();

  public int log2ClassSizeGroup =
      CommonDescriptor.getInstance().getConfig().getLog2SizeClassGroup();

  public boolean enableBinaryAllocator =
      CommonDescriptor.getInstance().getConfig().isEnableBinaryAllocator();

  /** The default value for {@code evictorShutdownTimeout} configuration attribute. */
  private long evictorShutdownTimeoutMillis = 10L * 1000L;

  /** The default value for {@code evictorShutdownTimeout} configuration attribute. */
  private Duration durationEvictorShutdownTimeout = Duration.ofMillis(evictorShutdownTimeoutMillis);

  /** The default value for the {@code timeBetweenEvictionRuns} configuration attribute. */
  private long timeBetweenEvictorRunsMillis = 10000L;

  /** The default value for the {@code timeBetweenEvictionRuns} configuration attribute. */
  private Duration durationBetweenEvictorRuns = Duration.ofMillis(timeBetweenEvictorRunsMillis);

  public int arenaPredictionWeight = 35;

  public static final AllocatorConfig DEFAULT_CONFIG = new AllocatorConfig();

  public void setEvictionShutdownTimeoutMillis(long timeout) {
    this.evictorShutdownTimeoutMillis = timeout;
    this.durationEvictorShutdownTimeout = Duration.ofMillis(timeout);
  }

  public void setTimeBetweenEvictorRunsMillis(long time) {
    this.timeBetweenEvictorRunsMillis = time;
    this.durationBetweenEvictorRuns = Duration.ofMillis(time);
  }

  public Duration getDurationEvictorShutdownTimeout() {
    return durationEvictorShutdownTimeout;
  }

  public Duration getDurationBetweenEvictorRuns() {
    return durationBetweenEvictorRuns;
  }
}
