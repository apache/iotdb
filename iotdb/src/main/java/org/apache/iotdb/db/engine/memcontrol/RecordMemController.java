/**
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
package org.apache.iotdb.db.engine.memcontrol;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class hold global memory usage of MemUsers. This only counts record(tuple) sizes.
 */
public class RecordMemController extends BasicMemController {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordMemController.class);

  // the key is the reference of the memory user, while the value is its memory usage in byte
  private Map<Object, AtomicLong> memMap;
  private AtomicLong totalMemUsed;

  private RecordMemController(IoTDBConfig config) {
    super(config);
    memMap = new ConcurrentHashMap<>();
    totalMemUsed = new AtomicLong(0);
  }

  public static RecordMemController getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public long getTotalUsage() {
    return totalMemUsed.get();
  }

  @Override
  public void clear() {
    memMap.clear();
    totalMemUsed.set(0);
  }

  /**
   * get the current memory usage level.
   */
  @Override
  public UsageLevel getCurrLevel() {
    long memUsage = totalMemUsed.get();
    if (memUsage < warningThreshold) {
      return UsageLevel.SAFE;
    } else if (memUsage < dangerouseThreshold) {
      return UsageLevel.WARNING;
    } else {
      return UsageLevel.DANGEROUS;
    }
  }

  /**
   * report the increased memory usage of the object user.
   */
  @Override
  public UsageLevel acquireUsage(Object user, long usage) {
    AtomicLong userUsage = memMap.computeIfAbsent(user, k -> new AtomicLong(0));
    long oldUsage = userUsage.get();

    long newTotUsage = totalMemUsed.addAndGet(usage);
    userUsage.addAndGet(usage);
    if (newTotUsage < warningThreshold) {
      logSafe(newTotUsage, user, usage, oldUsage);
      return UsageLevel.SAFE;
    } else if (newTotUsage < dangerouseThreshold) {
      logWarn(newTotUsage, user, usage, oldUsage);
      return UsageLevel.WARNING;
    } else {
      logDangerous(newTotUsage, user, usage, oldUsage);
      return UsageLevel.DANGEROUS;
    }
  }

  private void logSafe(long newTotUsage, Object user, long usage, long oldUsage) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Safe Threshold : {} allocated to {}, it is using {}, total usage {}",
          MemUtils.bytesCntToStr(usage), user,
          MemUtils.bytesCntToStr(oldUsage + usage),
          MemUtils.bytesCntToStr(newTotUsage));
    }
  }

  private void logWarn(long newTotUsage, Object user, long usage, long oldUsage) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Warning Threshold : {} allocated to {}, it is using {}, total usage {}",
          MemUtils.bytesCntToStr(usage), user,
          MemUtils.bytesCntToStr(oldUsage + usage),
          MemUtils.bytesCntToStr(newTotUsage));
    }
  }

  private void logDangerous(long newTotUsage, Object user, long usage, long oldUsage) {
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn("Warning Threshold : {} allocated to {}, it is using {}, total usage {}",
          MemUtils.bytesCntToStr(usage), user,
          MemUtils.bytesCntToStr(oldUsage + usage),
          MemUtils.bytesCntToStr(newTotUsage));
    }
  }

  /**
   * report the decreased memory usage of the object user.
   */
  @Override
  public void releaseUsage(Object user, long freeSize) {
    AtomicLong usage = memMap.get(user);
    if (usage == null) {
      LOGGER.error("Unregistered memory usage from {}", user);
      return;
    }
    long usageLong = usage.get();
    if (freeSize > usageLong) {
      LOGGER
          .error("{} requests to free {} bytes while it only registered {} bytes", user,
              freeSize, usage);
      totalMemUsed.addAndGet(-usageLong);
      usage.set(0);
    } else {
      long newTotalMemUsage = totalMemUsed.addAndGet(-freeSize);
      usage.addAndGet(-freeSize);
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("{} freed from {}, it is using {}, total usage {}",
            MemUtils.bytesCntToStr(freeSize),
            user, MemUtils.bytesCntToStr(usage.get()),
            MemUtils.bytesCntToStr(newTotalMemUsage));
      }
    }
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final RecordMemController INSTANCE = new RecordMemController(
        IoTDBDescriptor.getInstance().getConfig());
  }
}
