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
package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheHitRateMonitor implements CacheHitRateMonitorMXBean, IService {

  double chunkMetaDataHitRate;
  double tsfileMetaDataHitRate;

  private static Logger logger = LoggerFactory.getLogger(CacheHitRateMonitor.class);
  static final CacheHitRateMonitor instance = AsyncCacheHitRateHolder.DISPLAYER;
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(instance, mbeanName);
    } catch (Exception e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      throw new StartupException(errorMessage, e);
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
    logger.info("{}: stop {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CACHE_HIT_RATE_DISPLAY_SERVICE;
  }

  @Override
  public double getChunkMetaDataHitRate() {
    chunkMetaDataHitRate = DeviceMetaDataCache.getInstance().calculateChunkMetaDataHitRate();
    return chunkMetaDataHitRate;
  }

  @Override
  public double getTsfileMetaDataHitRate() {
    tsfileMetaDataHitRate = TsFileMetaDataCache.getInstance().calculateTsfileMetaDataHitRate();
    return tsfileMetaDataHitRate;
  }

  public static CacheHitRateMonitor getInstance() {
    return instance;
  }

  private static class AsyncCacheHitRateHolder {

    private static final CacheHitRateMonitor DISPLAYER = new CacheHitRateMonitor();

    private AsyncCacheHitRateHolder() {
    }
  }
}
