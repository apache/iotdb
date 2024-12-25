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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

/** This cache is for reducing duplicated DeviceId PartialPath initialization in write process. */
public class DataNodeDevicePathCache {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<String, PartialPath> devicePathCache;

  private DataNodeDevicePathCache() {
    devicePathCache =
        Caffeine.newBuilder()
            .maximumWeight(
                (long)
                    (config.getAllocateMemoryForStorageEngine()
                        * config.getWriteProportionForMemtable()
                        * config.getDevicePathCacheProportion()))
            .weigher(
                (Weigher<String, PartialPath>) (key, val) -> (PartialPath.estimateSize(val) + 32))
            .build();
  }

  public static DataNodeDevicePathCache getInstance() {
    return DataNodeDevicePathCache.DataNodeDevicePathCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeDevicePathCacheHolder {
    private static final DataNodeDevicePathCache INSTANCE = new DataNodeDevicePathCache();
  }

  public PartialPath getPartialPath(final String deviceId) throws IllegalPathException {
    try {
      return devicePathCache.get(
          deviceId,
          path -> {
            try {
              return new PartialPath(path);
            } catch (final IllegalPathException e) {
              try {
                return PartialPath.getDatabasePath(path);
              } catch (final IllegalPathException e1) {
                throw new IllegalArgumentException(e1);
              }
            }
          });
    } catch (IllegalArgumentException e) {
      throw new IllegalPathException(deviceId);
    }
  }

  public String getDeviceId(final String deviceId) {
    try {
      return getPartialPath(deviceId).getFullPath();
    } catch (IllegalPathException e) {
      return deviceId;
    }
  }

  public void cleanUp() {
    devicePathCache.cleanUp();
  }
}
