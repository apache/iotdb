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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;

import java.util.Map;

public class TimeFilterForDeviceTTL {

  private final Filter timeFilter;

  private Map<IDeviceID, Long> ttlCached;

  public TimeFilterForDeviceTTL(Filter timeFilter) {
    this.timeFilter = timeFilter;
  }

  public boolean satisfyStartEndTime(long startTime, long endTime, IDeviceID deviceID) {
    long ttl = getTTL(deviceID);
    if (ttl != Long.MAX_VALUE) {
      long validStartTime = CommonDateTimeUtils.currentTime() - ttl;
      if (validStartTime > endTime) {
        return false;
      }
      return timeFilter.satisfyStartEndTime(validStartTime, endTime);
    }
    return timeFilter.satisfyStartEndTime(startTime, endTime);
  }

  public boolean satisfy(long time, IDeviceID deviceID) {
    long ttl = getTTL(deviceID);
    if (ttl != Long.MAX_VALUE) {
      long validStartTime = CommonDateTimeUtils.currentTime() - ttl;
      if (validStartTime > time) {
        return false;
      }
      return timeFilter.satisfy(validStartTime, null);
    }
    return timeFilter.satisfy(time, null);
  }

  private long getTTL(IDeviceID deviceID) {
    if (ttlCached.containsKey(deviceID)) {
      return ttlCached.get(deviceID);
    }
    long ttl = DataNodeTTLCache.getInstance().getTTL(deviceID);
    ttlCached.put(deviceID, ttl);
    return ttl;
  }
}
