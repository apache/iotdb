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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.cache;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;

import java.util.Map;

public class DeviceSchemaRequestCache {
  private static final DeviceSchemaRequestCache INSTANCE = new DeviceSchemaRequestCache();

  private final Cache<FetchDevice, FetchMissingDeviceSchema> pendingRequests =
      Caffeine.newBuilder()
          .maximumSize(
              IoTDBDescriptor.getInstance().getConfig().getDeviceSchemaRequestCacheMaxSize())
          .build();

  private DeviceSchemaRequestCache() {}

  public static DeviceSchemaRequestCache getInstance() {
    return INSTANCE;
  }

  public FetchMissingDeviceSchema getOrCreatePendingRequest(final FetchDevice statement) {
    return pendingRequests.get(statement, k -> new FetchMissingDeviceSchema());
  }

  public void removeCompletedRequest(final FetchDevice statement) {
    pendingRequests.invalidate(statement);
  }

  public static class FetchMissingDeviceSchema {
    private volatile Map<IDeviceID, Map<String, Binary>> result;
    private volatile boolean done = false;
    private volatile long queryId = -1;

    public Map<IDeviceID, Map<String, Binary>> getResult() {
      return result;
    }

    public void setResult(final Map<IDeviceID, Map<String, Binary>> result) {
      this.result = result;
    }

    public synchronized void waitForQuery(final long queryId) {
      if (this.queryId != -1) {
        if (!done) {
          try {
            this.wait(
                IoTDBDescriptor.getInstance().getConfig().getDeviceSchemaRequestCacheWaitTimeMs());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } else {
        this.queryId = queryId;
      }
    }

    public synchronized void notifyFetchDone() {
      done = true;
      this.notifyAll();
    }
  }
}
