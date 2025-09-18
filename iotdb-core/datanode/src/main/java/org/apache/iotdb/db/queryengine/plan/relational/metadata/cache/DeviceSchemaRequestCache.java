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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DeviceSchemaRequestCache {
  private static final DeviceSchemaRequestCache INSTANCE = new DeviceSchemaRequestCache();

  private final Map<FetchDevice, FetchMissingDeviceSchema> pendingRequests =
      new ConcurrentHashMap<>();
  private final AtomicLong requestCounter = new AtomicLong(0);

  private static final int MAX_PENDING_REQUESTS =
      IoTDBDescriptor.getInstance().getConfig().getDeviceSchemaRequestCacheMaxSize();

  private DeviceSchemaRequestCache() {}

  public static DeviceSchemaRequestCache getInstance() {
    return INSTANCE;
  }

  public FetchMissingDeviceSchema getOrCreatePendingRequest(FetchDevice statement) {
    if (pendingRequests.size() >= MAX_PENDING_REQUESTS) {
      clearOldestRequests();
    }

    return pendingRequests.computeIfAbsent(
        statement,
        k -> {
          requestCounter.incrementAndGet();
          return new FetchMissingDeviceSchema();
        });
  }

  public void removeCompletedRequest(FetchDevice statement) {
    pendingRequests.remove(statement);
  }

  private void clearOldestRequests() {
    int toRemove = pendingRequests.size() - MAX_PENDING_REQUESTS + 50;
    if (toRemove <= 0) return;

    int removed = 0;
    for (Map.Entry<FetchDevice, FetchMissingDeviceSchema> entry : pendingRequests.entrySet()) {
      if (removed >= toRemove) break;
      pendingRequests.remove(entry.getKey());
      removed++;
    }
  }

  public static class FetchMissingDeviceSchema {
    private volatile Map<IDeviceID, Map<String, Binary>> result;
    private volatile boolean done = false;
    private volatile long queryId = -1;

    public Map<IDeviceID, Map<String, Binary>> getResult() {
      return result;
    }

    public void setResult(Map<IDeviceID, Map<String, Binary>> result) {
      this.result = result;
    }

    public synchronized void waitForQuery(long queryId) {
      if (this.queryId != -1) {
        for (int i = 0; i < 20 && !done; i++) {
          try {
            this.wait(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
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
