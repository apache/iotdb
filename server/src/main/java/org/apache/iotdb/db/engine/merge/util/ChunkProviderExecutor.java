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

package org.apache.iotdb.db.engine.merge.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

public class ChunkProviderExecutor implements IService {

  private static ChunkProviderExecutor INSTANCE = new ChunkProviderExecutor();

  private ExecutorService providerThreadPool =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  private ChunkProviderExecutor() {
  }

  public static ChunkProviderExecutor getINSTANCE() {
    return INSTANCE;
  }

  void submit(Callable providerTask) {
    providerThreadPool.submit(providerTask);
  }

  public void close() {
    providerThreadPool.shutdownNow();
    while (!providerThreadPool.isTerminated()) {
      // wait
    }
  }

  @Override
  public void start() throws StartupException {

  }

  @Override
  public void stop() {
    close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.CHUNK_PROVIDER_SERVICE;
  }
}