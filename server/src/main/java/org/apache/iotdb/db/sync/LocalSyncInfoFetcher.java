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
package org.apache.iotdb.db.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.receiver.AbstractSyncInfo;
import org.apache.iotdb.db.sync.receiver.manager.LocalSyncInfo;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LocalSyncInfoFetcher implements ISyncInfoFetcher {

  private static final Logger logger = LoggerFactory.getLogger(LocalSyncInfoFetcher.class);
  private AbstractSyncInfo syncInfo = new LocalSyncInfo();

  private LocalSyncInfoFetcher() {
    syncInfo = new LocalSyncInfo();
  }

  @Override
  public TSStatus addPipeSink(CreatePipeSinkPlan plan) {
    return null;
  }

  @Override
  public TSStatus dropPipeSink(String name) {
    return null;
  }

  @Override
  public PipeSink getPipeSink(String name) {
    return null;
  }

  @Override
  public List<PipeSink> getAllPipeSink() {
    return null;
  }

  // region singleton
  private static class LocalSyncInfoFetcherHolder {
    private static final LocalSyncInfoFetcher INSTANCE = new LocalSyncInfoFetcher();

    private LocalSyncInfoFetcherHolder() {}
  }

  public static LocalSyncInfoFetcher getInstance() {
    return LocalSyncInfoFetcher.LocalSyncInfoFetcherHolder.INSTANCE;
  }
  // endregion

}
