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

package org.apache.iotdb.db.quotas;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

// TODO: Store quota information of each sg
public class DataNodeSpaceQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeSpaceQuotaManager.class);

  private Map<String, TSpaceQuota> spaceQuotaLimit;
  private Map<String, TSpaceQuota> useSpaceQuota;
  private Map<Integer, Integer> regionDisk;

  public DataNodeSpaceQuotaManager() {
    spaceQuotaLimit = new HashMap<>();
    useSpaceQuota = new HashMap<>();
    regionDisk = new HashMap<>();
  }

  /** SingleTone */
  private static class DataNodeSpaceQuotaManagerHolder {
    private static final DataNodeSpaceQuotaManager INSTANCE = new DataNodeSpaceQuotaManager();

    private DataNodeSpaceQuotaManagerHolder() {}
  }

  public static DataNodeSpaceQuotaManager getInstance() {
    return DataNodeSpaceQuotaManager.DataNodeSpaceQuotaManagerHolder.INSTANCE;
  }

  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    for (String storageGroup : req.getStorageGroup()) {
      spaceQuotaLimit.put(storageGroup, req.getSpaceLimit());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
