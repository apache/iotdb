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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

// TODO: Store quota information of each sg
public class QuotaInfo {

  private static final Logger logger = LoggerFactory.getLogger(QuotaInfo.class);
  private final Map<String, TSpaceQuota> spaceQuotaLimit;
  private final Map<String, TSpaceQuota> useSpaceQuota;
  private final Map<Integer, Integer> regionDisk;

  public QuotaInfo() {
    spaceQuotaLimit = new HashMap<>();
    useSpaceQuota = new HashMap<>();
    regionDisk = new HashMap<>();
  }

  public TSStatus setSpaceQuota(SetSpaceQuotaPlan setSpaceQuotaPlan) {
    for (String storageGroup : setSpaceQuotaPlan.getPrefixPathList()) {
      spaceQuotaLimit.put(storageGroup, setSpaceQuotaPlan.getSpaceLimit());
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }
}
